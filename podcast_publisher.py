import gspread
import requests
import os
import re
import json
from oauth2client.service_account import ServiceAccountCredentials
from github import Github, Auth
from datetime import datetime, timezone

# ================= CẤU HÌNH HỆ THỐNG =================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = "techukr/podcastRSS" # ĐỔI THÀNH USERNAME/TÊN REPO CỦA BẠN
FILE_PATH = "rss.xml" 
BRANCH = "main"

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" # ĐỔI THÀNH LINK SHEET CỦA BẠN
WORKSHEET_NAME = "Sheet1"
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
PODCAST_AUTHOR = "ACDT"

# ================= HÀM XỬ LÝ (ĐÃ NÂNG CẤP LOGIC KIỂM TRA) =================
def fetch_json_metadata(json_url):
    """Đọc file JSON, trả về None nếu file chưa sẵn sàng"""
    try:
        response = requests.get(json_url, timeout=10)
        if response.status_code == 200: 
            return response.json()
        else:
            print(f"  -> Lỗi: File JSON chưa truy cập được (HTTP {response.status_code})")
            return None
    except Exception as e:
        print(f"  -> Lỗi kết nối JSON: {e}")
        return None

def get_audio_file_size(audio_url):
    """Kiểm tra file MP3 có tồn tại không và lấy dung lượng. Trả về None nếu file chưa sẵn sàng"""
    try:
        # Dùng phương thức HEAD để ping file cực nhanh mà không cần tải
        response = requests.head(audio_url, timeout=10, allow_redirects=True)
        # 200 là OK, 302 là Redirect (thường gặp ở Archive)
        if response.status_code in [200, 302]: 
            return response.headers.get('Content-Length', '1024000')
        else:
            print(f"  -> Lỗi: File Audio chưa sẵn sàng (HTTP {response.status_code})")
            return None
    except Exception as e:
        print(f"  -> Lỗi kết nối Audio: {e}")
        return None

def update_github_rss(new_item_xml):
    auth = Auth.Token(GITHUB_TOKEN)
    g = Github(auth=auth)
    
    repo = g.get_repo(REPO_NAME)
    file = repo.get_contents(FILE_PATH, ref=BRANCH)
    current_content = file.decoded_content.decode("utf-8")
    
    if "</channel>" in current_content:
        updated_content = current_content.replace("</channel>", f"{new_item_xml}\n\t</channel>")
        
        current_time_gmt = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        updated_content = re.sub(r"<lastBuildDate>.*?</lastBuildDate>", f"<lastBuildDate>{current_time_gmt}</lastBuildDate>", updated_content)
            
        repo.update_file(
            path=FILE_PATH,
            message=f"Auto-publish episode: {datetime.now().strftime('%Y-%m-%d')}",
            content=updated_content,
            sha=file.sha,
            branch=BRANCH
        )
        return True
    return False

def main():
    print(f"[{datetime.now()}] Checking Google Sheets...")
    
    if not GOOGLE_CREDENTIALS_JSON:
        print("Lỗi: Không tìm thấy biến môi trường GOOGLE_CREDENTIALS_JSON")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(GOOGLE_SHEET_URL).worksheet(WORKSHEET_NAME)
    
    records = sheet.get_all_records()
    
    for index, row in enumerate(records):
        row_number = index + 2 
        
        if row.get("Status") == "ready_for_ai":
            topic = row.get("Topic")
            guid = row.get("Notebook_ID")
            archive_audio = row.get("Archive_Audio")
            archive_cover = row.get("Archive_Cover")
            archive_json = row.get("Archive_JSON")
            
            # KIỂM TRA BƯỚC 1: Dữ liệu trong Sheets có trống không?
            if not archive_audio or not archive_json: 
                print(f"Bỏ qua '{topic}': Các cột link trên Sheets đang trống.")
                continue
                
            print(f"Đang kiểm tra dữ liệu thực tế tập: {topic}")
            
            # KIỂM TRA BƯỚC 2: Ping file Audio thực tế
            audio_length = get_audio_file_size(archive_audio)
            if not audio_length:
                print("  -> BỎ QUA: Audio chưa sẵn sàng. Sẽ thử lại ở lần chạy sau.")
                continue

            # KIỂM TRA BƯỚC 3: Ping file JSON thực tế
            metadata = fetch_json_metadata(archive_json)
            if not metadata:
                print("  -> BỎ QUA: JSON chưa sẵn sàng hoặc bị lỗi cấu trúc. Sẽ thử lại ở lần chạy sau.")
                continue
            
            # CHỈ KHI CẢ 2 FILE ĐỀU SẴN SÀNG, MỚI TIẾN HÀNH XÂY DỰNG RSS
            title = metadata.get("title", topic)
            raw_desc = metadata.get("description", "Nội dung đang được cập nhật.")
            description = f"<p>{raw_desc}</p>" if "<p>" not in raw_desc else raw_desc
            duration = metadata.get("duration", "00:15:00") 
            pub_date_gmt = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
            
            new_item_xml = f"""		<item>
			<title><![CDATA[{title}]]></title>
			<description><![CDATA[{description}]]></description>
			<guid isPermaLink="false">{guid}</guid>
			<dc:creator><![CDATA[{PODCAST_AUTHOR}]]></dc:creator>
			<pubDate>{pub_date_gmt}</pubDate>
			<enclosure url="{archive_audio}" length="{audio_length}" type="audio/mpeg"/>
			<itunes:summary><![CDATA[{description}]]></itunes:summary>
			<itunes:explicit>false</itunes:explicit>
			<itunes:duration>{duration}</itunes:duration>
			<itunes:image href="{archive_cover}"/>
			<itunes:episodeType>full</itunes:episodeType>
		</item>"""
            
            if update_github_rss(new_item_xml):
                sheet.update_cell(row_number, list(row.keys()).index("Status") + 1, "published")
                print(f"-> DONE: Đã xuất bản thành công lên RSS!")

if __name__ == "__main__":
    main()
