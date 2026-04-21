import gspread
import requests
import os
import re
import json # Thêm thư viện này
from oauth2client.service_account import ServiceAccountCredentials
from github import Github
from datetime import datetime, timezone

# ================= CẤU HÌNH HỆ THỐNG =================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = "techukr/podcastRSS" 
FILE_PATH = "rss.xml" 
BRANCH = "main"

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1rkvoyKQbquFP21lzCVQhIVF-Ma31chgZqZMy50ba4_I/edit" 
WORKSHEET_NAME = "Sheet3"

# Xóa dòng CREDENTIALS_FILE cũ, thay bằng biến môi trường
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
PODCAST_AUTHOR = "ACDT"

# ================= HÀM XỬ LÝ =================
def fetch_json_metadata(json_url):
    if not json_url: 
        print("-> CẢNH BÁO: Cột Archive_JSON bị trống!")
        return {}
    try:
        print(f"-> Đang tải JSON từ: {json_url}")
        response = requests.get(json_url, timeout=10)
        
        if response.status_code == 200: 
            data = response.json()
            print(f"-> Đọc JSON thành công! Dữ liệu: {list(data.keys())}")
            return data
        else:
            print(f"-> LỖI HTTP: Trạng thái {response.status_code} khi tải JSON.")
    except requests.exceptions.JSONDecodeError:
        print("-> LỖI GIẢI MÃ: Link cung cấp không phải là file JSON chuẩn (Có thể là link trang web HTML).")
    except Exception as e:
        print(f"-> LỖI KẾT NỐI: {e}")
    return {}
def update_github_rss(new_item_xml):
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(REPO_NAME)
    file = repo.get_contents(FILE_PATH, ref=BRANCH)
    current_content = file.decoded_content.decode("utf-8")
    
    if "</channel>" in current_content:
        # 1. Chèn item mới
        updated_content = current_content.replace("</channel>", f"{new_item_xml}\n\t</channel>")
        
        # 2. Cập nhật lastBuildDate để Spotify biết có bài mới
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
    
    # Đảm bảo biến môi trường JSON đã được nạp
    if not GOOGLE_CREDENTIALS_JSON:
        print("Lỗi: Không tìm thấy biến môi trường GOOGLE_CREDENTIALS_JSON")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Đọc trực tiếp từ chuỗi JSON thay vì đọc file
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
            
            if not archive_audio or not archive_json: continue
            print(f"Processing: {topic}")
            
            metadata = fetch_json_metadata(archive_json)
            title = metadata.get("title", topic)
            raw_desc = metadata.get("description", "Nội dung đang được cập nhật.")
            description = f"<p>{raw_desc}</p>" if "<p>" not in raw_desc else raw_desc
            
            audio_length = metadata.get("length", "1024000") 
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
                print(f"-> DONE!")

if __name__ == "__main__":
    main()
