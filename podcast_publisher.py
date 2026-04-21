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
REPO_NAME = "techukr/podcastRSS" 
FILE_PATH = "rss.xml" 
BRANCH = "main"

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1rkvoyKQbquFP21lzCVQhIVF-Ma31chgZqZMy50ba4_I/edit" 
WORKSHEET_NAME = "Sheet3"
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
PODCAST_AUTHOR = "ACDT"

# ================= HÀM XỬ LÝ RSS =================

def get_audio_file_size(audio_url):
    try:
        response = requests.head(audio_url, timeout=10, allow_redirects=True)
        if response.status_code in [200, 302]: 
            return response.headers.get('Content-Length', '1024000')
    except: pass
    return None

def fetch_json_metadata(json_url):
    try:
        response = requests.get(json_url, timeout=10)
        if response.status_code == 200: return response.json()
    except: pass
    return None

def process_github_rss(action, guid, new_item_xml=None):
    """
    Hàm xử lý file RSS trên GitHub: 
    - action='publish': Thêm item mới
    - action='unpublish': Xóa item dựa trên guid
    """
    auth = Auth.Token(GITHUB_TOKEN)
    g = Github(auth=auth)
    repo = g.get_repo(REPO_NAME)
    file = repo.get_contents(FILE_PATH, ref=BRANCH)
    content = file.decoded_content.decode("utf-8")
    
    updated = False
    new_content = content

    if action == "publish":
        # Kiểm tra xem guid đã tồn tại chưa để tránh trùng lặp
        if guid not in content:
            new_content = content.replace("</channel>", f"{new_item_xml}\n\t</channel>")
            updated = True
        else:
            print(f"  -> Bỏ qua: GUID {guid} đã tồn tại trong RSS.")

    elif action == "unpublish":
        # Regex để tìm và xóa toàn bộ khối <item>...</item> chứa guid cụ thể
        # Giải thích: Tìm khối <item> có chứa guid, kết thúc bằng </item> và xóa nó
        pattern = rf"\t*<item>[\s\S]*?<guid[^>]*>{guid}</guid>[\s\S]*?</item>\n?"
        if guid in content:
            new_content = re.sub(pattern, "", content)
            if new_content != content:
                updated = True
                print(f"  -> Đã tìm thấy và xóa item có GUID: {guid}")
        else:
            print(f"  -> GUID {guid} không tồn tại trong RSS, không cần unpublish.")

    if updated:
        # Cập nhật thời gian Build mới
        current_time_gmt = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        new_content = re.sub(r"<lastBuildDate>.*?</lastBuildDate>", f"<lastBuildDate>{current_time_gmt}</lastBuildDate>", new_content)
        
        repo.update_file(
            path=FILE_PATH,
            message=f"Podcast Engine: {action.upper()} {guid}",
            content=new_content,
            sha=file.sha,
            branch=BRANCH
        )
        return True
    return False

# ================= MAIN LOGIC =================

def main():
    print(f"[{datetime.now()}] Checking Google Sheets...")
    if not GOOGLE_CREDENTIALS_JSON: return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(GOOGLE_SHEET_URL).worksheet(WORKSHEET_NAME)
    records = sheet.get_all_records()
    
    for index, row in enumerate(records):
        row_number = index + 2 
        status = row.get("Status")
        guid = str(row.get("Notebook_ID"))
        topic = row.get("Topic")

        # TRƯỜNG HỢP 1: PUBLISH
        if status == "ready_for_ai":
            print(f"Đang Publish: {topic}")
            audio_url = row.get("Archive_Audio")
            json_url = row.get("Archive_JSON")
            cover_url = row.get("Archive_Cover")
            
            audio_length = get_audio_file_size(audio_url)
            metadata = fetch_json_metadata(json_url)
            
            if audio_length and metadata:
                title = metadata.get("title", topic)
                raw_desc = metadata.get("description", "Nội dung đang cập nhật.")
                description = f"<p>{raw_desc}</p>" if "<p>" not in raw_desc else raw_desc
                duration = metadata.get("duration", "00:15:00")
                pub_date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

                item_xml = f"""		<item>
			<title><![CDATA[{title}]]></title>
			<description><![CDATA[{description}]]></description>
			<guid isPermaLink="false">{guid}</guid>
			<dc:creator><![CDATA[{PODCAST_AUTHOR}]]></dc:creator>
			<pubDate>{pub_date}</pubDate>
			<enclosure url="{audio_url}" length="{audio_length}" type="audio/mpeg"/>
			<itunes:summary><![CDATA[{description}]]></itunes:summary>
			<itunes:explicit>false</itunes:explicit>
			<itunes:duration>{duration}</itunes:duration>
			<itunes:image href="{cover_url}"/>
			<itunes:episodeType>full</itunes:episodeType>
		</item>"""
                
                if process_github_rss("publish", guid, item_xml):
                    sheet.update_cell(row_number, list(row.keys()).index("Status") + 1, "published")
                    print("  -> Thành công!")

        # TRƯỜNG HỢP 2: UNPUBLISH
        elif status == "draft":
            print(f"Đang Unpublish (Draft): {topic}")
            if process_github_rss("unpublish", guid):
                # Sau khi gỡ khỏi RSS, đổi trạng thái thành 'draft_unpublished' để tránh quét lại liên tục
                sheet.update_cell(row_number, list(row.keys()).index("Status") + 1, "draft_unlisted")
                print("  -> Đã gỡ bài thành công!")

if __name__ == "__main__":
    main()
