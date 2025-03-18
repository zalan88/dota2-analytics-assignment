import requests
import os
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_BASE_URL = os.getenv("OPENDOTA_API_BASE_URL")
API_KEY = os.getenv("OPENDOTA_API_KEY")
TEAM_ID = 2163  # Change this if needed

# Request matches
headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
url = f"{API_BASE_URL}/teams/{TEAM_ID}/matches"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    matches = response.json()
    
    null_matches = [m for m in matches if not m.get("match_id")]
    
    if null_matches:
        print(f"⚠️ Found {len(null_matches)} matches with NULL match_id!")
        print(null_matches[:3])  # Show first 3 problematic cases
    else:
        print("✅ All matches have valid match_id!")
else:
    print(f"❌ API request failed: {response.status_code}")
