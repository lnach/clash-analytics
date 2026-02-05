import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv('COC_API_KEY')
CLAN_TAG = os.getenv('COC_CLAN_TAG')

def test_api_connection():
    """Test connection to Clash of Clans API"""
    
    url = f"https://api.clashofclans.com/v1/clans/%23{CLAN_TAG}"
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ Successfully connected to CoC API!")
        print(f"Clan Name: {data['name']}")
        print(f"Members: {data['members']}")
        print(f"Clan Level: {data['clanLevel']}")
        
        return data
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to API: {e}")
        return None

if __name__ == "__main__":
    test_api_connection()