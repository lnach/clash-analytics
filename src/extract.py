"""
Clash of Clans Data Extraction Module

This module handles data extraction from the Clash of Clans API.
It fetches clan member information and structures it for database insertion.

Author: Luke Nachnani
Date: 2026-02-05
"""

import requests
import os
from datetime import datetime, date
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
        print(f"Successfully connected to CoC API")
        print(f"Clan Name: {data['name']}")
        print(f"Members: {data['members']}")
        print(f"Clan Level: {data['clanLevel']}")
        
        return data
    
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to API: {e}")
        return None

def get_clan_members():
    """
    Fetch all member data from the Clash of Clans API.
    
    Returns:
        list: List of member dictionaries from API, or None if request fails.
    """

    url = f"https://api.clashofclans.com/v1/clans/%23{CLAN_TAG}/members"
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # data is a dict with an 'items' key containing list of members
        members = data['items']  # This is a list
        
        return members
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching members: {e}")

def structure_member_snapshot_data(members):
    """
    Transform raw API data into format for member_snapshots table.
    
    Args:
        members (list): Raw member data from CoC API.
        
    Returns:
        list: Formatted dictionaries for member_snapshots table.
    """

    all_snapshots = []

    for member in members:
        snapshot = {
            'player_tag': member['tag'],
            'player_name': member['name'],
            'role': member['role'],
            'trophies': member['trophies'],
            'league_name': member.get('league', {}).get('name'),
            'league_id': member.get('league', {}).get('id'),
            'town_hall_level': member['townHallLevel'],
            'donations_given': member['donations'],
            'donations_received': member['donationsReceived'],
            'clan_rank': member['clanRank']
        }
        all_snapshots.append(snapshot)

    return all_snapshots


def structure_members_table_data(members):
    """
    Transform raw API data into format for members table.
    
    Args:
        members (list): Raw member data from CoC API.
        
    Returns:
        list: Formatted dictionaries for members table.
    """

    
    members_data = []
    
    for member in members:
        member_data = {
            'player_tag': member['tag'],
            'player_name': member['name'],
            'join_date': date.today(),           # untracked by CoC, added manually on pull
            'is_active': True
        }
        members_data.append(member_data)
    
    return members_data

def extract_all_member_data():
    """
    Main extraction function - orchestrates the ETL process.
    
    Fetches member data from API and transforms it for both database tables.
    
    Returns:
        dict: Contains 'members', 'snapshots', 'snapshot_date', 'total_members'.
              Returns None if extraction fails.
    """
    
    print("Fetching clan members...")
    members = get_clan_members()
    
    if not members:
        return None
    
    print(f"Found {len(members)} members")
    
    # Extract for both tables
    members_table_data = structure_members_table_data(members)
    snapshots_data = structure_member_snapshot_data(members)
    
    return {
        'members': members_table_data,          # For members table
        'snapshots': snapshots_data,            # For member_snapshots table
        'snapshot_date': datetime.now().date(),
        'total_members': len(members)
    }


if __name__ == "__main__":
    data = extract_all_member_data()
    
    if data:
        print(f"\nExtraction Summary:")
        print(f"Date: {data['snapshot_date']}")
        print(f"Total Members: {data['total_members']}")
        
        print(f"\nSample from members table:")
        print(data['members'][0])
        
        print(f"\nSample from snapshots table:")
        print(data['snapshots'][0])


    