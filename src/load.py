"""
Clash of Clans Data Loading Module

Handles loading extracted data into PostgreSQL database.
Inserts member and snapshot data with proper error handling.

Author: Luke Nachnani
Date: 2026-02-06
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """
    Create connection to PostgreSQL database.
    
    Returns:
        connection object or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None
    
def load_members(conn, members_data):
    """
    Load member data into PostgreSQL database
    
    Args:
        conn: connection to PostgreSQL database
        members_data: member data extracted from API

    Returns: None; loads data to PostgreSQL database
    
    """

    cursor = conn.cursor()
    
    insert_sql = """
        INSERT INTO members (player_tag, player_name, join_date, is_active)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (player_tag) DO NOTHING
    """

    try:
        for member in members_data:
            values = (
                member['player_tag'],
                member['player_name'],
                member['join_date'],
                member['is_active']
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        print(f"Successfully loaded {len(members_data)} members into database")
        
    except psycopg2.Error as e:
        print(f"Error loading members: {e}")
        conn.rollback()  # Undo any partial inserts
        
    finally:
        cursor.close()

def load_snapshots(conn, snapshot_data, snapshot_date):
    """
    Load snapshot data into PostgreSQL database
    
    Args:
        conn: connection to PostgreSQL database
        snapshot_data: snapshot data extracted from API
        snapshot_date: date that load_snapshot is ran and snapshot data is loaded

    Returns: None; loads data to PostgreSQL database
    
    """

    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO member_snapshots (player_tag, snapshot_date, role, trophies, league_name, league_id, town_hall_level, donations_given, donations_received, clan_rank)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        for snapshot in snapshot_data:
            values = (
                snapshot['player_tag'],
                snapshot_date,
                snapshot['role'], 
                snapshot['trophies'],
                snapshot['league_name'],
                snapshot['league_id'], 
                snapshot['town_hall_level'],
                snapshot['donations_given'],
                snapshot['donations_received'],
                snapshot['clan_rank']
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        print(f"Successfully loaded {len(snapshot_data)} snapshots into database")
        
    except psycopg2.Error as e:
        print(f"Error loading snapshots: {e}")
        conn.rollback()  # Undo any partial inserts
        
    finally:
        cursor.close()



if __name__ == "__main__":
    from extract import extract_all_member_data
    
    print("Starting data load...")
        
    # Get data from API
    data = extract_all_member_data()
    if not data:
        print("Failed to extract data")
        exit(1)
        
    # Get database connection
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database")
        exit(1)
    
    # Load data into database
    load_members(conn, data['members'])
    load_snapshots(conn, data['snapshots'], data['snapshot_date'])
    
    
    # Close connection
    conn.close()
    
    print("\nData load complete!")