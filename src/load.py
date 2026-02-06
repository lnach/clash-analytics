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
    """Load member data into PostgreSQL database"""

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
        
    except psycopg2.Error as e:
        print(f"Error loading members: {e}")
        conn.rollback()  # Undo any partial inserts
        
    finally:
        cursor.close()
        print(f" Successfully loaded {len(members_data)} members into database")

def load_snapshot(conn, snapshot_data, snapshot_date):
    """Load snapshot data into PostgreSQL database"""

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
                snapshot['clan_rank'],
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        
    except psycopg2.Error as e:
        print(f"Error loading snapshots: {e}")
        conn.rollback()  # Undo any partial inserts
        
    finally:
        cursor.close()



if __name__ == "__main__":
    # Test loading members
    from extract import extract_all_member_data
    
    print("Starting test...")
    
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
    
    print("Connection successful!")
    
    # Test loading just members
    load_snapshot(conn, data['snapshots'], data['snapshot_date'])
    
    # Close connection
    conn.close()
    
    print("\nTest complete!")