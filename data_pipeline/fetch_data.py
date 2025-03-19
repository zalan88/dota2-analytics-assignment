# Standard library imports
import requests
import os
import json
import time
import random
import sys

# Third party imports
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Load environment variables from .env file
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")  # PostgreSQL connection string
API_BASE_URL = os.getenv("OPENDOTA_API_BASE_URL")  # OpenDota API base URL
API_KEY = os.getenv("DOTA2_API_KEY")  # Optional API key for higher rate limits

# Establish database connection
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

def is_initial_load():
    """
    Determines if this is an initial load by checking if fact_matches table exists and is empty
    Returns:
        bool: True if this is an initial load (table doesn't exist or is empty), False otherwise
    """
    try:
        # Check if fact_matches table exists and has any rows
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'fact_matches'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("🔍 fact_matches table does not exist - treating as initial load")
            return True
            
        cursor.execute("SELECT COUNT(*) FROM fact_matches LIMIT 1;")
        count = cursor.fetchone()[0]
        
        is_empty = count == 0
        print(f"🔍 fact_matches table {'is empty' if is_empty else 'has data'} - treating as {'initial' if is_empty else 'incremental'} load")
        return is_empty
        
    except Exception as e:
        print(f"⚠️ Error checking table state: {e} - defaulting to initial load")
        return True

# Configuration constants
INITIAL_LOAD = is_initial_load()  # Auto-detect if this is initial load
LOAD_OLDEST = os.getenv("LOAD_OLDEST", "false").lower() == "true"
MATCH_LIMIT = int(os.getenv("MATCH_LIMIT", "50" if INITIAL_LOAD else "3"))  # Get from env or use default based on INITIAL_LOAD
MATCH_HISTORY_DEPTH = 100  # How far back to look in match history
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed API calls
BACKOFF_FACTOR = 2  # Exponential backoff multiplier between retries
api_calls = 0  # Counter to track total API calls made

def request_with_retries(url):
    """
    Makes an HTTP GET request to the OpenDota API with retry logic
    
    Args:
        url (str): The API endpoint URL
        
    Returns:
        dict/None: JSON response if successful, None if all retries fail
    """
    global api_calls
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            api_calls += 1
            return response.json()
        except requests.exceptions.Timeout:
            if attempt < RETRY_ATTEMPTS:
                # Implement exponential backoff with random jitter
                sleep_time = BACKOFF_FACTOR ** attempt + random.uniform(5, 10)
                print(f"⏳ Timeout for {url}. Retrying {attempt}/{RETRY_ATTEMPTS} in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
            else:
                print(f"🚨 Final timeout for {url}, skipping request.")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
    print(f"Exceeded retries for {url}, skipping...")
    return None

def store_raw_data(table_name, data):
    """
    Stores raw JSON data into the specified staging table
    
    Args:
        table_name (str): Name of the staging table
        data (list): List of dictionaries containing the data to store
    """
    if not data:
        return
        
    if table_name == "stg_matches":
        # Get existing match IDs from staging table
        cursor.execute("SELECT match_id FROM stg_matches;")
        existing_stg_matches = {row[0] for row in cursor.fetchall()}
        
        # Filter out matches that already exist in staging
        new_data = [entry for entry in data if entry.get("match_id") not in existing_stg_matches]
        
        if not new_data:
            print("⚠️ All matches already exist in staging table, skipping insert.")
            return
            
        # Special handling for matches to ensure match_id is set
        query = f"INSERT INTO {table_name} (match_id, raw_json) VALUES %s;"
        values = [(entry.get("match_id"), json.dumps(entry)) for entry in new_data]
        print(f"Inserting {len(new_data)} new matches (skipped {len(data) - len(new_data)} existing matches)")
    else:
        # Default handling for other tables
        query = f"INSERT INTO {table_name} (raw_json) VALUES %s;"
        values = [(json.dumps(entry),) for entry in data]
    
    execute_values(cursor, query, values)
    conn.commit()

def store_unique_teams(teams):
    """
    Inserts unique team data into stg_teams table, avoiding duplicates
    
    Args:
        teams (list): List of team dictionaries from the API
    """
    if not teams:
        return

    # Get existing team IDs from database
    cursor.execute("SELECT (raw_json->>'team_id')::TEXT FROM stg_teams;")
    existing_teams = {row[0] for row in cursor.fetchall()}

    # Filter out teams that already exist
    new_teams = [t for t in teams if str(t.get("team_id")) not in existing_teams]
    print(f"🛠 Debug: Inserting {len(new_teams)} new teams (skipped {len(teams) - len(new_teams)} duplicates).")

    if new_teams:
        store_raw_data("stg_teams", new_teams)

def store_unique_players(players):
    """
    Inserts unique player data into stg_players table, avoiding duplicates
    
    Args:
        players (list): List of player dictionaries from the API
    """
    if not players:
        return

    # Get existing player account IDs from database
    cursor.execute("SELECT (raw_json->'profile'->>'account_id')::TEXT FROM stg_players;")
    existing_players = {row[0] for row in cursor.fetchall()}

    # Filter out players that already exist
    new_players = [p for p in players if str(p.get("profile", {}).get("account_id")) not in existing_players]
    print(f"🛠 Debug: Inserting {len(new_players)} new players (skipped {len(players) - len(new_players)} duplicates).")
    
    if new_players:
        store_raw_data("stg_players", new_players)

# API endpoint functions
def get_team_info(team_id):
    """Fetches detailed information about a specific team"""
    return request_with_retries(f"{API_BASE_URL}/teams/{team_id}")

def get_team_matches(team_id):
    """Fetches recent matches for a specific team"""
    matches = request_with_retries(f"{API_BASE_URL}/teams/{team_id}/matches")
    if matches is None:
        print("⚠️ API returned no matches, setting to empty list.")
        return []
    
    matches = matches[:MATCH_HISTORY_DEPTH]  # Look deeper into history
    print(f"🛠 API returned {len(matches)} matches (Looking at last {MATCH_HISTORY_DEPTH} matches)")
    
    # Sort matches by timestamp
    matches.sort(key=lambda x: x['start_time'], reverse=not LOAD_OLDEST)
    
    # Debug: Print first few matches timestamps
    for match in matches[:5]:
        print(f"Debug: Match {match['match_id']} timestamp: {match['start_time']} "
              f"({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(match['start_time']))})")
    
    return matches

def get_match_details(match_id):
    """Fetches detailed information about a specific match"""
    return request_with_retries(f"{API_BASE_URL}/matches/{match_id}")

def get_heroes():
    """Fetches list of all heroes in the game"""
    return request_with_retries(f"{API_BASE_URL}/heroes")

def get_player_info(player_id):
    """
    Fetches detailed information about a specific player
    Includes rate limiting delay between requests
    """
    if not player_id or player_id == 0:
        return None
    time.sleep(random.uniform(1, 3))  # Rate limiting delay
    return request_with_retries(f"{API_BASE_URL}/players/{player_id}")

def get_latest_match_time():
    """Get the most recent match time from fact_matches"""
    try:
        cursor.execute("SELECT MAX(start_time) FROM fact_matches;")
        result = cursor.fetchone()[0]
        return result if result else 0
    except Exception as e:
        print(f"Error getting latest match time: {e}")
        return 0

def get_existing_match_ids():
    """Get set of existing match IDs from fact_matches"""
    try:
        cursor.execute("SELECT match_id FROM fact_matches;")
        return {row[0] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error getting existing match IDs: {e}")
        return set()

def get_existing_team_ids():
    """Get set of existing team IDs from dim_teams"""
    try:
        cursor.execute("SELECT team_id FROM dim_teams;")
        return {row[0] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error getting existing team IDs: {e}")
        return set()

def get_existing_player_ids():
    """Get set of existing player IDs from dim_players"""
    try:
        cursor.execute("SELECT account_id FROM dim_players;")
        return {row[0] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error getting existing player IDs: {e}")
        return set()

# Main execution block
if __name__ == "__main__":
    team_id = 2163  # Team ID to analyze
    
    print(f"Running in {'INITIAL LOAD' if INITIAL_LOAD else 'INCREMENTAL'} mode")
    print(f"Loading {'OLDEST' if LOAD_OLDEST else 'NEWEST'} matches first")
    print(f"Will process up to {MATCH_LIMIT} new matches per run")
    
    # Get existing data to avoid duplicates
    existing_match_ids = get_existing_match_ids()
    existing_team_ids = get_existing_team_ids()
    existing_player_ids = get_existing_player_ids()
    latest_match_time = get_latest_match_time()
    
    print(f"Latest match time in DB: {latest_match_time} "
          f"({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(latest_match_time))})")
    print(f"Existing matches in DB: {len(existing_match_ids)}")

    # Step 1: Fetch basic match data
    print("Fetching matches...")
    matches = get_team_matches(team_id)
    
    # Filter out matches we already have
    new_matches = []
    for m in matches:
        match_id = int(m["match_id"])
        match_time = int(m["start_time"])
        
        if LOAD_OLDEST:
            # When loading oldest first, we want matches that are:
            # 1. Not already in our database
            # 2. Newer than our latest_match_time (which is 0 initially)
            is_new = match_id not in existing_match_ids and match_time > latest_match_time
        else:
            # When loading newest first (default), we want matches that are:
            # 1. Not already in our database
            # 2. Newer than our latest_match_time
            is_new = match_id not in existing_match_ids and match_time > latest_match_time
        
        if is_new:
            new_matches.append(m)
        else:
            reason = "Already exists" if match_id in existing_match_ids else "Too old"
            print(f"Skipping match {match_id}: {reason} "
                  f"(timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(match_time))})")
    
    # When loading oldest first, sort by timestamp ascending
    if LOAD_OLDEST:
        new_matches.sort(key=lambda x: x['start_time'])
    
    # Apply the processing limit to new matches
    new_matches = new_matches[:MATCH_LIMIT]
    
    print(f"✅ Found {len(new_matches)} new matches to process out of {len(matches)} matches checked")

    if not new_matches:
        print("No new matches to process, exiting...")
        sys.exit(0)

    # Step 2: Fetch and store detailed match data
    print("Fetching match details...")
    detailed_matches = []
    all_players = []
    
    for match in new_matches:  # No need for slice here as we already limited new_matches
        match_details = get_match_details(match["match_id"])
        
        if not match_details:
            print(f"⚠️ Skipping match {match['match_id']} due to timeout or missing data.")
            continue

        # Extract players from match details
        if "players" in match_details:
            match_players = [
                player for player in match_details["players"]
                if player.get("account_id") and 
                   player["account_id"] != 4294967295 and  # Filter out anonymous players
                   player["account_id"] not in existing_player_ids  # Filter out existing players
            ]
            all_players.extend(match_players)

        detailed_matches.append(match_details)
        time.sleep(random.uniform(3, 7))

    # Store the detailed match data
    if detailed_matches:
        print(f"Storing {len(detailed_matches)} new detailed matches...")
        store_raw_data("stg_matches", detailed_matches)
    else:
        print("⚠️ No new detailed matches to store!")

    # Process and store player data
    if all_players:
        print(f"Processing {len(all_players)} new players from matches...")
        unique_players = {}
        for player in all_players:
            account_id = str(player["account_id"])
            if account_id not in unique_players and int(account_id) not in existing_player_ids:
                unique_players[account_id] = {
                    "profile": {
                        "account_id": player["account_id"],
                        "personaname": player.get("personaname", "Unknown"),
                        "name": player.get("name", player.get("personaname", "Unknown")),
                    }
                }
        
        player_list = list(unique_players.values())
        if player_list:
            print(f"Storing {len(player_list)} new unique players...")
            store_unique_players(player_list)
        else:
            print("⚠️ No new players to store!")
    else:
        print("⚠️ No new players found in matches!")

    # Step 3: Extract and store team data from matches
    print("Fetching team info from match history...")
    team_data = []
    fetched_team_ids = set()

    for match in detailed_matches:
        for team_id in [match.get("radiant_team_id"), match.get("dire_team_id")]:
            if (team_id and 
                str(team_id) not in fetched_team_ids and 
                team_id not in existing_team_ids):
                team_info = get_team_info(team_id)
                if team_info:
                    team_data.append(team_info)
                    fetched_team_ids.add(str(team_id))

    if team_data:
        print(f"Found {len(team_data)} new unique teams...")
        store_unique_teams(team_data)
    else:
        print("⚠️ No new team data to store!")

    # Step 4: Update hero reference data only if needed
    print("Checking for hero updates...")
    cursor.execute("SELECT COUNT(*) FROM stg_heroes;")
    hero_count = cursor.fetchone()[0]
    
    if hero_count == 0:
        print("Fetching heroes...")
        heroes = get_heroes()
        if heroes:
            print(f"✅ Retrieved {len(heroes)} heroes.")
            store_raw_data("stg_heroes", heroes)
        else:
            print("⚠️ No hero data retrieved!")
    else:
        print(f"✅ Hero data already exists ({hero_count} heroes)")

    # Cleanup and summary
    print(f"🔄 Total API Calls Made: {api_calls}")
    cursor.close()
    conn.close()
    print("✅ Data fetching complete!")
