# Standard library imports
import requests
import os
import json
import time
import random

# Third party imports
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Load environment variables from .env file
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")  # PostgreSQL connection string
API_BASE_URL = os.getenv("OPENDOTA_API_BASE_URL")  # OpenDota API base URL
API_KEY = os.getenv("OPENDOTA_API_KEY")  # Optional API key for higher rate limits

# Establish database connection
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Configuration constants
MATCH_LIMIT = 3  # Number of matches to fetch per team (reduced for testing)
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
        # Special handling for matches to ensure match_id is set
        query = f"INSERT INTO {table_name} (match_id, raw_json) VALUES %s;"
        values = [(entry.get("match_id"), json.dumps(entry)) for entry in data]
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
    
    matches = matches[:MATCH_LIMIT]
    print(f"🛠 API returned {len(matches)} matches (Expected: {MATCH_LIMIT})")
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

# Main execution block
if __name__ == "__main__":
    team_id = 2163  # Team ID to analyze

    # Step 1: Fetch basic match data
    print("Fetching matches...")
    matches = get_team_matches(team_id)
    print(f"✅ Retrieved {len(matches)} matches (Expected: {MATCH_LIMIT})")

    # Step 2: Fetch and store detailed match data
    print("Fetching match details...")
    detailed_matches = []
    all_players = []  # Track all players across matches
    
    for match in matches[:MATCH_LIMIT]:
        match_details = get_match_details(match["match_id"])
        
        if not match_details:
            print(f"⚠️ Skipping match {match['match_id']} due to timeout or missing data.")
            continue

        # Extract players from match details
        if "players" in match_details:
            # Store only non-anonymous players
            match_players = [
                player for player in match_details["players"]
                if player.get("account_id") and player["account_id"] != 4294967295  # Filter out anonymous players
            ]
            all_players.extend(match_players)

        detailed_matches.append(match_details)
        time.sleep(random.uniform(3, 7))  # Rate limiting delay between match requests

    # Store the detailed match data
    if detailed_matches:
        print(f"Storing {len(detailed_matches)} detailed matches...")
        store_raw_data("stg_matches", detailed_matches)
    else:
        print("⚠️ No detailed matches to store!")

    # Process and store player data
    if all_players:
        print(f"Processing {len(all_players)} players from matches...")
        # Get unique players by account_id
        unique_players = {}
        for player in all_players:
            account_id = str(player["account_id"])
            if account_id not in unique_players:
                # Create player profile structure
                unique_players[account_id] = {
                    "profile": {
                        "account_id": player["account_id"],
                        "personaname": player.get("personaname", "Unknown"),
                        "name": player.get("name", player.get("personaname", "Unknown")),
                    }
                }
        
        player_list = list(unique_players.values())
        if player_list:
            print(f"Storing {len(player_list)} unique players...")
            store_unique_players(player_list)
        else:
            print("⚠️ No valid players to store!")
    else:
        print("⚠️ No players found in matches!")

    # Step 3: Extract and store team data from matches
    print("Fetching team info from match history...")
    team_data = []
    fetched_team_ids = set()

    for match in detailed_matches:
        for team_id in [match.get("radiant_team_id"), match.get("dire_team_id")]:
            if team_id and str(team_id) not in fetched_team_ids:
                team_info = get_team_info(team_id)
                if team_info:
                    team_data.append(team_info)
                    fetched_team_ids.add(str(team_id))

    if team_data:
        print(f"Found {len(team_data)} unique teams...")
        store_unique_teams(team_data)
    else:
        print("⚠️ No team data to store!")

    # Step 4: Fetch and store hero reference data
    print("Fetching heroes...")
    cursor.execute("DELETE FROM stg_heroes;")  # Clear existing hero data
    heroes = get_heroes()
    if heroes:
        print(f"✅ Retrieved {len(heroes)} heroes.")
        store_raw_data("stg_heroes", heroes)
    else:
        print("⚠️ No hero data retrieved!")

    # Cleanup and summary
    print(f"🔄 Total API Calls Made: {api_calls}")
    cursor.close()
    conn.close()
    print("✅ Data fetching complete!")
