import requests
import json

def get_player_stats(match_id, player_slot):
    # OpenDota API endpoint for match details
    url = f"https://api.opendota.com/api/matches/{match_id}"
    
    try:
        # Make the API request
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        
        # Parse the JSON response
        match_data = response.json()
        
        # Find the player data based on player_slot
        player_data = next((player for player in match_data['players'] if player['player_slot'] == player_slot), None)
        
        if player_data:
            # Display all player data
            print(f"Player Stats for Match ID {match_id}, Player Slot {player_slot}:")
            print(json.dumps(player_data, indent=4))  # Pretty print the JSON data
        else:
            print(f"No player found with player slot {player_slot} in match {match_id}.")
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

# Example usage
match_id = 8216689891  # Replace with a valid match ID
player_slot = 0  # Replace with a valid player slot (0-127 for Radiant, 128-255 for Dire)
get_player_stats(match_id, player_slot)