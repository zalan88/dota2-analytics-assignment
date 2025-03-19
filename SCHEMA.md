# Database Schema

This document outlines the database schema for the Dota 2 Analytics project.

```mermaid
erDiagram
    fact_matches {
        bigint match_id PK
        bigint start_time
        int game_mode
        int lobby_type
        int radiant_team_id FK
        int dire_team_id FK
        boolean radiant_win
        int duration
        int first_blood_time
        varchar game_version
    }

    fact_team_match_stats {
        bigint match_id PK
        int team_id PK, FK
        boolean is_radiant
        boolean win_flag
        int total_kills
        int total_deaths
        int total_assists
        int total_gold
        int total_xp
        int tower_kills
        int roshan_kills
        int total_objectives
    }

    fact_player_match_stats {
        bigint match_id PK
        bigint account_id PK
        int hero_id FK
        bigint start_time
        int team_id FK
        boolean win_flag
        numeric kda
        int kills
        int deaths
        int assists
        int total_gold
        int total_xp
        int objectives
        int tower_kills
        int ancient_kills
        int hero_kills
        int hero_damage
        int actions_per_minute
        int item_0
        int item_1
        int item_2
        int item_3
        int item_4
        int item_5
    }

    dim_heroes {
        int hero_id PK
        varchar hero_name
        int team_id FK
        varchar primary_attr
        varchar attack_type
        varchar roles
    }

    dim_players {
        bigint account_id PK
        varchar player_name
        int team_id FK
        varchar country_code
    }

    dim_teams {
        int team_id PK
        varchar team_name
        varchar tag
        varchar region
    }

    fact_matches }o--|| fact_player_match_stats : "match_id"
    fact_matches }o--|| fact_team_match_stats : "match_id"
    fact_matches }o--|| dim_teams : "radiant_team_id"
    fact_matches }o--|| dim_teams : "dire_team_id"
    fact_team_match_stats }o--|| dim_teams : "team_id"
    fact_player_match_stats }o--|| dim_heroes : "hero_id"
    fact_player_match_stats }o--|| dim_players : "account_id"
    fact_player_match_stats }o--|| dim_teams : "team_id"
    dim_heroes }o--|| dim_teams : "team_id"
    dim_players }o--|| dim_teams : "team_id"
} 