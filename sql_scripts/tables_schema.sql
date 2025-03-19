-- 1. First create staging tables (no dependencies)
CREATE TABLE IF NOT EXISTS stg_matches (
    match_id BIGINT PRIMARY KEY,
    raw_json JSONB,           
    radiant_team_id INT,      
    dire_team_id INT,        
    players_info JSONB        
);

CREATE TABLE IF NOT EXISTS stg_players (
    raw_json JSONB,
    last_login TIMESTAMP,
    full_history_time TIMESTAMP,
    country_code VARCHAR(2),
    last_match_time TIMESTAMP,
    profileurl TEXT
);

CREATE TABLE IF NOT EXISTS stg_teams (
    raw_json JSONB
);

CREATE TABLE IF NOT EXISTS stg_heroes (
    raw_json JSONB
);

-- 2. Then create dimension tables (no dependencies)
CREATE TABLE IF NOT EXISTS dim_players (
    account_id INT PRIMARY KEY,
    player_name TEXT,
    is_pro BOOLEAN,
    last_login TIMESTAMP,
    full_history_time TIMESTAMP,
    country_code VARCHAR(2),
    last_match_time TIMESTAMP,
    profileurl TEXT
);

CREATE TABLE IF NOT EXISTS dim_heroes (
    hero_id INT PRIMARY KEY,
    hero_name TEXT,
    hero_main_role TEXT
);

CREATE TABLE IF NOT EXISTS dim_teams (
    team_id INT PRIMARY KEY,
    team_name TEXT,
    region TEXT,
    total_matches INT,
    win_count INT,
    loss_count INT,
    avg_match_gold INT,
    avg_match_xp INT,
    avg_match_kills INT,
    avg_match_deaths INT,
    avg_match_kda FLOAT
);

-- 3. Then create fact_matches (no dependencies)
CREATE TABLE IF NOT EXISTS fact_matches (
    match_id BIGINT PRIMARY KEY,
    start_time BIGINT,
    duration INT,
    game_mode INT,
    radiant_team_id INT,      
    dire_team_id INT,       
    first_blood_time INT,
    team_fights INT,
    radiant_win BOOLEAN,
    version INT,
    patch INT
);

-- 4. Then create fact tables with foreign keys
CREATE TABLE IF NOT EXISTS fact_team_match_stats (
    match_id BIGINT,
    team_id INT,
    total_kills INT,
    total_deaths INT,
    total_assists INT,
    gold_earned INT,
    gold_spent INT,
    xp_earned INT,
    tower_kills INT,
    roshan_kills INT,
    win_flag BOOLEAN,
    PRIMARY KEY (match_id, team_id),
    FOREIGN KEY (match_id) REFERENCES fact_matches(match_id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES dim_teams(team_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS fact_player_match_stats (
    match_id BIGINT,
    account_id INT,
    hero_id INT,
    start_time BIGINT,
    team_id INT,
    win_flag BOOLEAN,
    kda FLOAT,
    kills INT,
    deaths INT,
    assists INT,
    total_gold INT,
    total_xp INT,
    objectives INT,
    tower_kills INT,
    ancient_kills INT,
    hero_kills INT,
    hero_damage INT,
    actions_per_minute INT,
    item_0 INT,
    item_1 INT,
    item_2 INT,
    item_3 INT,
    item_4 INT,
    item_5 INT,
    PRIMARY KEY (match_id, account_id),
    FOREIGN KEY (match_id) REFERENCES fact_matches(match_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES dim_players(account_id) ON DELETE CASCADE,
    FOREIGN KEY (hero_id) REFERENCES dim_heroes(hero_id) ON DELETE SET NULL,
    FOREIGN KEY (team_id) REFERENCES dim_teams(team_id) ON DELETE SET NULL
);

-- 5. Finally create indexes
CREATE INDEX IF NOT EXISTS idx_team_match_team_id ON fact_team_match_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_player_match_account_id ON fact_player_match_stats(account_id);
CREATE INDEX IF NOT EXISTS idx_player_match_hero_id ON fact_player_match_stats(hero_id);