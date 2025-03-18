-- First, truncate the table to avoid duplicates
TRUNCATE TABLE dim_teams CASCADE;

-- Insert all team data including stats from the staging table
INSERT INTO dim_teams (
    team_id, 
    team_name, 
    region, 
    total_matches, 
    win_count, 
    loss_count, 
    avg_match_gold, 
    avg_match_xp, 
    avg_match_kills, 
    avg_match_deaths, 
    avg_match_kda
)
SELECT 
    (raw_json->>'team_id')::INT AS team_id,
    COALESCE(raw_json->>'name', 'Unknown Team') AS team_name,
    COALESCE(raw_json->>'region', 'Unknown') AS region,
    -- Calculate total matches as the count of distinct matches
    (SELECT COUNT(DISTINCT match_id) FROM fact_team_match_stats WHERE team_id = (raw_json->>'team_id')::INT) AS total_matches,
    (SELECT COUNT(*) FROM fact_team_match_stats WHERE team_id = (raw_json->>'team_id')::INT AND win_flag = TRUE) AS win_count,
    (SELECT COUNT(*) FROM fact_team_match_stats WHERE team_id = (raw_json->>'team_id')::INT AND win_flag = FALSE) AS loss_count,
    COALESCE(
        (SELECT AVG(tm.gold_earned)::INT 
        FROM fact_team_match_stats tm
        WHERE tm.team_id = (raw_json->>'team_id')::INT
        ), 0) AS avg_match_gold,
    COALESCE(
        (SELECT AVG(tm.xp_earned)::INT 
        FROM fact_team_match_stats tm
        WHERE tm.team_id = (raw_json->>'team_id')::INT
        ), 0) AS avg_match_xp,       
    COALESCE(
        (SELECT AVG(tm.total_kills)::INT 
        FROM fact_team_match_stats tm
        WHERE tm.team_id = (raw_json->>'team_id')::INT
        ), 0) AS avg_match_kills,        
    COALESCE(
        (SELECT AVG(tm.total_deaths)::INT 
        FROM fact_team_match_stats tm
        WHERE tm.team_id = (raw_json->>'team_id')::INT
        ), 0) AS avg_match_deaths,
    -- Calculate KDA as (kills + assists) / (deaths + 1) to avoid division by zero
    COALESCE(
        (SELECT AVG(
            CASE WHEN tm.team_id = (raw_json->>'team_id')::INT 
            THEN 
                (tm.total_kills::FLOAT + tm.total_assists::FLOAT) / 
                GREATEST(tm.total_deaths::FLOAT, 1)
            ELSE NULL END
        )
        FROM fact_team_match_stats tm
        WHERE tm.team_id = (raw_json->>'team_id')::INT
        ), 0) AS avg_match_kda
FROM stg_teams;