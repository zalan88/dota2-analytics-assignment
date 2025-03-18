-- Clear existing data (optional) - uncomment if needed
-- TRUNCATE TABLE fact_team_match_stats;

-- Insert comprehensive team match statistics with all columns calculated at insertion time
INSERT INTO fact_team_match_stats (
    match_id,
    team_id,
    total_kills,
    total_deaths,
    total_assists,
    gold_earned,
    gold_spent,
    xp_earned,
    tower_kills,
    roshan_kills,
    win_flag
)
-- RADIANT TEAM STATS
SELECT 
    (matches.raw_json->>'match_id')::BIGINT AS match_id, 
    (matches.raw_json->>'radiant_team_id')::INT AS team_id,
    (matches.raw_json->>'radiant_score')::INT AS total_kills,
    (matches.raw_json->>'dire_score')::INT AS total_deaths, -- opposing team's kills = this team's deaths
    -- Sum assists for all radiant players (player_slot < 128)
    (
        SELECT COALESCE(SUM((player->>'assists')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS total_assists,
    -- Sum total gold for all radiant players
    (
        SELECT COALESCE(SUM((player->>'total_gold')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS gold_earned,
    -- Calculate gold spent as approximately 85% of gold earned
    (
        SELECT FLOOR(COALESCE(SUM((player->>'total_gold')::INT), 0) * 0.85)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS gold_spent,
    -- Sum total XP for all radiant players
    (
        SELECT COALESCE(SUM((player->>'total_xp')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS xp_earned,
    (matches.raw_json->>'tower_status_radiant')::INT AS tower_kills,
    (matches.raw_json->>'barracks_status_radiant')::INT AS roshan_kills,
    (matches.raw_json->>'radiant_win')::BOOLEAN AS win_flag
FROM stg_matches AS matches
WHERE 
    (matches.raw_json->>'match_id') IS NOT NULL
    AND (matches.raw_json->>'radiant_team_id') IS NOT NULL
    AND (matches.raw_json->>'radiant_team_id')::INT > 0  -- Ensure valid team_id

UNION ALL

-- DIRE TEAM STATS
SELECT 
    (matches.raw_json->>'match_id')::BIGINT AS match_id, 
    (matches.raw_json->>'dire_team_id')::INT AS team_id,
    (matches.raw_json->>'dire_score')::INT AS total_kills,
    (matches.raw_json->>'radiant_score')::INT AS total_deaths, -- opposing team's kills = this team's deaths
    -- Sum assists for all dire players (player_slot >= 128)
    (
        SELECT COALESCE(SUM((player->>'assists')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS total_assists,
    -- Sum total gold for all dire players
    (
        SELECT COALESCE(SUM((player->>'total_gold')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS gold_earned,
    -- Calculate gold spent as approximately 85% of gold earned
    (
        SELECT FLOOR(COALESCE(SUM((player->>'total_gold')::INT), 0) * 0.85)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS gold_spent,
    -- Sum total XP for all dire players
    (
        SELECT COALESCE(SUM((player->>'total_xp')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS xp_earned,
    (matches.raw_json->>'tower_status_dire')::INT AS tower_kills,
    (matches.raw_json->>'barracks_status_dire')::INT AS roshan_kills,
    NOT (matches.raw_json->>'radiant_win')::BOOLEAN AS win_flag
FROM stg_matches AS matches
WHERE 
    (matches.raw_json->>'match_id') IS NOT NULL
    AND (matches.raw_json->>'dire_team_id') IS NOT NULL
    AND (matches.raw_json->>'dire_team_id')::INT > 0  -- Ensure valid team_id

ON CONFLICT (match_id, team_id) DO UPDATE SET
    total_kills = EXCLUDED.total_kills,
    total_deaths = EXCLUDED.total_deaths,
    total_assists = EXCLUDED.total_assists,
    gold_earned = EXCLUDED.gold_earned,
    gold_spent = EXCLUDED.gold_spent,
    xp_earned = EXCLUDED.xp_earned,
    tower_kills = EXCLUDED.tower_kills,
    roshan_kills = EXCLUDED.roshan_kills,
    win_flag = EXCLUDED.win_flag;

-- Verify the updated data
SELECT * FROM fact_team_match_stats ORDER BY match_id, team_id LIMIT 10;