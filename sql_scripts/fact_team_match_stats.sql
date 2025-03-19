-- Clear existing data (optional) - uncomment if needed
-- TRUNCATE TABLE fact_team_match_stats;

-- Insert team match statistics only for new matches
WITH new_matches AS (
    SELECT match_id 
    FROM fact_matches fm
    WHERE NOT EXISTS (
        SELECT 1 
        FROM fact_team_match_stats ftms 
        WHERE ftms.match_id = fm.match_id
    )
)
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
-- RADIANT TEAM STATS for new matches
SELECT 
    (matches.raw_json->>'match_id')::BIGINT AS match_id, 
    (matches.raw_json->>'radiant_team_id')::INT AS team_id,
    (matches.raw_json->>'radiant_score')::INT AS total_kills,
    (matches.raw_json->>'dire_score')::INT AS total_deaths,
    (
        SELECT COALESCE(SUM((player->>'assists')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS total_assists,
    (
        SELECT COALESCE(SUM((player->>'total_gold')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS gold_earned,
    (
        SELECT FLOOR(COALESCE(SUM((player->>'total_gold')::INT), 0) * 0.85)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS gold_spent,
    (
        SELECT COALESCE(SUM((player->>'total_xp')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT < 128
    ) AS xp_earned,
    (matches.raw_json->>'tower_status_radiant')::INT AS tower_kills,
    (matches.raw_json->>'barracks_status_radiant')::INT AS roshan_kills,
    (matches.raw_json->>'radiant_win')::BOOLEAN AS win_flag
FROM stg_matches AS matches
JOIN new_matches nm ON (matches.raw_json->>'match_id')::BIGINT = nm.match_id
WHERE (matches.raw_json->>'radiant_team_id') IS NOT NULL
AND (matches.raw_json->>'radiant_team_id')::INT > 0

UNION ALL

-- DIRE TEAM STATS for new matches
SELECT 
    (matches.raw_json->>'match_id')::BIGINT AS match_id, 
    (matches.raw_json->>'dire_team_id')::INT AS team_id,
    (matches.raw_json->>'dire_score')::INT AS total_kills,
    (matches.raw_json->>'radiant_score')::INT AS total_deaths,
    (
        SELECT COALESCE(SUM((player->>'assists')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS total_assists,
    (
        SELECT COALESCE(SUM((player->>'total_gold')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS gold_earned,
    (
        SELECT FLOOR(COALESCE(SUM((player->>'total_gold')::INT), 0) * 0.85)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS gold_spent,
    (
        SELECT COALESCE(SUM((player->>'total_xp')::INT), 0)
        FROM jsonb_array_elements(matches.raw_json->'players') AS player
        WHERE (player->>'player_slot')::INT >= 128
    ) AS xp_earned,
    (matches.raw_json->>'tower_status_dire')::INT AS tower_kills,
    (matches.raw_json->>'barracks_status_dire')::INT AS roshan_kills,
    NOT (matches.raw_json->>'radiant_win')::BOOLEAN AS win_flag
FROM stg_matches AS matches
JOIN new_matches nm ON (matches.raw_json->>'match_id')::BIGINT = nm.match_id
WHERE (matches.raw_json->>'dire_team_id') IS NOT NULL
AND (matches.raw_json->>'dire_team_id')::INT > 0

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
SELECT COUNT(*) AS new_team_match_stats FROM fact_team_match_stats;