-- Script to fix missing data in fact_team_match_stats table

-- Update total_deaths column by calculating from match data
UPDATE fact_team_match_stats AS stats
SET total_deaths = subquery.deaths
FROM (
    SELECT 
        m.match_id,
        CASE 
            WHEN tm.team_id = m.radiant_team_id THEN dire_score
            WHEN tm.team_id = m.dire_team_id THEN radiant_score
            ELSE NULL
        END AS deaths
    FROM fact_team_match_stats tm
    JOIN fact_matches m ON tm.match_id = m.match_id
    JOIN stg_matches sm ON tm.match_id = sm.match_id
    CROSS JOIN LATERAL (
        SELECT 
            (sm.raw_json->>'radiant_score')::INT AS radiant_score,
            (sm.raw_json->>'dire_score')::INT AS dire_score
    ) AS scores
) AS subquery
WHERE stats.match_id = subquery.match_id;

-- Update total_assists column (sum of all assists by players on team)
UPDATE fact_team_match_stats AS stats
SET total_assists = subquery.assists
FROM (
    SELECT 
        tm.match_id,
        tm.team_id,
        COALESCE(SUM(
            CASE 
                WHEN (player->>'player_slot')::INT < 128 AND m.radiant_team_id = tm.team_id THEN (player->>'assists')::INT
                WHEN (player->>'player_slot')::INT >= 128 AND m.dire_team_id = tm.team_id THEN (player->>'assists')::INT
                ELSE 0
            END
        ), 0) AS assists
    FROM fact_team_match_stats tm
    JOIN fact_matches m ON tm.match_id = m.match_id
    JOIN stg_matches sm ON tm.match_id = sm.match_id
    CROSS JOIN LATERAL jsonb_array_elements(sm.raw_json->'players') AS player
    GROUP BY tm.match_id, tm.team_id
) AS subquery
WHERE stats.match_id = subquery.match_id AND stats.team_id = subquery.team_id;

-- Update gold_earned (sum of total gold for players on team)
UPDATE fact_team_match_stats AS stats
SET gold_earned = subquery.gold
FROM (
    SELECT 
        tm.match_id,
        tm.team_id,
        COALESCE(SUM(
            CASE 
                WHEN (player->>'player_slot')::INT < 128 AND m.radiant_team_id = tm.team_id THEN (player->>'total_gold')::INT
                WHEN (player->>'player_slot')::INT >= 128 AND m.dire_team_id = tm.team_id THEN (player->>'total_gold')::INT
                ELSE 0
            END
        ), 0) AS gold
    FROM fact_team_match_stats tm
    JOIN fact_matches m ON tm.match_id = m.match_id
    JOIN stg_matches sm ON tm.match_id = sm.match_id
    CROSS JOIN LATERAL jsonb_array_elements(sm.raw_json->'players') AS player
    GROUP BY tm.match_id, tm.team_id
) AS subquery
WHERE stats.match_id = subquery.match_id AND stats.team_id = subquery.team_id;

-- Update gold_spent (approximated as 85% of gold earned)
UPDATE fact_team_match_stats 
SET gold_spent = FLOOR(gold_earned * 0.85);

-- Update xp_earned (sum of total xp for players on team)
UPDATE fact_team_match_stats AS stats
SET xp_earned = subquery.xp
FROM (
    SELECT 
        tm.match_id,
        tm.team_id,
        COALESCE(SUM(
            CASE 
                WHEN (player->>'player_slot')::INT < 128 AND m.radiant_team_id = tm.team_id THEN (player->>'total_xp')::INT
                WHEN (player->>'player_slot')::INT >= 128 AND m.dire_team_id = tm.team_id THEN (player->>'total_xp')::INT
                ELSE 0
            END
        ), 0) AS xp
    FROM fact_team_match_stats tm
    JOIN fact_matches m ON tm.match_id = m.match_id
    JOIN stg_matches sm ON tm.match_id = sm.match_id
    CROSS JOIN LATERAL jsonb_array_elements(sm.raw_json->'players') AS player
    GROUP BY tm.match_id, tm.team_id
) AS subquery
WHERE stats.match_id = subquery.match_id AND stats.team_id = subquery.team_id;

-- Verify the updated data
SELECT * FROM fact_team_match_stats ORDER BY match_id, team_id; 