-- Insert player match statistics
INSERT INTO fact_player_match_stats (
    match_id,
    account_id,
    hero_id,
    start_time,
    team_id,
    win_flag,
    kda,
    kills,
    deaths,
    assists,
    total_gold,
    total_xp,
    objectives,
    tower_kills,
    ancient_kills,
    hero_kills,
    hero_damage,
    actions_per_minute,
    item_0,
    item_1,
    item_2,
    item_3,
    item_4,
    item_5
)
SELECT 
    (matches.raw_json->>'match_id')::BIGINT AS match_id,
    (player->>'account_id')::BIGINT AS account_id,
    (player->>'hero_id')::INT AS hero_id,
    (matches.raw_json->>'start_time')::BIGINT AS start_time,
    CASE 
        WHEN (player->>'player_slot')::INT < 128 THEN (matches.raw_json->>'radiant_team_id')::INT
        ELSE (matches.raw_json->>'dire_team_id')::INT
    END AS team_id,
    CASE 
        WHEN (player->>'player_slot')::INT < 128 THEN (matches.raw_json->>'radiant_win')::BOOLEAN
        ELSE NOT (matches.raw_json->>'radiant_win')::BOOLEAN
    END AS win_flag,
    CASE 
        WHEN (player->>'deaths')::INT = 0 THEN ((player->>'kills')::INT + (player->>'assists')::INT)::FLOAT
        ELSE (((player->>'kills')::INT + (player->>'assists')::INT)::FLOAT / NULLIF((player->>'deaths')::INT, 0))::NUMERIC(10,2)
    END AS kda,
    (player->>'kills')::INT AS kills,
    (player->>'deaths')::INT AS deaths,
    (player->>'assists')::INT AS assists,
    (player->>'total_gold')::INT AS total_gold,
    (player->>'total_xp')::INT AS total_xp,
    COALESCE((player->>'tower_kills')::INT, 0) + COALESCE((player->>'ancient_kills')::INT, 0) + COALESCE((player->>'roshan_kills')::INT, 0) AS objectives,
    COALESCE((player->>'tower_kills')::INT, 0) AS tower_kills,
    COALESCE((player->>'ancient_kills')::INT, 0) AS ancient_kills,
    (player->>'kills')::INT AS hero_kills,
    COALESCE((player->>'hero_damage')::INT, 0) AS hero_damage,
    COALESCE((player->>'actions_per_min')::INT, 0) AS actions_per_minute,
    COALESCE((player->>'item_0')::INT, 0) AS item_0,
    COALESCE((player->>'item_1')::INT, 0) AS item_1,
    COALESCE((player->>'item_2')::INT, 0) AS item_2,
    COALESCE((player->>'item_3')::INT, 0) AS item_3,
    COALESCE((player->>'item_4')::INT, 0) AS item_4,
    COALESCE((player->>'item_5')::INT, 0) AS item_5
FROM stg_matches AS matches
CROSS JOIN LATERAL jsonb_array_elements(matches.raw_json->'players') AS player
WHERE 
    (matches.raw_json->>'match_id') IS NOT NULL
    AND (player->>'account_id') IS NOT NULL
    AND (player->>'account_id')::BIGINT > 0  -- Filter out anonymous players

ON CONFLICT (match_id, account_id) DO UPDATE SET
    hero_id = EXCLUDED.hero_id,
    start_time = EXCLUDED.start_time,
    team_id = EXCLUDED.team_id,
    win_flag = EXCLUDED.win_flag,
    kda = EXCLUDED.kda,
    kills = EXCLUDED.kills,
    deaths = EXCLUDED.deaths,
    assists = EXCLUDED.assists,
    total_gold = EXCLUDED.total_gold,
    total_xp = EXCLUDED.total_xp,
    objectives = EXCLUDED.objectives,
    tower_kills = EXCLUDED.tower_kills,
    ancient_kills = EXCLUDED.ancient_kills,
    hero_kills = EXCLUDED.hero_kills,
    hero_damage = EXCLUDED.hero_damage,
    actions_per_minute = EXCLUDED.actions_per_minute,
    item_0 = EXCLUDED.item_0,
    item_1 = EXCLUDED.item_1,
    item_2 = EXCLUDED.item_2,
    item_3 = EXCLUDED.item_3,
    item_4 = EXCLUDED.item_4,
    item_5 = EXCLUDED.item_5;

-- Verify the updated data
SELECT * FROM fact_player_match_stats ORDER BY match_id, account_id LIMIT 10;