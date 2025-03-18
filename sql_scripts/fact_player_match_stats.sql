-- Insert player match statistics
INSERT INTO fact_player_match_stats (
    match_id,
    account_id,
    hero_id,
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
    CASE 
        WHEN (player->>'deaths')::INT = 0 THEN ((player->>'kills')::INT + (player->>'assists')::INT)::FLOAT
        ELSE ROUND(((player->>'kills')::INT + (player->>'assists')::INT)::FLOAT / NULLIF((player->>'deaths')::INT, 0), 2)
    END AS kda,
    (player->>'kills')::INT AS kills,
    (player->>'deaths')::INT AS deaths,
    (player->>'assists')::INT AS assists,
    (player->>'total_gold')::INT AS total_gold,
    (player->>'total_xp')::INT AS total_xp,
    COALESCE((player->>'objectives_taken')::INT, 0) AS objectives,
    COALESCE((player->>'tower_kills')::INT, 0) AS tower_kills,
    COALESCE((player->>'ancient_kills')::INT, 0) AS ancient_kills,
    (player->>'kills')::INT AS hero_kills,
    (player->>'actions_per_min')::INT AS actions_per_minute,
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
    actions_per_minute = EXCLUDED.actions_per_minute,
    item_0 = EXCLUDED.item_0,
    item_1 = EXCLUDED.item_1,
    item_2 = EXCLUDED.item_2,
    item_3 = EXCLUDED.item_3,
    item_4 = EXCLUDED.item_4,
    item_5 = EXCLUDED.item_5;

-- Verify the updated data
SELECT * FROM fact_player_match_stats ORDER BY match_id, account_id LIMIT 10;