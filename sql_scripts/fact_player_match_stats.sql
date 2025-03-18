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
    (raw_json->>'match_id')::BIGINT AS match_id,
    (player->>'account_id')::BIGINT AS account_id,
    (player->>'hero_id')::INT AS hero_id,
    ((player->>'kills')::INT + (player->>'assists')::INT) / NULLIF((player->>'deaths')::INT, 0) AS kda, -- Avoid division by zero
    (player->>'kills')::INT AS kills,
    (player->>'deaths')::INT AS deaths,
    (player->>'assists')::INT AS assists,
    (player->>'total_gold')::INT AS total_gold,
    (player->>'total_xp')::INT AS total_xp,
    (player->>'objectives')::INT AS objectives,
    (player->>'tower_kills')::INT AS tower_kills,
    (player->>'ancient_kills')::INT AS ancient_kills,
    (player->>'hero_kills')::INT AS hero_kills,
    (player->>'actions_per_minute')::INT AS actions_per_minute,
    ((player->>'items')::JSONB->>0)::INT AS item_0,
    ((player->>'items')::JSONB->>1)::INT AS item_1,
    ((player->>'items')::JSONB->>2)::INT AS item_2,
    ((player->>'items')::JSONB->>3)::INT AS item_3,
    ((player->>'items')::JSONB->>4)::INT AS item_4,
    (dim(player->>'items')::JSONB->>5)::INT AS item_5
FROM stg_matches,
LATERAL jsonb_array_elements(raw_json->'players_info') AS player
WHERE (player->>'account_id') IS NOT NULL
ON CONFLICT (match_id, account_id) DO NOTHING;