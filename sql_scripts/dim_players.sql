INSERT INTO dim_players (account_id, player_name, is_pro)
SELECT DISTINCT ON ((raw_json->'profile'->>'account_id')::INT) 
    (raw_json->'profile'->>'account_id')::INT, 
    COALESCE(raw_json->'profile'->>'personaname', 'Unknown Player'),
    (raw_json->'profile'->>'is_pro')::BOOLEAN
FROM stg_players
WHERE (raw_json->'profile'->>'account_id') IS NOT NULL;