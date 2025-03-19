INSERT INTO dim_players (
    account_id, 
    player_name, 
    is_pro,
    last_login,
    full_history_time,
    country_code,
    last_match_time,
    profileurl
)
SELECT DISTINCT ON ((raw_json->'profile'->>'account_id')::INT) 
    (raw_json->'profile'->>'account_id')::INT, 
    COALESCE(raw_json->'profile'->>'personaname', 'Unknown Player'),
    (raw_json->'profile'->>'is_pro')::BOOLEAN,
    TO_TIMESTAMP((raw_json->'profile'->>'last_login')::BIGINT) AS last_login,
    TO_TIMESTAMP((raw_json->'profile'->>'full_history_time')::BIGINT) AS full_history_time,
    raw_json->'profile'->>'loccountrycode' AS country_code,
    TO_TIMESTAMP((raw_json->'profile'->>'last_match_time')::BIGINT) AS last_match_time,
    raw_json->'profile'->>'profileurl' AS profileurl
FROM stg_players
WHERE (raw_json->'profile'->>'account_id') IS NOT NULL;