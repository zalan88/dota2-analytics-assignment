INSERT INTO fact_matches (
    match_id, 
    start_time, 
    duration, 
    game_mode, 
    radiant_team_id,      
    dire_team_id,       
    first_blood_time, 
    team_fights, 
    radiant_win, 
    version, 
    patch
)
SELECT DISTINCT ON ((raw_json->>'match_id')::BIGINT)
    (raw_json->>'match_id')::BIGINT AS match_id,
    (raw_json->>'start_time')::BIGINT AS start_time,
    (raw_json->>'duration')::INT AS duration,
    (raw_json->>'game_mode')::INT AS game_mode,
    (raw_json->>'radiant_team_id')::INT AS radiant_team_id,      
    (raw_json->>'dire_team_id')::INT AS dire_team_id,       
    (raw_json->>'first_blood_time')::INT AS first_blood_time,
    jsonb_array_length(raw_json->'teamfights') AS team_fights,
    (raw_json->>'radiant_win')::BOOLEAN AS radiant_win,
    (raw_json->>'version')::INT AS version,
    (raw_json->>'patch')::INT AS patch
FROM stg_matches
WHERE (raw_json->>'match_id') IS NOT NULL;
