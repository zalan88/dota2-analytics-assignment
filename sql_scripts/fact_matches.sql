-- Temporarily disable foreign key constraints
ALTER TABLE fact_team_match_stats DROP CONSTRAINT IF EXISTS fact_team_match_stats_match_id_fkey;
ALTER TABLE fact_player_match_stats DROP CONSTRAINT IF EXISTS fact_player_match_stats_match_id_fkey;

-- Clear existing data
TRUNCATE TABLE fact_matches;

-- Insert only new match data
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
WHERE (raw_json->>'match_id') IS NOT NULL
AND NOT EXISTS (
    SELECT 1 
    FROM fact_matches fm 
    WHERE fm.match_id = (raw_json->>'match_id')::BIGINT
)
ON CONFLICT (match_id) DO UPDATE SET
    start_time = EXCLUDED.start_time,
    duration = EXCLUDED.duration,
    game_mode = EXCLUDED.game_mode,
    radiant_team_id = EXCLUDED.radiant_team_id,
    dire_team_id = EXCLUDED.dire_team_id,
    first_blood_time = EXCLUDED.first_blood_time,
    team_fights = EXCLUDED.team_fights,
    radiant_win = EXCLUDED.radiant_win,
    version = EXCLUDED.version,
    patch = EXCLUDED.patch;

-- Restore foreign key constraints
ALTER TABLE fact_team_match_stats 
ADD CONSTRAINT fact_team_match_stats_match_id_fkey 
FOREIGN KEY (match_id) REFERENCES fact_matches(match_id) ON DELETE CASCADE;

ALTER TABLE fact_player_match_stats 
ADD CONSTRAINT fact_player_match_stats_match_id_fkey 
FOREIGN KEY (match_id) REFERENCES fact_matches(match_id) ON DELETE CASCADE;

-- Verify the newly added data
SELECT COUNT(*) AS new_matches 
FROM fact_matches fm
WHERE NOT EXISTS (
    SELECT 1 
    FROM fact_team_match_stats ftms 
    WHERE ftms.match_id = fm.match_id
);
