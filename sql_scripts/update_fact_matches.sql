-- SQL script to update fact_matches table structure
-- This script handles existing foreign key constraints and preserves data

-- 1. Create backup of existing data
CREATE TABLE fact_matches_backup AS SELECT * FROM fact_matches;

-- 2. Drop existing foreign key constraints
ALTER TABLE fact_team_match_stats DROP CONSTRAINT IF EXISTS fact_team_match_stats_match_id_fkey;
ALTER TABLE fact_player_match_stats DROP CONSTRAINT IF EXISTS fact_player_match_stats_match_id_fkey;

-- 3. Drop the original table
DROP TABLE fact_matches;

-- 4. Create the updated table with new columns
CREATE TABLE fact_matches (
    match_id BIGINT PRIMARY KEY,
    start_time BIGINT,
    duration INT,
    game_mode INT,
    radiant_team_id INT,      
    dire_team_id INT,     
    first_blood_time INT,
    team_fights INT,
    radiant_win BOOLEAN,
    version INT,
    patch INT
);

-- 5. Restore data from backup
INSERT INTO fact_matches (
    match_id, start_time, duration, game_mode, 
    first_blood_time, team_fights, radiant_win, 
    version, patch
)
SELECT 
    match_id, start_time, duration, game_mode, 
    first_blood_time, team_fights, radiant_win, 
    version, patch 
FROM fact_matches_backup;

-- 6. Recreate foreign key constraints
ALTER TABLE fact_team_match_stats 
ADD CONSTRAINT fact_team_match_stats_match_id_fkey 
FOREIGN KEY (match_id) REFERENCES fact_matches(match_id) ON DELETE CASCADE;

ALTER TABLE fact_player_match_stats 
ADD CONSTRAINT fact_player_match_stats_match_id_fkey 
FOREIGN KEY (match_id) REFERENCES fact_matches(match_id) ON DELETE CASCADE;

-- 7. Drop the backup table
DROP TABLE fact_matches_backup;

-- 8. Verify the updated table
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'fact_matches' 
ORDER BY ordinal_position;

-- 9. Count rows to verify data was preserved
SELECT COUNT(*) AS total_matches FROM fact_matches; 