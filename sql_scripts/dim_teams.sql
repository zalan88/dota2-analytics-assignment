INSERT INTO dim_teams (team_id, team_name, region)
SELECT DISTINCT (raw_json->>'team_id')::INT AS team_id,
    COALESCE(raw_json->>'name', 'Unknown Team') AS team_name,
    COALESCE(raw_json->>'region', 'Unknown') AS region
FROM stg_teams

-- Add teams from matches that may be missing
UNION

SELECT DISTINCT (raw_json->>'radiant_team_id')::INT AS team_id,
    'Unknown Team' AS team_name,
    'Unknown' AS region
FROM stg_matches
WHERE (raw_json->>'radiant_team_id') IS NOT NULL
AND (raw_json->>'radiant_team_id')::INT NOT IN (SELECT team_id FROM dim_teams)

UNION

SELECT DISTINCT (raw_json->>'dire_team_id')::INT AS team_id,
    'Unknown Team' AS team_name,
    'Unknown' AS region
FROM stg_matches
WHERE (raw_json->>'dire_team_id') IS NOT NULL
AND (raw_json->>'dire_team_id')::INT NOT IN (SELECT team_id FROM dim_teams);
