INSERT INTO fact_team_match_stats (
    match_id,
    team_id,
    total_kills,
    total_deaths,
    total_assists,
    gold_earned,
    gold_spent,
    xp_earned,
    tower_kills,
    roshan_kills,
    win_flag
)
SELECT DISTINCT ON ((raw_json->>'match_id')::BIGINT, (raw_json->>'radiant_team_id')::INT)
    (raw_json->>'match_id')::BIGINT AS match_id,
    (raw_json->>'radiant_team_id')::INT AS team_id,
    (raw_json->>'radiant_score')::INT AS total_kills,
    (SELECT SUM((p->>'deaths')::INT) FROM jsonb_array_elements(raw_json->'players') p WHERE (p->>'team')::INT = 0) AS total_deaths,
    (SELECT SUM((p->>'assists')::INT) FROM jsonb_array_elements(raw_json->'players') p WHERE (p->>'team')::INT = 0) AS total_assists,
    (SELECT SUM((p->>'gold_per_min')::INT * (raw_json->>'duration')::INT) FROM jsonb_array_elements(raw_json->'players') p WHERE (p->>'team')::INT = 0) AS gold_earned,
    NULL::INT AS gold_spent,  
    NULL::INT AS xp_earned,  
    (raw_json->>'tower_status_radiant')::INT AS tower_kills,
    (raw_json->>'barracks_status_radiant')::INT AS roshan_kills,
    (raw_json->>'radiant_win')::BOOLEAN AS win_flag
FROM stg_matches
WHERE (raw_json->>'match_id') IS NOT NULL
AND (raw_json->>'radiant_team_id') IS NOT NULL

UNION ALL

SELECT DISTINCT ON ((raw_json->>'match_id')::BIGINT, (raw_json->>'dire_team_id')::INT)
    (raw_json->>'match_id')::BIGINT AS match_id,
    (raw_json->>'dire_team_id')::INT AS team_id,
    (raw_json->>'dire_score')::INT AS total_kills,
    (SELECT SUM((p->>'deaths')::INT) FROM jsonb_array_elements(raw_json->'players') p WHERE (p->>'team')::INT = 1) AS total_deaths,
    (SELECT SUM((p->>'assists')::INT) FROM jsonb_array_elements(raw_json->'players') p WHERE (p->>'team')::INT = 1) AS total_assists,
    (SELECT SUM((p->>'gold_per_min')::INT * (raw_json->>'duration')::INT) FROM jsonb_array_elements(raw_json->'players') p WHERE (p->>'team')::INT = 1) AS gold_earned,
    NULL::INT AS gold_spent,  
    NULL::INT AS xp_earned,  
    (raw_json->>'tower_status_dire')::INT AS tower_kills,
    (raw_json->>'barracks_status_dire')::INT AS roshan_kills,
    NOT (raw_json->>'radiant_win')::BOOLEAN AS win_flag
FROM stg_matches
WHERE (raw_json->>'match_id') IS NOT NULL
AND (raw_json->>'dire_team_id') IS NOT NULL

ON CONFLICT (match_id, team_id) DO NOTHING;
