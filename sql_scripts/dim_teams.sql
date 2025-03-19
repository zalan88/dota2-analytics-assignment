-- First, insert teams from fact_matches and stg_teams
INSERT INTO dim_teams (team_id, team_name, region)
SELECT DISTINCT 
    teams.id,
    COALESCE(st.name, 'Team ' || teams.id) as team_name,
    'unknown' as region
FROM (
    SELECT radiant_team_id as id FROM fact_matches
    UNION
    SELECT dire_team_id as id FROM fact_matches
) teams
LEFT JOIN (
    SELECT 
        (raw_json->>'team_id')::integer as team_id,
        raw_json->>'name' as name
    FROM stg_teams
) st ON st.team_id = teams.id
WHERE teams.id IS NOT NULL;

-- Then, insert team match statistics for radiant teams
INSERT INTO fact_team_match_stats (match_id, team_id, total_kills, total_deaths, total_assists, 
                                 gold_earned, gold_spent, xp_earned, tower_kills, roshan_kills, win_flag)
SELECT 
    m.match_id,
    m.radiant_team_id,
    SUM(pms.kills) as total_kills,
    SUM(pms.deaths) as total_deaths,
    SUM(pms.assists) as total_assists,
    SUM(pms.total_gold) as gold_earned,
    0 as gold_spent,
    SUM(pms.total_xp) as xp_earned,
    SUM(pms.tower_kills) as tower_kills,
    0 as roshan_kills,
    m.radiant_win as win_flag
FROM fact_matches m
JOIN fact_player_match_stats pms ON m.match_id = pms.match_id
GROUP BY m.match_id, m.radiant_team_id, m.radiant_win
HAVING COUNT(*) <= 5;

-- And insert team match statistics for dire teams
INSERT INTO fact_team_match_stats (match_id, team_id, total_kills, total_deaths, total_assists, 
                                 gold_earned, gold_spent, xp_earned, tower_kills, roshan_kills, win_flag)
SELECT 
    m.match_id,
    m.dire_team_id,
    SUM(pms.kills) as total_kills,
    SUM(pms.deaths) as total_deaths,
    SUM(pms.assists) as total_assists,
    SUM(pms.total_gold) as gold_earned,
    0 as gold_spent,
    SUM(pms.total_xp) as xp_earned,
    SUM(pms.tower_kills) as tower_kills,
    0 as roshan_kills,
    NOT m.radiant_win as win_flag
FROM fact_matches m
JOIN fact_player_match_stats pms ON m.match_id = pms.match_id
GROUP BY m.match_id, m.dire_team_id, m.radiant_win
HAVING COUNT(*) <= 5;

-- Finally, update team statistics
UPDATE dim_teams t SET
    total_matches = s.match_count,
    win_count = s.wins,
    loss_count = s.losses,
    avg_match_gold = s.avg_gold,
    avg_match_xp = s.avg_xp,
    avg_match_kills = s.avg_kills,
    avg_match_deaths = s.avg_deaths,
    avg_match_kda = CASE 
        WHEN s.avg_deaths = 0 THEN s.avg_kills::numeric 
        ELSE ((s.avg_kills + s.avg_assists) / s.avg_deaths)::numeric 
    END
FROM (
    SELECT 
        team_id,
        COUNT(*) as match_count,
        SUM(CASE WHEN win_flag THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN NOT win_flag THEN 1 ELSE 0 END) as losses,
        ROUND(AVG(gold_earned)::numeric, 2) as avg_gold,
        ROUND(AVG(xp_earned)::numeric, 2) as avg_xp,
        ROUND(AVG(total_kills)::numeric, 2) as avg_kills,
        ROUND(AVG(total_deaths)::numeric, 2) as avg_deaths,
        ROUND(AVG(total_assists)::numeric, 2) as avg_assists
    FROM fact_team_match_stats
    GROUP BY team_id
) s
WHERE t.team_id = s.team_id; 