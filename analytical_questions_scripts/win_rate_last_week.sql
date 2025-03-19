/**
1. What is the team’s win rate over the past week?
**/

SELECT 
    team_name, 
    ROUND(CAST(COUNT(CASE WHEN win_flag = true THEN 1 END) AS DECIMAL) / COUNT(*) * 100, 2) as win_rate,
    COUNT(DISTINCT m.match_id) as matches_played
FROM 
    dim_teams t 
    JOIN fact_team_match_stats tm 
        ON t.team_id = tm.team_id 
    JOIN fact_matches m 
        ON tm.match_id = m.match_id 
WHERE 
    t.team_id = 2163 AND --Comment this line to run for all teams
    m.start_time >= extract(epoch from (CURRENT_TIMESTAMP - INTERVAL '1 week')) 
GROUP BY 1;