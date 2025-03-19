/**
2. What is the average match duration for the team?
**/

SELECT 
    team_name, 
    ROUND(AVG(duration)/60, 0) as avg_duration_minutes
FROM 
    dim_teams t 
    JOIN fact_team_match_stats tm 
        ON t.team_id = tm.team_id 
    JOIN fact_matches m 
        ON tm.match_id = m.match_id 
WHERE 
    t.team_id = 2163 --Comment this line to run for all teams
GROUP BY 1
ORDER BY 2 DESC;