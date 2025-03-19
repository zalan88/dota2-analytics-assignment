/**
4. Which hero has the highest win rate when picked by the team?
**/

SELECT 
    h.hero_name, 
    ROUND(CAST(COUNT(CASE WHEN pm.win_flag = true THEN 1 END) AS DECIMAL) / COUNT(*) * 100, 2) as win_rate,
    COUNT(*) as total_matches_played
FROM 
    fact_player_match_stats pm
    JOIN dim_heroes h ON h.hero_id = pm.hero_id 
WHERE 
    pm.team_id = 2163 --Comment this line to run for all teams
GROUP BY 
    h.hero_name 
HAVING 
    COUNT(*) >= 5 -- Only consider heroes played at least 5 times
ORDER BY 
    win_rate DESC;