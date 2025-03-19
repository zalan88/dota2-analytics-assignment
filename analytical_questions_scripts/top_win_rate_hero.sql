/**
4. Which hero has the highest win rate when picked by the team?
**/

SELECT 
    hero_name, 
    ROUND(CAST(COUNT(CASE WHEN win_flag = true THEN 1 END) AS DECIMAL) / COUNT(*) * 100, 2) as win_rate 
FROM 
    dim_heroes h 
    JOIN fact_player_match_stats pm 
        ON h.hero_id = pm.hero_id 
    JOIN fact_matches m 
        ON pm.match_id = m.match_id 
WHERE 
    h.team_id = 2163 --Comment this line to run for all teams
    --m.start_time >= extract(epoch from (CURRENT_TIMESTAMP - INTERVAL '1 week')) 
GROUP BY 1 
ORDER BY 2 DESC;