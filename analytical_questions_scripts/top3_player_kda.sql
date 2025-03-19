/**
3. Who are the top 3 players by KDA in the team’s matches?
**/

SELECT 
    player_name, 
    AVG(kda) as avg_kda 
FROM 
    dim_players p 
    JOIN fact_player_match_stats pm 
        ON p.account_id = pm.account_id 
    JOIN fact_matches m 
        ON pm.match_id = m.match_id 
WHERE 
    p.team_id = 2163 AND --Comment this line to run for all teams
    m.start_time >= extract(epoch from (CURRENT_TIMESTAMP - INTERVAL '1 week')) 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 3;