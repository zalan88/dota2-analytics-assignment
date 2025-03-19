/**
3. Who are the top 3 players by KDA in the team's matches?
**/

SELECT 
    p.player_name, 
    CAST(AVG(pm.kda) AS NUMERIC(10,2)) as avg_kda 
FROM 
    dim_players p 
    JOIN fact_player_match_stats pm ON p.account_id = pm.account_id 
WHERE 
    pm.team_id = 2163  -- Comment this line to run for all teams
GROUP BY 
    p.player_name 
ORDER BY 
    avg_kda DESC 
LIMIT 3;