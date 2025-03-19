# Dota 2 Analytics ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for Dota 2 match data analytics. It processes match data and provides analytical insights about teams, players, and heroes performance.

## Project Structure

```
dota2-analytics-assignment/
├── analytical_questions_scripts/  # SQL queries for analysis
│   ├── top3_player_kda.sql
│   ├── top_win_rate_hero.sql
│   └── ...
├── data_pipeline/                # Python scripts for data fetching
│   ├── fetch_data.py
│   └── api_client.py
├── sql_scripts/                  # SQL scripts for data transformation
│   ├── tables_schema.sql
│   ├── dim_heroes.sql
│   ├── dim_players.sql
│   ├── dim_teams.sql
│   ├── fact_matches.sql
│   ├── fact_team_match_stats.sql
│   └── fact_player_match_stats.sql
├── Dockerfile                    # Container definition
├── docker-compose.yml           # Service orchestration
├── requirements.txt             # Python dependencies
├── run_etl.py                  # Main ETL orchestration script
└── README.md                   # This file
```

## Prerequisites

- Docker (20.10.0 or higher)
- Docker Compose (2.0.0 or higher)
- 4GB RAM minimum
- Dota 2 API Key (get it [here](https://www.opendota.com/api-keys))
- Port 5432 available (default PostgreSQL port)

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/zalan88/dota2-analytics-assignment
   cd dota2-analytics-assignment
   ```

2. Set up environment:
   ```bash
   # Copy the example environment file
   cp .env.example .env
   
   # Edit .env file:
   # - Add your Dota 2 API key (get it from https://www.opendota.com/api-keys)
   # - Set a secure database password
   ```

3. Start the pipeline:
   ```bash
   docker-compose up
   ```
   Wait for it to complete. The ETL container will exit when done.

4. Incremental Load:
   ```bash
   docker-compose run --rm etl
   ```
   Run this code to re-run the ETL and fetch new matches from the team.

## First Time Setup Issues

If you encounter any database authentication issues during first setup:

1. Make sure you've properly set up your `.env` file from `.env.example`
2. If issues persist, try cleaning up Docker state:
   ```bash
   # Stop containers and remove volumes
   docker-compose down -v
   
   # Start fresh
   docker-compose up
   ```

## Accessing the Database

You can interact with the database in several ways:

1. Using psql inside the container:
   ```bash
   docker-compose exec db psql -U postgres -d dota2_analytics
   ```

2. Common psql commands:
   ```sql
   -- List all tables
   \dt

   -- Describe a specific table
   \d table_name

   -- List schemas
   \dn

   -- Basic query example
   SELECT COUNT(*) FROM fact_matches;
   ```

3. Running the Analytical Questions Scripts:
   ```bash
   # Run a specific file
   docker-compose exec db psql -U postgres -d dota2_analytics -f /app/analytical_questions_scripts/top3_player_kda.sql
   ```
4. Running analytical queries:
   ```bash
   # View top players by KDA
   docker-compose exec db psql -U postgres -d dota2_analytics -c "
   SELECT 
       p.player_name,
       ROUND(AVG(pms.kda), 2) as avg_kda,
       COUNT(*) as matches_played
   FROM fact_player_match_stats pms
   JOIN dim_players p ON p.account_id = pms.account_id
   GROUP BY p.player_name
   HAVING COUNT(*) > 5
   ORDER BY avg_kda DESC
   LIMIT 10;"
   ```

## Configuration (Optional)

In your `.env` file:
- `MATCH_LIMIT`: Number of matches to process
- `LOAD_OLDEST`: Set to true/false to load oldest/newest matches first (good to simulate incremental load)

If you need to reset the database and load everything from scratch:

1. Stop and remove existing containers:
   ```bash
   docker-compose down -v   # -v flag removes volumes, ensuring complete cleanup
   ```

2. Start fresh:
   ```bash
   docker-compose up   # This will start the database and run the ETL pipeline
   ```

## Prerequisites

## Running Analytics

Example queries are in the `analytical_questions_scripts/` directory:

```bash
# Run analysis
docker-compose exec db psql -U postgres -d dota2_analytics -f /app/analytical_questions_scripts/top3_player_kda.sql
```

## Assessment Questions & Answers

### Schema design explanation
This schema follows a dimensional model (OLAP) for efficient analytical queries, balancing normalization and denormalization to optimize usability and performance.

Fact Tables (fact_matches, fact_player_match_stats, fact_team_match_stats) store measurable game data.
Dimension Tables (dim_teams, dim_players, dim_heroes) store descriptive attributes for easy lookups.
Denormalization: Key fields (team_id, start_time, win_flag) were added to fact tables to reduce joins and improve query speed.
Composite Keys: fact_player_match_stats (match_id, account_id), fact_team_match_stats (match_id, team_id) ensure data integrity.
Multiple Granularities: Match-level, team-level, and player-level stats allow flexible analysis.
Optimized for Joins: Indexed foreign keys and consistent data types improve performance.
This schema enables fast aggregations, scalable queries, and easy exploration of Dota 2 match data. 🚀

### Tool Choices
1. PostgreSQL as the Database
PostgreSQL was chosen for its balance between flexibility, scalability, and analytical power:
- JSONB Support → Stores raw API data efficiently in stg_matches.
- Advanced Analytics → Built-in window functions and CTEs for complex game stats.
- Performance & Scalability → Indexing and query optimizations handle large match datasets.
- Easy to Dockerize → Official Docker image and volume support simplify deployment and persistence.

2. SQL-First ETL Pipeline
- Staging Layer (stg_*) → Stores raw JSON data with minimal transformation, preserving original values for reprocessing.
- Transformation Layer → SQL-based transformations ensure clarity, maintainability, and incremental updates.
- Analytics Layer → Pre-aggregated views and materialized tables optimize common queries for performance.

### Assumptions
1. Data Assumptions
- Each match has 10 players, and results are final.
- Players may change teams.

2. Transformations & Calculations
KDA: (Kills + Assists) / max(Deaths, 1), rounded to 2 decimals.
Objectives: tower_kills + ancient_kills + roshan_kills.

3. Data Limitations
- Missing values (hero damage, actions per minute) default to 0.
- Dependent on Dota 2 API availability and rate limits.

## License

MIT
## Troubleshooting

### Port 5432 Already in Use?

1. Check what's using the port:
   ```bash
   # On Linux/Mac
   sudo lsof -i :5432
   # On Windows
   netstat -ano | findstr :5432
   ```

2. Either:
   - Stop your local PostgreSQL:
     ```bash
     # On Mac
     brew services stop postgresql
     # On Linux
     sudo service postgresql stop
     # On Windows
     net stop postgresql
     ```
   
   - Or change the port in `docker-compose.yml`:
     ```yaml
     services:
       db:
         ports:
           - "5433:5432"  # Use 5433 instead
     ```
     Then update `DATABASE_URL` in `.env` to use the new port.

### Container Issues?

Try starting fresh:
```bash
docker-compose down -v
docker-compose up
```

## Project Structure

```
dota2-analytics-assignment/
├── analytical_questions_scripts/  # Example queries
├── data_pipeline/                # Data fetching code
├── sql_scripts/                  # Database setup
└── run_etl.py                   # Main ETL script
```

## License

MIT