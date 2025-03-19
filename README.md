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
- Dota 2 API Key (get it from https://steamcommunity.com/dev)

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/dota2-analytics-assignment.git
   cd dota2-analytics-assignment
   ```

2. Set up environment variables:
   ```bash
   # Create .env file
   cp .env.example .env
   
   # Edit .env file with your values
   POSTGRES_DB=dota2_analytics
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=your_secure_password
   DOTA2_API_KEY=your_api_key
   ```

3. Start the services:
   ```bash
   docker-compose up --build
   ```

4. Run the pipeline:
   ```bash
   python run_etl.py
   ```

## Loading Data From Scratch

If you need to reset the database and load everything from scratch:

1. Stop and remove existing containers:
   ```bash
   docker-compose down -v   # -v flag removes volumes, ensuring complete cleanup
   ```

2. Reset the database:
   ```bash
   # Using Docker
   docker-compose exec db psql -U postgres -c "DROP DATABASE IF EXISTS dota2_analytics;"
   docker-compose exec db psql -U postgres -c "CREATE DATABASE dota2_analytics;"

   # Or locally
   psql -U postgres -c "DROP DATABASE IF EXISTS dota2_analytics;"
   psql -U postgres -c "CREATE DATABASE dota2_analytics;"
   ```

3. Initialize schema and load data:
   ```bash
   # Initialize database schema
   docker-compose exec db psql -U postgres -d dota2_analytics -f /sql_scripts/tables_schema.sql

   # Run the ETL pipeline
   python run_etl.py --full-refresh
   ```

4. Verify the load:
   ```bash
   # Check table counts
   docker-compose exec db psql -U postgres -d dota2_analytics -c "
   SELECT 
       'fact_matches' as table_name, COUNT(*) as count FROM fact_matches
   UNION ALL
   SELECT 'fact_player_match_stats', COUNT(*) FROM fact_player_match_stats
   UNION ALL
   SELECT 'fact_team_match_stats', COUNT(*) FROM fact_team_match_stats
   ORDER BY table_name;"
   ```

## Running Analytics

Example queries are in the `analytical_questions_scripts/` directory:

```bash
# Run analysis
docker-compose exec db psql -U postgres -d dota2_analytics -f /scripts/top3_player_kda.sql
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