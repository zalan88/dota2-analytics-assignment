# Dota 2 Analytics ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for Dota 2 match data analytics.

## Project Structure

```
dota2-analytics-assignment/
├── data_pipeline/       # Python scripts for data fetching
├── sql_scripts/        # SQL scripts for data transformation and loading
├── Dockerfile          # Container definition
├── docker-compose.yml  # Service orchestration
├── requirements.txt    # Python dependencies
├── run_etl.py         # Main ETL orchestration script
└── README.md          # This file
```

## Prerequisites

- Docker
- Docker Compose

## Quick Start

1. Navigate to the project directory:
   ```bash
   cd dota2-analytics-assignment
   ```

2. Start the services:
   ```bash
   docker-compose up --build
   ```

This will:
- Start a PostgreSQL database
- Run the ETL pipeline
- Load data into the analytics tables

## ETL Process

The pipeline executes the following steps in order:

1. Creates database schema and tables (`tables_schema.sql`)
2. Fetches raw data from the Dota 2 API (`fetch_data.py`)
3. Loads dimension tables:
   - Heroes (`dim_heroes.sql`)
   - Players (`dim_players.sql`)
   - Teams (`dim_teams.sql`)
4. Loads fact tables:
   - Matches (`fact_matches.sql`)
   - Team match statistics (`fact_team_match_stats.sql`)
   - Player match statistics (`fact_player_match_stats.sql`)
5. Updates team statistics (`dim_teams.sql`)

## Monitoring

- Logs are written to `logs/etl.log`
- Container logs can be viewed with `docker-compose logs`

## Development

To run the ETL pipeline locally without Docker:

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set environment variables:
   ```bash
   export POSTGRES_DB=dota2_analytics
   export POSTGRES_USER=postgres
   export POSTGRES_PASSWORD=postgres
   ```

3. Run the pipeline:
   ```bash
   python3 run_etl.py
   ```

## Troubleshooting

If you encounter issues:

1. Check the logs in `logs/etl.log`
2. Verify PostgreSQL is running: `docker-compose ps`
3. Check container logs: `docker-compose logs -f`

## License

MIT