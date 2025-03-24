#!/usr/bin/env python3

import subprocess
import logging
import sys
import os
from pathlib import Path
import time
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class ETL:
    def __init__(self):
        self.db_name = os.getenv("POSTGRES_DB", "dota2_analytics")
        self.db_user = os.getenv("POSTGRES_USER", "postgres")
        self.db_password = os.getenv("POSTGRES_PASSWORD")
        self.db_host = os.getenv("POSTGRES_HOST", "db")  # Changed from "localhost" to "db"
        self.scripts_dir = Path("sql_scripts")
        
        # Set PGPASSWORD for all child processes
        os.environ["PGPASSWORD"] = self.db_password
        
    def execute_sql(self, script_name, description):
        """Execute a SQL script and handle errors"""
        try:
            start_time = time.time()
            logging.info(f"Starting {description}...")
            
            result = subprocess.run([
                "psql",
                "-h", self.db_host,
                "-U", self.db_user,
                "-d", self.db_name,
                "-f", str(self.scripts_dir / script_name)
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                logging.error(f"Error in {script_name}: {result.stderr}")
                raise Exception(f"SQL script {script_name} failed")
            
            duration = time.time() - start_time
            logging.info(f"Completed {description} in {duration:.2f} seconds")
            return True
            
        except Exception as e:
            logging.error(f"Failed to execute {script_name}: {str(e)}")
            return False

    def wait_for_postgres(self, max_retries=30, delay=2):
        """Wait for PostgreSQL to be ready"""
        logging.info("Waiting for PostgreSQL to be ready...")
        
        for attempt in range(max_retries):
            try:
                result = subprocess.run([
                    "psql",
                    "-h", self.db_host,
                    "-U", self.db_user,
                    "-d", self.db_name,
                    "-c", "SELECT 1"
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    logging.info("PostgreSQL is ready!")
                    return True
                    
            except Exception:
                pass
            
            logging.info(f"PostgreSQL not ready, waiting {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)
        
        logging.error("PostgreSQL failed to become ready")
        return False

    def run(self):
        """Execute the full ETL pipeline in the correct order"""
        try:
            # Wait for PostgreSQL to be ready
            if not self.wait_for_postgres():
                sys.exit(1)

            # Step 1: Create schema and tables
            if not self.execute_sql("tables_schema.sql", "Creating database schema"):
                sys.exit(1)

            # Step 2: Fetch raw data from API
            if not self.run_python_script("data_pipeline/fetch_data.py", "Fetching data from API"):
                sys.exit(1)

            # Step 3: Load dimension tables that don't depend on facts
            if not self.execute_sql("dim_heroes.sql", "Loading heroes dimension"):
                sys.exit(1)
                
            if not self.execute_sql("dim_players.sql", "Loading players dimension"):
                sys.exit(1)

            # Step 4: Load fact matches
            if not self.execute_sql("fact_matches.sql", "Loading matches fact table"):
                sys.exit(1)

            # Step 5: Initial team dimension load
            if not self.execute_sql("dim_teams.sql", "Initial team dimension load"):
                sys.exit(1)

            # Step 6: Load team match statistics
            if not self.execute_sql("fact_team_match_stats.sql", "Loading team match statistics"):
                sys.exit(1)

            # Step 7: Update team dimension with statistics
            if not self.execute_sql("dim_teams.sql", "Updating team statistics"):
                sys.exit(1)

            # Step 8: Load remaining fact tables
            if not self.execute_sql("fact_player_match_stats.sql", "Loading player match statistics"):
                sys.exit(1)

            logging.info("ETL pipeline completed successfully!")

        except Exception as e:
            logging.error(f"ETL pipeline failed: {str(e)}")
            sys.exit(1)

    def run_python_script(self, script_name, description):
        """Execute a Python script and handle errors"""
        try:
            start_time = time.time()
            logging.info(f"Starting {description}...")
            
            # Pass environment variables to the child process
            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'  # Force unbuffered output
            
            # Run the script and stream output in real-time
            process = subprocess.Popen([
                "python3", "-u",  # -u flag forces unbuffered output
                script_name
            ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
               text=True, bufsize=1, universal_newlines=True)
            
            # Print output in real-time
            while True:
                output = process.stdout.readline()
                if output:
                    print(output.strip(), flush=True)
                
                error = process.stderr.readline()
                if error:
                    print(error.strip(), file=sys.stderr, flush=True)
                
                # Check if process has finished
                if process.poll() is not None:
                    # Get any remaining output
                    remaining_output, remaining_error = process.communicate()
                    if remaining_output:
                        print(remaining_output.strip(), flush=True)
                    if remaining_error:
                        print(remaining_error.strip(), file=sys.stderr, flush=True)
                    break
            
            # Get the return code
            return_code = process.wait()
            
            if return_code != 0:
                raise Exception(f"Python script {script_name} failed")
            
            duration = time.time() - start_time
            logging.info(f"Completed {description} in {duration:.2f} seconds")
            return True
            
        except Exception as e:
            logging.error(f"Failed to execute {script_name}: {str(e)}")
            return False

if __name__ == "__main__":
    etl = ETL()
    etl.run() 