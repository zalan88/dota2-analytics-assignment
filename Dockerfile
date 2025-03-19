FROM python:3.9-slim

# Install PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Make the ETL script executable
RUN chmod +x run_etl.py

# Command to run the ETL pipeline
CMD ["python3", "run_etl.py"] 