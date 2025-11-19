"""
VENDOR DATA INGESTION PIPELINE - AUTOMATED CSV TO POSTGRESQL
=============================================================

PROJECT: Vendor Performance Analysis
AUTHOR: Dipanshu Kumar
DATE: 15/09/2025

PURPOSE:
- Automated ETL pipeline for vendor data from CSV files to PostgreSQL
- Handles large datasets efficiently using chunked processing
- Creates reusable framework for future data loads
- Provides comprehensive logging and error handling

DATA SOURCES:
- Multiple CSV files in /data directory representing vendor operations
- Includes purchases, sales, pricing, inventory, and invoice data

TECHNICAL STACK:
- PostgreSQL 13+ with psycopg2 adapter
- Pandas for data processing and chunked loading
- SQLAlchemy for database engine management
- Python logging for production-grade monitoring

BUSINESS VALUE:
- Enables scalable vendor performance analysis
- Supports data-driven procurement decisions
- Provides foundation for Tableau dashboards and reporting
"""

import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine

# ==================== CONFIGURATION ====================
# Database connection parameters
DB_USER = "postgres"
DB_PASS = "dk9871953483"
DB_HOST = "localhost"
DB_PORT = "5433"          # Custom PostgreSQL port
DB_NAME = "vendor_db"     # Target database for analysis

# File system configuration
DATA_DIR = "data"         # Source directory containing CSV files
LOG_DIR = "logs"          # Log directory for ingestion monitoring

# Performance tuning parameters
CHUNK_SIZE = 200_000      
WRITE_METHOD = "multi"     # Batch insert method for optimal performance

# ==================== LOGGING SETUP ====================
# Create logs directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)

# Configure comprehensive logging for production monitoring
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ingestion_db.log"),
    level=logging.DEBUG,  # Capture all levels for debugging
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",  # Append mode to preserve historical logs
)

# ==================== DATABASE ENGINE ====================
# Create SQLAlchemy engine for efficient database operations
# Using psycopg2 driver for PostgreSQL compatibility
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ==================== HELPER FUNCTIONS ====================
def _safe_table_name(name: str) -> str:
    """
    Convert filename to SQL-safe table name.
    
    BUSINESS LOGIC:
    - Ensures table names are compatible with PostgreSQL naming conventions
    - Handles special characters and case sensitivity issues
    - Maintains readability while ensuring database compatibility
    
    TRANSFORMATION RULES:
    - Convert to lowercase for consistency
    - Replace non-alphanumeric characters with underscores
    - Remove file extensions automatically
    
    EXAMPLE:
    'Purchase-Prices.csv' → 'purchase_prices'
    'Vendor Invoice.csv' → 'vendor_invoice'
    """
    # Remove file extension and clean special characters
    base = os.path.splitext(name)[0]
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in base)

def ingest_csv_chunked(file_path: str, table_name: str):
    """
    Stream large CSV files into PostgreSQL using chunked processing.
    
    MEMORY MANAGEMENT STRATEGY:
    - Processes data in configurable chunks to avoid RAM overload
    - First chunk creates table with 'replace' mode
    - Subsequent chunks append data for incremental loading
    
    ERROR HANDLING:
    - Comprehensive exception handling with detailed logging
    - Continues processing other files if one fails
    - Provides clear error messages for troubleshooting
    
    PERFORMANCE OPTIMIZATIONS:
    - Uses pandas read_csv with low_memory=True for efficiency
    - Implements batch inserts via 'multi' method
    - Cleans column names for SQL compatibility
    
    PARAMETERS:
    file_path: Full path to source CSV file
    table_name: Target table name in PostgreSQL
    """
    try:
        first = True    # Flag for table creation vs. appending
        rows_total = 0  # Track total rows processed

        # Stream CSV file in chunks to manage memory usage
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE, low_memory=True):
            # Clean column names for SQL compatibility
            # Convert to lowercase and replace special characters with underscores
            chunk.columns = ["".join(c.lower() if c.isalnum() else "_" for c in str(col)) for col in chunk.columns]

            # Determine write strategy: replace for first chunk, append for others
            if_exists = "replace" if first else "append"

            # Write chunk to database with optimized batch insert
            chunk.to_sql(
                table_name,
                con=engine,
                if_exists=if_exists,
                index=False,  # Exclude pandas index column
                method=WRITE_METHOD,  # Use batch insert for performance
            )
            
            rows_total += len(chunk)
            first = False   # Switch to append mode after first chunk

        # Log successful ingestion with summary statistics
        logging.info(f"✅ Ingested {rows_total} rows into '{table_name}' from {os.path.basename(file_path)}")
        print(f"✅ {os.path.basename(file_path)} → {table_name} ({rows_total} rows)")
        
    except Exception as e:
        # Comprehensive error handling with detailed logging
        logging.error(f"❌ Failed ingest for {file_path} → {table_name}: {e}")
        print(f"❌ Error: {file_path} → {table_name}: {e}")

def load_raw_data():
    """
    Main data ingestion controller function.
    
    WORKFLOW:
    1. Validates data directory existence
    2. Discovers all CSV files in data directory
    3. Processes each file through chunked ingestion
    4. Provides performance timing and completion summary
    
    ERROR PREVENTION:
    - Validates directory structure before processing
    - Handles missing files gracefully
    - Provides clear user feedback throughout process
    """
    start = time.time()   # Start timing for performance monitoring

    # Validate data directory exists
    if not os.path.isdir(DATA_DIR):
        logging.error(f"Data folder not found: {DATA_DIR}")
        print(f"❌ Data folder not found: {DATA_DIR}")
        return

    # Discover all CSV files in data directory
    files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".csv")]
    if not files:
        logging.warning("No CSV files found in 'data/'")
        print("⚠️ No CSVs in data/")
        return

    # Process each CSV file through ingestion pipeline
    for file in files:
        file_path = os.path.join(DATA_DIR, file)
        table_name = _safe_table_name(file)

        # Log ingestion start with file details
        logging.info(f"📥 Ingesting {file} → table '{table_name}' (chunked)")
        ingest_csv_chunked(file_path, table_name)

    # Calculate and log total processing time
    mins = (time.time() - start) / 60
    logging.info("-- Ingestion Complete --")
    logging.info(f"Total Time Taken: {mins:.2f} minutes")
    print(f"🎉 Done in {mins:.2f} minutes")

# ==================== MAIN EXECUTION ====================
if __name__ == "__main__":
    """
    Entry point for script execution.
    
    EXECUTION CONTEXT:
    - Runs when script is executed directly (not imported)
    - Initiates complete data ingestion workflow
    - Handles top-level exceptions if needed
    
    USAGE:
    python ingestion.py
    """
    load_raw_data()
