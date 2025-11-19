"""
VENDOR SUMMARY TABLE GENERATION - DATA CLEANING & FEATURE ENGINEERING
=====================================================================

PROJECT: Vendor Performance Analysis
AUTHOR: Dipanshu Kumar
DATE: 15/09/2025

PURPOSE:
- Create comprehensive vendor performance summary table from raw database tables
- Implement data cleaning and business logic identified during SQL exploration
- Calculate key performance metrics for vendor analysis
- Automate summary table generation for reproducible analysis

DATA SOURCE ANALYSIS:
• The purchases table contains actual purchase data, including the date of purchase, 
  products (brands) purchased by vendors, the amount paid (in dollars), and the quantity purchased.
• The purchase price column is derived from the purchase_prices table, which provides 
  product-wise actual and purchase prices. The combination of vendor and brand is unique in this table.
• The vendor_invoice table aggregates data from the purchases table, summarizing quantity 
  and dollar amounts, along with an additional column for freight. This table maintains 
  uniqueness based on vendor and PO number.
• The sales table captures actual sales transactions, detailing the brands purchased by 
  vendors, the quantity sold, the selling price, and the revenue earned.

BUSINESS NEED FOR SUMMARY TABLE:
As the data required for analysis is distributed across different tables, we need to create 
a unified summary table containing:
• Purchase transactions made by vendors
• Sales transaction data  
• Freight costs for each vendor
• Actual product prices from vendors

PERFORMANCE OPTIMIZATION BENEFITS:
• The query involves heavy joins and aggregations on large datasets like sales and purchases
• Storing pre-aggregated results avoids repeated expensive computations during analysis
• Enables efficient analysis of sales, purchases, and pricing across different vendors and brands
• Provides faster dashboarding and reporting - instead of running expensive queries each time, 
  dashboards can fetch data quickly from the summary table

BUSINESS LOGIC IMPLEMENTED:
- Filter for positive purchase prices (valid transactions only)
- Aggregate vendor performance at vendor-brand level
- Calculate derived metrics: gross profit, profit margin, stock turnover
- Handle data quality issues: null values, whitespace, division by zero

DATA FLOW:
Raw Tables → SQL Aggregation → Data Cleaning → Feature Engineering → Summary Table
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# ==================== LOGGING SETUP ====================
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# ==================== DATABASE CONFIGURATION ====================
# Database configuration - REPLACE WITH YOUR CREDENTIALS
DB_USER = "your_postgres_username"
DB_PASS = "your_postgres_password" 
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "vendor_db"

# Create database engine with connection pooling
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,  # Verify connections before use
)

# ==================== DATA INGESTION HELPER ====================
# Optional integration with ingestion.py for consistent data writing
try:
    from ingestion import ingest_db as _ingest_db_from_ingestion
    def ingest_db(df: pd.DataFrame, table_name: str, eng):
        """
        Use ingestion.py's writer if available for consistency
        """
        _ingest_db_from_ingestion(df, table_name, eng)
except Exception:
    def ingest_db(df: pd.DataFrame, table_name: str, eng):
        """
        Fallback data writer - replaces table on each run
        Uses batch processing for large datasets
        """
        df.to_sql(
            table_name, 
            con=eng, 
            if_exists="replace", 
            index=False, 
            method="multi", 
            chunksize=50_000
        )
        logging.info(f"✅ Wrote {len(df):,} rows to '{table_name}' (replace)")

# ==================== CORE BUSINESS LOGIC - SQL QUERY ====================
VENDOR_SUMMARY_SQL = """
-- UNIFIED VENDOR PERFORMANCE SUMMARY QUERY
-- Combines data from purchases, sales, pricing, and freight tables
-- Enables comprehensive vendor performance analysis

WITH freight_summary AS (
    -- Aggregate freight costs by vendor from vendor_invoice table
    -- Freight represents additional shipping costs for each vendor
    SELECT
        vendornumber,
        SUM(freight) AS freight_cost
    FROM vendor_invoice
    GROUP BY vendornumber
),

purchase_summary AS (
    -- Aggregate purchase data with pricing information from purchases and purchase_prices
    -- Combines vendor purchase transactions with actual product pricing
    -- Filter for positive purchase prices to include only valid transactions
    SELECT
        p."vendornumber",
        p."vendorname",
        p."brand",
        p."description",
        p."purchaseprice",
        pp."price" AS actual_price,  -- From purchase_prices table
        pp."volume",
        SUM(p."quantity") AS total_purchase_quantity,
        SUM(p."dollars") AS total_purchase_dollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p."brand" = pp."brand"
    WHERE p."purchaseprice" > 0  -- Business rule: only valid purchases
    GROUP BY p."vendornumber", p."vendorname", p."brand", p."description",
             p."purchaseprice", pp."price", pp."volume"
),

sales_summary AS (
    -- Aggregate sales performance by vendor and brand from sales table
    -- Captures actual sales transactions including quantities and revenues
    SELECT
        "vendorno",
        "brand",
        SUM("salesquantity") AS total_sales_quantity,
        SUM("salesdollars") AS total_sales_dollars,
        SUM("salesprice") AS total_sales_price,
        SUM("excisetax") AS total_excise_tax
    FROM sales
    GROUP BY vendorno, brand
)

-- Final unified vendor performance summary combining all data sources
-- LEFT JOIN preserves all vendors (even those with no sales data)
SELECT
    ps.vendornumber, 
    ps.vendorname, 
    ps.brand, 
    ps.description, 
    ps.purchaseprice, 
    ps.actual_price, 
    ps.volume, 
    ps.total_purchase_quantity, 
    ps.total_purchase_dollars, 
    ss.total_sales_quantity, 
    ss.total_sales_dollars, 
    ss.total_sales_price,
    ss.total_excise_tax,
    fs.freight_cost
FROM purchase_summary ps
LEFT JOIN sales_summary ss
    ON ps.vendornumber = ss.vendorno
   AND ps.brand = ss.brand
LEFT JOIN freight_summary fs
    ON ps.vendornumber = fs.vendornumber
ORDER BY ps.total_purchase_dollars DESC  -- Prioritize high-volume vendors
"""

# ==================== CORE FUNCTIONS ====================

def create_vendor_summary(engine):
    """
    Execute the main SQL query to create vendor summary dataset.
    
    PERFORMANCE NOTE:
    This query performs heavy joins and aggregations across multiple large tables.
    By pre-computing this summary, we avoid expensive repeated computations during analysis.
    
    RETURNS:
        pandas.DataFrame: Raw vendor summary data from database
    """
    logging.info("Running vendor summary CTE - combining purchases, sales, pricing, and freight data…")
    df = pd.read_sql_query(text(VENDOR_SUMMARY_SQL), con=engine)
    logging.info(f"Summary CTE returned {len(df):,} vendor-brand combinations")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Comprehensive data cleaning and feature engineering for vendor performance analysis.
    
    CLEANING STEPS:
    1. Data type standardization for consistent analysis
    2. Null value handling to ensure data completeness
    3. Text data normalization for accurate grouping and filtering
    4. Derived metric calculation for business performance indicators
    
    BUSINESS METRICS CALCULATED:
    - Gross Profit: Revenue minus cost of goods sold (indicates basic profitability)
    - Profit Margin: Profitability percentage (measures efficiency)
    - Stock Turnover: Inventory efficiency ratio (assesses inventory management)  
    - Sales-to-Purchase Ratio: Sales performance indicator (evaluates sales effectiveness)
    """
    logging.info("Cleaning data and calculating performance metrics…")

    # Convert volume to numeric type for calculations
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("float")

    # Replace null values with 0 for numerical calculations
    # Ensures all financial calculations can proceed without missing data
    df.fillna(0, inplace=True)

    # Standardize text data by removing whitespace
    # Critical for accurate vendor and product categorization
    if "vendorname" in df.columns:
        df["vendorname"] = df["vendorname"].astype(str).str.strip()
    if "description" in df.columns:
        df["description"] = df["description"].astype(str).str.strip()

    # ==================== FEATURE ENGINEERING ====================
    
    # Gross Profit: Total sales revenue minus total purchase cost
    # Fundamental measure of business profitability
    df["gross_profit"] = df["total_sales_dollars"] - df["total_purchase_dollars"]

    # Profit Margin: Gross profit as percentage of sales revenue
    # Division by zero protection for records with no sales
    # Key metric for comparing vendor profitability efficiency
    df["profit_margin"] = (
        (df["gross_profit"] / df["total_sales_dollars"]) * 100
    ).where(df["total_sales_dollars"] != 0, 0)

    # Stock Turnover: Sales quantity relative to purchase quantity
    # Measures inventory management efficiency
    # High turnover indicates good inventory management
    df["stock_turnover"] = (
        df["total_sales_quantity"] / df["total_purchase_quantity"]
    ).where(df["total_purchase_quantity"] != 0, 0)

    # Sales-to-Purchase Ratio: Sales dollars relative to purchase dollars
    # Indicates sales performance and pricing strategy
    # Values > 1 indicate profitable sales operations
    df["sales_to_purchase_ratio"] = (
        df["total_sales_dollars"] / df["total_purchase_dollars"]
    ).where(df["total_purchase_dollars"] != 0, 0)

    logging.info("Data cleaning and feature engineering complete.")
    return df


def main():
    """
    Main execution function for vendor summary pipeline.
    
    WORKFLOW:
    1. EXTRACT: Query data from database using comprehensive SQL joins and aggregations
    2. TRANSFORM: Clean data and calculate business performance metrics
    3. LOAD: Store pre-aggregated results back to database for fast access
    
    PERFORMANCE BENEFITS:
    • Avoids repeated expensive joins and aggregations during analysis
    • Enables fast dashboard queries by using pre-computed summary data
    • Provides consistent performance metrics across all reporting
    """
    logging.info("===== Building Vendor Summary Table (Performance Optimized) =====")
    try:
        # Step 1: EXTRACT - Build summary from live database
        # This is the computationally expensive step that we pre-compute
        summary_df = create_vendor_summary(engine)
        logging.info(f"Raw summary sample:\n{summary_df.head(5)}")

        # Step 2: TRANSFORM - Clean data and derive business metrics
        # Adds calculated fields for comprehensive performance analysis
        clean_df = clean_data(summary_df)
        logging.info(f"Cleaned summary sample:\n{clean_df.head(5)}")

        # Step 3: LOAD - Write cleaned summary to database
        # Creates optimized dataset for fast reporting and dashboarding
        logging.info("Writing pre-aggregated vendor summary to table: summary_table …")
        ingest_db(clean_df, "summary_table", engine)
        
        logging.info("✅ Vendor summary table generation completed successfully.")
        logging.info("🎯 Performance benefit: Future analyses can query summary_table instead of raw tables")
        print("✅ Vendor summary table created successfully!")
        print("📊 Performance optimized: Pre-aggregated data ready for fast dashboard queries")
        
    except Exception as e:
        logging.exception(f"❌ Pipeline failed: {e}")
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    """
    Script entry point for direct execution.
    
    USAGE:
    python get_vendor_summary.py
    
    BUSINESS VALUE:
    Running this script creates a performance-optimized dataset that enables:
    • Faster vendor performance analysis
    • Efficient dashboard and reporting queries  
    • Consistent business metric calculations
    • Scalable analytics as data volume grows
    """
    main()