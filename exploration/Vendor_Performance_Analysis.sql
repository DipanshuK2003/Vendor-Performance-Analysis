/* =======================================================
   VENDOR PERFORMANCE ANALYSIS - COMPLETE DATA PIPELINE
   =======================================================

   Project: Vendor Performance Analysis
   Author: Dipanshu Kumar
   Date: 15/09/2025
   
   OBJECTIVES:
   1. Understand database structure and relationships
   2. Identify data quality issues and cleaning requirements
   3. Create comprehensive vendor performance summary table
   4. Prepare data for Tableau dashboard and analysis
   =======================================================  
*/



/* =======================================================
   SECTION 1: DATABASE DISCOVERY & INITIAL ASSESSMENT
   ======================================================= 
   Purpose: Understand available data sources and their structure
   Key Findings: 6 primary tables with vendor, purchase, and sales data
*/

-- List all tables in vendor_db database
SELECT table_name AS "name"
FROM information_schema.tables
WHERE table_schema = 'public';

-- Purchases: count + preview
SELECT COUNT(*) AS "Total Rows" FROM "purchases";
SELECT * FROM "purchases" LIMIT 5;

-- Purchase Prices: count + preview
SELECT COUNT(*) AS "Total Rows" FROM "purchase_prices";
SELECT * FROM "purchase_prices" LIMIT 5;

-- Vendor Invoice: count + preview
SELECT COUNT(*) AS "Total Rows" FROM "vendor_invoice";
SELECT * FROM "vendor_invoice" LIMIT 5;

-- Begin Inventory: count + preview
SELECT COUNT(*) AS "Total Rows" FROM "begin_inventory";
SELECT * FROM "begin_inventory" LIMIT 5;

-- End Inventory: count + preview
SELECT COUNT(*) AS "Total Rows" FROM "end_inventory";
SELECT * FROM "end_inventory" LIMIT 5;

-- Sales: count + preview
SELECT COUNT(*) AS "Total Rows" FROM "sales";
SELECT * FROM "sales" LIMIT 5;



/* =======================================================
   SECTION 2: DEEP DIVE ANALYSIS - VENDOR 4466 CASE STUDY
   ======================================================= 
   Purpose: Understand data relationships and business context through specific vendor
   Key Insights: Vendor-brand hierarchy, purchase patterns, sales performance
*/

-- Raw records for Vendor 4466 across all tables
SELECT * FROM "purchases" WHERE VendorNumber = 4466;
SELECT * FROM "purchase_prices" WHERE VendorNumber = 4466;
SELECT * FROM "vendor_invoice" WHERE VendorNumber = 4466;
SELECT * FROM "sales" WHERE VendorNo = 4466;

-- Purchases summary by Brand & Price for Vendor 4466
-- Shows purchase patterns and pricing strategy
SELECT 
	"brand", 
	"purchaseprice", 
	SUM("quantity") AS total_quantity,
	SUM("dollars") AS total_dollars
FROM purchases
WHERE VendorNumber = 4466
GROUP BY "brand", "purchaseprice"
ORDER BY "brand", "purchaseprice";

-- Sales summary by Brand for Vendor 4466
-- Shows sales performance and revenue distribution
SELECT 
	"brand",
	SUM("salesdollars") AS sales_dollars,
	SUM("salesprice") AS sales_price,
	SUM("salesquantity") AS sales_quantity
FROM sales
WHERE VendorNo = 4466
GROUP BY "brand";



/* =======================================================
   SECTION 3: DATA QUALITY ASSESSMENT
   ======================================================= 
   Purpose: Identify data integrity issues before analysis
   Key Findings: Duplicate purchase orders detected
*/

-- Checking duplicates in vendor_invoice table
-- Critical for accurate financial calculations
SELECT 
    ponumber, 
    COUNT(ponumber) AS count_of_duplicates
FROM vendor_invoice 
GROUP BY ponumber
HAVING COUNT(ponumber) > 1
ORDER BY count_of_duplicates DESC;



/* =======================================================
   SECTION 4: SUMMARY TABLE CREATION - CORE DATA PIPELINE
   ======================================================= 
   Purpose: Create unified vendor performance dataset
   Business Logic: 
   - Join purchases, sales, and freight data
   - Aggregate at vendor-brand level for performance analysis
   - Include only positive purchase prices (valid transactions)
*/

CREATE TABLE summary_table AS

-- Freight cost aggregation by vendor
WITH freight_summary AS (
	SELECT vendornumber, SUM(freight) AS freight_cost
	FROM vendor_invoice
	GROUP BY vendornumber
),

-- Purchase data aggregation with pricing information
purchase_summary AS (
	SELECT
		p."vendornumber",
		p."vendorname",
		p."brand",
		p."description",
		p."purchaseprice",
		pp."price" AS actual_price,
		pp."volume",
		SUM(p."quantity") AS total_purchase_quantity,
		SUM(p."dollars") AS total_purchase_dollars
	FROM purchases p
	JOIN purchase_prices pp
		ON p."brand" = pp."brand"
	WHERE p."purchaseprice" > 0
	GROUP BY p."vendornumber", p."vendorname", p."brand", 
	         p."description", p."purchaseprice", pp."price", pp."volume"
),

-- Sales data aggregation by vendor and brand
sales_summary AS (
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

-- Final unified vendor performance table
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
ORDER BY ps.total_purchase_dollars DESC;



/* =======================================================
   SECTION 5: SUMMARY TABLE VALIDATION & QUALITY CHECKS
   ======================================================= 
   Purpose: Ensure data integrity and completeness
   Key Checks: Schema validation, null analysis, data anomalies
*/

-- Check schema of summary_table
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_name = 'summary_table' 
ORDER BY ordinal_position;

-- Comprehensive null analysis across all columns
SELECT
    COUNT(*) - COUNT(vendornumber) AS vendornumber_nulls,
    COUNT(*) - COUNT(vendorname) AS vendorname_nulls,
    COUNT(*) - COUNT(brand) AS brand_nulls,
    COUNT(*) - COUNT(description) AS description_nulls,
    COUNT(*) - COUNT(purchaseprice) AS purchaseprice_nulls,
    COUNT(*) - COUNT(actual_price) AS actual_price_nulls,
    COUNT(*) - COUNT(volume) AS volume_nulls,
    COUNT(*) - COUNT(total_purchase_quantity) AS total_purchase_quantity_nulls,
    COUNT(*) - COUNT(total_purchase_dollars) AS total_purchase_dollars_nulls,
    COUNT(*) - COUNT(total_sales_quantity) AS total_sales_quantity_nulls,
    COUNT(*) - COUNT(total_sales_dollars) AS total_sales_dollars_nulls,
    COUNT(*) - COUNT(total_sales_price) AS total_sales_price_nulls,
    COUNT(*) - COUNT(total_excise_tax) AS total_excise_tax_nulls,
    COUNT(*) - COUNT(freight_cost) AS freight_cost_nulls
FROM summary_table;

-- Check vendorname anomalies and inconsistencies
SELECT DISTINCT vendorname
FROM summary_table
ORDER BY vendorname;



/* =======================================================
   SECTION 6: DATA CLEANING & STANDARDIZATION
   ======================================================= 
   Purpose: Address data quality issues identified during exploration
   Key Actions: Whitespace removal, null handling, data type standardization
*/

-- Identify vendorname whitespace issues
SELECT 
    vendorname,
    LENGTH(vendorname) AS original_length,
    LENGTH(TRIM(vendorname)) AS trimmed_length
FROM summary_table
WHERE vendorname LIKE ' %' OR vendorname LIKE '% ' OR vendorname <> TRIM(vendorname);

-- Visualize whitespaces in vendorname for manual review
SELECT 
    vendorname,
    REPLACE(vendorname, ' ', '•') AS visible_whitespace
FROM summary_table
WHERE vendorname LIKE ' %' OR vendorname LIKE '% '
ORDER BY vendorname;

-- Remove whitespace from vendorname (data standardization)
UPDATE summary_table
SET vendorname = TRIM(vendorname)
WHERE vendorname IS NOT NULL
AND vendorname <> TRIM(vendorname);

-- Verify cleaning results
SELECT vendorname,
       LENGTH(vendorname) AS current_length,
       '>' || vendorname || '<' AS visualized
FROM summary_table
WHERE vendorname LIKE ' %' OR vendorname LIKE '% ' OR vendorname <> TRIM(vendorname);



/* =======================================================
   SECTION 7: NULL VALUE HANDLING
   ======================================================= 
   Purpose: Ensure data completeness for analysis
   Business Decision: Replace NULL sales metrics with 0 for accurate calculations
*/

-- Replace NULLs with 0 in numeric fields (business rule: no sales = 0 value)
UPDATE summary_table
SET 
    total_sales_quantity = COALESCE(total_sales_quantity, 0),
    total_sales_dollars = COALESCE(total_sales_dollars, 0),
    total_sales_price   = COALESCE(total_sales_price, 0),
    total_excise_tax    = COALESCE(total_excise_tax, 0)
WHERE total_sales_quantity IS NULL
   OR total_sales_dollars IS NULL
   OR total_sales_price IS NULL
   OR total_excise_tax IS NULL;



/* =======================================================
   SECTION 8: FEATURE ENGINEERING - KEY PERFORMANCE METRICS
   ======================================================= 
   Purpose: Create derived metrics for vendor performance analysis
   Business Metrics:
   - Gross Profit: Sales revenue minus purchase costs
   - Profit Margin: Profitability percentage
   - Stock Turnover: Inventory efficiency ratio
   - Sales-to-Purchase Ratio: Sales performance indicator
*/

-- Add calculated performance metrics columns
ALTER TABLE summary_table ADD COLUMN gross_profit NUMERIC(12,2);
ALTER TABLE summary_table ADD COLUMN profit_margin NUMERIC(10,2);
ALTER TABLE summary_table ADD COLUMN stock_turnover NUMERIC(10,4);
ALTER TABLE summary_table ADD COLUMN sales_to_purchase_ratio NUMERIC(10,4);

-- Calculate Gross Profit (Total Sales - Total Purchase Cost)
UPDATE summary_table
SET gross_profit = total_sales_dollars - total_purchase_dollars;

-- Calculate Profit Margin (%) with division by zero protection
UPDATE summary_table
SET profit_margin = CASE 
    WHEN total_sales_dollars = 0 THEN 0
    ELSE (gross_profit / total_sales_dollars) * 100
END;

-- Calculate Stock Turnover (Sales Quantity / Purchase Quantity)
UPDATE summary_table
SET stock_turnover = total_sales_quantity / total_purchase_quantity;

-- Calculate Sales-to-Purchase Ratio (Sales Dollars / Purchase Dollars)
UPDATE summary_table
SET sales_to_purchase_ratio = total_sales_dollars / total_purchase_dollars;



/* =======================================================
   SECTION 9: DATA TYPE STANDARDIZATION
   ======================================================= 
   Purpose: Ensure consistent data types for analysis and visualization
   Key Changes: Integer IDs, standardized decimals, appropriate text lengths
*/

-- Change vendor/brand to integers for efficient joins and filtering
ALTER TABLE summary_table 
ALTER COLUMN vendornumber TYPE INT USING vendornumber::int;

ALTER TABLE summary_table 
ALTER COLUMN brand TYPE INT USING brand::int;

-- Standardize text columns with appropriate lengths
ALTER TABLE summary_table 
ALTER COLUMN vendorname TYPE VARCHAR(100),
ALTER COLUMN description TYPE VARCHAR(100);

-- Standardize numeric/money precision for financial calculations
ALTER TABLE summary_table 
ALTER COLUMN purchaseprice TYPE DECIMAL(15,2),
ALTER COLUMN actual_price TYPE DECIMAL(15,2),
ALTER COLUMN total_purchase_dollars TYPE DECIMAL(15,2),
ALTER COLUMN total_sales_dollars TYPE DECIMAL(15,2),
ALTER COLUMN total_sales_price TYPE DECIMAL(15,2),
ALTER COLUMN total_excise_tax TYPE DECIMAL(15,2),
ALTER COLUMN freight_cost TYPE DECIMAL(15,2),
ALTER COLUMN gross_profit TYPE DECIMAL(15,2),
ALTER COLUMN profit_margin TYPE DECIMAL(15,2),
ALTER COLUMN stock_turnover TYPE DECIMAL(15,2),
ALTER COLUMN sales_to_purchase_ratio TYPE DECIMAL(15,2);

-- Change volume type to decimal for accurate calculations
ALTER TABLE summary_table 
ALTER COLUMN volume TYPE DECIMAL(10,2) USING volume::numeric;



/* =======================================================
   SECTION 10: DATA INTEGRITY & CONSTRAINTS
   ======================================================= 
   Purpose: Ensure data quality and prevent duplicates
   Key Action: Add composite primary key on vendor-brand combination
*/

-- Check for duplicates before adding primary key constraint
SELECT vendornumber, brand, COUNT(*)
FROM summary_table
GROUP BY vendornumber, brand
HAVING COUNT(*) > 1;

-- Add composite primary key to ensure data integrity
ALTER TABLE summary_table
ADD PRIMARY KEY (vendornumber, brand);



/* =======================================================
   SECTION 11: FINAL VERIFICATION
   ======================================================= 
   Purpose: Validate complete data pipeline
*/

-- Quick preview of final summary table
SELECT * FROM summary_table LIMIT 10;



/* =======================================================
   SECTION 12: ANALYSIS & DASHBOARD PREPARATION QUERIES
   ======================================================= 
   Purpose: Test queries for Tableau dashboard visualizations
   Focus Areas: Low-performing vendors, high-potential brands
*/

-- ====================
-- LOW PERFORMING VENDORS ANALYSIS
-- ====================

-- Basic low stock turnover vendors (initial test)
SELECT vendorname, AVG(stock_turnover) AS avg_stock
FROM summary_table
WHERE stock_turnover < 1  
GROUP BY vendorname
ORDER BY avg_stock ASC;

-- Refined: Low turnover with positive business metrics (production version)
SELECT vendorname, AVG(stock_turnover) AS avg_stock
FROM summary_table
WHERE stock_turnover < 1 AND total_sales_quantity > 0 AND gross_profit > 0 AND profit_margin > 0 
GROUP BY vendorname
ORDER BY avg_stock ASC; 

-- Alternative approach using HAVING clause
SELECT vendorname, AVG(stock_turnover) AS avg_stock
FROM summary_table
GROUP BY vendorname
HAVING AVG(stock_turnover) < 1
ORDER BY avg_stock ASC;



-- ====================
-- HIGH-POTENTIAL BRANDS ANALYSIS
-- ====================

-- Get distinct valid brand descriptions for analysis
SELECT DISTINCT description
FROM summary_table
WHERE total_sales_quantity > 0 AND gross_profit > 0 AND profit_margin > 0;

-- Comprehensive brand performance with percentiles (all brands)
WITH brand_performance AS (
    SELECT 
        description,
        SUM(total_sales_dollars) AS total_sales,
        AVG(profit_margin) AS avg_profit_margin
    FROM summary_table
    WHERE total_sales_quantity > 0 
      AND gross_profit > 0 
      AND profit_margin > 0
    GROUP BY description
),
percentiles AS (
    SELECT 
        PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY total_sales) AS low_sales_threshold,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY avg_profit_margin) AS high_margin_threshold
    FROM brand_performance
)
SELECT 
    bp.description,
    bp.total_sales,
    bp.avg_profit_margin,
    p.low_sales_threshold,
    p.high_margin_threshold,
    CASE 
        WHEN bp.total_sales <= p.low_sales_threshold 
             AND bp.avg_profit_margin >= p.high_margin_threshold 
        THEN 'Target Brand'
        ELSE 'Other Brand'
    END AS brand_category
FROM brand_performance bp
CROSS JOIN percentiles p
ORDER BY bp.total_sales;

-- Target brands only (low sales but high margins) - for focused analysis
WITH brand_performance AS (
    SELECT 
        description,
        SUM(total_sales_dollars) AS total_sales,
        AVG(profit_margin) AS avg_profit_margin
    FROM summary_table
    WHERE total_sales_quantity > 0 
      AND gross_profit > 0 
      AND profit_margin > 0
    GROUP BY description
),
percentiles AS (
    SELECT 
        PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY total_sales) AS low_sales_threshold,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY avg_profit_margin) AS high_margin_threshold
    FROM brand_performance
)
SELECT 
    bp.description,
    bp.total_sales,
    bp.avg_profit_margin
FROM brand_performance bp
CROSS JOIN percentiles p
WHERE bp.total_sales <= p.low_sales_threshold 
  AND bp.avg_profit_margin >= p.high_margin_threshold
ORDER BY bp.total_sales;



/* =======================================================
   DATA PIPELINE COMPLETE - READY FOR ANALYSIS & VISUALIZATION
   ======================================================= */


