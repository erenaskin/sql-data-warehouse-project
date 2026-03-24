/*
===============================================================================
Gold Layer - Data Model Integrity & Quality Validation
===============================================================================
Script Purpose:
    This validation suite enforces strict data quality rules on the Gold Layer 
    (presentation tier) to ensure absolute reliability for Business Intelligence 
    (BI) and downstream analytics. 

    Core Validations:
    - Surrogate Key Uniqueness: Guarantees 1:1 mapping in dimension tables to 
      prevent data inflation (row explosion) in reporting tools.
    - Referential Integrity (Orphan Detection): Validates that all transactional 
      facts map safely to valid, existing dimensions.
    - Star Schema Health: Confirms the overall structural soundness of the 
      analytical data model.

Usage Notes:
    - Execute this validation suite immediately following the ETL/ELT load.
    - Any returned records indicate structural anomalies (e.g., duplicates or 
      orphaned facts) that require immediate remediation.
===============================================================================
*/

-- ====================================================================
-- 1. Dimension Validation: 'gold.dim_customers'
-- ====================================================================
-- Objective: Ensure 'customer_key' is strictly unique. 
-- Duplicate surrogate keys cause inaccurate aggregations in BI dashboards.
-- Expected Outcome: 0 Rows. (Returned rows indicate duplicate keys).
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- 2. Dimension Validation: 'gold.dim_products'
-- ====================================================================
-- Objective: Verify the absolute uniqueness of the 'product_key'.
-- Multiple records sharing the same surrogate key violate dimension integrity.
-- Expected Outcome: 0 Rows. (Returned rows indicate duplicate keys).
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- 3. Referential Integrity Validation: 'gold.fact_sales'
-- ====================================================================
-- Objective: Detect 'orphaned' fact records. 
-- Every sales transaction must have a corresponding, valid dimension key. 
-- This query identifies sales linked to non-existent customers or products.
-- Expected Outcome: 0 Rows. (Returned rows indicate broken relationships).
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;