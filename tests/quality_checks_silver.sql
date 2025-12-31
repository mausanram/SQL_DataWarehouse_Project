/*
===============================================================================
=============================   Quality Checks   ==============================
===============================================================================
Script purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standarization across the 'silver' schemas. It includes checks for:
    - Null or duplicate  primary keys
    - Unwanted spaces in strig fields
    - Data standarization and consistency.
    - Invalid data ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run this ches after data loading Silver layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================   
*/

-- Check for NULLs or Duplicates in Primary Key
SELECT 
    cst_id, 
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*)>1 or cst_id is null

-- Check for unwanted spaces
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standarization and Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-- All the table
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


-- ===================================

-- Check for NULLs or Duplicates in Primary Key
SELECT 
    prd_id, 
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 or prd_id is null

-- Check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or negative numbers 
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL 

-- Data Standarization and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- All the table
SELECT *
FROM silver.crm_prd_info


-- ==========================================

-- Check for Invalid Dates
SELECT 
    NULLIF(sls_due_dt, 0) as sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
   OR LENGTH(sls_due_dt) != 8
   OR sls_due_dt > 20251212
   OR sls_due_dt < 19000101


-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check data consistency : between sales , quantity and Price
-- => Sales =  Quantity * Price
-- => Values must not be NULL, zero or negative.
SELECT DISTINCT
    sls_sale,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sale != sls_quantity * sls_price
   OR sls_sale IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sale <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sale, sls_quantity, sls_price


-- Entire table
SELECT DISTINCT * FROM silver.crm_sales_details

-- ===================================================================

-- Identify Out-of-Range dates
SELECT DISTINCT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > NOW()

-- Data Standarization and Consistency
SELECT DISTINCT
    gen
FROM silver.erp_cust_az12


SELECT DISTINCT * FROM silver.erp_cust_az12

-- ====================================================================

-- Data Standarization and Consistency
SELECT DISTINCT 
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

-- ====================================================================

-- Checj for unwanted spaces 
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) 

-- Data Standarization and Consistency
SELECT DISTINCT
    maintenance
FROM silver.erp_px_cat_g1v2 
ORDER BY maintenance

SELECT  * FROM silver.erp_px_cat_g1v2 
