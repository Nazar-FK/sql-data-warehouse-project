/*
=====================================================================
Quality Checks
====================================================================
Script Purpose:
      This script performs various quality checks for data consistency, 
      accuracy and standardization across the 'silver' schemas. It 
      includes checks for: 
      - Null or duplicate primary keys
      - Unwanted spaces in string fields
      - Data standardization and consistency
      - Invalid date ranges and orders
      - Data consistency between related fields

Usage Notes:
     - Run these checks after data loading silver layer
     - Investigate and resolve any discrepancies found during the checks
=========================================================================
*/


**===================================================================
** Checking 'silver.crm_cust_info'
**===================================================================
-- Data Standardization & Consistency
Select DISTINCT cst_gender
From silver.crm_cust_info

Select * from silver.crm_cust_info

**====================================================================
** Checking 'silver.crm_prd_info'
**====================================================================
--Quality Checks
Select
prd_id,
COUNT(*)
From silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted Spaces
Select prd_nm
From silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLS or Negative Numbers
Select prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
Select DISTINCT prd_line
From silver.crm_prd_info

-- Check for Invalid Date Orders
Select *
From silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt
  
**=====================================================================================
** Checking 'silver.crm_sales_details'
** =====================================================================================
-- Check for invalid date orders
Select *
From silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
  
**=======================================================================================
** Checking 'silver.erp_cust_az12'
**=======================================================================================
  Select Distinct
  gen
  From silver.erp_cust_az1
  
**======================================================================================
** Checking 'silver.erp_loc_a101'
**=======================================================================================
--Data Standardization & Consistency

Select Distinct cntry
From silver.erp_loc_a101
ORDER BY cntry
  
**=======================================================================================
** Checking 'silver.erp_px_cat_g1v2'
**=======================================================================================
-- Check for unwanted spaces
Select
*
From silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
Select Distinct
cat
From silver.erp_px_cat_g1v2

Select Distinct
subcat
From silver.erp_px_cat_g1v2


Select Distinct
maintenance
From silver.erp_px_cat_g1v2




