/*
======================================================================================
DDL Script: Create Gold Views
======================================================================================
Script Purpose:
      This script creates views for the gold layer in the data warehouse.
      The gold layer represents the final dimension and fact tables (Star Schema)

      Each view performstransformations and combines data from the silver layer
      to produce a clean, enriched, and business-ready datasets.

Usage:
     These vieews can be queried directly fir analytics and reporting.
=====================================================================================
*/

Select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gender,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

Select DISTINCT
ci.cst_gender,
ca.gen,
CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
     ELSE COALESCE(ca.gen, 'n/a')
END AS new_gen
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1,2

-- Instead of two gender columns we will get one common column

Select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
     ELSE COALESCE(ca.gen, 'n/a')
END AS new_gen,
ci.cst_create_date,
ca.bdate,
la.cntry
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- Rename columns to friendly, maningful names

Select 
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
     ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ci.cst_create_date AS create_date,
ca.bdate AS birthdate,
la.cntry AS country
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- Sort the columns into logical groups to improve readability

Select 
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
     ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- Surrogate key (Primary key)  (It is used to connect data model)

Select 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
     ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- Create object/ Build view

CREATE VIEW gold.dim_customers AS
Select 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
     ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
From silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- Quality check of the gold table

Select * From gold.dim_customers

Select Distinct gender From gold.dim_customers

-- Build Gold Layer, Create Dimension Products

Select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
From silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data

--Check DUPLICATES

Select prd_key, COUNT(*) From (
Select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
From silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data
)t GROUP BY prd_key
HAVING COUNT(*) > 1

--Sort the columns into logical groups to improve readability

Select
pn.prd_id,
pn.prd_key,
pn.prd_nm,
pn.cat_id,
pc.cat,
pc.subcat,
pc.maintenance,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
From silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data

--Rename columns to friendly, meaningful names

Select
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
From silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data

--Create Surrogate key (Primary key)

Select
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
From silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data

--Build view

CREATE VIEW gold.dim_products AS
Select
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
From silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data

Select * From gold.dim_products


--Build Gold Layer, Create Fact Sales

Select 
sd.sls_ord_num,
sd.sls_prd_key,
sd.sls_cust_id,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
From silver.crm_sales_details sd

--Building Fact: Use the dimension's surrogate keys instead of IDs to easily connect facts with dimensions

Select 
sd.sls_ord_num,
pr.product_key,
cu.customer_key,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
From silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIn gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

--Rename columns to friendly, meaningful names

Select 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
From silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIn gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

--Build view

CREATE VIEW gold.fact_sales AS
Select 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
From silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIn gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

--Quality check of the gold table

Select * From gold.fact_sales

-- Foreign Key Integrity (Dimensions)

Select * 
From gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL

Select * 
From gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
