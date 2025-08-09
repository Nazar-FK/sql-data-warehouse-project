/*
====================================================
DDL Script: Create Bronze Tables
====================================================
Script Purpose:
    This script creates tables in the 'bronze' schema,
    dropping existing tables
    if theyalready exist.
    Run this script to re-define the DDL structure of
    'bronze' Tables
=====================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
Create Table bronze.crm_cust_info (
cst_id INT, 
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50), 
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gender NVARCHAR(50),
cst_creaet_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
Create Table bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
Create Table bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;
Create Table bronze.erp_cust_az12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;
Create Table bronze.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2;
Create Table bronze.erp_px_cat_g1v2 (
ID NVARCHAR(50),
CAT NVARCHAR(50),
SubCat NVARCHAR(50),
maintenance NVARCHAR(50)
);

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

BULK INSERT bronze.crm_cust_info
From 'C:\Users\ASUS\Desktop\Python Baraa\SQL\SQL Data Warehouse Projects\SQL data warehouse project datasets\cust_info.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);

Select * From bronze.crm_cust_info

BULK INSERT bronze.crm_prd_info
From 'C:\Users\ASUS\Desktop\Python Baraa\SQL\SQL Data Warehouse Projects\SQL data warehouse project datasets\prd_info.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);

Select * From bronze.crm_prd_info

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\ASUS\Desktop\Python Baraa\SQL\SQL Data Warehouse Projects\SQL data warehouse project datasets\sales_details.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);

Select * From bronze.crm_sales_details

BULK INSERT bronze.erp_cust_az12
From 'C:\Users\ASUS\Desktop\Python Baraa\SQL\SQL Data Warehouse Projects\SQL data warehouse project datasets\source_erp\CUST_AZ12.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);

Select * From bronze.erp_cust_az12

BULK INSERT bronze.erp_loc_a101
From 'C:\Users\ASUS\Desktop\Python Baraa\SQL\SQL Data Warehouse Projects\SQL data warehouse project datasets\source_erp\LOC_A101.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);

Select * From bronze.erp_cust_az12

BULK INSERT bronze.erp_px_cat_g1v2
From 'C:\Users\ASUS\Desktop\Python Baraa\SQL\SQL Data Warehouse Projects\SQL data warehouse project datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);

Select * From bronze.erp_px_cat_g1v2
END
