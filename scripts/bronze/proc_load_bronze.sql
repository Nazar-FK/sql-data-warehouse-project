/*
===========================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===========================================================================
script Purpose:
     This stored procedure loads data into the 'bronze' schema from external 
     csv files. It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'Bulk INSERT' coomand to load data from csv files to bronze tables.

Parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage Examples:
     EXEC bronze.load_bronze;
================================================================================
*/

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
