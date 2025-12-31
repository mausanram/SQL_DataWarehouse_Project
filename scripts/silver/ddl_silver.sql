/*
==========================================================================================================
================================= DDL Script: Create silver Tables =======================================
==========================================================================================================

==> This script creates the tables in the "silver" schema, dropping existing tables if they already exist.
==> Run this script to re-define the DDL structure "silver" tables.

===========================================================================================================
*/

drop table if exists silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id INT,
	cst_key TEXT,
	cst_firstname TEXT,
	cst_lastname TEXT,
	cst_marital_status TEXT,
	cst_gndr TEXT,
	cst_create_date TEXT,
	dwh_cretate_data TIMESTAMP DEFAULT NOW()
);	

drop table if exists silver.crm_prd_info;
create table silver.crm_prd_info (
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT, 
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT NOW()	
);

drop table if exists silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num TEXT,
	sls_prd_key TEXT,
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sale INT,
	sls_quantity INT,
	sls_price INT,
	dwh_cretate_data TIMESTAMP DEFAULT NOW()
);

drop table if exists silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid TEXT,
	bdate DATE,
	gen TEXT,
	dwh_cretate_data TIMESTAMP DEFAULT NOW()
);

drop table if exists silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid TEXT,
	cntry TEXT,
	dwh_cretate_data TIMESTAMP DEFAULT NOW()
);

drop table if exists silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id TEXT,
	cat TEXT,
	subcat TEXT,
	maintenance TEXT,
	dwh_cretate_data TIMESTAMP DEFAULT NOW()
);

