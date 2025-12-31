/*
=============================================================
==== Stored Procedure: Load Silver Layer (Bronze->Silver) ===
=============================================================
Script purpose:
        - This stored procedure performes the ETL (Extract, Transform and Load) process to
        populate the 'silver' schema tables from the 'bronze' schema.
    Action  Performed:
        - Truncates Silver tables
        - Inserts transformed and cleaned data from Bronze into Silver tables

Parameters: 
    - None
    - This stored procedure doesn't accept any parameters or return any values.

Usage Example: 
    CALL silver.load_silver();

=============================================================
*/

create or replace procedure silver.load_silver() 
LANGUAGE plpgsql
as $$
DECLARE
    error_msg TEXT;
    error_code TEXT;

	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration_time INTERVAL;
	
	all_start_time TIMESTAMP;	
	all_end_time TIMESTAMP;	
	all_duration_time INTERVAL;	
begin
	all_start_time:= CLOCK_TIMESTAMP();
	raise notice '=========================================';
	raise notice 'Loading Silver Layer';
	raise notice '=========================================';
	
	raise notice '-----------------------------------------';
	raise notice ' Inserting Data into CRM Tables';
	raise notice '-----------------------------------------';
	
	start_time:= CLOCK_TIMESTAMP();
	raise notice '>>> Truncating Table: silver.crm_cust_info';
	truncate table silver.crm_cust_info;
	
	
	raise notice '>>> Inserting Data Info Into: silver.crm_cust_info';
	insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	select cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname, 
	TRIM(cst_lastname) as cst_lastname, -- this remove the unwanted spaces
	case when upper(TRIM(cst_marital_status )) = 'S' then 'Single'
		 when upper(TRIM(cst_marital_status )) = 'M' then 'Married'
		 else 'n/a' -- this change the values for default value 
	end cst_marital_status,
	case when upper(TRIM(cst_gndr)) = 'F' then 'Female'
		 when upper(TRIM(cst_gndr)) = 'M' then 'Male'
		 else 'n/a'
	end cst_gndr,
	cst_create_date
	from (
		select *, row_number() over (partition by cst_id order by cst_create_date desc ) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null 
		) -- all this query eliminate the duplicates
	where flag_last = 1;
	end_time:= CLOCK_TIMESTAMP();
	duration_time := end_time - start_time;
	raise notice '>> Duration time: %', duration_time;
	raise notice '                                 ';
	
	start_time:= CLOCK_TIMESTAMP();
	raise notice '>>> Truncating Table: silver.crm_prd_info';
	truncate table silver.crm_prd_info;

	raise notice '>>> Inserting Data Info Into: silver.crm_prd_info';
	insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost, 
		prd_line,
		prd_start_dt,
		prd_end_dt)
	select 
		prd_id,
		replace(substring(prd_key, 1,5), '-', '_') as cat_id, -- Extract category ID
		substring(prd_key, 7, LENGTH(prd_key)) as prd_key, -- Extract product key
		prd_nm,
		coalesce(prd_cost, 0) as prd_cost,
		case upper(TRIM(prd_line)) 
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		end as prd_line, -- Map product line codes to descriptive values
		prd_start_dt,
		lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt -- Calculate end day as one day before the next start date
	from bronze.crm_prd_info;
	end_time:= CLOCK_TIMESTAMP();
	duration_time := end_time - start_time;
	raise notice '>> Duration time: %', duration_time;
	raise notice '                                 ';
		
	start_time:= CLOCK_TIMESTAMP();
	raise notice '>>> Truncating Table: silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	
	raise notice '>>> Inserting Data Info Into: silver.crm_sales_details';
	INSERT into  silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sale,
		sls_quantity,
		sls_price
		)
	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or length(sls_order_dt::TEXT) !=8 then null
			 else cast(cast(sls_order_dt as varchar) as DATE)
		end sls_order_dt, 
		case when sls_ship_dt = 0 or length(sls_ship_dt::TEXT) !=8 then null
			 else cast(cast(sls_ship_dt as varchar) as DATE)
		end sls_ship_dt, 
		case when sls_due_dt = 0 or length(sls_due_dt::TEXT) !=8 then null
			 else cast(cast(sls_due_dt as varchar) as DATE)
		end sls_due_dt, 
		case when sls_sale is null or sls_sale <= 0 or sls_sale != sls_price * ABS(sls_quantity)
		     then sls_quantity * ABS(sls_price)
		     else sls_sale
		end sls_sale, -- Recalculate sales if original value is missing or incorrect
		sls_quantity,
		case when sls_price is null or sls_price <= 0
		     then sls_sale / NULLIF(sls_quantity, 0)
		     else sls_price
		end sls_price -- Derive price if original value is invalid
	from bronze.crm_sales_details;
	end_time:= CLOCK_TIMESTAMP();
	duration_time := end_time - start_time;
	raise notice '>> Duration time: %', duration_time;
	raise notice '                                 ';
	
	
	
	-- LOAD TO ERP SILVER LAYER --
	raise notice '-----------------------------------------';
	raise notice ' Inserting Data into ERP Tables';
	raise notice '-----------------------------------------';

	start_time:= CLOCK_TIMESTAMP();
	raise notice '>>> Truncating Table: silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;
	
	raise notice '>>> Inserting Data Info Into: silver.erp_cust_az12';
	insert into silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	select 	
		case when cid like 'NAS%' then substring(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if it present
			 else cid
		end as cid, 
		case when bdate > now() then null 
		     else bdate
		end as bdate, -- Set future birthdates to NULL
		case when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
			 when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
			 else 'n/a'
		end as gen -- Normalize gender values and handle unknown cases 
	from bronze.erp_cust_az12;
	end_time:= CLOCK_TIMESTAMP();
	duration_time := end_time - start_time;
	raise notice '>> Duration time: %', duration_time;
	raise notice '                                 ';
	
	start_time:= CLOCK_TIMESTAMP();
	raise notice '>>> Truncating Table: silver.erp_loc_a101';
	truncate table silver.erp_loc_a101;
	
	raise notice '>>> Inserting Data Info Into: silver.erp_loc_a101';
	insert into silver.erp_loc_a101 (
		cid,
		cntry
	)
	select 
		REPLACE(cid, '-', '') as cid,
		case when TRIM(cntry) =  'DE' then 'Germany'
			 when TRIM(cntry) in ('US', 'USA') then 'United States'
			 when TRIM(cntry) =  '' or cntry is null then 'n/a'
			 else TRIM(cntry)
		end cntry -- Normalize and handle missing or blank country codes
	from bronze.erp_loc_a101;
	end_time:= CLOCK_TIMESTAMP();
	duration_time := end_time - start_time;
	raise notice '>> Duration time: %', duration_time;
	raise notice '                                 ';
	
	
	start_time:= CLOCK_TIMESTAMP();
	raise notice '>>> Truncating Table: silver.erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2;
	
	raise notice '>>> Inserting Data Info Into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	select 
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2 epcgv;	-- This table is clean as-is from the source
	end_time:= CLOCK_TIMESTAMP();
	duration_time := end_time - start_time;
	raise notice '>> Duration time: %', duration_time;
	raise notice '                                 ';
	
	all_end_time:= CLOCK_TIMESTAMP();
	all_duration_time:= all_end_time - all_start_time;
	
	raise notice '=========================================';
	raise notice 'Loading Silver Layer is Completed';
	raise notice '   Total duration: %', all_duration_time;
	raise notice '=========================================';
exception
	when others then
		GET STACKED DIAGNOSTICS
			error_code = RETURNED_SQLSTATE,
			error_msg = MESSAGE_TEXT;
		raise notice '===========================================';
		raise notice 'ERROR OCCURED DURING INSERTING SILVER LAYER';
		raise notice 'Error mesage: %', error_msg;
		raise notice 'Error code: %', error_code;
		raise notice '===========================================';
end;
$$;