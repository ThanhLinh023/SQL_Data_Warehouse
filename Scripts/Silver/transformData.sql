-- Stored procedure to transfrom data from Bronze layer and insert into Silver layer

Create or alter procedure Silver.load_silver as
begin
	declare @start_batch datetime, @end_batch datetime;
	begin try
		set @start_batch = GETDATE();
		-- Table crm_cust_info
		truncate table Silver.crm_cust_info;
		print '>> Inserting into table Silver.crm_cust_info';
		insert into Silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date)
		Select cst_id, cst_key, 
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		case when UPPER(TRIM(cst_gndr)) = 'F' then 'Female' 
			 when UPPER(TRIM(cst_gndr)) = 'M' then 'Male' 
			 else 'N/A'
		end cst_gndr,
		case when UPPER(TRIM(cst_material_status)) = 'S' then 'Single' 
			 when UPPER(TRIM(cst_material_status)) = 'M' then 'Married' 
			 else 'N/A'
		end cst_material_status,
		cst_create_date
		from (
			Select *, ROW_NUMBER() over 
			(Partition by cst_id order by cst_create_date desc) as flag_last
			from Bronze.crm_cust_info
			where cst_id is not null
		)t
		where flag_last = 1;

		-- Table crm_prd_info
		truncate table Silver.crm_prd_info;
		print '>> Inserting into table Silver.crm_prd_info';
		insert into Silver.crm_prd_info (
				   prd_id,
				   cat_id,
				   prd_key,
				   prd_nm,
				   prd_cost,
				   prd_line,
				   prd_start_dt,
				   prd_end_dt)
		select prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') as cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) as prd_cost,
			case UPPER(TRIM(prd_line)) 
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
			else 'N/A'
			end as prd_line,
			prd_start_dt,
			LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) as prd_end_dt
		from Bronze.crm_prd_info;

		-- Table crm_sales_details
		truncate table Silver.crm_sales_details;
		print '>> Inserting into table Silver.crm_sales_details';
		insert into Silver.crm_sales_details (
				   sls_ord_num,
				   sls_prd_key,
				   sls_cust_id,
				   sls_order_dt,
				   sls_ship_dt,
				   sls_due_dt,
				   sls_sales,
				   sls_quantity,
				   sls_price)
		select sls_ord_num,
			  sls_prd_key,
			  sls_cust_id,
			  case when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
					else CAST(CAST(sls_order_dt as VARCHAR) as DATE)
			  end as sls_order_dt,
			  case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
					else CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
			  end as sls_ship_dt,
			  case when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
					else CAST(CAST(sls_due_dt as VARCHAR) as DATE)
			  end as sls_due_dt,
			  case when sls_sales is NULL or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs(sls_price)
					else sls_sales
			  end as sls_sales,
			  sls_quantity,
			  case when sls_price is NULL or sls_price <= 0
					then sls_sales / NULLIF(sls_quantity, 0)
					else sls_price
			  end as sls_price
		from Bronze.crm_sales_details

		-- Table erp_cust_az12
		truncate table Silver.erp_cust_az12;
		print '>> Inserting into table Silver.erp_cust_az12';
		insert into Silver.erp_cust_az12 (cid, bdate, gen)
		select 
			case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
				else cid
			end as cid, 
			case when bdate > GETDATE() then NULL
				else bdate
			end as bdate,
			case when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
				when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
				else 'N/A'
			end as gen 
		from Bronze.erp_cust_az12

		-- Table erp_loc_a101
		truncate table Silver.erp_loc_a101;
		print '>> Inserting into table Silver.erp_loc_a101';
		insert into Silver.erp_loc_a101 (cid, cntry)
		select 
			REPLACE(cid, '-', '') as cid,
			case when cntry = 'DE' then 'Germany'
				when TRIM(cntry) in ('USA', 'US') THEN 'United States'
				when TRIM(cntry) = '' or cntry is null then 'N/A'
				else TRIM(cntry)
			end as cntry
		from Bronze.erp_loc_a101;

		-- Table erp_px_cat_g1v2
		truncate table Silver.erp_px_cat_g1v2;
		print '>> Inserting into table Silver.erp_px_cat_g1v2';
		insert into Silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		select id, cat, subcat, maintenance
		from Bronze.erp_px_cat_g1v2;
		set @end_batch = GETDATE();
		print '=========================';
		print 'Loading Silver Layer is completed';
		print '		==> Total duration: ' + cast(DATEDIFF(second, @start_batch, @end_batch) as nvarchar) + ' seconds';
		print '=========================';
	end try
	begin catch
		print '=====================';
		print 'Error occured during loading Silver layer';
		print 'Error message: ' + ERROR_MESSAGE();
		print 'Error Number: ' + cast(ERROR_NUMBER() as nvarchar);
		print '=====================';
	end catch
end;