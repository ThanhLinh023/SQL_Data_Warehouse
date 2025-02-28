-- Create stored procedure to load data from CSV files to tables created
create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @start_batch datetime, @end_batch datetime;
	begin try
		set @start_batch = GETDATE();
		print '===========================';
		print 'Loading Bronze Layer';
		print '===========================';

		print '---------------------------';
		print 'Loading CRM Tables'
		print '---------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>> Inserting into table: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'G:\SQL_Data_Warehouse\Datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>> Inserting into table: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'G:\SQL_Data_Warehouse\Datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>> Inserting into table: crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'G:\SQL_Data_Warehouse\Datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds';
	
		print '---------------------------';
		print 'Loading ERP Tables'
		print '---------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>> Inserting into table: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'G:\SQL_Data_Warehouse\Datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>> Inserting into table: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'G:\SQL_Data_Warehouse\Datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>> Inserting into table: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'G:\SQL_Data_Warehouse\Datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------------------';

		set @end_batch = GETDATE();
		print '=========================';
		print 'Loading Bronze Layer is completed';
		print '		==> Total duration: ' + cast(DATEDIFF(second, @start_batch, @end_batch) as nvarchar) + ' seconds';
		print '=========================';
	end try
	begin catch
		print '=====================';
		print 'Error occured during loading bronze layer';
		print 'Error message: ' + ERROR_MESSAGE();
		print 'Error Number: ' + cast(ERROR_NUMBER() as nvarchar);
		print '=====================';
	end catch
end;