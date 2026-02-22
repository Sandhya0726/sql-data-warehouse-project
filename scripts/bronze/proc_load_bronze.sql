create or alter procedure bronze.load_bronze as 
	begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time=GETDATE();

		Print '====================================';
		Print 'Loading into the bronze layer';
		Print '====================================';

		set @start_time=GETDATE();

		Print '====================================';
		Print 'Loading crm table';
		Print '====================================';

		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info
		from 'C:\Users\DELL\Downloads\source_crm\cust_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);

		set @end_time =GETDATE();


		print('The loading time for cust_info table is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds');

		set @start_time=GETDATE();

		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info
		from 'C:\Users\DELL\Downloads\source_crm\prd_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);

		set @end_time=GETDATE();

		print('The loading time for prd_info table is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds');

		set @start_time=GETDATE();

		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details
		from 'C:\Users\DELL\Downloads\source_crm\sales_details.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);

		set @end_time=GETDATE();

		print('The loading time for sales_details table is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds')

		Print '====================================';
		Print 'Loading the erp tables';
		Print '====================================';

		set @start_time=GETDATE();

		truncate table bronze.erp_cust_az12;
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\DELL\Downloads\source_erp\cust_az12.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);

		set @end_time=GETDATE();

		print('The loading time for cust_az12 table is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds');


		set @start_time=GETDATE();

		truncate table bronze.erp_loc_a101;
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\DELL\Downloads\source_erp\loc_a101.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);

		set @end_time=GETDATE();

		print('The loading time for loc_a101 table is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds');


		set @start_time=GETDATE();

		truncate table bronze.erp_px_cat_g1v2;
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\DELL\Downloads\source_erp\px_cat_g1v2.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);

		set @end_time=GETDATE();

		print('The loading time for px_cat_g1v2 table is'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds');
	set @batch_end_time=GETDATE();

	print('The loading time for px_cat_g1v2 table is'+cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+'seconds');

	end try
	begin catch
	print 'Error occurred during loading bronze layer';
	print 'Error message'+error_message();
	print 'Error message'+cast(error_number() as nvarchar);

	end catch
	end;

exec bronze.load_bronze;
