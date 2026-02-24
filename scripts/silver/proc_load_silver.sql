--- loading into silver schema---

create or alter procedure silver.load_silver as
begin 
declare @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time=GETDATE()
		truncate table silver.crm_cust_info;
		insert into silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
		select cst_id, cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname, 
		case when upper(trim(cst_marital_status))='M' then 'Married'
		when upper(trim(cst_marital_status))='S' then 'Single'
		else 'n/a'
		end as cst_marital_status, 
		case when upper(trim(cst_gndr))='M' then 'Male'
		when upper(trim(cst_gndr))='F' then 'Female'
		else 'n/a'
		end as cst_gndr
		,cst_create_date from (
		select *, ROW_NUMBER() over(partition by cst_id 
		order by cst_create_date desc) as flag_lastest from bronze.crm_cust_info)t
		where flag_lastest=1 and cst_id is not null ;

		truncate table silver.crm_prd_info;
		Insert into silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, 
		replace(substring(prd_key,7,len(prd_key)),'-','_') as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost
		,case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a' 
		end as prd_line
		,cast(prd_start_dt as date),
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		from bronze.crm_prd_info;

		truncate table silver.crm_sales_details;
		insert into silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
		select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt, (sls_quantity *sls_price) as sls_sales ,sls_quantity,sls_price from(
		select sls_ord_num,
		REPLACE(sls_prd_key,'-','_') as sls_prd_key,
		sls_cust_id, case 
		when sls_order_dt<=0 or len(sls_order_dt)!=8 then null 
		else cast(cast(sls_order_dt as nvarchar)as date)
		 end as sls_order_dt,
		  case 
		when sls_ship_dt<=0 or len(sls_ship_dt)!=8 then null 
		else cast(cast(sls_ship_dt as nvarchar)as date)
		 end as sls_ship_dt, 
		 case
		 when sls_due_dt<=0 or len(sls_due_dt)!=8 then null 
		else cast(cast(sls_due_dt as nvarchar)as date)
		 end as sls_due_dt,
		 case when sls_quantity is null or sls_quantity <=0 then sls_sales/nullif(sls_price,0) else sls_quantity end as sls_quantity,
		case when sls_price <=0 or sls_price is null then sls_sales/nullif(sls_quantity,0) else sls_price end as sls_price
		from bronze.crm_sales_details)t;

		truncate table silver.erp_cust_az12;
		insert into silver.erp_cust_az12(cid,bdate,gen)
		select
		case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
		else cid 
		end cid,
		case when bdate>getdate() then null else bdate end as bdate,
		case when upper(trim(gen))='F' or trim(gen)='Female' then 'Female' 
		when upper(trim(gen))='M' or trim(gen)='Male' then 'Male' else 'n/a' end as gen
		from bronze.erp_cust_az12;

		truncate table silver.erp_loc_a101;
		insert into silver.erp_loc_a101(cid,cntry)
		select replace(cid,'-','') as cid, 
		case when upper(trim(cntry)) in ('US','USA') then 'United States'
		when upper(trim(cntry))='DE' then 'Germany'
		when trim(cntry) ='' or cntry is null then 'n/a' else trim(cntry)
		end as cntry 
		from bronze.erp_loc_a101 where replace(cid,'-','') in (select cst_key from silver.crm_cust_info) ;

		truncate table silver.erp_px_cat_g1v2;
		insert into silver.erp_px_cat_g1v2( id,cat,subcat,maintenance)
		select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2;
		set @batch_end_time=getdate()

		print'The total batch time is'+cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar(10))+'seconds'
	end try
	begin catch
		print'ERROR LOADING THE SILVER LAYER'
	end catch
end

exec silver.load_silver;

