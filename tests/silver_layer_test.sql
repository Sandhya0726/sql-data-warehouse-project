
-- checking for duplicate [rimary key prd_id in a table
select prd_id, count(*) from bronze.crm_prd_info
group by prd_id having count(*)>1;

select * from bronze.crm_prd_info;

--checking for unwanted spaces
select prd_key from silver.crm_prd_info
where prd_key!=trim(prd_key);

select prd_nm from bronze.crm_prd_info
where prd_nm!=trim(prd_nm);

--checking cost value if it is negative or null

select prd_cost from silver.crm_prd_info
where prd_cost<0 or prd_cost is null;

select distinct prd_line from silver.crm_prd_info;

-- check for invalid date orders in bronze tables

select * from silver.crm_prd_info
where prd_end_dt<prd_start_dt;

--id orderdate is bigger than ship date

select * from silver.crm_sales_details where sls_order_dt>sls_ship_dt or sls_order_dt >sls_due_dt;

--checking data consistency between sales, quantity and price

select * from silver.crm_sales_details where sls_sales is null or sls_sales <=0 or sls_price is null or sls_price <=0 or sls_quantity is null or sls_quantity<=0
or sls_sales!=sls_quantity*sls_price;

select * from silver.crm_sales_details;

-- quality check

select distinct gen from silver.erp_cust_az12;

select * from silver.crm_prd_info;

select * from silver.erp_px_cat_g1v2;
