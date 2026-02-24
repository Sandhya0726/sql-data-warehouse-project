--create view for dimension table customer

if object_id('gold.dim_customers') is not null
	drop view dim_customers;
	go 
create view gold.dim_customers as 
select 
ROW_NUMBER() over(order by cst_id) as customer_key,
cst_id as customer_id,
cst_key as customer_number,
cst_firstname as first_name,
cst_lastname as last_name,
c.CNTRY as country,
cst_marital_status as marital_status,
case when cst_gndr !='n/a' then cst_gndr
when cst_gndr='n/a' and b.gen!='n/a' then b.gen 
else coalesce(cst_gndr,'n/a') 
end as gender,
b.bdate as birthdate,
cst_create_date as create_date
from silver.crm_cust_info a left join silver.erp_cust_az12 b
on a.cst_key=b.cid 
left join silver.erp_loc_a101 c 
on a.cst_key=c.cid;

go

--create table for dimension table products

if object_id('gold.dim_products') is not null
	drop view dim_products;
	go 

create view gold.dim_products as 
select
ROW_NUMBER() over (order by prd_start_dt,prd_key) as product_key,
prd_id as product_id,
prd_key as product_number,
prd_nm as product_name,
cat_id as category_id,
b.CAT as category,
b.SUBCAT as subcategory,
b.MAINTENANCE as maintenance,
prd_cost as cost,
prd_line as product_line,
prd_start_dt as start_date
from silver.crm_prd_info a left join silver.erp_px_cat_g1v2 b 
on a.cat_id=b.id where prd_end_dt is null; -- filter out all historical data

go

--creating view for fact table sales 

if object_id('gold.fact_sales') is not null
	drop view fact_sales;
	go 

create view gold.fact_sales as
select 
sls_ord_num order_number,
b.product_key,
c.customer_key,
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_sales as total_sales,
sls_quantity as quantity,
sls_price as price_per_unit
from silver.crm_sales_details a left join gold.dim_products b 
on a.sls_prd_key=b.product_number left join gold.dim_customers c
on a.sls_cust_id=c.customer_id;



