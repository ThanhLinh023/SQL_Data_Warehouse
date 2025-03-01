-- Create dimension tables in Gold schema's view

-- Dim Customers table
create view gold.dim_customers as
select 
	ROW_NUMBER() over (order by cst_id) as customerKey,
	ci.cst_id as customerID,
	ci.cst_key as customerNumber,
	ci.cst_firstname as firstName,
	ci.cst_lastname as lastName,
	la.cntry as country,
	ci.cst_material_status as materialStatus,
	case when ci.cst_gndr != 'N/A' then ci.cst_gndr
		else COALESCE(ca.gen, 'N/A')
	end as gender,
	ca.bdate as birthDate,
	ci.cst_create_date as createDate
from Silver.crm_cust_info as ci
left join Silver.erp_cust_az12 as ca
on		ci.cst_key = ca.cid
left join Silver.erp_loc_a101 as la
on		ci.cst_key = la.cid;

-- Dim Products table
create view gold.dim_products as
select
	ROW_NUMBER() over (order by pd.prd_start_dt, pd.prd_key) as productKey,
	pd.prd_id as productID,
	pd.prd_key as productNumber,
	pd.prd_nm as productName,
	pd.cat_id as categoryID,
	pc.cat as category,
	pc.subcat as subCategory,
	pc.maintenance,
	pd.prd_cost as productCost,
	pd.prd_line as productLine,
	pd.prd_start_dt as startDate
from Silver.crm_prd_info as pd
left join Silver.erp_px_cat_g1v2 as pc
on		pd.cat_id = pc.id
where prd_end_dt is NULL

-- Sales fact table
create view gold.fact_sales as
select 
	sd.sls_ord_num as orderNumber,
	pr.productKey,
	cus.customerKey,
	sd.sls_order_dt as orderDate,
	sd.sls_ship_dt as shippingDate,
	sd.sls_due_dt as dueDate,
	sd.sls_sales as salesAmount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from Silver.crm_sales_details as sd
left join Gold.dim_products as pr
on		sd.sls_prd_key = pr.productNumber
left join Gold.dim_customers as cus
on		sd.sls_cust_id = cus.customerID