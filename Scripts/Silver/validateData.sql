-- Check in Bronze layer
-- Check for NULLS or Duplicates in Primary key
Select prd_id, count(*) from Bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;
-- Check for unwanted spaces
select cst_gndr from Bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr);
-- Data standardization & consistency
select distinct cst_gndr from Bronze.crm_cust_info;
select distinct cst_material_status from Bronze.crm_cust_info;