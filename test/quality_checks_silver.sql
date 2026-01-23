-- check for quality issues and then apply data transformations
/*
Applying Data Transformations in CRM Tables
*/

/*
Table: crm_cust_info 
*/
-- check for NULLs or Duplicates in primary key
-- expectation = No Result
SELECT 
cst_id, COUNT(*) as Count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
  
-- Data transformation and Data cleansing
SELECT *
FROM (SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info)t WHERE flag_last = 1

-- check for unwanted spaces
-- Expectations: No results
SELECT cst_firstname 
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- clean-up string columns
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM ( 
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1

-- low cardinality in marital status and gender
-- Data standardization and Consistency
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info

SELECT DISTINCT cst_marital_status 
FROM bronze.crm_cust_info
  
-- make sure the date is actual date and not just string
-- mapping to meaningful names
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM ( 
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1

/*
Table: crm_prod_details 
*/
  
-- checks of duplicates and NULLs in primary key
-- Expectation: No result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prod_info
GROUP BY prd_id
HAVING COUNT (*) > 1 OR prd_id IS NULL

-- split prd_key into two columns
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0),
prd_line,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info
/*WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)*/
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN 
(SELECT sls_prd_key FROM bronze.crm_sales_details)

-- check for unwanted spaces
-- Expectations: No result
SELECT prd_nm
FROM bronze.crm_prod_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULL or Negative costs
-- Expectations: No results
SELECT prd_cost
FROM bronze.crm_prod_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization and consistency (normalization)
SELECT DISTINCT prd_line
FROM bronze.crm_prod_info

-- Check invalid date orders
SELECT * 
FROM bronze.crm_prod_info
WHERE prd_end_dt < prd_start_dt

-- in complex cases like data transformations, easy to export some sample rows to excel and try there 
-- discuss excel data with SME and get confirmation
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END prd_line,
prd_start_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prod_info

/*
Table: crm_sales_details
*/

-- check dates - change numbers to date
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(sls_order_dt AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(sls_ship_dt AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(sls_due_dt AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- check for invalid date orders
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check data consistency between sales, quantity and proce
-- Check for negative, null or zero amounts
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=  sls_quantity * ABS(sls_price) 
	THEN sls_quantity * ABS(sls_price) 
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
	THEN sls_sales / NULLIF( sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details 
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

/*
Applying Data Transformations in ERP Tables
*/
/*
Table: erp_cust_az12
*/

-- there's an extra NAS in front of cid, we need to get rid of it to join it with crm cust info table
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

-- identify out of range birthdates
SELECT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE()

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate < '1926-01-01' OR bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE()

-- check for consistency in gen
SELECT DISTINCT 
gen
FROM bronze.erp_cust_az12

/*
Table: erp_loc_a101
*/

-- check if cid matches crm table cid
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101

-- check location for consistency 

SELECT DISTINCT 
CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN cntry IS NULL THEN 'n/a'
	WHEN TRIM(cntry) = '' THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

/*
Table: erp_px_cat_g1v2
*/

-- check id for consistency
SELECT DISTINCT id
FROM bronze.erp_px_cat_g1v2

-- check for unwanted spaces

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE TRIM(cat) != cat

-- check for consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

