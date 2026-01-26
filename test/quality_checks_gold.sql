/*Chcek for NULLs and duplicates to see if joins logic are good
SELECT cst_id, COUNT(*) FROM
(SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid)t GROUP BY cst_id
HAVING COUNT(*) > 1 */

/*Data Integration for Gender columns

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is master for gender
		ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid */

/*Use Friendly Names*/

/*Sort the columns into logical groups to improve readability*/

/*Dimension or Fact

Dimensions --> decsriptive information
Fact --> transactions, events

so, this is customer dimension

Primary key is required for dimension --> surrogate keys (system generated unique identifier)
*/



/* Check for duplicates and NULLs

SELECT prd_key, COUNT(*) FROM(
SELECT
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filter out all historical data
)t GROUP BY prd_key
HAVING COUNT(*) > 1*/

/*Descriptive data --> Dimension*/


-- create surrogate keys for data that comes from dimensions 
-- Data lookup
