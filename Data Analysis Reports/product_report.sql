/*
=========================================
Product Report
=========================================

Purpose: 
	- This report consolidates key customer metrics and behaviours
Highlights:
	- Gathers essential fields such as product name, category, subcategory, and cost.
	- Segments products by revenue to identify: High-performers, Mid-range, or low performers.
	- Aggregate customer-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	- Calculates valuable KPIs:
		- recency (months since last order)
		- average order revenue
		- average monthly revenue
*/

CREATE VIEW report_products AS
WITH base_query AS(
/*
-------------------------------------------------
1) Base Query: Retrieved core columns from tables
-------------------------------------------------
*/
	SELECT
	p.product_key,
	p.product_name,
	p.category,
	p.sub_category,
	s.price,
	s.sales_amount,
	s.quantity,
	s.customer_key,
	s.order_date,
	s.order_number
	FROM [gold.fact_sales]s
	LEFT JOIN [gold.dim_products]p
	ON s.product_key = p.product_key
	WHERE order_date IS NOT NULL
)
, product_aggregation AS(
/*
-------------------------------------------------
2) Customer Agregations: Summarizes key metrics
-------------------------------------------------
*/
	SELECT 
		product_key,
		product_name,
		category,
		sub_category,
		price,
		DATEDIFF(month, MIN(order_date), MAX(order_date) ) AS lifespan,
		MAX(order_date) AS last_sale_date,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT customer_key) AS total_customers,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
	FROM base_query
	GROUP BY 
	product_key,
	product_name,
	category,
	sub_category,
	price
)
SELECT 
/*
-------------------------------------------------
3) Final results
-------------------------------------------------
*/
	product_key,
	product_name,
	category,
	sub_category,
	price,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'High performer'
		WHEN total_sales >= 10000 THEN 'Mid-range'
		ELSE 'Low-performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	avg_selling_price
FROM
	product_aggregation

-- Now the data is ready for data visualization
/*
To view the report:

SELECT * FROM report_products
*/
