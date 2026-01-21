--Usage example: EXECUTE bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '==============================================='
		PRINT 'Loading Bronze Layer'
		PRINT '==============================================='
	-- truncate and insert

		PRINT '-----------------------------------------------'
		PRINT 'Loading CRM Table'
		PRINT '-----------------------------------------------'

	SET @start_time = GETDATE()
	PRINT '>> Truncating Table: bronze.crm_cust_info'
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT '>> Inserting Data Into: bronze.crm_cust_info'
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\rmmot\OneDrive\Desktop\Eshita\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT '-----------------------------------------------'

	-- Table bronze.crm_prod_info
	SET @start_time = GETDATE()
	PRINT '>> Truncating Table: bronze.crm_prod_info'
	TRUNCATE TABLE bronze.crm_prod_info;

	PRINT '>> Inserting Data Into: bronze.crm_prod_info'
	BULK INSERT bronze.crm_prod_info
	FROM 'C:\Users\rmmot\OneDrive\Desktop\Eshita\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT '-----------------------------------------------'

	-- Table bronze.crm_sales_details
	SET @start_time = GETDATE()
	PRINT '>> Truncating Table: bronze.crm_sales_details'
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT '>> Inserting Data Into: bronze.crm_sales_details'
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\rmmot\OneDrive\Desktop\Eshita\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' 
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT '-----------------------------------------------'


	PRINT '-----------------------------------------------'
	PRINT 'Loading ERP Table'
	PRINT '-----------------------------------------------'

	-- Table bronze.erp_cust_az12
	SET @start_time = GETDATE()
	PRINT '>> Truncating Table: bronze.erp_cust_az12'
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\rmmot\OneDrive\Desktop\Eshita\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT '-----------------------------------------------'

	-- Table bronze.erp_loc_a101\
	SET @start_time = GETDATE()
	PRINT '>> Truncating Table: bronze.erp_loc_a101'
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\rmmot\OneDrive\Desktop\Eshita\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT '-----------------------------------------------'

	-- Table bronze.erp_px_cat_g1v2
	SET @start_time = GETDATE()
	PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\rmmot\OneDrive\Desktop\Eshita\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT '-----------------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT 'Error occured during loading BRONZE LAYER'
		PRINT '================================================='
	END CATCH
END
