/*
===============================================================================
Stored Procedure: Extract, Transform, Load (ETL) - Source to Bronze
===============================================================================
Description: 
    This stored procedure performs the initial data extraction phase of the ETL 
    pipeline. It loads raw data from external CSV files directly into the 
    Bronze layer tables using the BULK INSERT command.
    
    Features:
    - Clears existing data (TRUNCATE) before loading to prevent duplication.
    - Tracks execution time for individual tables and the overall batch.
    - Includes robust TRY...CATCH error handling.
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Disables the 'X rows affected' messages to keep logs clean
    SET NOCOUNT ON; 

    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '=======================================================';
        PRINT 'STARTING ETL PIPELINE: SOURCE -> BRONZE';
        PRINT '=======================================================';

        --------------------------------------------------------------
        -- SECTION 1: CRM DATA LOAD
        --------------------------------------------------------------
        PRINT '-------------------------------------------------------';
        PRINT 'Loading CRM Tables...';
        PRINT '-------------------------------------------------------';

        -- 1.1 Load CRM Customer Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Bulk Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Success! Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------------------------------';
      
        -- 1.2 Load CRM Product Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Bulk Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Success! Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------------------------------';

        -- 1.3 Load CRM Sales Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Bulk Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Success! Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------------------------------';


        --------------------------------------------------------------
        -- SECTION 2: ERP DATA LOAD
        --------------------------------------------------------------
        PRINT '-------------------------------------------------------';
        PRINT 'Loading ERP Tables...';
        PRINT '-------------------------------------------------------';

        -- 2.1 Load ERP Customer Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Bulk Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Success! Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------------------------------';

        -- 2.2 Load ERP Location Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Bulk Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/datasets/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Success! Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------------------------------';

        -- 2.3 Load ERP Product Categories
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Bulk Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Success! Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-------------------------------------------------------';

        -- End of Batch Process
        SET @batch_end_time = GETDATE();
        PRINT '========================================================'
        PRINT 'ETL PROCESS COMPLETED SUCCESSFULLY!'
        PRINT 'TOTAL BRONZE LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
        PRINT '========================================================'

    END TRY
    BEGIN CATCH
    PRINT '========================================================'
    PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER!'
    PRINT 'Error Message' + ERROR_MESSAGE();
    PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '========================================================'
    END CATCH
END
GO

-- Execute the procedure
EXEC bronze.load_bronze;