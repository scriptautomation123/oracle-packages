-- =====================================================
-- Oracle 19c Online Table Conversion Examples
-- Convert non-partitioned heap tables to partitioned ONLINE (zero downtime)
-- Uses ALTER TABLE MODIFY with ONLINE keyword
-- =====================================================

-- =====================================================
-- Example 1: Convert to HASH Partitioned (Load Balancing)
-- =====================================================
-- Use case: Distribute data evenly across partitions for parallel processing
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'ORDERS',
        p_partition_type  => 'HASH',
        p_partition_column => 'ORDER_ID',
        p_partition_count  => 8,        -- 8 hash partitions
        p_parallel_degree  => 4         -- Parallel conversion
    );
END;
/

-- Generated DDL equivalent:
-- ALTER TABLE ORDERS
--   MODIFY PARTITION BY HASH (ORDER_ID) PARTITIONS 8
--   ONLINE
--   UPDATE INDEXES (IDX_ORDERS_CUSTOMER GLOBAL, IDX_ORDERS_DATE GLOBAL)
--   PARALLEL 4;

-- =====================================================
-- Example 2: Convert to RANGE Partitioned (Time-Series Data)
-- =====================================================
-- Use case: Partition historical data by date ranges
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'SALES',
        p_partition_type  => 'RANGE',
        p_partition_column => 'SALE_DATE',
        p_parallel_degree  => 4
    );
END;
/

-- Note: Creates single default partition VALUES LESS THAN (MAXVALUE)
-- Then use split_partition to create specific date ranges:
BEGIN
    table_ops_pkg.split_partition(
        p_table_name     => 'SALES',
        p_partition_name => 'P_DEFAULT',
        p_split_value    => 'TO_DATE(''2024-01-01'', ''YYYY-MM-DD'')',
        p_new_partition  => 'P_2024'
    );
END;
/

-- =====================================================
-- Example 3: Convert to LIST Partitioned (Categorical Data)
-- =====================================================
-- Use case: Partition by region, status, or other discrete values
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'CUSTOMERS',
        p_partition_type  => 'LIST',
        p_partition_column => 'REGION',
        p_parallel_degree  => 4
    );
END;
/

-- Note: Creates single default partition VALUES (DEFAULT)
-- Then split to create specific regions:
BEGIN
    table_ops_pkg.split_partition(
        p_table_name     => 'CUSTOMERS',
        p_partition_name => 'P_DEFAULT',
        p_split_value    => '''NORTH'', ''SOUTH''',
        p_new_partition  => 'P_NORTH_SOUTH'
    );
END;
/

-- =====================================================
-- Example 4: Convert to INTERVAL Partitioned (Auto-Create Partitions)
-- =====================================================
-- Use case: Automatically create monthly/yearly partitions as data arrives
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'TRANSACTIONS',
        p_partition_type  => 'INTERVAL',
        p_partition_column => 'TRANSACTION_DATE',
        p_interval_expr    => 'NUMTOYMINTERVAL(1,''MONTH'')', -- Monthly intervals
        p_parallel_degree  => 4
    );
END;
/

-- Daily interval example:
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'LOG_ENTRIES',
        p_partition_type  => 'INTERVAL',
        p_partition_column => 'LOG_DATE',
        p_interval_expr    => 'NUMTODSINTERVAL(1,''DAY'')',    -- Daily intervals
        p_parallel_degree  => 4
    );
END;
/

-- Yearly interval example:
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'ARCHIVE_DATA',
        p_partition_type  => 'INTERVAL',
        p_partition_column => 'ARCHIVE_YEAR',
        p_interval_expr    => 'NUMTOYMINTERVAL(1,''YEAR'')',   -- Yearly intervals
        p_parallel_degree  => 4
    );
END;
/

-- =====================================================
-- Example 5: Convert to REFERENCE Partitioned (Parent-Child Relationship)
-- =====================================================
-- Use case: Child table inherits partitioning from parent table
-- Prerequisites:
-- 1. Parent table must be partitioned
-- 2. Foreign key constraint must exist

-- First, partition the parent table
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'CUSTOMERS',
        p_partition_type  => 'HASH',
        p_partition_column => 'CUSTOMER_ID',
        p_partition_count  => 8,
        p_parallel_degree  => 4
    );
END;
/

-- Create FK constraint if not exists
ALTER TABLE ORDERS
ADD CONSTRAINT FK_ORDERS_CUSTOMER
FOREIGN KEY (CUSTOMER_ID)
REFERENCES CUSTOMERS(CUSTOMER_ID);

-- Convert child table to reference partitioning
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'ORDERS',
        p_partition_type  => 'REFERENCE',
        p_reference_table => 'CUSTOMERS',  -- Parent table
        p_parallel_degree  => 4
    );
END;
/

-- Generated DDL equivalent:
-- ALTER TABLE ORDERS
--   MODIFY PARTITION BY REFERENCE (FK_ORDERS_CUSTOMER)
--   ONLINE
--   UPDATE INDEXES (...)
--   PARALLEL 4;

-- =====================================================
-- Example 6: Generate DDL Without Executing (Preview)
-- =====================================================
SET SERVEROUTPUT ON
DECLARE
    v_ddl CLOB;
BEGIN
    -- Generate conversion DDL for review
    v_ddl := table_ops_pkg.generate_convert_to_partitioned_ddl(
        p_table_name      => 'EMPLOYEES',
        p_partition_type  => 'INTERVAL',
        p_partition_column => 'HIRE_DATE',
        p_interval_expr    => 'NUMTOYMINTERVAL(3,''MONTH'')',
        p_parallel_degree  => 8
    );
    
    DBMS_OUTPUT.PUT_LINE(v_ddl);
END;
/

-- =====================================================
-- Example 7: Check Partition Information After Conversion
-- =====================================================
-- Verify conversion was successful
SELECT * FROM TABLE(table_ops_pkg.get_partition_info('ORDERS'));

-- Check if table is partitioned
DECLARE
    v_is_partitioned BOOLEAN;
    v_partition_type VARCHAR2(20);
BEGIN
    v_is_partitioned := table_ops_pkg.is_partitioned('ORDERS');
    v_partition_type := table_ops_pkg.get_partition_type('ORDERS');
    
    DBMS_OUTPUT.PUT_LINE('Is Partitioned: ' || CASE WHEN v_is_partitioned THEN 'YES' ELSE 'NO' END);
    DBMS_OUTPUT.PUT_LINE('Partition Type: ' || NVL(v_partition_type, 'N/A'));
END;
/

-- =====================================================
-- Example 8: Complete Workflow - Sales Table with Auto-Cleanup
-- =====================================================
-- Convert to interval partitioned with automatic old partition cleanup

-- Step 1: Convert to interval partitioned (monthly)
BEGIN
    table_ops_pkg.convert_to_partitioned(
        p_table_name      => 'MONTHLY_SALES',
        p_partition_type  => 'INTERVAL',
        p_partition_column => 'SALE_DATE',
        p_interval_expr    => 'NUMTOYMINTERVAL(1,''MONTH'')',
        p_parallel_degree  => 4
    );
END;
/

-- Step 2: Configure optimal statistics (done automatically, but can be called explicitly)
BEGIN
    table_ops_pkg.configure_table_stats_optimal(
        p_table_name         => 'MONTHLY_SALES',
        p_enable_incremental => TRUE
    );
END;
/

-- Step 3: Schedule regular cleanup of old partitions (use DBMS_SCHEDULER)
BEGIN
    table_ops_pkg.drop_old_partitions(
        p_table_name     => 'MONTHLY_SALES',
        p_retention_days => 365  -- Keep 1 year of data
    );
END;
/

-- =====================================================
-- BENEFITS OF ONLINE CONVERSION
-- =====================================================
-- ✓ ZERO DOWNTIME: DML operations continue during conversion
-- ✓ AUTOMATIC INDEX HANDLING: Indexes converted automatically
-- ✓ PARALLEL EXECUTION: Fast conversion using parallel workers
-- ✓ INCREMENTAL STATISTICS: Auto-configured for optimal performance
-- ✓ PRODUCTION SAFE: No table locking or application interruption

-- =====================================================
-- RESTRICTIONS AND CONSIDERATIONS
-- =====================================================
-- × Cannot convert SYS-owned tables
-- × Cannot convert tables with domain indexes online
-- × Cannot convert index-organized tables (IOT) online
-- × Requires sufficient tablespace for temporary segments
-- × Monitor space in TEMP and table tablespace during conversion

-- =====================================================
-- MONITORING ONLINE CONVERSION PROGRESS
-- =====================================================
-- Check long-running operations
SELECT 
    opname,
    target,
    ROUND(sofar/totalwork*100,2) as pct_complete,
    time_remaining,
    elapsed_seconds
FROM v$session_longops
WHERE opname LIKE '%Table%'
  AND totalwork != 0
  AND sofar <> totalwork
ORDER BY start_time DESC;

-- Check space usage during conversion
SELECT 
    tablespace_name,
    ROUND(bytes/1024/1024,2) as mb_used,
    ROUND(maxbytes/1024/1024,2) as mb_max,
    ROUND(bytes/maxbytes*100,2) as pct_used
FROM dba_temp_files;
