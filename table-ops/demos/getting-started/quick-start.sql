-- =====================================================
-- Oracle Table & Partition Management - Practical Usage Guide
-- Complete examples for real-world scenarios
-- Run sections individually to learn and test
-- =====================================================

SET SERVEROUTPUT ON SIZE 1000000
SET LINESIZE 200
SET PAGESIZE 1000

PROMPT ========================================
PROMPT Oracle Table & Partition Management
PROMPT Practical Usage Guide
PROMPT ========================================

-- =====================================================
-- SECTION 1: TABLE CREATION (table_ddl_pkg)
-- =====================================================

PROMPT
PROMPT === SECTION 1: TABLE CREATION ===
PROMPT

-- Example 1.1: Simple heap table
DECLARE
    v_ddl CLOB;
BEGIN
    v_ddl := q'[
        CREATE TABLE customers (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name VARCHAR2(100) NOT NULL,
            email VARCHAR2(255),
            created_date DATE DEFAULT SYSDATE
        ) TABLESPACE USERS
    ]';
    
    DBMS_OUTPUT.PUT_LINE('Example 1.1: Simple Heap Table DDL:');
    DBMS_OUTPUT.PUT_LINE(v_ddl);
END;
/

-- Example 1.2: Partitioned table (range by date)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales_data PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE sales_data (
    sale_id NUMBER PRIMARY KEY,
    sale_date DATE NOT NULL,
    amount NUMBER(10,2),
    customer_id NUMBER,
    region VARCHAR2(50)
) PARTITION BY RANGE (sale_date) (
    PARTITION p_2024_q1 VALUES LESS THAN (DATE '2024-04-01'),
    PARTITION p_2024_q2 VALUES LESS THAN (DATE '2024-07-01'),
    PARTITION p_2024_q3 VALUES LESS THAN (DATE '2024-10-01'),
    PARTITION p_2024_q4 VALUES LESS THAN (DATE '2025-01-01')
) TABLESPACE USERS;

PROMPT ✓ Created partitioned table: sales_data

-- Example 1.3: Interval partitioning (auto-create partitions)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales_monthly PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE sales_monthly (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER(10,2)
) PARTITION BY RANGE (sale_date)
INTERVAL(NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_start VALUES LESS THAN (DATE '2025-01-01')
) TABLESPACE USERS;

PROMPT ✓ Created interval partitioned table: sales_monthly

-- Example 1.4: Check if table exists
DECLARE
    v_exists BOOLEAN;
BEGIN
    v_exists := table_ddl_pkg.table_exists('SALES_DATA');
    DBMS_OUTPUT.PUT_LINE('Table SALES_DATA exists: ' || CASE WHEN v_exists THEN 'YES' ELSE 'NO' END);
END;
/

-- =====================================================
-- SECTION 2: PARTITION OPERATIONS (table_ops_pkg)
-- =====================================================

PROMPT
PROMPT === SECTION 2: PARTITION OPERATIONS ===
PROMPT

-- Insert test data
INSERT INTO sales_data VALUES (1, DATE '2024-01-15', 1500.00, 101, 'NORTH');
INSERT INTO sales_data VALUES (2, DATE '2024-05-10', 2500.00, 102, 'SOUTH');
INSERT INTO sales_data VALUES (3, DATE '2024-08-20', 3200.00, 103, 'EAST');
COMMIT;

-- Example 2.1: View partition information
PROMPT Example 2.1: Current Partition Information
SELECT * FROM TABLE(table_ops_pkg.get_partition_info('SALES_DATA'));

-- Example 2.2: Create new partition
BEGIN
    table_ops_pkg.create_partition(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2025_Q1',
        p_high_value => 'DATE ''2025-04-01''',
        p_tablespace => 'USERS'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created partition P_2025_Q1');
END;
/

-- Example 2.3: Split partition
BEGIN
    table_ops_pkg.split_partition(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q1',
        p_split_value => 'DATE ''2024-02-01''',
        p_new_partition => 'P_2024_JAN',
        p_tablespace => 'USERS'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Split partition P_2024_Q1');
END;
/

-- Example 2.4: Merge partitions
BEGIN
    table_ops_pkg.merge_partitions(
        p_table_name => 'SALES_DATA',
        p_partition1 => 'P_2024_JAN',
        p_partition2 => 'P_2024_Q1',
        p_new_partition => 'P_2024_Q1_MERGED'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Merged partitions');
END;
/

-- Example 2.5: Truncate partition
BEGIN
    table_ops_pkg.truncate_partition(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q2'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Truncated partition P_2024_Q2');
END;
/

-- Example 2.6: Enable interval partitioning on existing table
BEGIN
    table_ops_pkg.enable_interval_partitioning(
        p_table_name => 'SALES_DATA',
        p_interval_expr => 'NUMTOYMINTERVAL(1,''MONTH'')'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Enabled monthly interval partitioning');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('⚠ Interval partitioning: ' || SQLERRM);
END;
/

-- Example 2.7: Drop partition
BEGIN
    table_ops_pkg.drop_partition(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2025_Q1',
        p_update_indexes => TRUE
    );
    DBMS_OUTPUT.PUT_LINE('✓ Dropped partition P_2025_Q1');
END;
/

-- =====================================================
-- SECTION 3: DDL GENERATION (Planning without execution)
-- =====================================================

PROMPT
PROMPT === SECTION 3: DDL GENERATION (Plan Before Execute) ===
PROMPT

-- Example 3.1: Generate partition DDL
DECLARE
    v_ddl CLOB;
BEGIN
    v_ddl := table_ops_pkg.generate_partition_ddl(
        p_operation_type => 'CREATE',
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2025_Q2',
        p_parameters => 'VALUES LESS THAN (DATE ''2025-07-01'') TABLESPACE USERS'
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated CREATE PARTITION DDL:');
    DBMS_OUTPUT.PUT_LINE(v_ddl);
END;
/

-- Example 3.2: Generate table move DDL
DECLARE
    v_ddl CLOB;
    v_length NUMBER;
    v_chunk VARCHAR2(4000);
BEGIN
    v_ddl := table_ops_pkg.generate_move_table_ddl(
        p_table_name => 'SALES_DATA',
        p_new_tablespace => 'USERS',
        p_parallel_degree => 4
    );
    
    v_length := DBMS_LOB.GETLENGTH(v_ddl);
    v_chunk := DBMS_LOB.SUBSTR(v_ddl, 4000, 1);
    
    DBMS_OUTPUT.PUT_LINE('Generated MOVE TABLE DDL:');
    DBMS_OUTPUT.PUT_LINE(v_chunk);
END;
/

-- =====================================================
-- SECTION 4: ORACLE 19c STATISTICS (partition_stats_pkg)
-- =====================================================

PROMPT
PROMPT === SECTION 4: ORACLE 19c STATISTICS ===
PROMPT

-- Example 4.1: Configure table for optimal statistics
BEGIN
    partition_stats_pkg.configure_table_for_stats(
        p_table_name => 'SALES_DATA',
        p_incremental => TRUE,
        p_concurrent => TRUE
    );
    DBMS_OUTPUT.PUT_LINE('✓ Configured for Oracle 19c optimal statistics');
END;
/

-- Example 4.2: Collect partition statistics
BEGIN
    partition_stats_pkg.collect_partition_stats(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q3',
        p_degree => partition_stats_pkg.AUTO_DEGREE,
        p_cascade_indexes => TRUE
    );
    DBMS_OUTPUT.PUT_LINE('✓ Collected partition statistics');
END;
/

-- Example 4.3: Check if incremental stats enabled
DECLARE
    v_enabled BOOLEAN;
BEGIN
    v_enabled := partition_stats_pkg.is_incremental_stats_enabled('SALES_DATA');
    DBMS_OUTPUT.PUT_LINE('Incremental stats enabled: ' || 
        CASE WHEN v_enabled THEN 'YES' ELSE 'NO' END);
END;
/

-- Example 4.4: Get recommended strategy
DECLARE
    v_strategy partition_stats_pkg.stats_strategy_rec;
BEGIN
    v_strategy := partition_stats_pkg.get_recommended_strategy('SALES_DATA');
    DBMS_OUTPUT.PUT_LINE('Recommended Strategy: ' || v_strategy.strategy_name);
    DBMS_OUTPUT.PUT_LINE('Description: ' || v_strategy.description);
    DBMS_OUTPUT.PUT_LINE('Degree: ' || v_strategy.degree);
END;
/

-- Example 4.5: Estimate stats collection time
DECLARE
    v_minutes NUMBER;
BEGIN
    v_minutes := partition_stats_pkg.estimate_stats_collection_time(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q3'
    );
    DBMS_OUTPUT.PUT_LINE('Estimated collection time: ' || v_minutes || ' minutes');
END;
/

-- Example 4.6: Check stats freshness
DECLARE
    v_stale_partitions DBMS_UTILITY.UNCL_ARRAY;
BEGIN
    v_stale_partitions := partition_stats_pkg.check_stats_freshness(
        p_table_name => 'SALES_DATA',
        p_days_threshold => 7
    );
    
    IF v_stale_partitions.COUNT > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Stale partitions found: ' || v_stale_partitions.COUNT);
        FOR i IN 1..v_stale_partitions.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE('  - ' || v_stale_partitions(i));
        END LOOP;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✓ All statistics are fresh');
    END IF;
END;
/

-- =====================================================
-- SECTION 5: MIGRATION & COPY OPERATIONS
-- =====================================================

PROMPT
PROMPT === SECTION 5: MIGRATION & COPY OPERATIONS ===
PROMPT

-- Example 5.1: Copy table structure
BEGIN
    table_ops_pkg.copy_table_structure(
        p_source_table => 'SALES_DATA',
        p_target_table => 'SALES_DATA_BACKUP',
        p_include_indexes => TRUE
    );
    DBMS_OUTPUT.PUT_LINE('✓ Copied table structure to SALES_DATA_BACKUP');
END;
/

-- Example 5.2: Copy partition data
BEGIN
    table_ops_pkg.copy_partition_data(
        p_source_table => 'SALES_DATA',
        p_target_table => 'SALES_DATA_BACKUP',
        p_partition_name => 'P_2024_Q3',
        p_parallel_degree => 4
    );
    DBMS_OUTPUT.PUT_LINE('✓ Copied partition data');
END;
/

-- =====================================================
-- SECTION 6: MODERN LOGGING (modern_logging_pkg)
-- =====================================================

PROMPT
PROMPT === SECTION 6: MODERN LOGGING ===
PROMPT

-- Example 6.1: Log simple message
BEGIN
    modern_logging_pkg.log_message(
        p_level => 'INFO',
        p_message => 'Starting maintenance operation'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Logged info message');
END;
/

-- Example 6.2: Log operation lifecycle
DECLARE
    v_operation_id NUMBER;
    v_start_time TIMESTAMP := SYSTIMESTAMP;
    v_duration_ms NUMBER;
BEGIN
    -- Start operation
    modern_logging_pkg.log_operation_start(
        p_operation_type => 'PARTITION_MAINTENANCE',
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q4',
        p_operation_id => v_operation_id
    );
    
    -- Simulate operation
    DBMS_LOCK.SLEEP(1);
    
    -- Calculate duration
    v_duration_ms := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
    
    -- End operation
    modern_logging_pkg.log_operation_end(
        p_operation_id => v_operation_id,
        p_status => 'SUCCESS',
        p_message => 'Partition maintenance completed',
        p_duration_ms => v_duration_ms,
        p_rows_processed => 1000
    );
    
    DBMS_OUTPUT.PUT_LINE('✓ Logged operation lifecycle (ID: ' || v_operation_id || ')');
END;
/

-- Example 6.3: View recent logs
PROMPT Recent Operations Log:
SELECT 
    TO_CHAR(log_timestamp, 'HH24:MI:SS') as time,
    RPAD(operation_type, 25) as operation,
    RPAD(operation_status, 10) as status,
    SUBSTR(message, 1, 50) as message
FROM partition_operations_log
WHERE log_timestamp > SYSDATE - 1/24
ORDER BY log_timestamp DESC
FETCH FIRST 10 ROWS ONLY;

-- =====================================================
-- SECTION 7: ONLINE OPERATIONS (Oracle 19c+)
-- =====================================================

PROMPT
PROMPT === SECTION 7: ONLINE OPERATIONS (Oracle 19c) ===
PROMPT

-- Example 7.1: Online table move (zero downtime)
BEGIN
    table_ops_pkg.move_table_online(
        p_table_name => 'SALES_DATA',
        p_new_tablespace => 'USERS',
        p_parallel_degree => 4
    );
    DBMS_OUTPUT.PUT_LINE('✓ Moved table online (zero downtime)');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('⚠ Online move: ' || SQLERRM);
END;
/

-- Example 7.2: Online partition move
BEGIN
    table_ops_pkg.move_partition_online(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q4',
        p_new_tablespace => 'USERS',
        p_parallel_degree => 4
    );
    DBMS_OUTPUT.PUT_LINE('✓ Moved partition online');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('⚠ Online partition move: ' || SQLERRM);
END;
/

-- =====================================================
-- SECTION 8: MAINTENANCE WORKFLOWS
-- =====================================================

PROMPT
PROMPT === SECTION 8: COMPLETE MAINTENANCE WORKFLOWS ===
PROMPT

-- Workflow 1: Add new partition with statistics
BEGIN
    DBMS_OUTPUT.PUT_LINE('Workflow: Adding new partition with auto-statistics');
    
    -- Step 1: Create partition
    table_ops_pkg.create_partition(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2025_Q2',
        p_high_value => 'DATE ''2025-07-01'''
    );
    
    -- Step 2: Collect statistics (automatic with integration)
    table_ops_pkg.collect_partition_stats_after_maintenance(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2025_Q2',
        p_auto_configure => TRUE
    );
    
    DBMS_OUTPUT.PUT_LINE('✓ Complete: New partition with statistics');
END;
/

-- Workflow 2: Drop old partitions (retention policy)
BEGIN
    DBMS_OUTPUT.PUT_LINE('Workflow: Drop partitions older than 365 days');
    
    table_ops_pkg.drop_old_partitions(
        p_table_name => 'SALES_DATA',
        p_retention_days => 365
    );
    
    DBMS_OUTPUT.PUT_LINE('✓ Complete: Old partitions cleanup');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No old partitions to drop');
END;
/

-- Workflow 3: Rebuild indexes after maintenance
BEGIN
    DBMS_OUTPUT.PUT_LINE('Workflow: Rebuild partition indexes');
    
    table_ops_pkg.rebuild_partition_indexes(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q4'
    );
    
    DBMS_OUTPUT.PUT_LINE('✓ Complete: Indexes rebuilt');
END;
/

-- =====================================================
-- SECTION 9: QUICK REFERENCE
-- =====================================================

PROMPT
PROMPT === SECTION 9: QUICK REFERENCE ===
PROMPT

-- Quick syntax examples for copy/paste

-- Create partition:
-- table_ops_pkg.create_partition('TABLE_NAME', 'PARTITION_NAME', 'DATE ''2025-01-01''');

-- Split partition:
-- table_ops_pkg.split_partition('TABLE_NAME', 'OLD_PART', 'DATE ''2025-06-01''', 'NEW_PART');

-- Merge partitions:
-- table_ops_pkg.merge_partitions('TABLE_NAME', 'PART1', 'PART2', 'MERGED_PART');

-- Collect stats:
-- partition_stats_pkg.collect_partition_stats('TABLE_NAME', 'PARTITION_NAME');

-- Configure optimal stats:
-- partition_stats_pkg.configure_table_for_stats('TABLE_NAME', TRUE, TRUE);

-- Log message:
-- modern_logging_pkg.log_message('INFO', 'Your message here');

-- Generate DDL:
-- table_ops_pkg.generate_partition_ddl('CREATE', 'TABLE', 'PARTITION', 'PARAMS');

PROMPT
PROMPT ========================================
PROMPT CLEANUP
PROMPT ========================================

-- Cleanup test objects
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales_data PURGE';
    EXECUTE IMMEDIATE 'DROP TABLE sales_data_backup PURGE';
    EXECUTE IMMEDIATE 'DROP TABLE sales_monthly PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Test objects cleaned up');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('⚠ Cleanup: ' || SQLERRM);
END;
/

PROMPT
PROMPT ========================================
PROMPT PRACTICAL USAGE GUIDE COMPLETE
PROMPT ========================================
PROMPT
PROMPT Key Packages Available:
PROMPT • table_ddl_pkg     - Create tables (heap, partitioned, IOT, etc.)
PROMPT • table_ops_pkg     - Partition operations and maintenance
PROMPT • partition_stats_pkg - Oracle 19c statistics management
PROMPT • modern_logging_pkg  - High-performance logging
PROMPT
PROMPT Best Practices:
PROMPT 1. Always configure tables with configure_table_for_stats()
PROMPT 2. Use DDL generation functions to plan before execution
PROMPT 3. Enable incremental stats for partitioned tables
PROMPT 4. Use online operations for zero-downtime maintenance
PROMPT 5. Monitor logs regularly for performance insights
PROMPT
PROMPT ========================================
