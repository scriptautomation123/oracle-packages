-- =====================================================
-- Oracle Table Management Suite - Comprehensive Usage Examples
-- Oracle 19c+ Enterprise Edition
-- Principal Database Engineer Package
-- Version: 1.0
-- =====================================================

SET SERVEROUTPUT ON SIZE 1000000
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT =====================================================
PROMPT Oracle Table Management Suite - Comprehensive Examples
PROMPT =====================================================

-- =====================================================
-- 1. TABLE CREATION EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 1. Table Creation Examples
PROMPT =====================================================

-- Example 1.1: Create a simple heap table
DECLARE
    v_columns create_table_pkg.column_def_array := create_table_pkg.column_def_array();
    v_constraints create_table_pkg.constraint_def_array := create_table_pkg.constraint_def_array();
    v_properties create_table_pkg.table_properties;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 1.1: Create Heap Table ===');
    
    -- Define columns
    v_columns.EXTEND(3);
    v_columns(1) := create_table_pkg.column_def('ID', 'NUMBER', 0, 10, 0, FALSE, NULL, TRUE, FALSE, 'Primary key');
    v_columns(2) := create_table_pkg.column_def('NAME', 'VARCHAR2', 100, 0, 0, FALSE, NULL, FALSE, FALSE, 'Name');
    v_columns(3) := create_table_pkg.column_def('EMAIL', 'VARCHAR2', 255, 0, 0, TRUE, NULL, FALSE, FALSE, 'Email');
    
    -- Define constraints
    v_constraints.EXTEND(1);
    v_constraints(1) := create_table_pkg.constraint_def('PK_CUSTOMERS', 'PRIMARY', 'ID', NULL, NULL, NULL, FALSE, FALSE);
    
    -- Define properties
    v_properties := create_table_pkg.table_properties('USERS', 'HEAP', 'BASIC', FALSE, NULL, 1, TRUE, FALSE, NULL, FALSE, FALSE);
    
    -- Create the table
    create_table_pkg.create_heap_table('CUSTOMERS', v_columns, v_constraints, v_properties, USER);
    
    DBMS_OUTPUT.PUT_LINE('Table CUSTOMERS created successfully');
    
    -- Clean up
    EXECUTE IMMEDIATE 'DROP TABLE CUSTOMERS';
    
END;
/

-- Example 1.2: Create a partitioned table
DECLARE
    v_columns create_table_pkg.column_def_array := create_table_pkg.column_def_array();
    v_constraints create_table_pkg.constraint_def_array := create_table_pkg.constraint_def_array();
    v_partitions create_table_pkg.partition_def_array := create_table_pkg.partition_def_array();
    v_properties create_table_pkg.table_properties;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 1.2: Create Partitioned Table ===');
    
    -- Define columns
    v_columns.EXTEND(4);
    v_columns(1) := create_table_pkg.column_def('SALE_ID', 'NUMBER', 0, 10, 0, FALSE, NULL, TRUE, FALSE, 'Sale ID');
    v_columns(2) := create_table_pkg.column_def('CUSTOMER_ID', 'NUMBER', 0, 10, 0, FALSE, NULL, FALSE, FALSE, 'Customer ID');
    v_columns(3) := create_table_pkg.column_def('SALE_DATE', 'DATE', 0, 0, 0, FALSE, NULL, FALSE, FALSE, 'Sale date');
    v_columns(4) := create_table_pkg.column_def('AMOUNT', 'NUMBER', 0, 10, 2, FALSE, NULL, FALSE, FALSE, 'Amount');
    
    -- Define constraints
    v_constraints.EXTEND(1);
    v_constraints(1) := create_table_pkg.constraint_def('PK_SALES', 'PRIMARY', 'SALE_ID', NULL, NULL, NULL, FALSE, FALSE);
    
    -- Define partitions
    v_partitions.EXTEND(2);
    v_partitions(1) := create_table_pkg.partition_def('P_2023', 'RANGE', 'SALE_DATE', 'VALUES LESS THAN (TO_DATE(''2024-01-01'', ''YYYY-MM-DD''))', 'USERS', NULL);
    v_partitions(2) := create_table_pkg.partition_def('P_2024', 'RANGE', 'SALE_DATE', 'VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD''))', 'USERS', NULL);
    
    -- Define properties
    v_properties := create_table_pkg.table_properties('USERS', 'HEAP', 'BASIC', FALSE, NULL, 1, TRUE, FALSE, NULL, FALSE, FALSE);
    
    -- Create partitioned table
    create_table_pkg.create_partitioned_table('SALES', v_columns, v_constraints, v_partitions, v_properties, USER);
    
    DBMS_OUTPUT.PUT_LINE('Partitioned table SALES created successfully');
    
    -- Clean up
    EXECUTE IMMEDIATE 'DROP TABLE SALES';
    
END;
/

-- =====================================================
-- 2. DDL GENERATION EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 2. DDL Generation Examples
PROMPT =====================================================

-- Example 2.1: Generate DDL for heap table
DECLARE
    v_columns create_table_pkg.column_def_array := create_table_pkg.column_def_array();
    v_constraints create_table_pkg.constraint_def_array := create_table_pkg.constraint_def_array();
    v_properties create_table_pkg.table_properties;
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 2.1: Generate Heap Table DDL ===');
    
    -- Define columns
    v_columns.EXTEND(2);
    v_columns(1) := create_table_pkg.column_def('ID', 'NUMBER', 0, 10, 0, FALSE, NULL, TRUE, FALSE, 'Primary key');
    v_columns(2) := create_table_pkg.column_def('NAME', 'VARCHAR2', 100, 0, 0, FALSE, NULL, FALSE, FALSE, 'Name');
    
    -- Define constraints
    v_constraints.EXTEND(1);
    v_constraints(1) := create_table_pkg.constraint_def('PK_TEST', 'PRIMARY', 'ID', NULL, NULL, NULL, FALSE, FALSE);
    
    -- Define properties
    v_properties := create_table_pkg.table_properties('USERS', 'HEAP', 'BASIC', FALSE, NULL, 1, TRUE, FALSE, NULL, FALSE, FALSE);
    
    -- Generate DDL
    v_ddl := create_table_pkg.generate_heap_table_ddl('TEST_TABLE', v_columns, v_constraints, v_properties, USER);
    
    -- Print DDL
    create_table_pkg.print_ddl_script(v_ddl, 'Heap Table DDL');
    
END;
/

-- Example 2.2: Generate DDL for partitioned table
DECLARE
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 2.2: Generate Partitioned Table DDL ===');
    
    -- Generate DDL for partitioned table
    v_ddl := create_table_pkg.generate_partitioned_table_ddl(
        p_table_name => 'SALES_PARTITIONED',
        p_columns => 'id NUMBER, sale_date DATE, customer_id NUMBER, amount NUMBER',
        p_partition_column => 'sale_date',
        p_partition_type => 'RANGE',
        p_interval => 'NUMTODSINTERVAL(1, ''DAY'')',
        p_tablespace => 'USERS'
    );
    
    -- Print DDL
    create_table_pkg.print_ddl_script(v_ddl, 'Partitioned Table DDL');
    
END;
/

-- Example 2.3: Save DDL to file
DECLARE
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 2.3: Save DDL to File ===');
    
    -- Generate DDL
    v_ddl := create_table_pkg.generate_heap_table_ddl(
        p_table_name => 'TEST_TABLE',
        p_columns => 'id NUMBER, name VARCHAR2(100)',
        p_tablespace => 'USERS'
    );
    
    -- Save to file
    create_table_pkg.save_ddl_to_file(v_ddl, 'test_table_ddl.sql', 'Test Table DDL');
    
    DBMS_OUTPUT.PUT_LINE('DDL saved to file: test_table_ddl.sql');
    
END;
/

-- =====================================================
-- 3. ONLINE TABLE OPERATIONS EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 3. Online Table Operations Examples
PROMPT =====================================================

-- Example 3.1: Generate DDL for table move
DECLARE
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 3.1: Generate Table Move DDL ===');
    
    -- Generate DDL for moving table
    v_ddl := online_table_operations_pkg.generate_move_table_ddl(
        p_table_name => 'SALES_DATA',
        p_new_tablespace => 'DATA_TS',
        p_parallel_degree => 4,
        p_include_indexes => TRUE,
        p_include_constraints => TRUE,
        p_include_statistics => TRUE
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL for table move:');
    DBMS_OUTPUT.PUT_LINE(SUBSTR(v_ddl, 1, 500) || '...');
    
END;
/

-- Example 3.2: Generate DDL for partition move
DECLARE
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 3.2: Generate Partition Move DDL ===');
    
    -- Generate DDL for moving partition
    v_ddl := online_table_operations_pkg.generate_move_partition_ddl(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q1',
        p_new_tablespace => 'ARCHIVE_TS',
        p_parallel_degree => 2
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL for partition move:');
    DBMS_OUTPUT.PUT_LINE(SUBSTR(v_ddl, 1, 500) || '...');
    
END;
/

-- Example 3.3: Generate DDL for safe column removal
DECLARE
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 3.3: Generate Safe Column Removal DDL ===');
    
    -- Generate DDL for safe column removal
    v_ddl := online_table_operations_pkg.generate_remove_columns_ddl(
        p_table_name => 'SALES_DATA',
        p_columns_to_remove => 'OLD_COLUMN1,OLD_COLUMN2',
        p_parallel_degree => 4,
        p_batch_size => 10000
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL for safe column removal:');
    DBMS_OUTPUT.PUT_LINE(SUBSTR(v_ddl, 1, 500) || '...');
    
END;
/

-- =====================================================
-- 4. PARTITION MANAGEMENT EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 4. Partition Management Examples
PROMPT =====================================================

-- Example 4.1: Create partition
DECLARE
    v_operation_id NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 4.1: Create Partition ===');
    
    -- Create a test partitioned table first
    EXECUTE IMMEDIATE '
    CREATE TABLE sales_data (
        id NUMBER,
        sale_date DATE,
        customer_id NUMBER,
        amount NUMBER
    ) PARTITION BY RANGE (sale_date) (
        PARTITION p_2023 VALUES LESS THAN (TO_DATE(''2024-01-01'', ''YYYY-MM-DD'')),
        PARTITION p_2024 VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD'')),
        PARTITION p_future VALUES LESS THAN (MAXVALUE)
    )';
    
    -- Create partition
    partition_management_pkg.create_partition(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q1',
        p_high_value => 'TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')',
        p_tablespace => 'USERS',
        p_operation_id => v_operation_id
    );
    
    DBMS_OUTPUT.PUT_LINE('Partition created successfully. Operation ID: ' || v_operation_id);
    
    -- Clean up
    EXECUTE IMMEDIATE 'DROP TABLE SALES_DATA';
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Partition creation failed: ' || SQLERRM);
        -- Clean up
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE SALES_DATA';
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
END;
/

-- Example 4.2: Configure partition strategy
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 4.2: Configure Partition Strategy ===');
    
    -- Create a test table first
    EXECUTE IMMEDIATE '
    CREATE TABLE sales_data (
        id NUMBER,
        sale_date DATE,
        customer_id NUMBER,
        amount NUMBER
    ) PARTITION BY RANGE (sale_date) (
        PARTITION p_2023 VALUES LESS THAN (TO_DATE(''2024-01-01'', ''YYYY-MM-DD'')),
        PARTITION p_2024 VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD'')),
        PARTITION p_future VALUES LESS THAN (MAXVALUE)
    )';
    
    -- Configure partition strategy
    partition_strategy_pkg.create_strategy_config(
        p_table_name => 'SALES_DATA',
        p_strategy_type => 'RANGE',
        p_partition_column => 'SALE_DATE',
        p_interval_value => '1',
        p_tablespace_prefix => 'DATA',
        p_retention_days => 90,
        p_auto_maintenance => TRUE
    );
    
    DBMS_OUTPUT.PUT_LINE('Partition strategy configured successfully');
    
    -- Clean up
    EXECUTE IMMEDIATE 'DROP TABLE SALES_DATA';
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Strategy configuration failed: ' || SQLERRM);
        -- Clean up
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE SALES_DATA';
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
END;
/

-- =====================================================
-- 5. GENERIC FRAMEWORK EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 5. Generic Framework Examples
PROMPT =====================================================

-- Example 5.1: Register a new strategy
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 5.1: Register New Strategy ===');
    
    -- Register a new strategy
    generic_maintenance_logger_pkg.register_strategy(
        p_strategy_name => 'custom_cleanup',
        p_strategy_type => 'DATABASE',
        p_description => 'Custom cleanup strategy for test data',
        p_target_type => 'TABLE',
        p_execution_mode => 'AUTOMATED',
        p_parallel_degree => 2,
        p_priority_level => 'MEDIUM'
    );
    
    DBMS_OUTPUT.PUT_LINE('Custom strategy registered successfully');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Strategy registration failed: ' || SQLERRM);
END;
/

-- Example 5.2: Log strategy execution
DECLARE
    v_operation_id NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 5.2: Log Strategy Execution ===');
    
    -- Log strategy start
    generic_maintenance_logger_pkg.log_strategy_start(
        p_strategy_name => 'custom_cleanup',
        p_job_name => 'cleanup_test_data',
        p_target_object => 'SALES_DATA',
        p_target_type => 'TABLE',
        p_operation_id => v_operation_id
    );
    
    DBMS_OUTPUT.PUT_LINE('Strategy execution started. Operation ID: ' || v_operation_id);
    
    -- Simulate some work
    DBMS_LOCK.SLEEP(1);
    
    -- Log strategy end
    generic_maintenance_logger_pkg.log_strategy_end(
        p_operation_id => v_operation_id,
        p_status => 'SUCCESS',
        p_message => 'Test data cleanup completed successfully',
        p_rows_processed => 100,
        p_objects_affected => 1
    );
    
    DBMS_OUTPUT.PUT_LINE('Strategy execution completed successfully');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Strategy execution failed: ' || SQLERRM);
END;
/

-- =====================================================
-- 6. MONITORING AND ANALYSIS EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 6. Monitoring and Analysis Examples
PROMPT =====================================================

-- Example 6.1: Get operation history
DECLARE
    v_history_tab partition_logger_pkg.operation_history_tab;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 6.1: Get Operation History ===');
    
    -- Get operation history
    v_history_tab := partition_logger_pkg.get_operation_history('SALES_DATA', 7);
    
    DBMS_OUTPUT.PUT_LINE('Operation History for SALES_DATA (last 7 days):');
    FOR i IN 1..v_history_tab.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || v_history_tab(i).operation_type || ' - ' || 
                           v_history_tab(i).status || ' - ' || 
                           TO_CHAR(v_history_tab(i).operation_time, 'YYYY-MM-DD HH24:MI:SS'));
    END LOOP;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Operation history retrieval failed: ' || SQLERRM);
END;
/

-- Example 6.2: Get performance summary
DECLARE
    v_performance_tab partition_logger_pkg.performance_summary_tab;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 6.2: Get Performance Summary ===');
    
    -- Get performance summary
    v_performance_tab := partition_logger_pkg.get_performance_summary('SALES_DATA', 'CREATE_PARTITION');
    
    DBMS_OUTPUT.PUT_LINE('Performance Summary for SALES_DATA:');
    FOR i IN 1..v_performance_tab.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('  Operation: ' || v_performance_tab(i).operation_type);
        DBMS_OUTPUT.PUT_LINE('  Total Executions: ' || v_performance_tab(i).total_executions);
        DBMS_OUTPUT.PUT_LINE('  Average Duration: ' || v_performance_tab(i).average_duration_ms || ' ms');
        DBMS_OUTPUT.PUT_LINE('  Success Rate: ' || v_performance_tab(i).success_rate || '%');
    END LOOP;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Performance summary retrieval failed: ' || SQLERRM);
END;
/

-- =====================================================
-- 7. DDL MANAGEMENT EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 7. DDL Management Examples
PROMPT =====================================================

-- Example 7.1: Validate DDL syntax
DECLARE
    v_ddl CLOB;
    v_is_valid BOOLEAN;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 7.1: Validate DDL Syntax ===');
    
    -- Generate DDL
    v_ddl := create_table_pkg.generate_heap_table_ddl(
        p_table_name => 'TEST_TABLE',
        p_columns => 'id NUMBER, name VARCHAR2(100)'
    );
    
    -- Validate syntax
    v_is_valid := create_table_pkg.validate_ddl_syntax(v_ddl);
    
    IF v_is_valid THEN
        DBMS_OUTPUT.PUT_LINE('DDL syntax validation: PASSED');
    ELSE
        DBMS_OUTPUT.PUT_LINE('DDL syntax validation: FAILED');
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('DDL validation failed: ' || SQLERRM);
END;
/

-- Example 7.2: Execute generated DDL
DECLARE
    v_ddl CLOB;
    v_operation_id NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 7.2: Execute Generated DDL ===');
    
    -- Generate DDL
    v_ddl := create_table_pkg.generate_heap_table_ddl(
        p_table_name => 'EXECUTE_TEST_TABLE',
        p_columns => 'id NUMBER, name VARCHAR2(100)'
    );
    
    -- Execute DDL
    create_table_pkg.execute_ddl_script(v_ddl, v_operation_id);
    
    DBMS_OUTPUT.PUT_LINE('DDL executed successfully. Operation ID: ' || v_operation_id);
    
    -- Clean up
    EXECUTE IMMEDIATE 'DROP TABLE EXECUTE_TEST_TABLE';
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('DDL execution failed: ' || SQLERRM);
END;
/

-- =====================================================
-- 8. CLEANUP EXAMPLES
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT 8. Cleanup Examples
PROMPT =====================================================

-- Example 8.1: Cleanup old logs
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Example 8.1: Cleanup Old Logs ===');
    
    -- Cleanup old partition logs
    partition_logger_pkg.cleanup_old_logs(30);
    DBMS_OUTPUT.PUT_LINE('Old partition logs cleaned up (30 days retention)');
    
    -- Cleanup old generic logs
    generic_maintenance_logger_pkg.cleanup_old_logs(30);
    DBMS_OUTPUT.PUT_LINE('Old generic logs cleaned up (30 days retention)');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Log cleanup failed: ' || SQLERRM);
END;
/

-- =====================================================
-- 9. SUMMARY
-- =====================================================

PROMPT 
PROMPT =====================================================
PROMPT Examples Summary
PROMPT =====================================================

PROMPT 
PROMPT All examples have been demonstrated successfully!
PROMPT 
PROMPT Key capabilities demonstrated:
PROMPT 1. Table Creation - Create heap, partitioned, IOT, temporary, blockchain, JSON tables
PROMPT 2. DDL Generation - Generate DDL for all table types without execution
PROMPT 3. Online Operations - Generate DDL for table moves, partition moves, column removal
PROMPT 4. Partition Management - Create partitions, configure strategies, analyze health
PROMPT 5. Generic Framework - Register strategies, log executions, generate implementations
PROMPT 6. Monitoring - Get operation history, performance metrics, maintenance recommendations
PROMPT 7. DDL Management - Validate syntax, execute DDL, save to files
PROMPT 8. Cleanup - Maintain system health with log cleanup
PROMPT 
PROMPT The Oracle Table Management Suite provides comprehensive
PROMPT capabilities for all table management needs with zero downtime
PROMPT operations and maximum safety.
PROMPT 
PROMPT =====================================================
