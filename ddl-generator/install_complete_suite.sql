-- =====================================================
-- Oracle Table Management Suite - Complete Installation
-- =====================================================
-- This script installs the complete Oracle Table Management Suite
-- including partition management, online operations, and generic framework
--
-- Prerequisites:
-- 1. Oracle Database 19c or later
-- 2. Schema owner with CREATE privileges
-- 3. Application user with EXECUTE and SELECT privileges
--
-- Usage:
-- 1. Run as DBA: @1_schema-owner-privileges/grant_privileges.sql
-- 2. Run as Schema Owner: @install_complete_suite.sql
-- 3. Grant permissions to application users
-- =====================================================

SET SERVEROUTPUT ON SIZE 1000000
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT =====================================================
PROMPT Oracle Table Management Suite - Installation Started
PROMPT =====================================================

-- =====================================================
-- 1. Install Configuration Tables
-- =====================================================
PROMPT 
PROMPT Step 1: Installing Configuration Tables...

-- Install partition-specific tables
@@2_config-tables/partition_maintenance_jobs_table.sql
@@2_config-tables/partition_operation_log_table.sql
@@2_config-tables/partition_strategy_config_table.sql
@@2_config-tables/lookup_tables.sql

-- Install generic framework tables
@@5_examples/generic_framework_examples/1_config-tables/generic_maintenance_jobs_table.sql
@@5_examples/generic_framework_examples/1_config-tables/generic_operation_log_table.sql
@@5_examples/generic_framework_examples/1_config-tables/generic_strategy_config_table.sql
@@5_examples/generic_framework_examples/1_config-tables/generic_lookup_tables.sql

PROMPT Configuration tables installed successfully.

-- =====================================================
-- 2. Install Core Packages
-- =====================================================
PROMPT 
PROMPT Step 2: Installing Core Packages...

-- Install partition management packages
@@3_install_packages/partition_logger_pkg.sql
@@3_install_packages/partition_strategy_pkg.sql
@@3_install_packages/partition_management_pkg.sql
@@3_install_packages/partition_maintenance_pkg.sql
@@3_install_packages/partition_utils_pkg.sql

-- Install online operations packages
@@4_online_operations/operation_log_table.sql
@@4_online_operations/online_table_operations_pkg.sql
@@4_online_operations/create_table_pkg.sql

PROMPT Core packages installed successfully.

-- =====================================================
-- 3. Install Generic Framework
-- =====================================================
PROMPT 
PROMPT Step 3: Installing Generic Framework...

-- Install generic maintenance framework
@@5_examples/generic_framework_examples/2_packages/generic_maintenance_logger_pkg.sql
@@5_examples/generic_framework_examples/3_utilities/strategy_implementation_generator.sql

PROMPT Generic framework installed successfully.

-- =====================================================
-- 4. Install Production Examples
-- =====================================================
PROMPT 
PROMPT Step 4: Installing Production Examples...

-- Install database maintenance strategies
@@2_config-tables/4_generic_maintenance_framework/7_production_examples/1_database_maintenance/index_maintenance_strategy.sql
@@2_config-tables/4_generic_maintenance_framework/7_production_examples/1_database_maintenance/statistics_maintenance_strategy.sql
@@2_config-tables/4_generic_maintenance_framework/7_production_examples/1_database_maintenance/data_cleanup_strategy.sql
@@2_config-tables/4_generic_maintenance_framework/7_production_examples/1_database_maintenance/partition_maintenance_strategy.sql

PROMPT Production examples installed successfully.

-- =====================================================
-- 5. Initialize Configuration
-- =====================================================
PROMPT 
PROMPT Step 5: Initializing Configuration...

-- Initialize logging configuration
BEGIN
    partition_logger_pkg.set_logging_enabled(TRUE);
    partition_logger_pkg.set_log_retention_days(90);
    DBMS_OUTPUT.PUT_LINE('Partition logging configured.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Warning: Could not configure partition logging: ' || SQLERRM);
END;
/

-- Initialize generic logging configuration
BEGIN
    generic_maintenance_logger_pkg.set_logging_enabled(TRUE);
    generic_maintenance_logger_pkg.set_log_retention_days(90);
    DBMS_OUTPUT.PUT_LINE('Generic logging configured.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Warning: Could not configure generic logging: ' || SQLERRM);
END;
/

PROMPT Configuration initialized successfully.

-- =====================================================
-- 6. Validation
-- =====================================================
PROMPT 
PROMPT Step 6: Validating Installation...

-- Check installed packages
PROMPT 
PROMPT Checking installed packages...
SELECT 
    object_name, 
    object_type, 
    status,
    CASE 
        WHEN object_name LIKE 'PARTITION_%' THEN 'Partition Management'
        WHEN object_name LIKE 'ONLINE_%' THEN 'Online Operations'
        WHEN object_name LIKE 'CREATE_%' THEN 'Table Creation'
        WHEN object_name LIKE 'GENERIC_%' THEN 'Generic Framework'
        WHEN object_name LIKE 'STRATEGY_%' THEN 'Strategy Generator'
        ELSE 'Other'
    END as package_category
FROM user_objects
WHERE object_name IN (
    'PARTITION_LOGGER_PKG', 'PARTITION_STRATEGY_PKG', 'PARTITION_MANAGEMENT_PKG',
    'PARTITION_MAINTENANCE_PKG', 'PARTITION_UTILS_PKG',
    'ONLINE_TABLE_OPERATIONS_PKG', 'CREATE_TABLE_PKG',
    'GENERIC_MAINTENANCE_LOGGER_PKG', 'STRATEGY_IMPLEMENTATION_GENERATOR'
)
ORDER BY package_category, object_name;

-- Check installed tables
PROMPT 
PROMPT Checking installed tables...
SELECT 
    table_name,
    CASE 
        WHEN table_name LIKE 'PARTITION_%' THEN 'Partition Management'
        WHEN table_name LIKE 'GENERIC_%' THEN 'Generic Framework'
        WHEN table_name LIKE 'OPERATION_%' THEN 'Online Operations'
        ELSE 'Other'
    END as table_category
FROM user_tables
WHERE table_name IN (
    'PARTITION_MAINTENANCE_JOBS', 'PARTITION_OPERATION_LOG', 'PARTITION_STRATEGY_CONFIG',
    'GENERIC_MAINTENANCE_JOBS', 'GENERIC_OPERATION_LOG', 'GENERIC_STRATEGY_CONFIG',
    'OPERATION_LOG'
)
ORDER BY table_category, table_name;

-- Check lookup tables
PROMPT 
PROMPT Checking lookup tables...
SELECT 
    table_name,
    num_rows
FROM user_tables
WHERE table_name LIKE '%_TYPES' OR table_name LIKE '%_STATUS' OR table_name LIKE '%_YES_NO'
ORDER BY table_name;

-- =====================================================
-- 7. Test Basic Functionality
-- =====================================================
PROMPT 
PROMPT Step 7: Testing Basic Functionality...

-- Test partition logger
BEGIN
    partition_logger_pkg.log_operation(
        p_operation_type => 'TEST',
        p_table_name => 'TEST_TABLE',
        p_partition_name => 'TEST_PARTITION',
        p_status => 'SUCCESS',
        p_message => 'Installation test successful'
    );
    DBMS_OUTPUT.PUT_LINE('Partition logger test: PASSED');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Partition logger test: FAILED - ' || SQLERRM);
END;
/

-- Test generic logger
BEGIN
    generic_maintenance_logger_pkg.log_operation(
        p_strategy_name => 'TEST_STRATEGY',
        p_job_name => 'TEST_JOB',
        p_target_object => 'TEST_TABLE',
        p_target_type => 'TABLE',
        p_operation_type => 'TEST',
        p_status => 'SUCCESS',
        p_message => 'Installation test successful'
    );
    DBMS_OUTPUT.PUT_LINE('Generic logger test: PASSED');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Generic logger test: FAILED - ' || SQLERRM);
END;
/

-- Test online operations package
BEGIN
    -- Test DDL generation (without execution)
    DECLARE
        v_ddl CLOB;
    BEGIN
        v_ddl := online_table_operations_pkg.generate_move_table_ddl(
            p_table_name => 'TEST_TABLE',
            p_new_tablespace => 'USERS'
        );
        DBMS_OUTPUT.PUT_LINE('Online operations test: PASSED');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Online operations test: FAILED - ' || SQLERRM);
    END;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Online operations test: FAILED - ' || SQLERRM);
END;
/

-- Test table creation package
BEGIN
    -- Test DDL generation (without execution)
    DECLARE
        v_ddl CLOB;
    BEGIN
        v_ddl := create_table_pkg.generate_heap_table_ddl(
            p_table_name => 'TEST_TABLE',
            p_columns => 'id NUMBER, name VARCHAR2(100)'
        );
        DBMS_OUTPUT.PUT_LINE('Table creation test: PASSED');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Table creation test: FAILED - ' || SQLERRM);
    END;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Table creation test: FAILED - ' || SQLERRM);
END;
/

-- =====================================================
-- 8. Installation Summary
-- =====================================================
PROMPT 
PROMPT =====================================================
PROMPT Installation Summary
PROMPT =====================================================

-- Count installed objects
DECLARE
    v_package_count NUMBER;
    v_table_count NUMBER;
    v_sequence_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_package_count
    FROM user_objects
    WHERE object_type = 'PACKAGE' 
    AND object_name IN (
        'PARTITION_LOGGER_PKG', 'PARTITION_STRATEGY_PKG', 'PARTITION_MANAGEMENT_PKG',
        'PARTITION_MAINTENANCE_PKG', 'PARTITION_UTILS_PKG',
        'ONLINE_TABLE_OPERATIONS_PKG', 'CREATE_TABLE_PKG',
        'GENERIC_MAINTENANCE_LOGGER_PKG', 'STRATEGY_IMPLEMENTATION_GENERATOR'
    );
    
    SELECT COUNT(*) INTO v_table_count
    FROM user_tables
    WHERE table_name IN (
        'PARTITION_MAINTENANCE_JOBS', 'PARTITION_OPERATION_LOG', 'PARTITION_STRATEGY_CONFIG',
        'GENERIC_MAINTENANCE_JOBS', 'GENERIC_OPERATION_LOG', 'GENERIC_STRATEGY_CONFIG',
        'OPERATION_LOG'
    );
    
    SELECT COUNT(*) INTO v_sequence_count
    FROM user_objects
    WHERE object_type = 'SEQUENCE'
    AND object_name LIKE '%_LOG_SEQ';
    
    DBMS_OUTPUT.PUT_LINE('Packages installed: ' || v_package_count);
    DBMS_OUTPUT.PUT_LINE('Tables installed: ' || v_table_count);
    DBMS_OUTPUT.PUT_LINE('Sequences installed: ' || v_sequence_count);
END;
/

PROMPT 
PROMPT =====================================================
PROMPT Installation Complete!
PROMPT =====================================================
PROMPT 
PROMPT Next Steps:
PROMPT 1. Grant permissions to application users:
PROMPT    @1_schema-owner-privileges/grant_privileges.sql
PROMPT 
PROMPT 2. Test the installation with examples:
PROMPT    @5_examples/comprehensive_usage_examples.sql
PROMPT 
PROMPT 3. Review the documentation:
PROMPT    @6_documentation/README.md
PROMPT 
PROMPT 4. Configure maintenance jobs as needed
PROMPT 
PROMPT For support and troubleshooting, check the operation logs
PROMPT and review the comprehensive documentation.
PROMPT 
PROMPT =====================================================
