-- =====================================================
-- Oracle Table Management Suite - Unified Installation
-- Principal Engineer - Consolidated Edition
-- =====================================================
-- Installs: Logging + Partition Management + Table Operations
-- Compatible: Oracle 11g+ (with graceful degradation)
-- =====================================================

SET ECHO ON
SET FEEDBACK ON
SET SERVEROUTPUT ON SIZE 1000000
SET LINESIZE 120
SET PAGESIZE 50

PROMPT =====================================================
PROMPT Oracle Table Management Suite - Installation
PROMPT =====================================================

-- =====================================================
-- Step 1: Environment Check
-- =====================================================
PROMPT
PROMPT Step 1: Checking Environment...

SELECT banner FROM v$version WHERE banner LIKE 'Oracle Database%';

SELECT privilege FROM user_sys_privs 
WHERE privilege IN ('CREATE TABLE', 'ALTER ANY TABLE', 'DROP ANY TABLE', 'CREATE PROCEDURE')
ORDER BY privilege;

-- =====================================================
-- Step 2: Install Logging Infrastructure (19c+)
-- =====================================================
PROMPT
PROMPT Step 2: Installing Logging Infrastructure...

WHENEVER SQLERROR CONTINUE

@@../loggiing/logging_table.sql
@@../loggiing/logging_views.sql
@@../loggiing/modern_logging_pkg.sql
@@../loggiing/modern_logging_pkg_body.sql

WHENEVER SQLERROR EXIT FAILURE

-- =====================================================
-- Step 3: Install Core Packages
-- =====================================================
PROMPT
PROMPT Step 3: Installing Lookup Tables...


-- =====================================================
-- Step 4: Install Core Packages
-- =====================================================
PROMPT
PROMPT Step 4: Installing Core Packages...

@@../table_ops_pkg.sql
@@../partition_analysis_pkg.sql
@@../table_ddl_pkg.sql

PROMPT
PROMPT Installing Package Bodies...

@@../table_ops_pkg_body.sql
@@../partition_analysis_pkg_body.sql
@@../table_ddl_pkg_body.sql

-- =====================================================
-- Step 5: Validation
-- =====================================================
PROMPT
PROMPT Step 5: Validating Installation...

SELECT object_name, object_type, status
FROM user_objects
WHERE object_name IN (
    'MODERN_LOGGING_PKG',
    'TABLE_OPS_PKG',
    'PARTITION_ANALYSIS_PKG',
    'TABLE_DDL_PKG'
)
ORDER BY object_type DESC, object_name;

-- Check for errors
SELECT name, type, line, position, text
FROM user_errors
WHERE name IN (
    'MODERN_LOGGING_PKG',
    'TABLE_OPS_PKG',
    'PARTITION_ANALYSIS_PKG',
    'TABLE_DDL_PKG'
)
ORDER BY name, type, line;

-- =====================================================
-- Step 6: Functional Tests
-- =====================================================
PROMPT
PROMPT Step 6: Running Functional Tests...

DECLARE
    v_operation_id NUMBER;
    v_test_status VARCHAR2(20) := 'PASSED';
    v_logging_available BOOLEAN := FALSE;
BEGIN
    -- Test 1: Check logging availability
    BEGIN
        modern_logging_pkg.log_message('INFO', 'Installation test');
        v_logging_available := TRUE;
        DBMS_OUTPUT.PUT_LINE('✓ Logging: Available');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ℹ Logging: Not available (requires Oracle 19c+)');
    END;
    
    -- Test 2: Validate partition operations
    DECLARE
        v_valid_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_valid_count
        FROM user_objects
        WHERE object_name = 'TABLE_OPS_PKG'
        AND object_type IN ('PACKAGE', 'PACKAGE BODY')
        AND status = 'VALID';
        
        IF v_valid_count = 2 THEN
            DBMS_OUTPUT.PUT_LINE('✓ Table Operations: Valid');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ Table Operations: Invalid');
            v_test_status := 'FAILED';
        END IF;
    END;
    
    -- Test 3: Validate partition analysis
    DECLARE
        v_valid_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_valid_count
        FROM user_objects
        WHERE object_name = 'PARTITION_ANALYSIS_PKG'
        AND object_type IN ('PACKAGE', 'PACKAGE BODY')
        AND status = 'VALID';
        
        IF v_valid_count = 2 THEN
            DBMS_OUTPUT.PUT_LINE('✓ Partition Analysis: Valid');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ Partition Analysis: Invalid');
            v_test_status := 'FAILED';
        END IF;
    END;
    
    -- Test 4: Validate table creation
    DECLARE
        v_valid_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_valid_count
        FROM user_objects
        WHERE object_name = 'TABLE_DDL_PKG'
        AND object_type IN ('PACKAGE', 'PACKAGE BODY')
        AND status = 'VALID';
        
        IF v_valid_count = 2 THEN
            DBMS_OUTPUT.PUT_LINE('✓ Table DDL: Valid');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ Table DDL: Invalid');
            v_test_status := 'FAILED';
        END IF;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Test Status: ' || v_test_status);
END;
/

-- =====================================================
-- Step 7: Installation Summary
-- =====================================================
PROMPT
PROMPT =====================================================
PROMPT Installation Summary
PROMPT =====================================================

SELECT 
    'Packages' as component,
    COUNT(CASE WHEN status = 'VALID' THEN 1 END) as valid,
    COUNT(CASE WHEN status = 'INVALID' THEN 1 END) as invalid
FROM user_objects
WHERE object_type = 'PACKAGE'
AND object_name IN ('MODERN_LOGGING_PKG', 'TABLE_OPS_PKG', 'PARTITION_ANALYSIS_PKG', 'TABLE_DDL_PKG')

UNION ALL

SELECT 
    'Package Bodies' as component,
    COUNT(CASE WHEN status = 'VALID' THEN 1 END) as valid,
    COUNT(CASE WHEN status = 'INVALID' THEN 1 END) as invalid
FROM user_objects
WHERE object_type = 'PACKAGE BODY'
AND object_name IN ('MODERN_LOGGING_PKG', 'TABLE_OPS_PKG', 'PARTITION_ANALYSIS_PKG', 'TABLE_DDL_PKG')

UNION ALL

SELECT 
    'Tables' as component,
    COUNT(*) as valid,
    0 as invalid
FROM user_tables
WHERE table_name IN ('PARTITION_OPERATIONS_LOG', 'PARTITION_TYPES', 'PARTITION_STATUS', 'YES_NO_TYPES')

UNION ALL

SELECT 
    'Views' as component,
    COUNT(*) as valid,
    0 as invalid
FROM user_views
WHERE view_name LIKE 'V_PARTITION_%';

PROMPT
PROMPT =====================================================
PROMPT Installation Complete
PROMPT =====================================================
PROMPT
PROMPT Next Steps:
PROMPT   1. Grant permissions: @grant-schema-owner.sql
PROMPT   2. Run examples: @../comprehensive_usage_examples.sql
PROMPT   3. Start using the suite!
PROMPT
PROMPT =====================================================
