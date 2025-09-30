-- =====================================================
-- Comprehensive Test Suite for Logging Infrastructure
-- Tests: Table, Views, Sequences, and Package
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

SET ECHO ON
SET FEEDBACK ON
SET SERVEROUTPUT ON SIZE 1000000
SET LINESIZE 200

PROMPT ========================================
PROMPT Comprehensive Test Suite for Logging Infrastructure
PROMPT Testing: Table + Views + Package + Sequences
PROMPT ========================================

DECLARE
    v_test_count NUMBER := 0;
    v_pass_count NUMBER := 0;
    v_fail_count NUMBER := 0;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting logging infrastructure validation tests...');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 1: TABLE INFRASTRUCTURE TESTS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 1: TABLE INFRASTRUCTURE ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 1: Logging table exists
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_tables
            WHERE table_name = 'PARTITION_OPERATIONS_LOG';
            
            IF v_count = 1 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 1: Table PARTITION_OPERATIONS_LOG exists - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 1: Table PARTITION_OPERATIONS_LOG exists - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 2: Table is partitioned
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_part_tables
            WHERE table_name = 'PARTITION_OPERATIONS_LOG';
            
            IF v_count = 1 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 2: Table is partitioned - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 2: Table is partitioned - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 3: Required columns exist
    BEGIN
        DECLARE
            v_count NUMBER;
            v_required_cols NUMBER := 15; -- Minimum required columns
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_tab_columns
            WHERE table_name = 'PARTITION_OPERATIONS_LOG'
            AND column_name IN ('LOG_ID', 'LOG_TIMESTAMP', 'OPERATION_ID', 'LOG_LEVEL', 
                               'LOG_TYPE', 'TABLE_NAME', 'PARTITION_NAME', 'OPERATION_TYPE',
                               'OPERATION_STATUS', 'MESSAGE', 'ERROR_CODE', 'DURATION_MS',
                               'ROWS_PROCESSED', 'ATTRIBUTES', 'USERNAME');
            
            IF v_count >= v_required_cols THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 3: Required columns exist - PASS (' || v_count || ' columns)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 3: Required columns exist - FAIL (Found ' || v_count || ')');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 4: JSON column exists and has check constraint
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_tab_columns
            WHERE table_name = 'PARTITION_OPERATIONS_LOG'
            AND column_name = 'ATTRIBUTES'
            AND data_type = 'JSON';
            
            IF v_count = 1 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 4: JSON column ATTRIBUTES exists - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 4: JSON column ATTRIBUTES exists - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 5: Indexes exist
    BEGIN
        DECLARE
            v_count NUMBER;
            v_expected_indexes NUMBER := 5; -- Minimum expected indexes
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_indexes
            WHERE table_name = 'PARTITION_OPERATIONS_LOG'
            AND index_name LIKE 'IDX_PARTITION_LOG%';
            
            IF v_count >= v_expected_indexes THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 5: Required indexes exist - PASS (' || v_count || ' indexes)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 5: Required indexes exist - FAIL (Found ' || v_count || ')');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 6: Sequence exists
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_sequences
            WHERE sequence_name = 'PARTITION_OPERATION_SEQ';
            
            IF v_count = 1 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 6: Sequence PARTITION_OPERATION_SEQ exists - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 6: Sequence PARTITION_OPERATION_SEQ exists - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 2: VIEWS TESTS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 2: VIEWS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 7: All required views exist
    BEGIN
        DECLARE
            v_count NUMBER;
            v_expected_views NUMBER := 6;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_views
            WHERE view_name IN (
                'V_PARTITION_OPERATIONS',
                'V_PARTITION_ERRORS',
                'V_PARTITION_PERFORMANCE',
                'V_PARTITION_AUDIT',
                'V_PARTITION_RECENT_ACTIVITY',
                'V_PARTITION_STATISTICS'
            );
            
            IF v_count = v_expected_views THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 7: All required views exist - PASS (' || v_count || ' views)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 7: All required views exist - FAIL (Found ' || v_count || ')');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 8: Views are valid
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_type = 'VIEW'
            AND object_name LIKE 'V_PARTITION%'
            AND status = 'VALID';
            
            IF v_count = 6 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 8: All views are valid - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 8: All views are valid - FAIL (Valid: ' || v_count || ')');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 3: PACKAGE TESTS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 3: PACKAGE ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 9: Package exists and is valid
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_name = 'MODERN_LOGGING_PKG'
            AND object_type IN ('PACKAGE', 'PACKAGE BODY')
            AND status = 'VALID';
            
            IF v_count = 2 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 9: Package installation - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 9: Package installation - FAIL (Found ' || v_count || ')');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 10: Package constants are accessible
    BEGIN
        DECLARE
            v_level VARCHAR2(10);
        BEGIN
            v_level := modern_logging_pkg.c_info;
            
            IF v_level = 'INFO' THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 10: Package constants accessible - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 10: Package constants accessible - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 4: FUNCTIONAL TESTS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 4: FUNCTIONAL TESTS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 11: Basic message logging
    BEGIN
        modern_logging_pkg.log_message(
            p_level => 'INFO',
            p_message => 'Infrastructure test message'
        );
        v_pass_count := v_pass_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test 11: Basic message logging - PASS');
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 11: Basic message logging - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 12: Operation lifecycle logging
    BEGIN
        DECLARE
            v_operation_id NUMBER;
        BEGIN
            modern_logging_pkg.log_operation_start(
                p_operation_type => 'INFRA_TEST',
                p_table_name => 'TEST_TABLE',
                p_operation_id => v_operation_id
            );
            
            modern_logging_pkg.log_operation_end(
                p_operation_id => v_operation_id,
                p_status => 'SUCCESS',
                p_duration_ms => 100
            );
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 12: Operation lifecycle logging - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 12: Operation lifecycle logging - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 13: JSON attributes creation
    BEGIN
        DECLARE
            v_json JSON;
        BEGIN
            v_json := modern_logging_pkg.create_attributes_json(
                p_parallel_degree => 4,
                p_tablespace => 'USERS',
                p_batch_size => 1000,
                p_online => TRUE,
                p_compression => 'BASIC'
            );
            
            IF v_json IS NOT NULL THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 13: JSON attributes creation - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 13: JSON attributes creation - FAIL (NULL returned)');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 13: JSON attributes creation - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 14: Performance logging
    BEGIN
        DECLARE
            v_operation_id NUMBER;
        BEGIN
            v_operation_id := modern_logging_pkg.get_next_operation_id();
            
            modern_logging_pkg.log_performance(
                p_operation_id => v_operation_id,
                p_duration_ms => 5000,
                p_rows_processed => 10000
            );
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 14: Performance logging - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 14: Performance logging - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 15: Error logging
    BEGIN
        modern_logging_pkg.log_error(
            p_error_code => -20001,
            p_error_message => 'Test error message',
            p_context => 'INFRASTRUCTURE_TEST'
        );
        v_pass_count := v_pass_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test 15: Error logging - PASS');
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 15: Error logging - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 16: Sequence usage
    BEGIN
        DECLARE
            v_seq_val NUMBER;
        BEGIN
            SELECT partition_operation_seq.NEXTVAL INTO v_seq_val FROM DUAL;
            
            IF v_seq_val > 0 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 16: Sequence usage - PASS (Value: ' || v_seq_val || ')');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 16: Sequence usage - FAIL');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 16: Sequence usage - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 17: Data insertion via package
    BEGIN
        DECLARE
            v_operation_id NUMBER;
            v_count NUMBER;
        BEGIN
            v_operation_id := modern_logging_pkg.get_next_operation_id();
            
            modern_logging_pkg.log_message(
                p_level => 'INFO',
                p_message => 'Test data insertion',
                p_operation_id => v_operation_id,
                p_table_name => 'TEST_TABLE_INFRA'
            );
            
            -- Verify insertion
            SELECT COUNT(*) INTO v_count
            FROM partition_operations_log
            WHERE operation_id = v_operation_id;
            
            IF v_count > 0 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 17: Data insertion via package - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 17: Data insertion via package - FAIL');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 17: Data insertion via package - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 18: View data retrieval
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM v_partition_operations
            WHERE ROWNUM <= 10;
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 18: View data retrieval - PASS (Rows: ' || v_count || ')');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 18: View data retrieval - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 19: Error view functionality
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM v_partition_errors
            WHERE ROWNUM <= 10;
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 19: Error view functionality - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 19: Error view functionality - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 20: Performance view functionality
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM v_partition_performance
            WHERE ROWNUM <= 10;
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 20: Performance view functionality - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 20: Performance view functionality - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 21: Statistics view functionality
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM v_partition_statistics
            WHERE ROWNUM <= 10;
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 21: Statistics view functionality - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 21: Statistics view functionality - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 22: JSON extraction in views
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM v_partition_operations
            WHERE parallel_degree IS NOT NULL
            AND ROWNUM <= 10;
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 22: JSON extraction in views - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 22: JSON extraction in views - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 23: Log level validation
    BEGIN
        DECLARE
            v_enabled BOOLEAN;
        BEGIN
            v_enabled := modern_logging_pkg.is_logging_enabled('INFO');
            
            IF v_enabled IS NOT NULL THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 23: Log level validation - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 23: Log level validation - FAIL');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 23: Log level validation - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 24: Cleanup functionality
    BEGIN
        modern_logging_pkg.cleanup_old_logs(p_days_old => 365);
        v_pass_count := v_pass_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test 24: Cleanup functionality - PASS');
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 24: Cleanup functionality - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 25: Archive functionality
    BEGIN
        modern_logging_pkg.archive_logs(p_days_old => 30);
        v_pass_count := v_pass_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test 25: Archive functionality - PASS');
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 25: Archive functionality - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- FINAL SUMMARY
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('LOGGING INFRASTRUCTURE TEST SUMMARY');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Total Tests: ' || v_test_count);
    DBMS_OUTPUT.PUT_LINE('Passed: ' || v_pass_count);
    DBMS_OUTPUT.PUT_LINE('Failed: ' || v_fail_count);
    DBMS_OUTPUT.PUT_LINE('Success Rate: ' || ROUND((v_pass_count/v_test_count)*100, 1) || '%');
    DBMS_OUTPUT.PUT_LINE('');
    
    DBMS_OUTPUT.PUT_LINE('Components Tested:');
    DBMS_OUTPUT.PUT_LINE('• Table: partition_operations_log');
    DBMS_OUTPUT.PUT_LINE('• Sequence: partition_operation_seq');
    DBMS_OUTPUT.PUT_LINE('• Package: modern_logging_pkg');
    DBMS_OUTPUT.PUT_LINE('• Views: 6 specialized views');
    DBMS_OUTPUT.PUT_LINE('• Indexes: 5+ performance indexes');
    DBMS_OUTPUT.PUT_LINE('• JSON Support: Attributes column');
    DBMS_OUTPUT.PUT_LINE('• Partitioning: Interval partitioning');
    DBMS_OUTPUT.PUT_LINE('');
    
    IF v_fail_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('🎉 ALL TESTS PASSED! Logging infrastructure is fully operational.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('⚠️  Some tests failed. Review errors above.');
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/

SET ECHO OFF
PROMPT
PROMPT ✅ Logging infrastructure validation complete!
PROMPT
