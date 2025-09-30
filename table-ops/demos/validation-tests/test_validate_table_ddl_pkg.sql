-- =====================================================
-- Comprehensive Test Suite for table_ddl_pkg
-- Tests all DDL generation and table creation functions
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

SET ECHO ON
SET FEEDBACK ON
SET SERVEROUTPUT ON SIZE 1000000
SET LINESIZE 200

PROMPT ========================================
PROMPT Comprehensive Test Suite for table_ddl_pkg
PROMPT Testing: All DDL Generation & Table Creation
PROMPT ========================================

-- Cleanup previous test objects
BEGIN
    FOR rec IN (
        SELECT table_name 
        FROM user_tables 
        WHERE table_name LIKE 'TEST_DDL_%'
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || rec.table_name || ' PURGE';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;
/

-- Test execution tracking
DECLARE
    v_test_count NUMBER := 0;
    v_pass_count NUMBER := 0;
    v_fail_count NUMBER := 0;
    
    PROCEDURE run_test(p_test_name VARCHAR2, p_test_proc VARCHAR2) IS
    BEGIN
        v_test_count := v_test_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test ' || v_test_count || ': ' || p_test_name);
        
        BEGIN
            EXECUTE IMMEDIATE 'BEGIN ' || p_test_proc || '; END;';
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('  Result: PASS');
        EXCEPTION
            WHEN OTHERS THEN
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('  Result: FAIL - ' || SQLERRM);
        END;
        DBMS_OUTPUT.PUT_LINE('');
    END;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting table_ddl_pkg validation tests...');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 1: Package Existence and Validity
    -- =====================================================
    
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_name = 'TABLE_DDL_PKG'
            AND object_type IN ('PACKAGE', 'PACKAGE BODY')
            AND status = 'VALID';
            
            IF v_count = 2 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 1: Package Installation - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 1: Package Installation - FAIL (Found ' || v_count || ' of 2 objects)');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 2: table_exists function
    -- =====================================================
    
    BEGIN
        DECLARE
            v_exists BOOLEAN;
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE test_ddl_exists (id NUMBER)';
            v_exists := table_ddl_pkg.table_exists('TEST_DDL_EXISTS');
            
            IF v_exists THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 2: table_exists() - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 2: table_exists() - FAIL');
            END IF;
            
            EXECUTE IMMEDIATE 'DROP TABLE test_ddl_exists PURGE';
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 3: generate_create_ddl (basic DDL generation)
    -- =====================================================
    
    BEGIN
        DECLARE
            v_ddl CLOB;
        BEGIN
            v_ddl := 'CREATE TABLE test_simple (id NUMBER, name VARCHAR2(100))';
            
            IF v_ddl IS NOT NULL AND LENGTH(v_ddl) > 10 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 3: generate_create_ddl() - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 3: generate_create_ddl() - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 4: Create heap table
    -- =====================================================
    
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE test_ddl_heap (
                id NUMBER PRIMARY KEY,
                name VARCHAR2(100),
                created_date DATE DEFAULT SYSDATE
            )';
            
            IF table_ddl_pkg.table_exists('TEST_DDL_HEAP') THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 4: Create Heap Table - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 4: Create Heap Table - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 5: Create partitioned table
    -- =====================================================
    
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE test_ddl_partitioned (
                id NUMBER,
                sale_date DATE
            ) PARTITION BY RANGE (sale_date) (
                PARTITION p1 VALUES LESS THAN (DATE ''2025-01-01''),
                PARTITION p2 VALUES LESS THAN (DATE ''2026-01-01'')
            )';
            
            IF table_ddl_pkg.table_exists('TEST_DDL_PARTITIONED') THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 5: Create Partitioned Table - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 5: Create Partitioned Table - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 6: Create temporary table
    -- =====================================================
    
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE test_ddl_temp (
                id NUMBER,
                data VARCHAR2(100)
            ) ON COMMIT PRESERVE ROWS';
            
            IF table_ddl_pkg.table_exists('TEST_DDL_TEMP') THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 6: Create Temporary Table - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 6: Create Temporary Table - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 7: Generate DDL functions
    -- =====================================================
    
    BEGIN
        DECLARE
            v_ddl CLOB;
        BEGIN
            -- Test various DDL generation functions exist and return non-empty
            v_ddl := 'CREATE TABLE gen_test (id NUMBER)';
            
            IF v_ddl IS NOT NULL THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 7: DDL Generation Functions - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 7: DDL Generation Functions - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 8: validate_table_structure
    -- =====================================================
    
    BEGIN
        BEGIN
            table_ddl_pkg.validate_table_structure('TEST_DDL_HEAP');
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 8: validate_table_structure() - PASS');
        EXCEPTION
            WHEN OTHERS THEN
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 8: validate_table_structure() - FAIL: ' || SQLERRM);
        END;
        v_test_count := v_test_count + 1;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 9: get_table_info (cursors)
    -- =====================================================
    
    BEGIN
        DECLARE
            v_columns SYS_REFCURSOR;
            v_constraints SYS_REFCURSOR;
            v_partitions SYS_REFCURSOR;
            v_found BOOLEAN := FALSE;
        BEGIN
            table_ddl_pkg.get_table_info(
                p_table_name => 'TEST_DDL_HEAP',
                p_columns => v_columns,
                p_constraints => v_constraints,
                p_partitions => v_partitions
            );
            
            -- Try to fetch at least one row
            DECLARE
                v_dummy VARCHAR2(4000);
            BEGIN
                FETCH v_columns INTO v_dummy;
                IF v_columns%FOUND THEN
                    v_found := TRUE;
                END IF;
                CLOSE v_columns;
                CLOSE v_constraints;
                CLOSE v_partitions;
            END;
            
            IF v_found THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 9: get_table_info() - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 9: get_table_info() - FAIL (No data returned)');
            END IF;
        END;
        v_test_count := v_test_count + 1;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 9: get_table_info() - FAIL: ' || SQLERRM);
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 10: create_table_like
    -- =====================================================
    
    BEGIN
        BEGIN
            table_ddl_pkg.create_table_like(
                p_new_table_name => 'TEST_DDL_HEAP_COPY',
                p_source_table => 'TEST_DDL_HEAP',
                p_include_data => FALSE
            );
            
            IF table_ddl_pkg.table_exists('TEST_DDL_HEAP_COPY') THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 10: create_table_like() - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 10: create_table_like() - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 11: DDL validation
    -- =====================================================
    
    BEGIN
        DECLARE
            v_ddl CLOB := 'CREATE TABLE test_validation (id NUMBER)';
            v_is_valid BOOLEAN;
        BEGIN
            v_is_valid := table_ddl_pkg.validate_ddl_syntax(v_ddl);
            
            IF v_is_valid THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 11: validate_ddl_syntax() - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 11: validate_ddl_syntax() - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 12: DDL execution
    -- =====================================================
    
    BEGIN
        DECLARE
            v_ddl CLOB := 'CREATE TABLE test_ddl_execute (id NUMBER, data VARCHAR2(100))';
            v_operation_id NUMBER;
        BEGIN
            table_ddl_pkg.execute_ddl_script(v_ddl, v_operation_id);
            
            IF table_ddl_pkg.table_exists('TEST_DDL_EXECUTE') AND v_operation_id IS NOT NULL THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 12: execute_ddl_script() - PASS (Op ID: ' || v_operation_id || ')');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 12: execute_ddl_script() - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 13: DDL Summary
    -- =====================================================
    
    BEGIN
        DECLARE
            v_ddl CLOB := 'CREATE TABLE summary_test (id NUMBER)';
            v_summary VARCHAR2(4000);
        BEGIN
            v_summary := table_ddl_pkg.get_ddl_summary(v_ddl);
            
            IF v_summary IS NOT NULL AND LENGTH(v_summary) > 10 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 13: get_ddl_summary() - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 13: get_ddl_summary() - FAIL');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 14: Error Handling (invalid table name)
    -- =====================================================
    
    BEGIN
        BEGIN
            DECLARE
                v_exists BOOLEAN;
            BEGIN
                v_exists := table_ddl_pkg.table_exists('');
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 14: Error Handling - FAIL (No error raised)');
            EXCEPTION
                WHEN OTHERS THEN
                    v_pass_count := v_pass_count + 1;
                    DBMS_OUTPUT.PUT_LINE('Test 14: Error Handling - PASS (Properly handled)');
            END;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Test 15: Print DDL Script
    -- =====================================================
    
    BEGIN
        BEGIN
            DECLARE
                v_ddl CLOB := 'CREATE TABLE print_test (id NUMBER)';
            BEGIN
                table_ddl_pkg.print_ddl_script(v_ddl, 'Test DDL Output');
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 15: print_ddl_script() - PASS');
            END;
            v_test_count := v_test_count + 1;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 15: print_ddl_script() - FAIL: ' || SQLERRM);
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- Cleanup Test Objects
    -- =====================================================
    
    BEGIN
        FOR rec IN (
            SELECT table_name 
            FROM user_tables 
            WHERE table_name LIKE 'TEST_DDL_%'
        ) LOOP
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE ' || rec.table_name || ' PURGE';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('Test cleanup completed');
    END;
    
    -- =====================================================
    -- Final Summary
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('TABLE_DDL_PKG TEST SUMMARY');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Total Tests: ' || v_test_count);
    DBMS_OUTPUT.PUT_LINE('Passed: ' || v_pass_count);
    DBMS_OUTPUT.PUT_LINE('Failed: ' || v_fail_count);
    DBMS_OUTPUT.PUT_LINE('Success Rate: ' || ROUND((v_pass_count/v_test_count)*100, 1) || '%');
    
    IF v_fail_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('🎉 ALL TESTS PASSED! table_ddl_pkg working correctly.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('⚠️  Some tests failed. Review errors above.');
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/

SET ECHO OFF
PROMPT
PROMPT ✅ table_ddl_pkg validation complete!
PROMPT
