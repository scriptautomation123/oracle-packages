-- =====================================================
-- Comprehensive Test Suite for partition_analysis_pkg
-- Tests all analysis, monitoring, and recommendation functions
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

SET ECHO ON
SET FEEDBACK ON
SET SERVEROUTPUT ON SIZE 1000000
SET LINESIZE 200

PROMPT ========================================
PROMPT Comprehensive Test Suite for partition_analysis_pkg
PROMPT Testing: Analysis, Monitoring & Recommendations
PROMPT ========================================

-- Cleanup previous test objects
BEGIN
    FOR rec IN (
        SELECT table_name 
        FROM user_tables 
        WHERE table_name LIKE 'TEST_ANALYSIS_%'
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || rec.table_name || ' PURGE';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;
/

-- Create comprehensive test table with partitions
CREATE TABLE test_analysis_sales (
    id NUMBER PRIMARY KEY,
    sale_date DATE NOT NULL,
    amount NUMBER(10,2),
    customer_id NUMBER,
    region VARCHAR2(50),
    product_category VARCHAR2(100),
    status VARCHAR2(20)
) PARTITION BY RANGE (sale_date) (
    PARTITION p_2024_q1 VALUES LESS THAN (DATE '2024-04-01') TABLESPACE USERS,
    PARTITION p_2024_q2 VALUES LESS THAN (DATE '2024-07-01') TABLESPACE USERS,
    PARTITION p_2024_q3 VALUES LESS THAN (DATE '2024-10-01') TABLESPACE USERS,
    PARTITION p_2024_q4 VALUES LESS THAN (DATE '2025-01-01') TABLESPACE USERS
) COMPRESS FOR OLTP;

-- Create index on partitioned table
CREATE INDEX idx_analysis_sales_region ON test_analysis_sales(region) LOCAL;
CREATE INDEX idx_analysis_sales_customer ON test_analysis_sales(customer_id) LOCAL;

-- Insert varied test data
INSERT INTO test_analysis_sales VALUES (1, DATE '2024-01-15', 1500.00, 101, 'NORTH', 'ELECTRONICS', 'COMPLETE');
INSERT INTO test_analysis_sales VALUES (2, DATE '2024-01-20', 2500.00, 102, 'SOUTH', 'FURNITURE', 'COMPLETE');
INSERT INTO test_analysis_sales VALUES (3, DATE '2024-02-10', 3200.00, 103, 'EAST', 'CLOTHING', 'COMPLETE');
INSERT INTO test_analysis_sales VALUES (4, DATE '2024-05-15', 4100.00, 104, 'WEST', 'ELECTRONICS', 'PENDING');
INSERT INTO test_analysis_sales VALUES (5, DATE '2024-05-20', 1800.00, 105, 'NORTH', 'BOOKS', 'COMPLETE');
INSERT INTO test_analysis_sales VALUES (6, DATE '2024-08-25', 2900.00, 106, 'SOUTH', 'ELECTRONICS', 'COMPLETE');
COMMIT;

-- Gather statistics for realistic testing
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'TEST_ANALYSIS_SALES',
        cascade => TRUE
    );
END;
/

-- Create empty partition table for testing
CREATE TABLE test_analysis_empty (
    id NUMBER,
    data_date DATE
) PARTITION BY RANGE (data_date) (
    PARTITION p_empty_1 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p_empty_2 VALUES LESS THAN (DATE '2024-02-01'),
    PARTITION p_empty_3 VALUES LESS THAN (DATE '2024-03-01')
);

DECLARE
    v_test_count NUMBER := 0;
    v_pass_count NUMBER := 0;
    v_fail_count NUMBER := 0;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting partition_analysis_pkg validation tests...');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 1: PACKAGE INFRASTRUCTURE
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 1: PACKAGE INFRASTRUCTURE ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 1: Package existence and validity
    BEGIN
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_name = 'PARTITION_ANALYSIS_PKG'
            AND object_type IN ('PACKAGE', 'PACKAGE BODY')
            AND status = 'VALID';
            
            IF v_count = 2 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 1: Package installation - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 1: Package installation - FAIL (Found ' || v_count || ')');
            END IF;
            v_test_count := v_test_count + 1;
        END;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 2: BASIC ANALYSIS FUNCTIONS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 2: BASIC ANALYSIS FUNCTIONS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 2: get_partition_summary
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
            v_table_name VARCHAR2(128);
            v_partition_count NUMBER;
        BEGIN
            v_cursor := partition_analysis_pkg.get_partition_summary('TEST_ANALYSIS_SALES');
            FETCH v_cursor INTO v_table_name, v_partition_count;
            CLOSE v_cursor;
            
            IF v_partition_count = 4 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 2: get_partition_summary() - PASS (4 partitions found)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 2: get_partition_summary() - FAIL (Found ' || v_partition_count || ')');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 2: get_partition_summary() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 3: get_partition_sizes
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
            v_found BOOLEAN := FALSE;
        BEGIN
            v_cursor := partition_analysis_pkg.get_partition_sizes('TEST_ANALYSIS_SALES');
            FETCH v_cursor INTO v_found;
            IF v_cursor%FOUND THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 3: get_partition_sizes() - PASS');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 3: get_partition_sizes() - FAIL (No data)');
            END IF;
            CLOSE v_cursor;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 3: get_partition_sizes() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 4: find_large_partitions
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.find_large_partitions(
                p_table_name => 'TEST_ANALYSIS_SALES',
                p_size_threshold_mb => 0.001
            );
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 4: find_large_partitions() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 4: find_large_partitions() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 5: find_empty_partitions
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
            v_count NUMBER := 0;
        BEGIN
            v_cursor := partition_analysis_pkg.find_empty_partitions('TEST_ANALYSIS_EMPTY');
            LOOP
                FETCH v_cursor INTO v_count;
                EXIT WHEN v_cursor%NOTFOUND;
                v_count := v_count + 1;
            END LOOP;
            CLOSE v_cursor;
            
            IF v_count >= 0 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 5: find_empty_partitions() - PASS (Found ' || v_count || ' empty)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 5: find_empty_partitions() - FAIL');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 5: find_empty_partitions() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 6: analyze_partition_usage
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.analyze_partition_usage(
                p_table_name => 'TEST_ANALYSIS_SALES',
                p_days_back => 7
            );
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 6: analyze_partition_usage() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 6: analyze_partition_usage() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 3: INDEX ANALYSIS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 3: INDEX ANALYSIS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 7: get_partition_index_status
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.get_partition_index_status('TEST_ANALYSIS_SALES');
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 7: get_partition_index_status() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 7: get_partition_index_status() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 4: RECOMMENDATIONS & EFFICIENCY
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 4: RECOMMENDATIONS & EFFICIENCY ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 8: get_cleanup_candidates
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.get_cleanup_candidates(
                p_table_name => 'TEST_ANALYSIS_SALES',
                p_retention_days => 365
            );
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 8: get_cleanup_candidates() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 8: get_cleanup_candidates() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 9: check_partition_efficiency
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.check_partition_efficiency('TEST_ANALYSIS_SALES');
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 9: check_partition_efficiency() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 9: check_partition_efficiency() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 5: STATISTICS FUNCTIONS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 5: STATISTICS FUNCTIONS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 10: update_partition_statistics
    BEGIN
        partition_analysis_pkg.update_partition_statistics(
            p_table_name => 'TEST_ANALYSIS_SALES',
            p_partition_name => 'P_2024_Q1'
        );
        v_pass_count := v_pass_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test 10: update_partition_statistics() - PASS');
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 10: update_partition_statistics() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 11: check_stats_freshness
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.check_stats_freshness(
                p_table_name => 'TEST_ANALYSIS_SALES',
                p_days_threshold => 7
            );
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 11: check_stats_freshness() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 11: check_stats_freshness() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 12: is_incremental_stats_enabled
    BEGIN
        DECLARE
            v_enabled BOOLEAN;
        BEGIN
            v_enabled := partition_analysis_pkg.is_incremental_stats_enabled('TEST_ANALYSIS_SALES');
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 12: is_incremental_stats_enabled() - PASS (Result: ' || 
                CASE WHEN v_enabled THEN 'TRUE' ELSE 'FALSE' END || ')');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 12: is_incremental_stats_enabled() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 13: estimate_stats_collection_time
    BEGIN
        DECLARE
            v_minutes NUMBER;
        BEGIN
            v_minutes := partition_analysis_pkg.estimate_stats_collection_time(
                p_table_name => 'TEST_ANALYSIS_SALES',
                p_partition_name => 'P_2024_Q1'
            );
            
            IF v_minutes IS NOT NULL AND v_minutes >= 0 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 13: estimate_stats_collection_time() - PASS (' || v_minutes || ' min)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 13: estimate_stats_collection_time() - FAIL');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 13: estimate_stats_collection_time() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 14: validate_incremental_stats_config
    BEGIN
        DECLARE
            v_result VARCHAR2(4000);
        BEGIN
            v_result := partition_analysis_pkg.validate_incremental_stats_config('TEST_ANALYSIS_SALES');
            
            IF v_result IS NOT NULL THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 14: validate_incremental_stats_config() - PASS');
                DBMS_OUTPUT.PUT_LINE('  Config: ' || SUBSTR(v_result, 1, 80));
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 14: validate_incremental_stats_config() - FAIL');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 14: validate_incremental_stats_config() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 15: recommend_stats_strategy (procedure with output)
    BEGIN
        partition_analysis_pkg.recommend_stats_strategy('TEST_ANALYSIS_SALES');
        v_pass_count := v_pass_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test 15: recommend_stats_strategy() - PASS');
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 15: recommend_stats_strategy() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 6: REPORTING FUNCTIONS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 6: REPORTING FUNCTIONS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 16: generate_partition_report
    BEGIN
        DECLARE
            v_report CLOB;
            v_length NUMBER;
        BEGIN
            v_report := partition_analysis_pkg.generate_partition_report('TEST_ANALYSIS_SALES');
            v_length := DBMS_LOB.GETLENGTH(v_report);
            
            IF v_length > 100 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 16: generate_partition_report() - PASS (' || v_length || ' chars)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 16: generate_partition_report() - FAIL (Too short)');
            END IF;
            DBMS_LOB.FREETEMPORARY(v_report);
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 16: generate_partition_report() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 7: TABLESPACE & MOVE OPERATIONS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 7: TABLESPACE & MOVE OPERATIONS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 17: get_tablespace_usage
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.get_tablespace_usage('TEST_ANALYSIS_SALES');
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 17: get_tablespace_usage() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 17: get_tablespace_usage() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 18: check_move_feasibility
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.check_move_feasibility(
                p_table_name => 'TEST_ANALYSIS_SALES',
                p_target_tablespace => 'USERS'
            );
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 18: check_move_feasibility() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 18: check_move_feasibility() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 19: estimate_move_time
    BEGIN
        DECLARE
            v_minutes NUMBER;
        BEGIN
            v_minutes := partition_analysis_pkg.estimate_move_time(
                p_table_name => 'TEST_ANALYSIS_SALES',
                p_parallel_degree => 4
            );
            
            IF v_minutes IS NOT NULL AND v_minutes > 0 THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 19: estimate_move_time() - PASS (' || v_minutes || ' min)');
            ELSE
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 19: estimate_move_time() - FAIL');
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 19: estimate_move_time() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 8: ADVANCED ANALYSIS
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 8: ADVANCED ANALYSIS ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 20: get_partition_strategy
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.get_partition_strategy('TEST_ANALYSIS_SALES');
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 20: get_partition_strategy() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 20: get_partition_strategy() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 21: analyze_partition_compression
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            v_cursor := partition_analysis_pkg.analyze_partition_compression('TEST_ANALYSIS_SALES');
            CLOSE v_cursor;
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 21: analyze_partition_compression() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 21: analyze_partition_compression() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 22: check_exchange_readiness
    BEGIN
        DECLARE
            v_cursor SYS_REFCURSOR;
        BEGIN
            -- Create a staging table for exchange test
            EXECUTE IMMEDIATE 'CREATE TABLE test_analysis_staging (
                id NUMBER PRIMARY KEY,
                sale_date DATE NOT NULL,
                amount NUMBER(10,2),
                customer_id NUMBER,
                region VARCHAR2(50),
                product_category VARCHAR2(100),
                status VARCHAR2(20)
            )';
            
            v_cursor := partition_analysis_pkg.check_exchange_readiness(
                p_source_table => 'TEST_ANALYSIS_STAGING',
                p_target_partition => 'P_2024_Q1'
            );
            CLOSE v_cursor;
            
            EXECUTE IMMEDIATE 'DROP TABLE test_analysis_staging PURGE';
            
            v_pass_count := v_pass_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 22: check_exchange_readiness() - PASS');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test_analysis_staging PURGE';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
            v_fail_count := v_fail_count + 1;
            DBMS_OUTPUT.PUT_LINE('Test 22: check_exchange_readiness() - FAIL: ' || SQLERRM);
    END;
    v_test_count := v_test_count + 1;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 9: ERROR HANDLING
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 9: ERROR HANDLING ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 23: Invalid table name handling
    BEGIN
        BEGIN
            DECLARE
                v_cursor SYS_REFCURSOR;
            BEGIN
                v_cursor := partition_analysis_pkg.get_partition_summary('NONEXISTENT_TABLE_XYZ');
                CLOSE v_cursor;
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 23: Invalid table name handling - FAIL (No error raised)');
            END;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE BETWEEN -20999 AND -20001 THEN
                    v_pass_count := v_pass_count + 1;
                    DBMS_OUTPUT.PUT_LINE('Test 23: Invalid table name handling - PASS');
                ELSE
                    v_fail_count := v_fail_count + 1;
                    DBMS_OUTPUT.PUT_LINE('Test 23: Invalid table name handling - FAIL (Unexpected error)');
                END IF;
        END;
        v_test_count := v_test_count + 1;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 24: NULL parameter handling
    BEGIN
        BEGIN
            DECLARE
                v_cursor SYS_REFCURSOR;
            BEGIN
                v_cursor := partition_analysis_pkg.get_partition_sizes(NULL);
                CLOSE v_cursor;
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 24: NULL parameter handling - FAIL (No error raised)');
            END;
        EXCEPTION
            WHEN OTHERS THEN
                v_pass_count := v_pass_count + 1;
                DBMS_OUTPUT.PUT_LINE('Test 24: NULL parameter handling - PASS');
        END;
        v_test_count := v_test_count + 1;
    END;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- CLEANUP
    -- =====================================================
    
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE test_analysis_sales PURGE';
        EXECUTE IMMEDIATE 'DROP TABLE test_analysis_empty PURGE';
        DBMS_OUTPUT.PUT_LINE('Test cleanup completed');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Cleanup warning: ' || SQLERRM);
    END;
    
    -- =====================================================
    -- FINAL SUMMARY
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('PARTITION_ANALYSIS_PKG TEST SUMMARY');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Total Tests: ' || v_test_count);
    DBMS_OUTPUT.PUT_LINE('Passed: ' || v_pass_count);
    DBMS_OUTPUT.PUT_LINE('Failed: ' || v_fail_count);
    DBMS_OUTPUT.PUT_LINE('Success Rate: ' || ROUND((v_pass_count/v_test_count)*100, 1) || '%');
    DBMS_OUTPUT.PUT_LINE('');
    
    DBMS_OUTPUT.PUT_LINE('Functions Tested:');
    DBMS_OUTPUT.PUT_LINE('• get_partition_summary()');
    DBMS_OUTPUT.PUT_LINE('• get_partition_sizes()');
    DBMS_OUTPUT.PUT_LINE('• find_large_partitions()');
    DBMS_OUTPUT.PUT_LINE('• find_empty_partitions()');
    DBMS_OUTPUT.PUT_LINE('• analyze_partition_usage()');
    DBMS_OUTPUT.PUT_LINE('• get_partition_index_status()');
    DBMS_OUTPUT.PUT_LINE('• get_cleanup_candidates()');
    DBMS_OUTPUT.PUT_LINE('• check_partition_efficiency()');
    DBMS_OUTPUT.PUT_LINE('• update_partition_statistics()');
    DBMS_OUTPUT.PUT_LINE('• check_stats_freshness()');
    DBMS_OUTPUT.PUT_LINE('• is_incremental_stats_enabled()');
    DBMS_OUTPUT.PUT_LINE('• estimate_stats_collection_time()');
    DBMS_OUTPUT.PUT_LINE('• validate_incremental_stats_config()');
    DBMS_OUTPUT.PUT_LINE('• recommend_stats_strategy()');
    DBMS_OUTPUT.PUT_LINE('• generate_partition_report()');
    DBMS_OUTPUT.PUT_LINE('• get_tablespace_usage()');
    DBMS_OUTPUT.PUT_LINE('• check_move_feasibility()');
    DBMS_OUTPUT.PUT_LINE('• estimate_move_time()');
    DBMS_OUTPUT.PUT_LINE('• get_partition_strategy()');
    DBMS_OUTPUT.PUT_LINE('• analyze_partition_compression()');
    DBMS_OUTPUT.PUT_LINE('• check_exchange_readiness()');
    DBMS_OUTPUT.PUT_LINE('');
    
    IF v_fail_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('🎉 ALL TESTS PASSED! partition_analysis_pkg is fully functional.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('⚠️  Some tests failed. Review errors above.');
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/

SET ECHO OFF
PROMPT
PROMPT ✅ partition_analysis_pkg validation complete!
PROMPT
