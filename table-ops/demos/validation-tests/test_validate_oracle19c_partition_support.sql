-- =====================================================
-- Comprehensive Test Suite for Oracle 19c Partition Support
-- Tests all supported partition types and composite combinations
-- Author: Principal Oracle Database Application Engineer  
-- Version: 2.0 (Oracle 19c Complete Support)
-- =====================================================

SET ECHO ON
SET FEEDBACK ON
SET SERVEROUTPUT ON SIZE 1000000
SET LINESIZE 200

PROMPT ========================================
PROMPT Comprehensive Test Suite for Oracle 19c Partition Support
PROMPT Testing: All 15 Partition Types & Combinations
PROMPT ========================================

-- Cleanup previous test objects
BEGIN
    FOR rec IN (
        SELECT table_name 
        FROM user_tables 
        WHERE table_name LIKE 'TEST_PART_%'
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || rec.table_name || ' PURGE';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;
/

DECLARE
    v_test_count NUMBER := 0;
    v_pass_count NUMBER := 0;
    v_fail_count NUMBER := 0;
    
    PROCEDURE test_partition_type(
        p_test_name VARCHAR2,
        p_table_name VARCHAR2,
        p_ddl CLOB
    ) IS
    BEGIN
        v_test_count := v_test_count + 1;
        DBMS_OUTPUT.PUT_LINE('Test ' || v_test_count || ': ' || p_test_name);
        
        BEGIN
            -- Execute DDL
            EXECUTE IMMEDIATE p_ddl;
            
            -- Verify table exists and is partitioned
            DECLARE
                v_partitioned NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_partitioned
                FROM user_part_tables
                WHERE table_name = UPPER(p_table_name);
                
                IF v_partitioned > 0 THEN
                    v_pass_count := v_pass_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  Result: PASS');
                ELSE
                    v_fail_count := v_fail_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  Result: FAIL (Not partitioned)');
                END IF;
            END;
            
        EXCEPTION
            WHEN OTHERS THEN
                v_fail_count := v_fail_count + 1;
                DBMS_OUTPUT.PUT_LINE('  Result: FAIL - ' || SQLERRM);
        END;
        DBMS_OUTPUT.PUT_LINE('');
    END;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting Oracle 19c partition support validation...');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- =====================================================
    -- SECTION 1: SINGLE-LEVEL PARTITIONING (6 Types)
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 1: SINGLE-LEVEL PARTITIONING ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 1: RANGE Partitioning
    test_partition_type(
        'RANGE Partitioning',
        'test_part_range',
        'CREATE TABLE test_part_range (
            id NUMBER,
            sale_date DATE,
            amount NUMBER(10,2)
        ) PARTITION BY RANGE (sale_date) (
            PARTITION p_2024_q1 VALUES LESS THAN (DATE ''2024-04-01''),
            PARTITION p_2024_q2 VALUES LESS THAN (DATE ''2024-07-01''),
            PARTITION p_2024_q3 VALUES LESS THAN (DATE ''2024-10-01''),
            PARTITION p_2024_q4 VALUES LESS THAN (DATE ''2025-01-01'')
        )'
    );
    
    -- Test 2: LIST Partitioning
    test_partition_type(
        'LIST Partitioning',
        'test_part_list',
        'CREATE TABLE test_part_list (
            id NUMBER,
            region VARCHAR2(20),
            sales NUMBER(10,2)
        ) PARTITION BY LIST (region) (
            PARTITION p_north VALUES (''NORTH'', ''NORTHEAST''),
            PARTITION p_south VALUES (''SOUTH'', ''SOUTHEAST''),
            PARTITION p_west VALUES (''WEST'', ''NORTHWEST''),
            PARTITION p_east VALUES (''EAST'')
        )'
    );
    
    -- Test 3: HASH Partitioning
    test_partition_type(
        'HASH Partitioning',
        'test_part_hash',
        'CREATE TABLE test_part_hash (
            id NUMBER,
            customer_id NUMBER,
            data VARCHAR2(100)
        ) PARTITION BY HASH (customer_id) PARTITIONS 8'
    );
    
    -- Test 4: INTERVAL Partitioning (Oracle 11g+)
    test_partition_type(
        'INTERVAL Partitioning',
        'test_part_interval',
        'CREATE TABLE test_part_interval (
            id NUMBER,
            log_date DATE,
            message VARCHAR2(500)
        ) PARTITION BY RANGE (log_date)
        INTERVAL (NUMTOYMINTERVAL(1, ''MONTH'')) (
            PARTITION p_base VALUES LESS THAN (DATE ''2024-01-01'')
        )'
    );
    
    -- Test 5: REFERENCE Partitioning (requires parent table)
    BEGIN
        -- Create parent table first
        EXECUTE IMMEDIATE 'CREATE TABLE test_part_customers (
            customer_id NUMBER PRIMARY KEY,
            region VARCHAR2(20)
        ) PARTITION BY LIST (region) (
            PARTITION p_north VALUES (''NORTH''),
            PARTITION p_south VALUES (''SOUTH''),
            PARTITION p_east VALUES (''EAST''),
            PARTITION p_west VALUES (''WEST'')
        )';
        
        test_partition_type(
            'REFERENCE Partitioning',
            'test_part_reference',
            'CREATE TABLE test_part_reference (
                order_id NUMBER,
                customer_id NUMBER,
                order_date DATE,
                CONSTRAINT fk_ref_customer 
                    FOREIGN KEY (customer_id) 
                    REFERENCES test_part_customers (customer_id)
            ) PARTITION BY REFERENCE (fk_ref_customer)'
        );
    END;
    
    -- Test 6: AUTO_LIST Partitioning (Oracle 19c)
    test_partition_type(
        'AUTO_LIST Partitioning (19c)',
        'test_part_autolist',
        'CREATE TABLE test_part_autolist (
            id NUMBER,
            category VARCHAR2(50),
            value NUMBER
        ) PARTITION BY LIST (category) AUTOMATIC (
            PARTITION p_default VALUES (DEFAULT)
        )'
    );
    
    -- =====================================================
    -- SECTION 2: COMPOSITE PARTITIONING (9 Combinations)
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 2: COMPOSITE PARTITIONING ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 7: RANGE-RANGE Composite
    test_partition_type(
        'RANGE-RANGE Composite',
        'test_part_range_range',
        'CREATE TABLE test_part_range_range (
            id NUMBER,
            sale_date DATE,
            delivery_date DATE,
            amount NUMBER
        ) PARTITION BY RANGE (sale_date)
        SUBPARTITION BY RANGE (delivery_date) (
            PARTITION p_2024_q1 VALUES LESS THAN (DATE ''2024-04-01'') (
                SUBPARTITION sp_q1_early VALUES LESS THAN (DATE ''2024-02-15''),
                SUBPARTITION sp_q1_late VALUES LESS THAN (MAXVALUE)
            ),
            PARTITION p_2024_q2 VALUES LESS THAN (DATE ''2024-07-01'') (
                SUBPARTITION sp_q2_early VALUES LESS THAN (DATE ''2024-05-15''),
                SUBPARTITION sp_q2_late VALUES LESS THAN (MAXVALUE)
            )
        )'
    );
    
    -- Test 8: RANGE-HASH Composite (Most Common)
    test_partition_type(
        'RANGE-HASH Composite',
        'test_part_range_hash',
        'CREATE TABLE test_part_range_hash (
            id NUMBER,
            sale_date DATE,
            customer_id NUMBER,
            amount NUMBER
        ) PARTITION BY RANGE (sale_date)
        SUBPARTITION BY HASH (customer_id) SUBPARTITIONS 4 (
            PARTITION p_2024_q1 VALUES LESS THAN (DATE ''2024-04-01''),
            PARTITION p_2024_q2 VALUES LESS THAN (DATE ''2024-07-01''),
            PARTITION p_2024_q3 VALUES LESS THAN (DATE ''2024-10-01'')
        )'
    );
    
    -- Test 9: RANGE-LIST Composite
    test_partition_type(
        'RANGE-LIST Composite',
        'test_part_range_list',
        'CREATE TABLE test_part_range_list (
            id NUMBER,
            sale_date DATE,
            status VARCHAR2(20),
            amount NUMBER
        ) PARTITION BY RANGE (sale_date)
        SUBPARTITION BY LIST (status) (
            PARTITION p_2024_q1 VALUES LESS THAN (DATE ''2024-04-01'') (
                SUBPARTITION sp_q1_active VALUES (''ACTIVE'', ''PENDING''),
                SUBPARTITION sp_q1_closed VALUES (''CLOSED'', ''CANCELLED'')
            ),
            PARTITION p_2024_q2 VALUES LESS THAN (DATE ''2024-07-01'') (
                SUBPARTITION sp_q2_active VALUES (''ACTIVE'', ''PENDING''),
                SUBPARTITION sp_q2_closed VALUES (''CLOSED'', ''CANCELLED'')
            )
        )'
    );
    
    -- Test 10: LIST-RANGE Composite
    test_partition_type(
        'LIST-RANGE Composite',
        'test_part_list_range',
        'CREATE TABLE test_part_list_range (
            id NUMBER,
            region VARCHAR2(20),
            sale_date DATE,
            amount NUMBER
        ) PARTITION BY LIST (region)
        SUBPARTITION BY RANGE (sale_date) (
            PARTITION p_north VALUES (''NORTH'') (
                SUBPARTITION sp_north_q1 VALUES LESS THAN (DATE ''2024-04-01''),
                SUBPARTITION sp_north_q2 VALUES LESS THAN (DATE ''2024-07-01'')
            ),
            PARTITION p_south VALUES (''SOUTH'') (
                SUBPARTITION sp_south_q1 VALUES LESS THAN (DATE ''2024-04-01''),
                SUBPARTITION sp_south_q2 VALUES LESS THAN (DATE ''2024-07-01'')
            )
        )'
    );
    
    -- Test 11: LIST-HASH Composite
    test_partition_type(
        'LIST-HASH Composite',
        'test_part_list_hash',
        'CREATE TABLE test_part_list_hash (
            id NUMBER,
            region VARCHAR2(20),
            customer_id NUMBER,
            amount NUMBER
        ) PARTITION BY LIST (region)
        SUBPARTITION BY HASH (customer_id) SUBPARTITIONS 4 (
            PARTITION p_north VALUES (''NORTH''),
            PARTITION p_south VALUES (''SOUTH''),
            PARTITION p_east VALUES (''EAST''),
            PARTITION p_west VALUES (''WEST'')
        )'
    );
    
    -- Test 12: LIST-LIST Composite
    test_partition_type(
        'LIST-LIST Composite',
        'test_part_list_list',
        'CREATE TABLE test_part_list_list (
            id NUMBER,
            region VARCHAR2(20),
            category VARCHAR2(30),
            amount NUMBER
        ) PARTITION BY LIST (region)
        SUBPARTITION BY LIST (category) (
            PARTITION p_north VALUES (''NORTH'') (
                SUBPARTITION sp_north_electronics VALUES (''ELECTRONICS'', ''COMPUTERS''),
                SUBPARTITION sp_north_furniture VALUES (''FURNITURE'', ''HOME'')
            ),
            PARTITION p_south VALUES (''SOUTH'') (
                SUBPARTITION sp_south_electronics VALUES (''ELECTRONICS'', ''COMPUTERS''),
                SUBPARTITION sp_south_furniture VALUES (''FURNITURE'', ''HOME'')
            )
        )'
    );
    
    -- Test 13: HASH-RANGE Composite (Oracle 19c Enhancement)
    test_partition_type(
        'HASH-RANGE Composite (19c)',
        'test_part_hash_range',
        'CREATE TABLE test_part_hash_range (
            id NUMBER,
            customer_id NUMBER,
            sale_date DATE,
            amount NUMBER
        ) PARTITION BY HASH (customer_id)
        SUBPARTITION BY RANGE (sale_date) 
        SUBPARTITION TEMPLATE (
            SUBPARTITION sp_q1 VALUES LESS THAN (DATE ''2024-04-01''),
            SUBPARTITION sp_q2 VALUES LESS THAN (DATE ''2024-07-01''),
            SUBPARTITION sp_q3 VALUES LESS THAN (DATE ''2024-10-01''),
            SUBPARTITION sp_q4 VALUES LESS THAN (MAXVALUE)
        ) PARTITIONS 4'
    );
    
    -- Test 14: HASH-HASH Composite (Oracle 19c Enhancement)
    test_partition_type(
        'HASH-HASH Composite (19c)',
        'test_part_hash_hash',
        'CREATE TABLE test_part_hash_hash (
            id NUMBER,
            customer_id NUMBER,
            product_id NUMBER,
            amount NUMBER
        ) PARTITION BY HASH (customer_id)
        SUBPARTITION BY HASH (product_id) 
        SUBPARTITIONS 4 
        PARTITIONS 8'
    );
    
    -- Test 15: HASH-LIST Composite (Oracle 19c Enhancement)
    test_partition_type(
        'HASH-LIST Composite (19c)',
        'test_part_hash_list',
        'CREATE TABLE test_part_hash_list (
            id NUMBER,
            customer_id NUMBER,
            status VARCHAR2(20),
            amount NUMBER
        ) PARTITION BY HASH (customer_id)
        SUBPARTITION BY LIST (status)
        SUBPARTITION TEMPLATE (
            SUBPARTITION sp_active VALUES (''ACTIVE'', ''PENDING''),
            SUBPARTITION sp_closed VALUES (''CLOSED'', ''CANCELLED''),
            SUBPARTITION sp_other VALUES (DEFAULT)
        ) PARTITIONS 4'
    );
    
    -- =====================================================
    -- SECTION 3: ADVANCED ORACLE 19C FEATURES
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 3: ADVANCED ORACLE 19C FEATURES ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Test 16: Multi-Column LIST Partitioning
    test_partition_type(
        'Multi-Column LIST Partitioning',
        'test_part_multicolumn',
        'CREATE TABLE test_part_multicolumn (
            id NUMBER,
            country VARCHAR2(10),
            state VARCHAR2(10),
            city VARCHAR2(50),
            sales NUMBER
        ) PARTITION BY LIST (country, state) (
            PARTITION p_usa_east VALUES ((''USA'', ''NY''), (''USA'', ''NJ''), (''USA'', ''CT'')),
            PARTITION p_usa_west VALUES ((''USA'', ''CA''), (''USA'', ''WA''), (''USA'', ''OR'')),
            PARTITION p_canada VALUES ((''CAN'', ''ON''), (''CAN'', ''BC''), (''CAN'', ''AB'')),
            PARTITION p_other VALUES (DEFAULT)
        )'
    );
    
    -- Test 17: Virtual Column Partitioning
    test_partition_type(
        'Virtual Column Partitioning',
        'test_part_virtual',
        'CREATE TABLE test_part_virtual (
            id NUMBER,
            sale_date DATE,
            amount NUMBER(10,2),
            sale_year AS (EXTRACT(YEAR FROM sale_date)) VIRTUAL
        ) PARTITION BY RANGE (sale_year) (
            PARTITION p_2023 VALUES LESS THAN (2024),
            PARTITION p_2024 VALUES LESS THAN (2025),
            PARTITION p_2025 VALUES LESS THAN (2026),
            PARTITION p_future VALUES LESS THAN (MAXVALUE)
        )'
    );
    
    -- Test 18: Interval-Reference Combination (Advanced)
    BEGIN
        -- Create interval-partitioned parent
        EXECUTE IMMEDIATE 'CREATE TABLE test_part_parent_interval (
            parent_id NUMBER PRIMARY KEY,
            created_date DATE
        ) PARTITION BY RANGE (created_date)
        INTERVAL (NUMTODSINTERVAL(1, ''DAY'')) (
            PARTITION p_base VALUES LESS THAN (DATE ''2024-01-01'')
        )';
        
        test_partition_type(
            'Interval-Reference Combination',
            'test_part_interval_ref',
            'CREATE TABLE test_part_interval_ref (
                child_id NUMBER,
                parent_id NUMBER,
                data VARCHAR2(100),
                CONSTRAINT fk_interval_ref 
                    FOREIGN KEY (parent_id) 
                    REFERENCES test_part_parent_interval (parent_id)
            ) PARTITION BY REFERENCE (fk_interval_ref)'
        );
    END;
    
    -- =====================================================
    -- SECTION 4: VALIDATION & SUMMARY
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('=== SECTION 4: VALIDATION SUMMARY ===');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Validate comprehensive partition support
    DECLARE
        v_total_partitioned NUMBER;
        v_total_subpartitioned NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_total_partitioned
        FROM user_part_tables
        WHERE table_name LIKE 'TEST_PART_%';
        
        SELECT COUNT(*) INTO v_total_subpartitioned
        FROM user_part_tables
        WHERE table_name LIKE 'TEST_PART_%'
        AND subpartitioning_type IS NOT NULL;
        
        DBMS_OUTPUT.PUT_LINE('Validation Results:');
        DBMS_OUTPUT.PUT_LINE('• Total partitioned tables created: ' || v_total_partitioned);
        DBMS_OUTPUT.PUT_LINE('• Total subpartitioned tables: ' || v_total_subpartitioned);
        DBMS_OUTPUT.PUT_LINE('');
    END;
    
    -- =====================================================
    -- CLEANUP
    -- =====================================================
    
    BEGIN
        FOR rec IN (
            SELECT table_name 
            FROM user_tables 
            WHERE table_name LIKE 'TEST_PART_%'
        ) LOOP
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE ' || rec.table_name || ' PURGE';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('Test cleanup completed');
    END;
    
    -- =====================================================
    -- FINAL SUMMARY
    -- =====================================================
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('ORACLE 19C PARTITION SUPPORT SUMMARY');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Total Tests: ' || v_test_count);
    DBMS_OUTPUT.PUT_LINE('Passed: ' || v_pass_count);
    DBMS_OUTPUT.PUT_LINE('Failed: ' || v_fail_count);
    DBMS_OUTPUT.PUT_LINE('Success Rate: ' || ROUND((v_pass_count/v_test_count)*100, 1) || '%');
    DBMS_OUTPUT.PUT_LINE('');
    
    DBMS_OUTPUT.PUT_LINE('Oracle 19c Partition Types Supported:');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('📋 Single-Level (6 types):');
    DBMS_OUTPUT.PUT_LINE('  • RANGE - Time-series, sequential data');
    DBMS_OUTPUT.PUT_LINE('  • LIST - Discrete values, categories');  
    DBMS_OUTPUT.PUT_LINE('  • HASH - Even distribution, load balancing');
    DBMS_OUTPUT.PUT_LINE('  • INTERVAL - Automatic range partition creation');
    DBMS_OUTPUT.PUT_LINE('  • REFERENCE - Parent-child relationships');
    DBMS_OUTPUT.PUT_LINE('  • AUTO_LIST - Automatic list partition (19c)');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('🔀 Composite (9 combinations):');
    DBMS_OUTPUT.PUT_LINE('  • RANGE-RANGE, RANGE-HASH, RANGE-LIST');
    DBMS_OUTPUT.PUT_LINE('  • LIST-RANGE, LIST-HASH, LIST-LIST');
    DBMS_OUTPUT.PUT_LINE('  • HASH-RANGE, HASH-HASH, HASH-LIST (19c)');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('⚡ Advanced Features:');
    DBMS_OUTPUT.PUT_LINE('  • Multi-column LIST partitioning');
    DBMS_OUTPUT.PUT_LINE('  • Virtual column partitioning');
    DBMS_OUTPUT.PUT_LINE('  • Interval-Reference combinations');
    DBMS_OUTPUT.PUT_LINE('  • Automatic partition creation');
    DBMS_OUTPUT.PUT_LINE('  • Subpartition templates');
    DBMS_OUTPUT.PUT_LINE('');
    
    IF v_fail_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('🎉 ALL PARTITION TYPES SUPPORTED! Oracle 19c comprehensive coverage complete.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('⚠️  Some partition types failed. Review errors above.');
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/

SET ECHO OFF
PROMPT
PROMPT ✅ Oracle 19c partition support validation complete!
PROMPT