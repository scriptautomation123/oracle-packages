-- =====================================================
-- QUICK PARTITION TABLE CREATION GUIDE
-- How to create tables with any partition type using your packages
-- =====================================================

SET SERVEROUTPUT ON SIZE 1000000

PROMPT ========================================
PROMPT QUICK PARTITION TABLE CREATION GUIDE
PROMPT Your Complete Partitioning Toolkit
PROMPT ========================================

-- =====================================================
-- METHOD 1: USING YOUR TABLE_DDL_PKG (RECOMMENDED)
-- =====================================================

PROMPT
PROMPT Method 1: Using table_ddl_pkg for complete table creation
PROMPT

-- Example: Create a sales table with RANGE partitioning
DECLARE
    v_columns column_def_array;
    v_constraints constraint_def_array;
    v_partitions partition_def_array;
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== CREATING RANGE PARTITIONED SALES TABLE ===');
    
    -- Define columns
    v_columns := column_def_array(
        column_def('sale_id', 'NUMBER', 0, 10, 0, FALSE, NULL, FALSE, FALSE, 'Primary key'),
        column_def('sale_date', 'DATE', 0, 0, 0, FALSE, NULL, FALSE, FALSE, 'Partition key'),
        column_def('customer_id', 'NUMBER', 0, 10, 0, FALSE, NULL, FALSE, FALSE, 'Customer reference'),
        column_def('amount', 'NUMBER', 0, 10, 2, FALSE, '0', FALSE, FALSE, 'Sale amount'),
        column_def('region', 'VARCHAR2', 50, 0, 0, TRUE, '''UNKNOWN''', FALSE, FALSE, 'Sales region')
    );
    
    -- Define constraints
    v_constraints := constraint_def_array(
        constraint_def('pk_sales', 'PRIMARY', 'sale_id', NULL, NULL, NULL, FALSE, FALSE)
    );
    
    -- Define partitions
    v_partitions := partition_def_array(
        partition_def('p_2024_q1', 'RANGE', 'sale_date', 'TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')', 'USERS', NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE),
        partition_def('p_2024_q2', 'RANGE', 'sale_date', 'TO_DATE(''2024-07-01'', ''YYYY-MM-DD'')', 'USERS', NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE),
        partition_def('p_2024_q3', 'RANGE', 'sale_date', 'TO_DATE(''2024-10-01'', ''YYYY-MM-DD'')', 'USERS', NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE),
        partition_def('p_future', 'RANGE', 'sale_date', 'MAXVALUE', 'USERS', NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE)
    );
    
    -- Generate and execute DDL
    v_ddl := table_ddl_pkg.generate_partitioned_table_ddl('my_sales_table', v_columns, v_constraints, v_partitions);
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL:');
    DBMS_OUTPUT.PUT_LINE(v_ddl);
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Execute the DDL
    EXECUTE IMMEDIATE v_ddl;
    
    DBMS_OUTPUT.PUT_LINE('✓ Table my_sales_table created successfully!');
    
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('⚠ Table already exists');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ Error: ' || SQLERRM);
        END IF;
END;
/

-- =====================================================
-- METHOD 2: USING TABLE_OPS_PKG FOR EXISTING TABLES
-- =====================================================

PROMPT
PROMPT Method 2: Convert existing table to partitioned using table_ops_pkg
PROMPT

-- First create a regular table
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE regular_orders CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE regular_orders (
    order_id NUMBER PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id NUMBER,
    status VARCHAR2(20) DEFAULT 'PENDING'
);

-- Insert sample data
INSERT INTO regular_orders VALUES (1, DATE '2024-01-15', 101, 'COMPLETE');
INSERT INTO regular_orders VALUES (2, DATE '2024-05-20', 102, 'PENDING');
INSERT INTO regular_orders VALUES (3, DATE '2024-08-10', 103, 'SHIPPED');
COMMIT;

-- Convert to partitioned table
DECLARE
    v_partition_strategy table_ops_pkg.partitioning_strategy_rec;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== CONVERTING REGULAR TABLE TO PARTITIONED ===');
    
    -- Define partitioning strategy
    v_partition_strategy.partition_type := 'RANGE';
    v_partition_strategy.partition_column := 'order_date';
    v_partition_strategy.partition_count := 4;
    v_partition_strategy.tablespace_name := 'USERS';
    
    -- Convert the table
    table_ops_pkg.convert_to_partitioned(
        p_table_name => 'regular_orders',
        p_strategy => v_partition_strategy
    );
    
    DBMS_OUTPUT.PUT_LINE('✓ Table converted to partitioned successfully!');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Conversion error: ' || SQLERRM);
END;
/

-- =====================================================
-- METHOD 3: STANDARD DDL WITH PARTITION TEMPLATE
-- =====================================================

PROMPT
PROMPT Method 3: Standard DDL templates for common partition types
PROMPT

-- Template 1: Simple INTERVAL partitioned table
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE logs_interval CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- INTERVAL partitioned table - auto-creates daily partitions
CREATE TABLE logs_interval (
    log_id NUMBER GENERATED ALWAYS AS IDENTITY,
    log_date DATE NOT NULL,
    log_level VARCHAR2(10) DEFAULT 'INFO',
    message CLOB
) PARTITION BY RANGE (log_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION p_initial VALUES LESS THAN (DATE '2025-01-01')
) TABLESPACE users;

PROMPT ✓ INTERVAL partitioned logs table created

-- Template 2: LIST partitioned table with AUTO
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE products_autolist CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- AUTO LIST partitioned table (Oracle 19c) - auto-creates partitions for new values
CREATE TABLE products_autolist (
    product_id NUMBER PRIMARY KEY,
    category VARCHAR2(50) NOT NULL,
    product_name VARCHAR2(200),
    price NUMBER(10,2)
) PARTITION BY LIST (category) AUTOMATIC
(
    PARTITION p_auto VALUES ('ELECTRONICS')
) TABLESPACE users;

PROMPT ✓ AUTO LIST partitioned products table created

-- Template 3: Composite RANGE-HASH partitioned table
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE transactions_composite CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE transactions_composite (
    transaction_id NUMBER PRIMARY KEY,
    transaction_date DATE NOT NULL,
    account_id NUMBER NOT NULL,
    amount NUMBER(10,2)
) PARTITION BY RANGE (transaction_date)
SUBPARTITION BY HASH (account_id) SUBPARTITIONS 4
(
    PARTITION p_2024 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION p_2025 VALUES LESS THAN (DATE '2026-01-01'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
) TABLESPACE users;

PROMPT ✓ Composite RANGE-HASH partitioned transactions table created

-- =====================================================
-- QUICK REFERENCE: PARTITION TYPE SELECTION GUIDE
-- =====================================================

PROMPT
PROMPT ========================================
PROMPT PARTITION TYPE SELECTION GUIDE
PROMPT ========================================
PROMPT
PROMPT 📅 TIME-SERIES DATA (dates, timestamps):
PROMPT   → Use RANGE partitioning for historical data
PROMPT   → Use INTERVAL partitioning for continuous data
PROMPT   Example: sales by date, logs by timestamp
PROMPT
PROMPT 🏷️  CATEGORICAL DATA (regions, status, types):
PROMPT   → Use LIST partitioning for known values
PROMPT   → Use AUTO_LIST for dynamic categories (Oracle 19c)
PROMPT   Example: sales by region, orders by status
PROMPT
PROMPT ⚖️  LOAD BALANCING (user IDs, hash keys):
PROMPT   → Use HASH partitioning for even distribution
PROMPT   → Consider with HIGH DML workloads
PROMPT   Example: users by ID, sessions by hash
PROMPT
PROMPT 🔗 PARENT-CHILD RELATIONSHIPS:
PROMPT   → Use REFERENCE partitioning
PROMPT   → Child table inherits parent's partitioning
PROMPT   Example: orders -> order_items
PROMPT
PROMPT 🎯 MIXED REQUIREMENTS:
PROMPT   → Use COMPOSITE partitioning (2-level)
PROMPT   → Combine time + distribution or category + load balancing
PROMPT   Example: RANGE(date) + HASH(customer_id)
PROMPT
PROMPT 📊 APPLICATION-CONTROLLED:
PROMPT   → Use SYSTEM partitioning
PROMPT   → Full application control over data placement
PROMPT   Example: data archival, special handling
PROMPT
PROMPT ========================================

-- =====================================================
-- PRACTICAL EXAMPLES FOR COMMON USE CASES
-- =====================================================

PROMPT
PROMPT ========================================
PROMPT COMMON USE CASE EXAMPLES
PROMPT ========================================

-- Use Case 1: E-commerce Order Management
PROMPT
PROMPT Use Case 1: E-commerce Order Management
PROMPT Requirement: Query by date ranges, even load distribution

DECLARE
    v_ddl_template VARCHAR2(4000);
BEGIN
    v_ddl_template := q'[
CREATE TABLE ecommerce_orders (
    order_id NUMBER PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id NUMBER NOT NULL,
    order_status VARCHAR2(20),
    total_amount NUMBER(10,2)
) PARTITION BY RANGE (order_date)
SUBPARTITION BY HASH (customer_id) SUBPARTITIONS 4
(
    PARTITION p_2024_q1 VALUES LESS THAN (DATE '2024-04-01'),
    PARTITION p_2024_q2 VALUES LESS THAN (DATE '2024-07-01'),
    PARTITION p_2024_q3 VALUES LESS THAN (DATE '2024-10-01'),
    PARTITION p_2024_q4 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);]';
    
    DBMS_OUTPUT.PUT_LINE('Recommended: RANGE-HASH composite partitioning');
    DBMS_OUTPUT.PUT_LINE('Benefits: Date-based queries + load distribution');
    DBMS_OUTPUT.PUT_LINE(v_ddl_template);
END;
/

-- Use Case 2: IoT Sensor Data
PROMPT
PROMPT Use Case 2: IoT Sensor Data  
PROMPT Requirement: High-volume inserts, time-based queries

DECLARE
    v_ddl_template VARCHAR2(4000);
BEGIN
    v_ddl_template := q'[
CREATE TABLE iot_sensor_data (
    sensor_id NUMBER NOT NULL,
    reading_time TIMESTAMP NOT NULL,
    sensor_value NUMBER(10,3),
    location_id NUMBER
) PARTITION BY RANGE (reading_time)
INTERVAL (NUMTODSINTERVAL(1, 'HOUR'))
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2025-01-01 00:00:00')
);]';
    
    DBMS_OUTPUT.PUT_LINE('Recommended: INTERVAL partitioning');
    DBMS_OUTPUT.PUT_LINE('Benefits: Auto-partition creation, optimal for continuous data');
    DBMS_OUTPUT.PUT_LINE(v_ddl_template);
END;
/

-- Use Case 3: Multi-tenant SaaS Application
PROMPT
PROMPT Use Case 3: Multi-tenant SaaS Application
PROMPT Requirement: Tenant isolation, performance

DECLARE
    v_ddl_template VARCHAR2(4000);
BEGIN
    v_ddl_template := q'[
CREATE TABLE saas_user_data (
    user_id NUMBER NOT NULL,
    tenant_id VARCHAR2(50) NOT NULL,
    created_date DATE,
    user_data JSON
) PARTITION BY LIST (tenant_id)
(
    PARTITION p_tenant_a VALUES ('TENANT_A'),
    PARTITION p_tenant_b VALUES ('TENANT_B'),
    PARTITION p_tenant_c VALUES ('TENANT_C'),
    PARTITION p_default VALUES (DEFAULT)
);]';
    
    DBMS_OUTPUT.PUT_LINE('Recommended: LIST partitioning by tenant');
    DBMS_OUTPUT.PUT_LINE('Benefits: Tenant isolation, easier maintenance');
    DBMS_OUTPUT.PUT_LINE(v_ddl_template);
END;
/

PROMPT
PROMPT ========================================
PROMPT SUMMARY: YES, YOU CAN CREATE ANY PARTITION TYPE!
PROMPT ========================================
PROMPT
PROMPT 🎯 YOUR TOOLKIT SUPPORTS:
PROMPT   ✅ All Oracle 19c+ partition types (17+ variations)
PROMPT   ✅ Composite partitioning (2-level combinations)
PROMPT   ✅ Modern features (JSON, AUTO_LIST, HYBRID)
PROMPT   ✅ Automated DDL generation
PROMPT   ✅ Table conversion utilities
PROMPT   ✅ Best practices integration
PROMPT
PROMPT 🚀 RECOMMENDED APPROACH:
PROMPT   1. Use table_ddl_pkg for new tables (most flexible)
PROMPT   2. Use table_ops_pkg for converting existing tables
PROMPT   3. Use templates for quick standard patterns
PROMPT   4. Always configure Oracle 19c statistics optimization
PROMPT
PROMPT 💡 NEXT STEPS:
PROMPT   → Choose partition type based on your data access patterns
PROMPT   → Use the examples above as templates
PROMPT   → Test with your actual data volumes
PROMPT   → Monitor performance and adjust as needed
PROMPT
PROMPT ========================================