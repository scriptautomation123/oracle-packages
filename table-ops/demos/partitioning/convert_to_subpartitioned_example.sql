-- =====================================================
-- EXAMPLE: Convert Table to Subpartitioned using table_ops_pkg
-- Convert CIM_OFM_SMG3_LOGGING to RANGE-HASH subpartitioned
-- =====================================================

SET SERVEROUTPUT ON SIZE 1000000

PROMPT ========================================
PROMPT Converting Table to Subpartitioned
PROMPT Using table_ops_pkg DDL Generation
PROMPT ========================================

-- =====================================================
-- METHOD 1: Generate DDL for Conversion (Recommended)
-- =====================================================

DECLARE
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== GENERATING CONVERSION DDL FOR SUBPARTITIONED TABLE ===');
    
    -- Generate DDL to convert existing table to partitioned with subpartitions
    v_ddl := table_ops_pkg.generate_convert_to_partitioned_ddl(
        p_table_name        => 'CIM_OFM_SMG3_LOGGING_SIMPLE',
        p_partition_type    => 'RANGE',           -- Primary partitioning by date
        p_partition_column  => 'AUDIT_CREATE_DATE',
        p_partition_count   => 4,                 -- Not used for RANGE, but required
        p_interval_expr     => 'NUMTODSINTERVAL(1,''DAY'')', -- Daily intervals
        p_reference_table   => NULL,              -- Not using reference partitioning
        p_parallel_degree   => 4                  -- Parallel processing
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL:');
    DBMS_OUTPUT.PUT_LINE('=====================================');
    DBMS_OUTPUT.PUT_LINE(v_ddl);
    DBMS_OUTPUT.PUT_LINE('=====================================');
    
END;
/

-- =====================================================
-- METHOD 2: Manual DDL Template for Your Exact Requirements
-- =====================================================

PROMPT
PROMPT Method 2: Complete DDL Template for Your Requirements
PROMPT

DECLARE
    v_manual_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== MANUAL DDL TEMPLATE FOR SUBPARTITIONED TABLE ===');
    
    -- This is the exact DDL you need for your requirements
    v_manual_ddl := q'[
-- Step 1: Create new subpartitioned table structure
CREATE TABLE CIM_OFM_SMG3_LOGGING_NEW (
    TRACE_ID VARCHAR2(36 BYTE),
    ALIAS VARCHAR2(8 BYTE),
    AUDIT_CREATE_DATE TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
    SMG3_LOGGING_DATA CLOB,
    FILTRATION_JSON_DATA CLOB
)
-- Primary partition by date (RANGE)
PARTITION BY RANGE (AUDIT_CREATE_DATE)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
-- Subpartition by hash on TRACE_ID across 2 tablespaces
SUBPARTITION BY HASH (TRACE_ID)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp1 TABLESPACE MAV_LOB,
    SUBPARTITION sp2 TABLESPACE MAV_HIST_LOB
)
(
    -- Initial partition before interval partitioning takes over
    PARTITION p_before_2025 VALUES LESS THAN (TIMESTAMP '2025-01-01 00:00:00')
)
-- LOB storage specifications
LOB (FILTRATION_JSON_DATA) STORE AS SECUREFILE (TABLESPACE MAV_LOB)
LOB (SMG3_LOGGING_DATA) STORE AS SECUREFILE (TABLESPACE MAV_HIST_LOB)
TABLESPACE DBRT_DATA;

-- Step 2: Copy data from old table to new table
INSERT /*+ PARALLEL(4) */ INTO CIM_OFM_SMG3_LOGGING_NEW
SELECT * FROM CIM_OFM_SMG3_LOGGING_SIMPLE;

-- Step 3: Create indexes on new table (if any exist on old table)
-- Add your index creation statements here

-- Step 4: Rename tables (during maintenance window)
ALTER TABLE CIM_OFM_SMG3_LOGGING_SIMPLE RENAME TO CIM_OFM_SMG3_LOGGING_OLD;
ALTER TABLE CIM_OFM_SMG3_LOGGING_NEW RENAME TO CIM_OFM_SMG3_LOGGING_SIMPLE;

-- Step 5: Drop old table after verification
-- DROP TABLE CIM_OFM_SMG3_LOGGING_OLD CASCADE CONSTRAINTS;
]';
    
    DBMS_OUTPUT.PUT_LINE('Complete Conversion DDL:');
    DBMS_OUTPUT.PUT_LINE('=====================================');
    DBMS_OUTPUT.PUT_LINE(v_manual_ddl);
    DBMS_OUTPUT.PUT_LINE('=====================================');
    
END;
/

-- =====================================================
-- METHOD 3: Using table_ddl_pkg for New Table Creation
-- =====================================================

PROMPT
PROMPT Method 3: Using table_ddl_pkg for Complete Table Definition
PROMPT

DECLARE
    v_columns column_def_array;
    v_partitions partition_def_array;
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== USING TABLE_DDL_PKG FOR SUBPARTITIONED TABLE ===');
    
    -- Define columns
    v_columns := column_def_array(
        column_def('TRACE_ID', 'VARCHAR2', 36, 0, 0, TRUE, NULL, FALSE, FALSE, 'Trace identifier for subpartitioning'),
        column_def('ALIAS', 'VARCHAR2', 8, 0, 0, TRUE, NULL, FALSE, FALSE, 'Alias field'),
        column_def('AUDIT_CREATE_DATE', 'TIMESTAMP', 0, 0, 6, TRUE, 'SYSTIMESTAMP', FALSE, FALSE, 'Partition key - audit date'),
        column_def('SMG3_LOGGING_DATA', 'CLOB', 0, 0, 0, TRUE, NULL, FALSE, FALSE, 'Main logging data'),
        column_def('FILTRATION_JSON_DATA', 'CLOB', 0, 0, 0, TRUE, NULL, FALSE, FALSE, 'JSON filtration data')
    );
    
    -- Define partitions with subpartitioning
    v_partitions := partition_def_array(
        partition_def(
            'p_before_2025',           -- partition_name
            'RANGE',                   -- partition_type  
            'AUDIT_CREATE_DATE',       -- partition_key
            'TIMESTAMP ''2025-01-01 00:00:00''', -- high_value
            'DBRT_DATA',              -- tablespace_name
            'HASH',                   -- subpartition_type
            'TRACE_ID',               -- subpartition_key
            2,                        -- subpartition_count (2 subpartitions)
            'NUMTODSINTERVAL(1,''DAY'')', -- interval_expr
            'MAV_LOB,MAV_HIST_LOB',   -- subpartition_tablespaces
            NULL,                     -- reference_table
            FALSE,                    -- is_default
            TRUE                      -- interval_reference
        )
    );
    
    -- Generate DDL using table_ddl_pkg
    v_ddl := table_ddl_pkg.generate_partitioned_table_ddl(
        p_table_name => 'CIM_OFM_SMG3_LOGGING_SUBPART',
        p_columns => v_columns,
        p_constraints => constraint_def_array(), -- No constraints for this example
        p_partitions => v_partitions,
        p_schema => USER
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL using table_ddl_pkg:');
    DBMS_OUTPUT.PUT_LINE('=====================================');
    DBMS_OUTPUT.PUT_LINE(v_ddl);
    DBMS_OUTPUT.PUT_LINE('=====================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

-- =====================================================
-- METHOD 4: Step-by-Step Conversion Process
-- =====================================================

PROMPT
PROMPT Method 4: Step-by-Step Conversion Process
PROMPT

DECLARE
    v_table_exists NUMBER;
    v_is_partitioned BOOLEAN;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== STEP-BY-STEP CONVERSION ANALYSIS ===');
    
    -- Check if source table exists
    SELECT COUNT(*) INTO v_table_exists 
    FROM user_tables 
    WHERE table_name = 'CIM_OFM_SMG3_LOGGING_SIMPLE';
    
    IF v_table_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ Source table CIM_OFM_SMG3_LOGGING_SIMPLE exists');
        
        -- Check if it's already partitioned
        v_is_partitioned := table_ops_pkg.is_partitioned('CIM_OFM_SMG3_LOGGING_SIMPLE');
        
        IF v_is_partitioned THEN
            DBMS_OUTPUT.PUT_LINE('ℹ Table is already partitioned');
            DBMS_OUTPUT.PUT_LINE('  Current partition type: ' || table_ops_pkg.get_partition_type('CIM_OFM_SMG3_LOGGING_SIMPLE'));
            DBMS_OUTPUT.PUT_LINE('  Recommendation: Create new subpartitioned table and migrate data');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ℹ Table is not partitioned - perfect for conversion');
            DBMS_OUTPUT.PUT_LINE('  Recommendation: Use convert_to_partitioned procedure');
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('CONVERSION STEPS:');
        DBMS_OUTPUT.PUT_LINE('1. Create new subpartitioned table structure');
        DBMS_OUTPUT.PUT_LINE('2. Copy data with parallel processing');
        DBMS_OUTPUT.PUT_LINE('3. Create necessary indexes');
        DBMS_OUTPUT.PUT_LINE('4. Rename tables during maintenance window');
        DBMS_OUTPUT.PUT_LINE('5. Configure Oracle 19c statistics optimization');
        DBMS_OUTPUT.PUT_LINE('6. Verify data integrity and performance');
        
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ Source table CIM_OFM_SMG3_LOGGING_SIMPLE does not exist');
        DBMS_OUTPUT.PUT_LINE('  Please create the source table first or update the table name');
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error in analysis: ' || SQLERRM);
END;
/

-- =====================================================
-- QUICK REFERENCE: Your Conversion Command
-- =====================================================

PROMPT
PROMPT ========================================
PROMPT QUICK REFERENCE: YOUR CONVERSION COMMAND
PROMPT ========================================
PROMPT
PROMPT To generate DDL for converting your table to subpartitioned:
PROMPT
PROMPT DECLARE
PROMPT     v_ddl CLOB;
PROMPT BEGIN
PROMPT     v_ddl := table_ops_pkg.generate_convert_to_partitioned_ddl(
PROMPT         p_table_name        => 'CIM_OFM_SMG3_LOGGING_SIMPLE',
PROMPT         p_partition_type    => 'RANGE',
PROMPT         p_partition_column  => 'AUDIT_CREATE_DATE',
PROMPT         p_partition_count   => 2,
PROMPT         p_interval_expr     => 'NUMTODSINTERVAL(1,''DAY'')',
PROMPT         p_reference_table   => NULL,
PROMPT         p_parallel_degree   => 4
PROMPT     );
PROMPT     DBMS_OUTPUT.PUT_LINE(v_ddl);
PROMPT END;
PROMPT /
PROMPT
PROMPT ========================================
PROMPT KEY BENEFITS OF SUBPARTITIONING:
PROMPT ========================================
PROMPT ✅ Partition Pruning: Faster queries by date
PROMPT ✅ Subpartition Pruning: Faster queries by TRACE_ID
PROMPT ✅ Load Balancing: Even distribution across tablespaces
PROMPT ✅ Parallel Operations: Better DML performance
PROMPT ✅ Maintenance Windows: Operations on subpartition level
PROMPT ✅ Storage Management: Different tablespaces for different data
PROMPT
PROMPT ========================================