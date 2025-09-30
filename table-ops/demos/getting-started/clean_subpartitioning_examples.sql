-- =====================================================
-- CLEAN EXAMPLES: Using Your Packages for Subpartitioning
-- Examples showing how to USE the packages, not implementation
-- =====================================================

SET SERVEROUTPUT ON SIZE 1000000

PROMPT ========================================
PROMPT USING YOUR PACKAGES FOR SUBPARTITIONING
PROMPT Clean examples with package usage only
PROMPT ========================================

-- =====================================================
-- EXAMPLE 1: Generate Subpartitioning DDL
-- =====================================================

PROMPT
PROMPT Example 1: Generate DDL for Adding Subpartitioning
PROMPT

DECLARE
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== GENERATING SUBPARTITIONING DDL ===');
    
    -- Use your table_ops_pkg to generate subpartitioning DDL
    v_ddl := table_ops_pkg.generate_add_subpartitioning_ddl(
        p_table_name            => 'CIM_OFM_SMG3_LOGGING_SIMPLE',
        p_subpartition_column   => 'TRACE_ID',
        p_subpartition_type     => 'HASH',
        p_tablespace_list       => 'MAV_LOB,MAV_HIST_LOB',
        p_subpartition_count    => 2,
        p_parallel_degree       => 4
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL:');
    DBMS_OUTPUT.PUT_LINE('=====================================');
    DBMS_OUTPUT.PUT_LINE(v_ddl);
    DBMS_OUTPUT.PUT_LINE('=====================================');
    
END;
/

-- =====================================================
-- EXAMPLE 2: Online Subpartitioning with DBMS_REDEFINITION
-- =====================================================

PROMPT
PROMPT Example 2: Generate Online Redefinition Script
PROMPT

DECLARE
    v_online_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== GENERATING ONLINE REDEFINITION SCRIPT ===');
    
    -- Use your table_ops_pkg for online redefinition approach
    v_online_ddl := table_ops_pkg.generate_online_subpartitioning_ddl(
        p_table_name            => 'CIM_OFM_SMG3_LOGGING_SIMPLE',
        p_subpartition_column   => 'TRACE_ID',
        p_subpartition_type     => 'HASH',
        p_tablespace_list       => 'MAV_LOB,MAV_HIST_LOB,DBRT_DATA',
        p_subpartition_count    => 3,
        p_parallel_degree       => 4
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated Online Redefinition Script:');
    DBMS_OUTPUT.PUT_LINE('=====================================');
    DBMS_OUTPUT.PUT_LINE(v_online_ddl);
    DBMS_OUTPUT.PUT_LINE('=====================================');
    
END;
/

-- =====================================================
-- EXAMPLE 3: Using table_ddl_pkg for New Subpartitioned Table
-- =====================================================

PROMPT
PROMPT Example 3: Create New Subpartitioned Table
PROMPT

DECLARE
    v_columns column_def_array;
    v_partitions partition_def_array;
    v_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== USING TABLE_DDL_PKG FOR NEW SUBPARTITIONED TABLE ===');
    
    -- Define columns
    v_columns := column_def_array(
        column_def('TRACE_ID', 'VARCHAR2', 36, 0, 0, TRUE, NULL, FALSE, FALSE, 'Trace identifier'),
        column_def('ALIAS', 'VARCHAR2', 8, 0, 0, TRUE, NULL, FALSE, FALSE, 'Alias field'),
        column_def('AUDIT_CREATE_DATE', 'TIMESTAMP', 0, 0, 6, TRUE, 'SYSTIMESTAMP', FALSE, FALSE, 'Audit date'),
        column_def('SMG3_LOGGING_DATA', 'CLOB', 0, 0, 0, TRUE, NULL, FALSE, FALSE, 'Logging data'),
        column_def('FILTRATION_JSON_DATA', 'CLOB', 0, 0, 0, TRUE, NULL, FALSE, FALSE, 'JSON data')
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
            2,                        -- subpartition_count
            'NUMTODSINTERVAL(1,''DAY'')', -- interval_expr
            'MAV_LOB,MAV_HIST_LOB',   -- subpartition_tablespaces
            NULL,                     -- reference_table
            FALSE,                    -- is_default
            TRUE                      -- interval_reference
        )
    );
    
    -- Generate DDL using table_ddl_pkg
    v_ddl := table_ddl_pkg.generate_partitioned_table_ddl(
        p_table_name => 'CIM_OFM_SMG3_LOGGING_NEW',
        p_columns => v_columns,
        p_constraints => constraint_def_array(),
        p_partitions => v_partitions,
        p_schema => USER
    );
    
    DBMS_OUTPUT.PUT_LINE('Generated Table DDL:');
    DBMS_OUTPUT.PUT_LINE('=====================================');
    DBMS_OUTPUT.PUT_LINE(v_ddl);
    DBMS_OUTPUT.PUT_LINE('=====================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Note: Make sure table_ddl_pkg is properly installed');
END;
/

-- =====================================================
-- EXAMPLE 4: Check Current Table Status
-- =====================================================

PROMPT
PROMPT Example 4: Analyze Current Table
PROMPT

DECLARE
    v_table_exists NUMBER;
    v_is_partitioned BOOLEAN;
    v_partition_type VARCHAR2(20);
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== CURRENT TABLE ANALYSIS ===');
    
    -- Check if table exists
    SELECT COUNT(*) INTO v_table_exists 
    FROM user_tables 
    WHERE table_name = 'CIM_OFM_SMG3_LOGGING_SIMPLE';
    
    IF v_table_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ Table CIM_OFM_SMG3_LOGGING_SIMPLE exists');
        
        -- Use table_ops_pkg to check partitioning
        v_is_partitioned := table_ops_pkg.is_partitioned('CIM_OFM_SMG3_LOGGING_SIMPLE');
        
        IF v_is_partitioned THEN
            v_partition_type := table_ops_pkg.get_partition_type('CIM_OFM_SMG3_LOGGING_SIMPLE');
            DBMS_OUTPUT.PUT_LINE('✓ Table is partitioned');
            DBMS_OUTPUT.PUT_LINE('  Partition type: ' || v_partition_type);
            DBMS_OUTPUT.PUT_LINE('  Ready for subpartitioning conversion');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ℹ Table is not partitioned');
            DBMS_OUTPUT.PUT_LINE('  Can use convert_to_partitioned procedure');
        END IF;
        
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ Table CIM_OFM_SMG3_LOGGING_SIMPLE does not exist');
        DBMS_OUTPUT.PUT_LINE('  Create the table first using table_ddl_pkg');
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

-- =====================================================
-- EXAMPLE 5: Quick Reference Commands
-- =====================================================

PROMPT
PROMPT ========================================
PROMPT QUICK REFERENCE: YOUR PACKAGE COMMANDS
PROMPT ========================================
PROMPT
PROMPT 🎯 FOR ADDING SUBPARTITIONING TO EXISTING TABLE:
PROMPT
PROMPT DECLARE
PROMPT     v_ddl CLOB;
PROMPT BEGIN
PROMPT     v_ddl := table_ops_pkg.generate_add_subpartitioning_ddl(
PROMPT         p_table_name            => 'YOUR_TABLE_NAME',
PROMPT         p_subpartition_column   => 'YOUR_HASH_COLUMN',
PROMPT         p_subpartition_type     => 'HASH',
PROMPT         p_tablespace_list       => 'TS1,TS2,TS3',
PROMPT         p_subpartition_count    => 4
PROMPT     );
PROMPT     DBMS_OUTPUT.PUT_LINE(v_ddl);
PROMPT END;
PROMPT /
PROMPT
PROMPT 🚀 FOR ONLINE CONVERSION (MINIMAL DOWNTIME):
PROMPT
PROMPT DECLARE
PROMPT     v_ddl CLOB;
PROMPT BEGIN
PROMPT     v_ddl := table_ops_pkg.generate_online_subpartitioning_ddl(
PROMPT         p_table_name            => 'YOUR_TABLE_NAME',
PROMPT         p_subpartition_column   => 'YOUR_HASH_COLUMN',
PROMPT         p_tablespace_list       => 'TS1,TS2'
PROMPT     );
PROMPT     DBMS_OUTPUT.PUT_LINE(v_ddl);
PROMPT END;
PROMPT /
PROMPT
PROMPT 📋 FOR NEW SUBPARTITIONED TABLES:
PROMPT     → Use table_ddl_pkg.generate_partitioned_table_ddl()
PROMPT     → Define columns with column_def_array
PROMPT     → Define partitions with partition_def_array
PROMPT
PROMPT 🔍 FOR TABLE ANALYSIS:
PROMPT     → table_ops_pkg.is_partitioned('table_name')
PROMPT     → table_ops_pkg.get_partition_type('table_name')
PROMPT
PROMPT ========================================
PROMPT NOTE: All implementation code is in the packages
PROMPT These are just USAGE examples!
PROMPT ========================================