-- =====================================================
-- SUBPARTITION TEMPLATE GENERATION EXAMPLES
-- Demonstrating how the function generates subpartition templates
-- =====================================================

SET SERVEROUTPUT ON SIZE 1000000

PROMPT ========================================
PROMPT SUBPARTITION TEMPLATE GENERATION EXAMPLES
PROMPT ========================================

-- =====================================================
-- EXAMPLE 1: Basic 2-Tablespace Round-Robin Template
-- =====================================================

PROMPT
PROMPT Example 1: 2-Tablespace Round-Robin Template
PROMPT

DECLARE
    v_template_example CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== BASIC 2-TABLESPACE TEMPLATE ===');
    DBMS_OUTPUT.PUT_LINE('Input: p_tablespace_list => ''MAV_LOB,MAV_HIST_LOB''');
    DBMS_OUTPUT.PUT_LINE('Input: p_subpartition_count => 2');
    DBMS_OUTPUT.PUT_LINE('');
    
    v_template_example := q'[
CREATE TABLE your_table (...)
PARTITION BY RANGE (AUDIT_CREATE_DATE)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY HASH (TRACE_ID)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp1 TABLESPACE MAV_LOB,
    SUBPARTITION sp2 TABLESPACE MAV_HIST_LOB
)
(
    PARTITION p_before_2025 VALUES LESS THAN (TIMESTAMP '2025-01-01 00:00:00')
);]';
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL:');
    DBMS_OUTPUT.PUT_LINE(v_template_example);
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Result: Every new INTERVAL partition will automatically get:');
    DBMS_OUTPUT.PUT_LINE('  - 2 subpartitions (sp1, sp2)');
    DBMS_OUTPUT.PUT_LINE('  - sp1 → MAV_LOB tablespace');
    DBMS_OUTPUT.PUT_LINE('  - sp2 → MAV_HIST_LOB tablespace');
    
END;
/

-- =====================================================
-- EXAMPLE 2: 4-Subpartition Template with Round-Robin
-- =====================================================

PROMPT
PROMPT Example 2: 4-Subpartition Template with Round-Robin
PROMPT

DECLARE
    v_template_example CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== 4-SUBPARTITION TEMPLATE WITH ROUND-ROBIN ===');
    DBMS_OUTPUT.PUT_LINE('Input: p_tablespace_list => ''MAV_LOB,MAV_HIST_LOB,DBRT_DATA''');
    DBMS_OUTPUT.PUT_LINE('Input: p_subpartition_count => 4');
    DBMS_OUTPUT.PUT_LINE('');
    
    v_template_example := q'[
CREATE TABLE your_table (...)
PARTITION BY RANGE (AUDIT_CREATE_DATE)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY HASH (TRACE_ID)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp1 TABLESPACE MAV_LOB,
    SUBPARTITION sp2 TABLESPACE MAV_HIST_LOB,
    SUBPARTITION sp3 TABLESPACE DBRT_DATA,
    SUBPARTITION sp4 TABLESPACE MAV_LOB
)
(
    PARTITION p_before_2025 VALUES LESS THAN (TIMESTAMP '2025-01-01 00:00:00')
);]';
    
    DBMS_OUTPUT.PUT_LINE('Generated DDL:');
    DBMS_OUTPUT.PUT_LINE(v_template_example);
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Result: Every new INTERVAL partition will automatically get:');
    DBMS_OUTPUT.PUT_LINE('  - 4 subpartitions (sp1, sp2, sp3, sp4)');
    DBMS_OUTPUT.PUT_LINE('  - sp1 → MAV_LOB tablespace');
    DBMS_OUTPUT.PUT_LINE('  - sp2 → MAV_HIST_LOB tablespace');
    DBMS_OUTPUT.PUT_LINE('  - sp3 → DBRT_DATA tablespace');
    DBMS_OUTPUT.PUT_LINE('  - sp4 → MAV_LOB tablespace (round-robin back to first)');
    
END;
/

-- =====================================================
-- EXAMPLE 3: Show the Template Generation Logic
-- =====================================================

PROMPT
PROMPT Example 3: Template Generation Logic Demonstration
PROMPT

DECLARE
    v_tablespace_list VARCHAR2(1000) := 'MAV_LOB,MAV_HIST_LOB,DBRT_DATA,USERS';
    v_subpartition_count NUMBER := 6;
    v_tablespaces sys.odcivarchar2list;
    v_subpart_template CLOB := '';
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== TEMPLATE GENERATION LOGIC ===');
    DBMS_OUTPUT.PUT_LINE('Input Tablespaces: ' || v_tablespace_list);
    DBMS_OUTPUT.PUT_LINE('Requested Subpartitions: ' || v_subpartition_count);
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Parse tablespace list (same logic as in the function)
    SELECT TRIM(REGEXP_SUBSTR(v_tablespace_list, '[^,]+', 1, level))
    BULK COLLECT INTO v_tablespaces
    FROM dual
    CONNECT BY REGEXP_SUBSTR(v_tablespace_list, '[^,]+', 1, level) IS NOT NULL;
    
    DBMS_OUTPUT.PUT_LINE('Parsed Tablespaces:');
    FOR i IN 1..v_tablespaces.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('  [' || i || '] ' || v_tablespaces(i));
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Build subpartition template (same logic as in the function)
    v_subpart_template := 'SUBPARTITION TEMPLATE (' || CHR(10);
    FOR i IN 1..v_subpartition_count LOOP
        v_subpart_template := v_subpart_template || 
            '    SUBPARTITION sp' || i || ' TABLESPACE ' || 
            v_tablespaces(MOD(i-1, v_tablespaces.COUNT) + 1);
        IF i < v_subpartition_count THEN
            v_subpart_template := v_subpart_template || ',';
        END IF;
        v_subpart_template := v_subpart_template || CHR(10);
        
        -- Show the round-robin logic
        DBMS_OUTPUT.PUT_LINE('Subpartition ' || i || ' → ' || 
            v_tablespaces(MOD(i-1, v_tablespaces.COUNT) + 1) || 
            ' (index ' || (MOD(i-1, v_tablespaces.COUNT) + 1) || ')');
    END LOOP;
    v_subpart_template := v_subpart_template || ')';
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Generated Template:');
    DBMS_OUTPUT.PUT_LINE(v_subpart_template);
    
END;
/

-- =====================================================
-- EXAMPLE 4: Complete Table DDL with Template
-- =====================================================

PROMPT
PROMPT Example 4: Complete Table DDL with Subpartition Template
PROMPT

DECLARE
    v_complete_ddl CLOB;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== COMPLETE TABLE DDL WITH SUBPARTITION TEMPLATE ===');
    
    v_complete_ddl := q'[
CREATE TABLE CIM_OFM_SMG3_LOGGING_SUBPART (
    TRACE_ID VARCHAR2(36 BYTE),
    ALIAS VARCHAR2(8 BYTE),
    AUDIT_CREATE_DATE TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
    SMG3_LOGGING_DATA CLOB,
    FILTRATION_JSON_DATA CLOB
)
-- Primary partitioning: RANGE with INTERVAL (preserves your existing structure)
PARTITION BY RANGE (AUDIT_CREATE_DATE)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
-- Subpartitioning: HASH on TRACE_ID
SUBPARTITION BY HASH (TRACE_ID)
-- TEMPLATE: Defines structure for all future interval partitions
SUBPARTITION TEMPLATE (
    SUBPARTITION sp1 TABLESPACE MAV_LOB,
    SUBPARTITION sp2 TABLESPACE MAV_HIST_LOB
)
(
    -- Initial partition (existing structure preserved)
    PARTITION p_before_2025 VALUES LESS THAN (TIMESTAMP '2025-01-01 00:00:00')
)
-- LOB storage (your existing settings preserved)
LOB (FILTRATION_JSON_DATA) STORE AS SECUREFILE (TABLESPACE MAV_LOB)
LOB (SMG3_LOGGING_DATA) STORE AS SECUREFILE (TABLESPACE MAV_HIST_LOB)
TABLESPACE DBRT_DATA;]';
    
    DBMS_OUTPUT.PUT_LINE(v_complete_ddl);
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('HOW THE TEMPLATE WORKS:');
    DBMS_OUTPUT.PUT_LINE('======================');
    DBMS_OUTPUT.PUT_LINE('1. When you insert data with AUDIT_CREATE_DATE = ''2025-10-01''');
    DBMS_OUTPUT.PUT_LINE('   → Oracle automatically creates partition for Oct 1, 2025');
    DBMS_OUTPUT.PUT_LINE('   → This partition gets 2 subpartitions using the template:');
    DBMS_OUTPUT.PUT_LINE('     - SYS_SUBP001 (based on sp1) → MAV_LOB tablespace');
    DBMS_OUTPUT.PUT_LINE('     - SYS_SUBP002 (based on sp2) → MAV_HIST_LOB tablespace');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('2. Data is distributed by HASH(TRACE_ID) across the 2 subpartitions');
    DBMS_OUTPUT.PUT_LINE('3. Same template applies to ALL future interval partitions');
    DBMS_OUTPUT.PUT_LINE('4. No manual intervention needed for new partitions!');
    
END;
/

-- =====================================================
-- REAL-WORLD BENEFITS OF SUBPARTITION TEMPLATES
-- =====================================================

PROMPT
PROMPT ========================================
PROMPT REAL-WORLD BENEFITS OF SUBPARTITION TEMPLATES
PROMPT ========================================
PROMPT
PROMPT 🎯 AUTOMATIC MANAGEMENT:
PROMPT   ✅ Every new daily partition gets identical subpartition structure
PROMPT   ✅ No manual intervention required
PROMPT   ✅ Consistent performance characteristics
PROMPT
PROMPT 🚀 PERFORMANCE BENEFITS:
PROMPT   ✅ Partition pruning by date (existing)
PROMPT   ✅ Subpartition pruning by TRACE_ID (new)
PROMPT   ✅ Parallel query execution across subpartitions
PROMPT   ✅ Load balancing across multiple tablespaces
PROMPT
PROMPT 🛠️ OPERATIONAL ADVANTAGES:
PROMPT   ✅ Granular maintenance at subpartition level
PROMPT   ✅ Independent backup/recovery of subpartitions
PROMPT   ✅ Tablespace-level storage management
PROMPT   ✅ Better I/O distribution
PROMPT
PROMPT 📊 QUERY EXAMPLES THAT BENEFIT:
PROMPT   - SELECT * FROM table WHERE audit_create_date >= DATE '2025-10-01'
PROMPT     AND trace_id = 'ABC123'
PROMPT     → Uses both partition AND subpartition pruning!
PROMPT
PROMPT   - Parallel operations automatically use all subpartitions
PROMPT   - Maintenance operations can target specific subpartitions
PROMPT
PROMPT ========================================