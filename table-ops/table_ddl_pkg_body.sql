-- =====================================================
-- TABLE DDL PACKAGE BODY - Oracle 19c+ Enterprise Edition
-- Principal Database Engineer Package Implementation
-- Version: 1.0
-- =====================================================

-- Supporting Types
CREATE OR REPLACE TYPE column_def_array IS TABLE OF table_ddl_pkg.column_def;
/
CREATE OR REPLACE TYPE constraint_def_array IS TABLE OF table_ddl_pkg.constraint_def;
/
CREATE OR REPLACE TYPE partition_def_array IS TABLE OF table_ddl_pkg.partition_def;
/
CREATE OR REPLACE TYPE table_definition IS OBJECT (
    table_name     VARCHAR2(128),
    columns        column_def_array,
    constraints    constraint_def_array,
    partitions     partition_def_array,
    properties     table_ddl_pkg.table_properties,
    table_type     VARCHAR2(20)
);
/
CREATE OR REPLACE TYPE table_definition_array IS TABLE OF table_definition;
/

-- Package Body
CREATE OR REPLACE PACKAGE BODY table_ddl_pkg AS
    
    -- =====================================================
    -- PRIVATE VARIABLES
    -- =====================================================
    
    g_debug_mode BOOLEAN := FALSE;
    g_log_level  VARCHAR2(20) := 'INFO';
    
    -- =====================================================
    -- PRIVATE UTILITIES
    -- =====================================================
    
    PROCEDURE log_message(
        p_level   IN VARCHAR2,
        p_message IN VARCHAR2
    ) IS
    BEGIN
        IF g_debug_mode OR p_level IN ('ERROR', 'WARN') THEN
            DBMS_OUTPUT.PUT_LINE('[' || p_level || '] ' || p_message);
        END IF;
    END log_message;
    
    FUNCTION validate_table_name(p_table_name IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        IF p_table_name IS NULL OR LENGTH(p_table_name) = 0 THEN
            RETURN FALSE;
        END IF;
        
        -- Check for valid Oracle identifier
        IF NOT REGEXP_LIKE(p_table_name, '^[A-Za-z][A-Za-z0-9_$#]*$') THEN
            RETURN FALSE;
        END IF;
        
        RETURN TRUE;
    END validate_table_name;
    
    FUNCTION validate_column_definition(p_column IN column_def) RETURN BOOLEAN IS
    BEGIN
        IF p_column.column_name IS NULL OR LENGTH(p_column.column_name) = 0 THEN
            RETURN FALSE;
        END IF;
        
        IF p_column.data_type IS NULL OR LENGTH(p_column.data_type) = 0 THEN
            RETURN FALSE;
        END IF;
        
        RETURN TRUE;
    END validate_column_definition;
    
    FUNCTION build_column_definition(p_column IN column_def) RETURN VARCHAR2 IS
        v_sql VARCHAR2(4000);
    BEGIN
        v_sql := p_column.column_name || ' ' || p_column.data_type;
        
        -- Add length/precision/scale
        IF p_column.data_length > 0 THEN
            v_sql := v_sql || '(' || p_column.data_length;
            IF p_column.data_precision > 0 THEN
                v_sql := v_sql || ',' || p_column.data_precision;
            END IF;
            v_sql := v_sql || ')';
        END IF;
        
        -- Add NOT NULL
        IF NOT p_column.nullable THEN
            v_sql := v_sql || ' NOT NULL';
        END IF;
        
        -- Add DEFAULT value
        IF p_column.default_value IS NOT NULL THEN
            v_sql := v_sql || ' DEFAULT ' || p_column.default_value;
        END IF;
        
        -- Add IDENTITY
        IF p_column.identity_gen THEN
            v_sql := v_sql || ' GENERATED ALWAYS AS IDENTITY';
        END IF;
        
        -- Add INVISIBLE
        IF p_column.invisible THEN
            v_sql := v_sql || ' INVISIBLE';
        END IF;
        
        RETURN v_sql;
    END build_column_definition;
    
    FUNCTION build_constraint_definition(p_constraint IN constraint_def) RETURN VARCHAR2 IS
        v_sql VARCHAR2(4000);
    BEGIN
        v_sql := 'CONSTRAINT ' || p_constraint.constraint_name || ' ';
        
        CASE p_constraint.constraint_type
            WHEN 'PRIMARY' THEN
                v_sql := v_sql || 'PRIMARY KEY (' || p_constraint.column_list || ')';
            WHEN 'UNIQUE' THEN
                v_sql := v_sql || 'UNIQUE (' || p_constraint.column_list || ')';
            WHEN 'FOREIGN' THEN
                v_sql := v_sql || 'FOREIGN KEY (' || p_constraint.column_list || ') REFERENCES ' || 
                         p_constraint.references_table || '(' || p_constraint.references_column || ')';
            WHEN 'CHECK' THEN
                v_sql := v_sql || 'CHECK (' || p_constraint.check_condition || ')';
        END CASE;
        
        IF p_constraint.deferrable THEN
            v_sql := v_sql || ' DEFERRABLE';
            IF p_constraint.initially_deferred THEN
                v_sql := v_sql || ' INITIALLY DEFERRED';
            END IF;
        END IF;
        
        RETURN v_sql;
    END build_constraint_definition;
    
    FUNCTION build_partition_clause(
        p_partitions IN partition_def_array
    ) RETURN VARCHAR2 IS
        v_clause CLOB := '';
        v_part_type VARCHAR2(30);
        v_subpart_clause VARCHAR2(4000);
    BEGIN
        IF p_partitions IS NULL OR p_partitions.COUNT = 0 THEN
            RETURN '';
        END IF;
        
        v_part_type := UPPER(p_partitions(1).partition_type);
        
        -- Build partition clause based on type
        CASE v_part_type
            WHEN 'RANGE' THEN
                v_clause := ' PARTITION BY RANGE (' || p_partitions(1).column_list || ')';
                
            WHEN 'LIST' THEN
                v_clause := ' PARTITION BY LIST (' || p_partitions(1).column_list || ')';
                
            WHEN 'HASH' THEN
                v_clause := ' PARTITION BY HASH (' || p_partitions(1).column_list || ')';
                
            WHEN 'INTERVAL' THEN
                v_clause := ' PARTITION BY RANGE (' || p_partitions(1).column_list || ')' ||
                           ' INTERVAL(' || p_partitions(1).interval_expr || ')';
                           
            WHEN 'REFERENCE' THEN
                v_clause := ' PARTITION BY REFERENCE(' || p_partitions(1).reference_constraint || ')';
                RETURN v_clause; -- Reference doesn't need partition list
                
            WHEN 'SYSTEM' THEN
                v_clause := ' PARTITION BY SYSTEM';
                
            WHEN 'AUTO_LIST' THEN
                v_clause := ' PARTITION BY LIST (' || p_partitions(1).column_list || ') AUTOMATIC';
                
            ELSE
                RAISE_APPLICATION_ERROR(-20100, 'Unsupported partition type: ' || v_part_type);
        END CASE;
        
        -- Add subpartitioning for composite partitions
        IF p_partitions(1).subpartition_type IS NOT NULL THEN
            CASE UPPER(p_partitions(1).subpartition_type)
                WHEN 'RANGE' THEN
                    v_subpart_clause := ' SUBPARTITION BY RANGE (' || p_partitions(1).subpartition_column || ')';
                WHEN 'LIST' THEN
                    v_subpart_clause := ' SUBPARTITION BY LIST (' || p_partitions(1).subpartition_column || ')';
                WHEN 'HASH' THEN
                    v_subpart_clause := ' SUBPARTITION BY HASH (' || p_partitions(1).subpartition_column || ')';
                    IF p_partitions(1).subpartition_count > 0 THEN
                        v_subpart_clause := v_subpart_clause || ' SUBPARTITIONS ' || p_partitions(1).subpartition_count;
                    END IF;
            END CASE;
            v_clause := v_clause || v_subpart_clause;
        END IF;
        
        -- Add partition definitions (skip for REFERENCE)
        IF v_part_type NOT IN ('REFERENCE', 'SYSTEM') THEN
            v_clause := v_clause || ' (' || CHR(10);
            
            FOR i IN 1..p_partitions.COUNT LOOP
                IF i > 1 THEN
                    v_clause := v_clause || ',' || CHR(10);
                END IF;
                
                v_clause := v_clause || '  PARTITION ' || p_partitions(i).partition_name;
                
                -- Add VALUES clause based on partition type
                IF p_partitions(i).values_clause IS NOT NULL THEN
                    IF v_part_type = 'RANGE' OR v_part_type = 'INTERVAL' THEN
                        v_clause := v_clause || ' VALUES LESS THAN (' || p_partitions(i).values_clause || ')';
                    ELSIF v_part_type = 'LIST' OR v_part_type = 'AUTO_LIST' THEN
                        v_clause := v_clause || ' VALUES (' || p_partitions(i).values_clause || ')';
                    END IF;
                END IF;
                
                -- Add tablespace
                IF p_partitions(i).tablespace IS NOT NULL THEN
                    v_clause := v_clause || ' TABLESPACE ' || p_partitions(i).tablespace;
                END IF;
            END LOOP;
            
            v_clause := v_clause || CHR(10) || ')';
        END IF;
        
        RETURN v_clause;
    END build_partition_clause;
    
    -- =====================================================
    -- MAIN PROCEDURES IMPLEMENTATION
    -- =====================================================
    
    PROCEDURE create_heap_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) IS
        v_sql CLOB;
        v_full_table_name VARCHAR2(256);
    BEGIN
        log_message('INFO', 'Creating heap table: ' || p_table_name);
        
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RAISE invalid_table_name;
        END IF;
        
        v_full_table_name := p_schema || '.' || p_table_name;
        
        -- Generate DDL
        v_sql := generate_create_ddl(p_table_name, p_columns, p_constraints, NULL, p_properties, 'HEAP');
        
        -- Execute DDL
        EXECUTE IMMEDIATE v_sql;
        
        log_message('INFO', 'Heap table created successfully: ' || p_table_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to create heap table: ' || SQLERRM);
            RAISE;
    END create_heap_table;
    
    PROCEDURE create_partitioned_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_partitions     IN partition_def_array,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) IS
        v_sql CLOB;
    BEGIN
        log_message('INFO', 'Creating partitioned table: ' || p_table_name);
        
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RAISE invalid_table_name;
        END IF;
        
        -- Generate DDL
        v_sql := generate_create_ddl(p_table_name, p_columns, p_constraints, p_partitions, p_properties, 'PARTITIONED');
        
        -- Execute DDL
        EXECUTE IMMEDIATE v_sql;
        
        log_message('INFO', 'Partitioned table created successfully: ' || p_table_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to create partitioned table: ' || SQLERRM);
            RAISE;
    END create_partitioned_table;
    
    PROCEDURE create_iot_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_primary_key     IN VARCHAR2,
        p_overflow        IN VARCHAR2 DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) IS
        v_sql CLOB;
    BEGIN
        log_message('INFO', 'Creating IOT table: ' || p_table_name);
        
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RAISE invalid_table_name;
        END IF;
        
        -- Build IOT-specific DDL
        v_sql := 'CREATE TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_sql := v_sql || ',';
            END IF;
            v_sql := v_sql || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_sql := v_sql || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_sql := v_sql || CHR(10) || ') ORGANIZATION INDEX';
        
        -- Add overflow if specified
        IF p_overflow IS NOT NULL THEN
            v_sql := v_sql || ' OVERFLOW TABLESPACE ' || p_overflow;
        END IF;
        
        -- Add properties
        IF p_properties IS NOT NULL THEN
            IF p_properties.tablespace IS NOT NULL THEN
                v_sql := v_sql || ' TABLESPACE ' || p_properties.tablespace;
            END IF;
        END IF;
        
        -- Execute DDL
        EXECUTE IMMEDIATE v_sql;
        
        log_message('INFO', 'IOT table created successfully: ' || p_table_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to create IOT table: ' || SQLERRM);
            RAISE;
    END create_iot_table;
    
    PROCEDURE create_temp_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_scope          IN VARCHAR2 DEFAULT 'SESSION',
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) IS
        v_sql CLOB;
        v_scope_clause VARCHAR2(50);
    BEGIN
        log_message('INFO', 'Creating temporary table: ' || p_table_name);
        
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RAISE invalid_table_name;
        END IF;
        
        -- Set scope clause
        IF p_scope = 'TRANSACTION' THEN
            v_scope_clause := 'ON COMMIT DELETE ROWS';
        ELSE
            v_scope_clause := 'ON COMMIT PRESERVE ROWS';
        END IF;
        
        -- Build DDL
        v_sql := 'CREATE GLOBAL TEMPORARY TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_sql := v_sql || ',';
            END IF;
            v_sql := v_sql || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_sql := v_sql || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_sql := v_sql || CHR(10) || ') ' || v_scope_clause;
        
        -- Execute DDL
        EXECUTE IMMEDIATE v_sql;
        
        log_message('INFO', 'Temporary table created successfully: ' || p_table_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to create temporary table: ' || SQLERRM);
            RAISE;
    END create_temp_table;
    
    PROCEDURE create_blockchain_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) IS
        v_sql CLOB;
    BEGIN
        log_message('INFO', 'Creating blockchain table: ' || p_table_name);
        
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RAISE invalid_table_name;
        END IF;
        
        -- Build blockchain table DDL
        v_sql := 'CREATE BLOCKCHAIN TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_sql := v_sql || ',';
            END IF;
            v_sql := v_sql || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_sql := v_sql || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_sql := v_sql || CHR(10) || ')';
        
        -- Add properties
        IF p_properties IS NOT NULL THEN
            IF p_properties.tablespace IS NOT NULL THEN
                v_sql := v_sql || ' TABLESPACE ' || p_properties.tablespace;
            END IF;
        END IF;
        
        -- Execute DDL
        EXECUTE IMMEDIATE v_sql;
        
        log_message('INFO', 'Blockchain table created successfully: ' || p_table_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to create blockchain table: ' || SQLERRM);
            RAISE;
    END create_blockchain_table;
    
    -- =====================================================
    -- UTILITY FUNCTIONS IMPLEMENTATION
    -- =====================================================
    
    FUNCTION validate_table_definition(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL
    ) RETURN BOOLEAN IS
    BEGIN
        -- Validate table name
        IF NOT validate_table_name(p_table_name) THEN
            RETURN FALSE;
        END IF;
        
        -- Validate columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF NOT validate_column_definition(p_columns(i)) THEN
                RETURN FALSE;
            END IF;
        END LOOP;
        
        -- Validate constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                IF p_constraints(i).constraint_name IS NULL OR 
                   p_constraints(i).constraint_type IS NULL THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
        END IF;
        
        RETURN TRUE;
    END validate_table_definition;
    
    FUNCTION generate_create_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_partitions     IN partition_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_table_type     IN VARCHAR2 DEFAULT 'HEAP'
    ) RETURN CLOB IS
        v_sql CLOB;
    BEGIN
        v_sql := 'CREATE TABLE ' || p_table_name || ' (';
        
        -- Add columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_sql := v_sql || ',';
            END IF;
            v_sql := v_sql || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_sql := v_sql || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_sql := v_sql || CHR(10) || ')';
        
        -- Add table type specific clauses
        CASE p_table_type
            WHEN 'IOT' THEN
                v_sql := v_sql || ' ORGANIZATION INDEX';
            WHEN 'EXTERNAL' THEN
                v_sql := v_sql || ' ORGANIZATION EXTERNAL';
        END CASE;
        
        -- Add properties
        IF p_properties IS NOT NULL THEN
            IF p_properties.tablespace IS NOT NULL THEN
                v_sql := v_sql || ' TABLESPACE ' || p_properties.tablespace;
            END IF;
            
            IF p_properties.compression IS NOT NULL THEN
                v_sql := v_sql || ' COMPRESS ' || p_properties.compression;
            END IF;
            
            IF p_properties.inmemory THEN
                v_sql := v_sql || ' INMEMORY';
                IF p_properties.inmemory_priority IS NOT NULL THEN
                    v_sql := v_sql || ' PRIORITY ' || p_properties.inmemory_priority;
                END IF;
            END IF;
        END IF;
        
        RETURN v_sql;
    END generate_create_ddl;
    
    FUNCTION table_exists(
        p_table_name     IN VARCHAR2,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM user_tables
        WHERE table_name = UPPER(p_table_name);
        
        RETURN v_count > 0;
    END table_exists;
    
    -- =====================================================
    -- MODERN ORACLE 19c+ FEATURES IMPLEMENTATION
    -- =====================================================
    
    PROCEDURE create_json_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_json_columns   IN VARCHAR2_ARRAY,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) IS
        v_sql CLOB;
    BEGIN
        log_message('INFO', 'Creating JSON table: ' || p_table_name);
        
        -- Build DDL with JSON columns
        v_sql := 'CREATE TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add regular columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_sql := v_sql || ',';
            END IF;
            v_sql := v_sql || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add JSON columns
        IF p_json_columns IS NOT NULL THEN
            FOR i IN 1..p_json_columns.COUNT LOOP
                v_sql := v_sql || ',' || CHR(10) || '  ' || p_json_columns(i) || ' JSON';
            END LOOP;
        END IF;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_sql := v_sql || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_sql := v_sql || CHR(10) || ')';
        
        -- Add properties
        IF p_properties IS NOT NULL AND p_properties.tablespace IS NOT NULL THEN
            v_sql := v_sql || ' TABLESPACE ' || p_properties.tablespace;
        END IF;
        
        -- Execute DDL
        EXECUTE IMMEDIATE v_sql;
        
        log_message('INFO', 'JSON table created successfully: ' || p_table_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to create JSON table: ' || SQLERRM);
            RAISE;
    END create_json_table;
    
    -- =====================================================
    -- BULK OPERATIONS IMPLEMENTATION
    -- =====================================================
    
    PROCEDURE create_tables_bulk(
        p_table_definitions IN table_definition_array
    ) IS
    BEGIN
        log_message('INFO', 'Creating ' || p_table_definitions.COUNT || ' tables in bulk');
        
        FOR i IN 1..p_table_definitions.COUNT LOOP
            CASE p_table_definitions(i).table_type
                WHEN 'HEAP' THEN
                    create_heap_table(
                        p_table_definitions(i).table_name,
                        p_table_definitions(i).columns,
                        p_table_definitions(i).constraints,
                        p_table_definitions(i).properties
                    );
                WHEN 'PARTITIONED' THEN
                    create_partitioned_table(
                        p_table_definitions(i).table_name,
                        p_table_definitions(i).columns,
                        p_table_definitions(i).constraints,
                        p_table_definitions(i).partitions,
                        p_table_definitions(i).properties
                    );
                WHEN 'IOT' THEN
                    create_iot_table(
                        p_table_definitions(i).table_name,
                        p_table_definitions(i).columns,
                        p_table_definitions(i).constraints,
                        'ID', -- Default primary key
                        NULL,
                        p_table_definitions(i).properties
                    );
            END CASE;
        END LOOP;
        
        log_message('INFO', 'Bulk table creation completed successfully');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to create tables in bulk: ' || SQLERRM);
            RAISE;
    END create_tables_bulk;
    
    -- =====================================================
    -- VALIDATION AND TESTING IMPLEMENTATION
    -- =====================================================
    
    FUNCTION test_table_creation(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL
    ) RETURN BOOLEAN IS
        v_test_table_name VARCHAR2(128);
        v_sql CLOB;
    BEGIN
        v_test_table_name := 'TEST_' || p_table_name || '_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
        
        BEGIN
            -- Generate and execute DDL
            v_sql := generate_create_ddl(v_test_table_name, p_columns, p_constraints, NULL, p_properties, 'HEAP');
            EXECUTE IMMEDIATE v_sql;
            
            -- Drop test table
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_test_table_name;
            
            RETURN TRUE;
            
        EXCEPTION
            WHEN OTHERS THEN
                -- Try to drop test table if it exists
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE ' || v_test_table_name;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
                RETURN FALSE;
        END;
    END test_table_creation;
    
    -- =====================================================
    -- DDL GENERATION FUNCTIONS IMPLEMENTATION
    -- =====================================================
    
    FUNCTION generate_heap_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating heap table DDL for: ' || p_table_name);
        
        v_ddl := generate_create_ddl(p_table_name, p_columns, p_constraints, NULL, p_properties, 'HEAP');
        
        -- Add schema prefix if not current user
        IF p_schema != USER THEN
            v_ddl := REPLACE(v_ddl, 'CREATE TABLE ' || p_table_name, 'CREATE TABLE ' || p_schema || '.' || p_table_name);
        END IF;
        
        RETURN v_ddl;
    END generate_heap_table_ddl;
    
    FUNCTION generate_partitioned_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_partitions     IN partition_def_array,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
        v_partition_clause CLOB;
    BEGIN
        log_message('INFO', 'Generating partitioned table DDL for: ' || p_table_name);
        
        -- Build partition clause using comprehensive partition builder
        v_partition_clause := build_partition_clause(p_partitions);
        
        -- Generate base DDL
        v_ddl := generate_create_ddl(p_table_name, p_columns, p_constraints, NULL, p_properties, 'HEAP');
        
        -- Add partition clause
        v_ddl := v_ddl || v_partition_clause;
        
        -- Add schema prefix if not current user
        IF p_schema != USER THEN
            v_ddl := REPLACE(v_ddl, 'CREATE TABLE ' || p_table_name, 'CREATE TABLE ' || p_schema || '.' || p_table_name);
        END IF;
        
        RETURN v_ddl;
    END generate_partitioned_table_ddl;
    
    FUNCTION generate_iot_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_primary_key     IN VARCHAR2,
        p_overflow        IN VARCHAR2 DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating IOT table DDL for: ' || p_table_name);
        
        -- Generate base DDL
        v_ddl := generate_create_ddl(p_table_name, p_columns, p_constraints, NULL, p_properties, 'IOT');
        
        -- Add overflow clause
        IF p_overflow IS NOT NULL THEN
            v_ddl := v_ddl || ' OVERFLOW TABLESPACE ' || p_overflow;
        END IF;
        
        -- Add schema prefix if not current user
        IF p_schema != USER THEN
            v_ddl := REPLACE(v_ddl, 'CREATE TABLE ' || p_table_name, 'CREATE TABLE ' || p_schema || '.' || p_table_name);
        END IF;
        
        RETURN v_ddl;
    END generate_iot_table_ddl;
    
    FUNCTION generate_temp_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_scope          IN VARCHAR2 DEFAULT 'SESSION',
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
        v_scope_clause VARCHAR2(50);
    BEGIN
        log_message('INFO', 'Generating temporary table DDL for: ' || p_table_name);
        
        -- Set scope clause
        IF p_scope = 'TRANSACTION' THEN
            v_scope_clause := 'ON COMMIT DELETE ROWS';
        ELSE
            v_scope_clause := 'ON COMMIT PRESERVE ROWS';
        END IF;
        
        -- Generate base DDL
        v_ddl := 'CREATE GLOBAL TEMPORARY TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_ddl := v_ddl || ',';
            END IF;
            v_ddl := v_ddl || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_ddl := v_ddl || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_ddl := v_ddl || CHR(10) || ') ' || v_scope_clause;
        
        RETURN v_ddl;
    END generate_temp_table_ddl;
    
    FUNCTION generate_blockchain_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating blockchain table DDL for: ' || p_table_name);
        
        -- Build blockchain table DDL
        v_ddl := 'CREATE BLOCKCHAIN TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_ddl := v_ddl || ',';
            END IF;
            v_ddl := v_ddl || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_ddl := v_ddl || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_ddl := v_ddl || CHR(10) || ')';
        
        -- Add properties
        IF p_properties IS NOT NULL AND p_properties.tablespace IS NOT NULL THEN
            v_ddl := v_ddl || ' TABLESPACE ' || p_properties.tablespace;
        END IF;
        
        RETURN v_ddl;
    END generate_blockchain_table_ddl;
    
    FUNCTION generate_json_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_json_columns   IN VARCHAR2_ARRAY,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating JSON table DDL for: ' || p_table_name);
        
        -- Build DDL with JSON columns
        v_ddl := 'CREATE TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add regular columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_ddl := v_ddl || ',';
            END IF;
            v_ddl := v_ddl || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add JSON columns
        IF p_json_columns IS NOT NULL THEN
            FOR i IN 1..p_json_columns.COUNT LOOP
                v_ddl := v_ddl || ',' || CHR(10) || '  ' || p_json_columns(i) || ' JSON';
            END LOOP;
        END IF;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_ddl := v_ddl || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_ddl := v_ddl || CHR(10) || ')';
        
        -- Add properties
        IF p_properties IS NOT NULL AND p_properties.tablespace IS NOT NULL THEN
            v_ddl := v_ddl || ' TABLESPACE ' || p_properties.tablespace;
        END IF;
        
        RETURN v_ddl;
    END generate_json_table_ddl;
    
    FUNCTION generate_spatial_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_spatial_columns IN VARCHAR2_ARRAY,
        p_srid           IN NUMBER DEFAULT 4326,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating spatial table DDL for: ' || p_table_name);
        
        -- Build DDL with spatial columns
        v_ddl := 'CREATE TABLE ' || p_schema || '.' || p_table_name || ' (';
        
        -- Add regular columns
        FOR i IN 1..p_columns.COUNT LOOP
            IF i > 1 THEN
                v_ddl := v_ddl || ',';
            END IF;
            v_ddl := v_ddl || CHR(10) || '  ' || build_column_definition(p_columns(i));
        END LOOP;
        
        -- Add spatial columns
        IF p_spatial_columns IS NOT NULL THEN
            FOR i IN 1..p_spatial_columns.COUNT LOOP
                v_ddl := v_ddl || ',' || CHR(10) || '  ' || p_spatial_columns(i) || ' SDO_GEOMETRY';
            END LOOP;
        END IF;
        
        -- Add constraints
        IF p_constraints IS NOT NULL THEN
            FOR i IN 1..p_constraints.COUNT LOOP
                v_ddl := v_ddl || ',' || CHR(10) || '  ' || build_constraint_definition(p_constraints(i));
            END LOOP;
        END IF;
        
        v_ddl := v_ddl || CHR(10) || ')';
        
        -- Add properties
        IF p_properties IS NOT NULL AND p_properties.tablespace IS NOT NULL THEN
            v_ddl := v_ddl || ' TABLESPACE ' || p_properties.tablespace;
        END IF;
        
        RETURN v_ddl;
    END generate_spatial_table_ddl;
    
    FUNCTION generate_inmemory_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_inmemory_attrs IN VARCHAR2 DEFAULT 'PRIORITY HIGH',
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating in-memory table DDL for: ' || p_table_name);
        
        -- Generate base DDL
        v_ddl := generate_create_ddl(p_table_name, p_columns, p_constraints, NULL, p_properties, 'HEAP');
        
        -- Add in-memory attributes
        v_ddl := v_ddl || ' INMEMORY ' || p_inmemory_attrs;
        
        -- Add schema prefix if not current user
        IF p_schema != USER THEN
            v_ddl := REPLACE(v_ddl, 'CREATE TABLE ' || p_table_name, 'CREATE TABLE ' || p_schema || '.' || p_table_name);
        END IF;
        
        RETURN v_ddl;
    END generate_inmemory_table_ddl;
    
    FUNCTION generate_bulk_tables_ddl(
        p_table_definitions IN table_definition_array
    ) RETURN CLOB IS
        v_ddl CLOB := '';
        v_table_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating bulk tables DDL for ' || p_table_definitions.COUNT || ' tables');
        
        v_ddl := '-- Generated DDL Script for ' || p_table_definitions.COUNT || ' tables' || CHR(10);
        v_ddl := v_ddl || '-- Generated on: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10);
        v_ddl := v_ddl || '-- Total Tables: ' || p_table_definitions.COUNT || CHR(10) || CHR(10);
        
        FOR i IN 1..p_table_definitions.COUNT LOOP
            v_ddl := v_ddl || '-- Table ' || i || ': ' || p_table_definitions(i).table_name || CHR(10);
            
            CASE p_table_definitions(i).table_type
                WHEN 'HEAP' THEN
                    v_table_ddl := generate_heap_table_ddl(
                        p_table_definitions(i).table_name,
                        p_table_definitions(i).columns,
                        p_table_definitions(i).constraints,
                        p_table_definitions(i).properties
                    );
                WHEN 'PARTITIONED' THEN
                    v_table_ddl := generate_partitioned_table_ddl(
                        p_table_definitions(i).table_name,
                        p_table_definitions(i).columns,
                        p_table_definitions(i).constraints,
                        p_table_definitions(i).partitions,
                        p_table_definitions(i).properties
                    );
                WHEN 'IOT' THEN
                    v_table_ddl := generate_iot_table_ddl(
                        p_table_definitions(i).table_name,
                        p_table_definitions(i).columns,
                        p_table_definitions(i).constraints,
                        'ID', -- Default primary key
                        NULL,
                        p_table_definitions(i).properties
                    );
            END CASE;
            
            v_ddl := v_ddl || v_table_ddl || ';' || CHR(10) || CHR(10);
        END LOOP;
        
        RETURN v_ddl;
    END generate_bulk_tables_ddl;
    
    FUNCTION generate_table_like_ddl(
        p_new_table_name IN VARCHAR2,
        p_source_table   IN VARCHAR2,
        p_include_data   IN BOOLEAN DEFAULT FALSE,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB IS
        v_ddl CLOB;
    BEGIN
        log_message('INFO', 'Generating table like DDL for: ' || p_new_table_name);
        
        IF p_include_data THEN
            v_ddl := 'CREATE TABLE ' || p_schema || '.' || p_new_table_name || ' AS SELECT * FROM ' || p_source_table;
        ELSE
            v_ddl := 'CREATE TABLE ' || p_schema || '.' || p_new_table_name || ' AS SELECT * FROM ' || p_source_table || ' WHERE 1=0';
        END IF;
        
        RETURN v_ddl;
    END generate_table_like_ddl;
    
    -- =====================================================
    -- DDL OUTPUT FUNCTIONS IMPLEMENTATION
    -- =====================================================
    
    PROCEDURE print_ddl_script(
        p_ddl_script     IN CLOB,
        p_title          IN VARCHAR2 DEFAULT 'Generated DDL Script'
    ) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('=====================================================');
        DBMS_OUTPUT.PUT_LINE(p_title);
        DBMS_OUTPUT.PUT_LINE('=====================================================');
        DBMS_OUTPUT.PUT_LINE(p_ddl_script);
        DBMS_OUTPUT.PUT_LINE('=====================================================');
    END print_ddl_script;
    
    PROCEDURE save_ddl_to_file(
        p_ddl_script     IN CLOB,
        p_filename       IN VARCHAR2,
        p_title          IN VARCHAR2 DEFAULT 'Generated DDL Script'
    ) IS
        v_file UTL_FILE.FILE_TYPE;
    BEGIN
        v_file := UTL_FILE.FOPEN('TEMP_DIR', p_filename, 'W');
        
        UTL_FILE.PUT_LINE(v_file, '-- ' || p_title);
        UTL_FILE.PUT_LINE(v_file, '-- Generated on: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
        UTL_FILE.PUT_LINE(v_file, '-- Total Length: ' || LENGTH(p_ddl_script) || ' characters');
        UTL_FILE.PUT_LINE(v_file, '');
        UTL_FILE.PUT_LINE(v_file, p_ddl_script);
        
        UTL_FILE.FCLOSE(v_file);
    END save_ddl_to_file;
    
    FUNCTION get_ddl_summary(
        p_ddl_script     IN CLOB
    ) RETURN VARCHAR2 IS
        v_summary VARCHAR2(4000);
    BEGIN
        v_summary := 'DDL Script Summary:' || CHR(10) ||
                    'Total Length: ' || LENGTH(p_ddl_script) || ' characters' || CHR(10) ||
                    'Lines: ' || (LENGTH(p_ddl_script) - LENGTH(REPLACE(p_ddl_script, CHR(10), '')) + 1) || CHR(10) ||
                    'Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        
        RETURN v_summary;
    END get_ddl_summary;
    
    -- =====================================================
    -- DDL EXECUTION FUNCTIONS IMPLEMENTATION
    -- =====================================================
    
    PROCEDURE execute_ddl_script(
        p_ddl_script     IN CLOB,
        p_operation_id   OUT NUMBER
    ) IS
        v_operation_id NUMBER;
    BEGIN
        v_operation_id := DBMS_RANDOM.VALUE(100000, 999999);
        p_operation_id := v_operation_id;
        
        log_message('INFO', 'Executing DDL script. Operation ID: ' || v_operation_id);
        
        EXECUTE IMMEDIATE p_ddl_script;
        
        log_message('INFO', 'DDL script executed successfully. Operation ID: ' || v_operation_id);
        
    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR', 'Failed to execute DDL script: ' || SQLERRM);
            RAISE;
    END execute_ddl_script;
    
    FUNCTION validate_ddl_syntax(
        p_ddl_script     IN CLOB
    ) RETURN BOOLEAN IS
        v_test_table VARCHAR2(128);
    BEGIN
        v_test_table := 'TEST_DDL_VALIDATION_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
        
        BEGIN
            -- Try to parse the DDL by replacing table name with test table
            EXECUTE IMMEDIATE REPLACE(p_ddl_script, 'CREATE TABLE', 'CREATE TABLE ' || v_test_table);
            
            -- If successful, drop the test table
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_test_table;
            
            RETURN TRUE;
            
        EXCEPTION
            WHEN OTHERS THEN
                -- Try to drop test table if it exists
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE ' || v_test_table;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
                RETURN FALSE;
        END;
    END validate_ddl_syntax;
    
    -- =====================================================
    -- INITIALIZATION
    -- =====================================================
    
BEGIN
    log_message('INFO', 'CREATE_TABLE_PKG initialized successfully');
    
END table_ddl_pkg;
/
