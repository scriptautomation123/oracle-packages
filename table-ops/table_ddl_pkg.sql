-- =====================================================
-- TABLE DDL PACKAGE - Oracle 19c+ Enterprise Edition
-- Principal Database Engineer Package
-- Version: 1.0
-- =====================================================

-- Package Specification
CREATE OR REPLACE PACKAGE table_ddl_pkg AS
    -- =====================================================
    -- TYPE DEFINITIONS
    -- =====================================================
    
    -- Column definition record
    TYPE column_def IS RECORD (
        column_name    VARCHAR2(128),
        data_type      VARCHAR2(100),
        data_length    NUMBER,
        data_precision NUMBER,
        data_scale     NUMBER,
        nullable       BOOLEAN DEFAULT TRUE,
        default_value  VARCHAR2(4000),
        identity_gen   BOOLEAN DEFAULT FALSE,
        invisible      BOOLEAN DEFAULT FALSE,
        comment_text   VARCHAR2(4000)
    );
    
    -- Table constraint definition
    TYPE constraint_def IS RECORD (
        constraint_name VARCHAR2(128),
        constraint_type VARCHAR2(20), -- PRIMARY, UNIQUE, FOREIGN, CHECK
        column_list     VARCHAR2(4000),
        references_table VARCHAR2(128),
        references_column VARCHAR2(128),
        check_condition VARCHAR2(4000),
        deferrable      BOOLEAN DEFAULT FALSE,
        initially_deferred BOOLEAN DEFAULT FALSE
    );
    
    -- Partition definition (supports all Oracle 19c partition types)
    TYPE partition_def IS RECORD (
        partition_name VARCHAR2(128),
        partition_type VARCHAR2(30), -- RANGE, LIST, HASH, INTERVAL, REFERENCE, SYSTEM, AUTO_LIST
        column_list    VARCHAR2(4000),
        values_clause  VARCHAR2(4000),
        tablespace     VARCHAR2(128),
        interval_expr  VARCHAR2(4000),
        -- Composite partitioning (2-level)
        subpartition_type VARCHAR2(30), -- RANGE, LIST, HASH for composite
        subpartition_column VARCHAR2(4000),
        subpartition_count NUMBER, -- for HASH subpartitions
        -- Reference partitioning
        reference_table VARCHAR2(128),
        reference_constraint VARCHAR2(128),
        -- Auto List (19c)
        auto_list_enabled BOOLEAN DEFAULT FALSE,
        -- Interval-Reference (19c)
        interval_reference BOOLEAN DEFAULT FALSE
    );
    
    -- Table properties
    TYPE table_properties IS RECORD (
        tablespace        VARCHAR2(128),
        organization      VARCHAR2(20) DEFAULT 'HEAP', -- HEAP, INDEX, EXTERNAL
        compression       VARCHAR2(20), -- BASIC, OLTP, QUERY HIGH, etc.
        inmemory          BOOLEAN DEFAULT FALSE,
        inmemory_priority VARCHAR2(20), -- NONE, LOW, MEDIUM, HIGH, CRITICAL
        parallel_degree   NUMBER DEFAULT 1,
        logging           BOOLEAN DEFAULT TRUE,
        row_movement      BOOLEAN DEFAULT FALSE,
        flashback_archive VARCHAR2(128),
        blockchain        BOOLEAN DEFAULT FALSE,
        immutable         BOOLEAN DEFAULT FALSE
    );
    
    -- =====================================================
    -- MAIN PROCEDURES
    -- =====================================================
    
    -- Create a standard heap table
    PROCEDURE create_heap_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints    IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- Create a partitioned table
    PROCEDURE create_partitioned_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_partitions     IN partition_def_array,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- Create an index-organized table (IOT)
    PROCEDURE create_iot_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_primary_key     IN VARCHAR2,
        p_overflow        IN VARCHAR2 DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- Create a temporary table
    PROCEDURE create_temp_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_scope          IN VARCHAR2 DEFAULT 'SESSION', -- SESSION, TRANSACTION
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- Create a blockchain table
    PROCEDURE create_blockchain_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- Create an external table
    PROCEDURE create_external_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_directory      IN VARCHAR2,
        p_location       IN VARCHAR2,
        p_access_params  IN VARCHAR2 DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- =====================================================
    -- UTILITY PROCEDURES
    -- =====================================================
    
    -- Validate table definition
    FUNCTION validate_table_definition(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL
    ) RETURN BOOLEAN;
    
    -- Generate DDL for table creation
    FUNCTION generate_create_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_partitions     IN partition_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_table_type     IN VARCHAR2 DEFAULT 'HEAP'
    ) RETURN CLOB;
    
    -- DDL Generation Functions for all table types
    FUNCTION generate_heap_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    FUNCTION generate_partitioned_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_partitions     IN partition_def_array,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    FUNCTION generate_iot_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_primary_key     IN VARCHAR2,
        p_overflow        IN VARCHAR2 DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    FUNCTION generate_temp_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_scope          IN VARCHAR2 DEFAULT 'SESSION',
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    FUNCTION generate_blockchain_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    FUNCTION generate_json_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_json_columns   IN VARCHAR2_ARRAY,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    FUNCTION generate_spatial_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_spatial_columns IN VARCHAR2_ARRAY,
        p_srid           IN NUMBER DEFAULT 4326,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    FUNCTION generate_inmemory_table_ddl(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_inmemory_attrs IN VARCHAR2 DEFAULT 'PRIORITY HIGH',
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    -- DDL Management Functions
    FUNCTION generate_bulk_tables_ddl(
        p_table_definitions IN table_definition_array
    ) RETURN CLOB;
    
    FUNCTION generate_table_like_ddl(
        p_new_table_name IN VARCHAR2,
        p_source_table   IN VARCHAR2,
        p_include_data   IN BOOLEAN DEFAULT FALSE,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN CLOB;
    
    -- DDL Output Functions
    PROCEDURE print_ddl_script(
        p_ddl_script     IN CLOB,
        p_title          IN VARCHAR2 DEFAULT 'Generated DDL Script'
    );
    
    PROCEDURE save_ddl_to_file(
        p_ddl_script     IN CLOB,
        p_filename       IN VARCHAR2,
        p_title          IN VARCHAR2 DEFAULT 'Generated DDL Script'
    );
    
    FUNCTION get_ddl_summary(
        p_ddl_script     IN CLOB
    ) RETURN VARCHAR2;
    
    -- DDL Execution Functions
    PROCEDURE execute_ddl_script(
        p_ddl_script     IN CLOB,
        p_operation_id   OUT NUMBER
    );
    
    FUNCTION validate_ddl_syntax(
        p_ddl_script     IN CLOB
    ) RETURN BOOLEAN;
    
    -- Check if table exists
    FUNCTION table_exists(
        p_table_name     IN VARCHAR2,
        p_schema          IN VARCHAR2 DEFAULT USER
    ) RETURN BOOLEAN;
    
    -- Get table information
    PROCEDURE get_table_info(
        p_table_name     IN VARCHAR2,
        p_schema          IN VARCHAR2 DEFAULT USER,
        p_columns         OUT SYS_REFCURSOR,
        p_constraints    OUT SYS_REFCURSOR,
        p_partitions     OUT SYS_REFCURSOR
    );
    
    -- =====================================================
    -- MODERN ORACLE 19c+ FEATURES
    -- =====================================================
    
    -- Create table with JSON columns
    PROCEDURE create_json_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_json_columns   IN VARCHAR2_ARRAY,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- Create table with spatial columns
    PROCEDURE create_spatial_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_spatial_columns IN VARCHAR2_ARRAY,
        p_srid           IN NUMBER DEFAULT 4326,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- Create table with in-memory optimization
    PROCEDURE create_inmemory_table(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_inmemory_attrs IN VARCHAR2 DEFAULT 'PRIORITY HIGH',
        p_properties     IN table_properties DEFAULT NULL,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- =====================================================
    -- BULK OPERATIONS
    -- =====================================================
    
    -- Create multiple tables from definition
    PROCEDURE create_tables_bulk(
        p_table_definitions IN table_definition_array
    );
    
    -- Create table from existing table structure
    PROCEDURE create_table_like(
        p_new_table_name IN VARCHAR2,
        p_source_table   IN VARCHAR2,
        p_include_data   IN BOOLEAN DEFAULT FALSE,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- =====================================================
    -- VALIDATION AND TESTING
    -- =====================================================
    
    -- Test table creation with rollback
    FUNCTION test_table_creation(
        p_table_name     IN VARCHAR2,
        p_columns         IN column_def_array,
        p_constraints     IN constraint_def_array DEFAULT NULL,
        p_properties     IN table_properties DEFAULT NULL
    ) RETURN BOOLEAN;
    
    -- Validate table structure
    PROCEDURE validate_table_structure(
        p_table_name     IN VARCHAR2,
        p_schema          IN VARCHAR2 DEFAULT USER
    );
    
    -- =====================================================
    -- CONSTANTS
    -- =====================================================
    
    -- Supported data types
    TYPE data_type_array IS TABLE OF VARCHAR2(100);
    
    -- Supported constraint types
    TYPE constraint_type_array IS TABLE OF VARCHAR2(20);
    
    -- Supported partition types
    TYPE partition_type_array IS TABLE OF VARCHAR2(20);
    
    -- =====================================================
    -- EXCEPTIONS
    -- =====================================================
    
    invalid_table_name     EXCEPTION;
    invalid_column_definition EXCEPTION;
    invalid_constraint_definition EXCEPTION;
    table_already_exists   EXCEPTION;
    insufficient_privileges EXCEPTION;
    invalid_data_type      EXCEPTION;
    
    -- =====================================================
    -- PRAGMA
    -- =====================================================
    
    PRAGMA EXCEPTION_INIT(invalid_table_name, -20001);
    PRAGMA EXCEPTION_INIT(invalid_column_definition, -20002);
    PRAGMA EXCEPTION_INIT(invalid_constraint_definition, -20003);
    PRAGMA EXCEPTION_INIT(table_already_exists, -20004);
    PRAGMA EXCEPTION_INIT(insufficient_privileges, -20005);
    PRAGMA EXCEPTION_INIT(invalid_data_type, -20006);
    
END table_ddl_pkg;
/
