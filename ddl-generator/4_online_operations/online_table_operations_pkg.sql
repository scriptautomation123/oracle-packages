-- =====================================================
-- Oracle 19c Online Table Operations Package
-- Comprehensive online table operations with parallel data migration
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

CREATE OR REPLACE PACKAGE online_table_operations_pkg
AUTHID DEFINER
AS
    -- Types for operation tracking
    TYPE operation_rec IS RECORD (
        operation_id     NUMBER,
        table_name       VARCHAR2(128),
        operation_type   VARCHAR2(50),
        status           VARCHAR2(20),
        start_time       TIMESTAMP,
        end_time         TIMESTAMP,
        duration_ms      NUMBER,
        rows_processed   NUMBER,
        error_message    VARCHAR2(4000)
    );
    
    TYPE operation_tab IS TABLE OF operation_rec;
    
    -- DDL Generation Types
    TYPE ddl_rec IS RECORD (
        step_number     NUMBER,
        step_name       VARCHAR2(100),
        ddl_statement  CLOB,
        description     VARCHAR2(400),
        is_parallel     BOOLEAN,
        parallel_degree NUMBER
    );
    
    TYPE ddl_tab IS TABLE OF ddl_rec;
    
    -- Main online table operations
    PROCEDURE move_table_online(
        p_table_name           IN VARCHAR2,
        p_new_tablespace       IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE,
        p_operation_id         OUT NUMBER
    );
    
    PROCEDURE move_partition_online(
        p_table_name           IN VARCHAR2,
        p_partition_name       IN VARCHAR2,
        p_new_tablespace       IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE,
        p_operation_id         OUT NUMBER
    );
    
    PROCEDURE move_subpartition_online(
        p_table_name           IN VARCHAR2,
        p_subpartition_name    IN VARCHAR2,
        p_new_tablespace       IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE,
        p_operation_id         OUT NUMBER
    );
    
    -- Parallel data migration operations
    PROCEDURE migrate_table_parallel(
        p_source_table         IN VARCHAR2,
        p_target_table         IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000,
        p_where_clause         IN VARCHAR2 DEFAULT NULL,
        p_operation_id         OUT NUMBER
    );
    
    PROCEDURE sync_tables_parallel(
        p_source_table         IN VARCHAR2,
        p_target_table         IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000,
        p_operation_id         OUT NUMBER
    );
    
    -- Safe column removal operations
    PROCEDURE remove_columns_safe(
        p_table_name           IN VARCHAR2,
        p_columns_to_remove    IN VARCHAR2, -- Comma-separated column names
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000,
        p_operation_id         OUT NUMBER
    );
    
    -- Table synchronization and rename operations
    PROCEDURE sync_and_rename_tables(
        p_old_table            IN VARCHAR2,
        p_new_table            IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000,
        p_operation_id         OUT NUMBER
    );
    
    -- DDL Generation Procedures
    FUNCTION generate_move_table_ddl(
        p_table_name           IN VARCHAR2,
        p_new_tablespace       IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE
    ) RETURN ddl_tab;
    
    FUNCTION generate_move_partition_ddl(
        p_table_name           IN VARCHAR2,
        p_partition_name       IN VARCHAR2,
        p_new_tablespace       IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE
    ) RETURN ddl_tab;
    
    FUNCTION generate_move_subpartition_ddl(
        p_table_name           IN VARCHAR2,
        p_subpartition_name    IN VARCHAR2,
        p_new_tablespace       IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE
    ) RETURN ddl_tab;
    
    FUNCTION generate_migrate_table_ddl(
        p_source_table         IN VARCHAR2,
        p_target_table         IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000,
        p_where_clause         IN VARCHAR2 DEFAULT NULL
    ) RETURN ddl_tab;
    
    FUNCTION generate_remove_columns_ddl(
        p_table_name           IN VARCHAR2,
        p_columns_to_remove    IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000
    ) RETURN ddl_tab;
    
    FUNCTION generate_sync_rename_ddl(
        p_old_table            IN VARCHAR2,
        p_new_table            IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000
    ) RETURN ddl_tab;
    
    -- DDL Execution Procedures
    PROCEDURE execute_ddl_script(
        p_ddl_script           IN CLOB,
        p_operation_id          OUT NUMBER
    );
    
    PROCEDURE execute_ddl_steps(
        p_ddl_steps            IN ddl_tab,
        p_operation_id         OUT NUMBER
    );
    
    -- Utility procedures
    PROCEDURE create_table_copy(
        p_source_table         IN VARCHAR2,
        p_target_table         IN VARCHAR2,
        p_include_data         IN BOOLEAN DEFAULT TRUE,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE,
        p_parallel_degree      IN NUMBER DEFAULT 4
    );
    
    PROCEDURE copy_table_structure(
        p_source_table         IN VARCHAR2,
        p_target_table         IN VARCHAR2,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE,
        p_include_statistics   IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE copy_table_data_parallel(
        p_source_table         IN VARCHAR2,
        p_target_table         IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_batch_size           IN NUMBER DEFAULT 10000,
        p_where_clause         IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE disable_constraints(
        p_table_name           IN VARCHAR2
    );
    
    PROCEDURE enable_constraints(
        p_table_name           IN VARCHAR2
    );
    
    PROCEDURE create_indexes_parallel(
        p_table_name           IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4
    );
    
    PROCEDURE gather_statistics(
        p_table_name           IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_estimate_percent     IN NUMBER DEFAULT 10
    );
    
    PROCEDURE rename_tables_safe(
        p_old_table            IN VARCHAR2,
        p_new_table            IN VARCHAR2
    );
    
    -- DDL Utility Functions
    FUNCTION generate_create_table_ddl(
        p_source_table         IN VARCHAR2,
        p_target_table         IN VARCHAR2,
        p_include_indexes      IN BOOLEAN DEFAULT TRUE,
        p_include_constraints  IN BOOLEAN DEFAULT TRUE
    ) RETURN CLOB;
    
    FUNCTION generate_index_ddl(
        p_table_name           IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4
    ) RETURN ddl_tab;
    
    FUNCTION generate_constraint_ddl(
        p_table_name           IN VARCHAR2
    ) RETURN ddl_tab;
    
    FUNCTION generate_statistics_ddl(
        p_table_name           IN VARCHAR2,
        p_parallel_degree      IN NUMBER DEFAULT 4,
        p_estimate_percent     IN NUMBER DEFAULT 10
    ) RETURN ddl_tab;
    
    -- Monitoring and status procedures
    FUNCTION get_operation_status(
        p_operation_id         IN NUMBER
    ) RETURN operation_rec;
    
    FUNCTION get_operation_history(
        p_table_name           IN VARCHAR2 DEFAULT NULL,
        p_days_back            IN NUMBER DEFAULT 7
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE monitor_operation(
        p_operation_id          IN NUMBER
    );
    
    PROCEDURE cancel_operation(
        p_operation_id          IN NUMBER
    );
    
    -- Validation procedures
    FUNCTION validate_table_exists(
        p_table_name           IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION validate_tablespace_exists(
        p_tablespace_name      IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION validate_partition_exists(
        p_table_name           IN VARCHAR2,
        p_partition_name       IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION validate_subpartition_exists(
        p_table_name           IN VARCHAR2,
        p_subpartition_name    IN VARCHAR2
    ) RETURN BOOLEAN;
    
    -- Cleanup procedures
    PROCEDURE cleanup_failed_operations;
    
    PROCEDURE cleanup_old_operations(
        p_days_old             IN NUMBER DEFAULT 30
    );
    
    -- DDL Output Procedures
    PROCEDURE print_ddl_script(
        p_ddl_steps            IN ddl_tab
    );
    
    PROCEDURE save_ddl_to_file(
        p_ddl_steps            IN ddl_tab,
        p_filename             IN VARCHAR2
    );
    
    FUNCTION get_ddl_summary(
        p_ddl_steps            IN ddl_tab
    ) RETURN VARCHAR2;
    
END online_table_operations_pkg;
/