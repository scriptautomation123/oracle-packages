-- =====================================================
-- Oracle Table Operations Package - Simplified
-- Core table and partition operations without complexity overhead
-- Author: Principal Oracle Database Application Engineer
-- Version: 2.0 (Refactored)
-- =====================================================

CREATE OR REPLACE PACKAGE table_ops_pkg
AUTHID DEFINER
AS
    -- Simple types for basic operations
    TYPE partition_info_rec IS RECORD (
        table_name        VARCHAR2(128),
        partition_name    VARCHAR2(128),
        partition_type    VARCHAR2(20),
        high_value        VARCHAR2(4000),
        tablespace_name   VARCHAR2(30),
        num_rows          NUMBER,
        size_mb           NUMBER
    );
    
    TYPE partition_info_tab IS TABLE OF partition_info_rec;
    
    -- Core DDL Operations (the essentials)
    PROCEDURE create_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_high_value      IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE drop_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_update_indexes  IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE split_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_split_value     IN VARCHAR2,
        p_new_partition   IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE merge_partitions(
        p_table_name      IN VARCHAR2,
        p_partition1      IN VARCHAR2,
        p_partition2      IN VARCHAR2,
        p_new_partition   IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE truncate_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2
    );
    
    -- Data Movement Operations
    PROCEDURE exchange_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_external_table  IN VARCHAR2,
        p_validate        IN BOOLEAN DEFAULT TRUE
    );
    
    -- Oracle 19c Online Conversion Operations
    PROCEDURE convert_to_partitioned(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2, -- RANGE, LIST, HASH, INTERVAL, REFERENCE
        p_partition_column  IN VARCHAR2,
        p_partition_count   IN NUMBER DEFAULT 4, -- for hash
        p_interval_expr     IN VARCHAR2 DEFAULT NULL, -- for interval: e.g., 'NUMTOYMINTERVAL(1,''MONTH'')'
        p_reference_table   IN VARCHAR2 DEFAULT NULL, -- for reference partitioning
        p_parallel_degree   IN NUMBER DEFAULT 4
    );
    
    PROCEDURE enable_interval_partitioning(
        p_table_name      IN VARCHAR2,
        p_interval_expr   IN VARCHAR2 -- e.g., 'INTERVAL(NUMTOYMINTERVAL(1,''MONTH''))'
    );
    
    -- Essential Utilities
    FUNCTION get_partition_info(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL
    ) RETURN partition_info_tab PIPELINED;
    
    FUNCTION is_partitioned(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION get_partition_type(
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2;
    
    PROCEDURE rebuild_partition_indexes(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL
    );
    
    -- Simple cleanup (no complex scheduling)
    PROCEDURE drop_old_partitions(
        p_table_name      IN VARCHAR2,
        p_retention_days  IN NUMBER
    );
    
    -- Oracle 19c+ Online Operations (high value, low complexity)
    PROCEDURE move_table_online(
        p_table_name      IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    );
    
    PROCEDURE move_partition_online(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    );
    
    -- DDL Generation (planning without execution)
    FUNCTION generate_partition_ddl(
        p_operation_type  IN VARCHAR2, -- CREATE, DROP, SPLIT, MERGE
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_parameters      IN VARCHAR2 DEFAULT NULL
    ) RETURN CLOB;
    
    FUNCTION generate_move_table_ddl(
        p_table_name      IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    ) RETURN CLOB;
    
    FUNCTION generate_convert_to_partitioned_ddl(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_count   IN NUMBER DEFAULT 4,
        p_interval_expr     IN VARCHAR2 DEFAULT NULL,
        p_reference_table   IN VARCHAR2 DEFAULT NULL,
        p_parallel_degree   IN NUMBER DEFAULT 4
    ) RETURN CLOB;
    
    -- Subpartitioning DDL Generation Functions
    FUNCTION generate_add_subpartitioning_ddl(
        p_table_name            IN VARCHAR2,
        p_subpartition_column   IN VARCHAR2,
        p_subpartition_type     IN VARCHAR2 DEFAULT 'HASH',
        p_tablespace_list       IN VARCHAR2, -- Comma-separated: 'TS1,TS2,TS3'
        p_subpartition_count    IN NUMBER DEFAULT NULL, -- If NULL, uses tablespace count
        p_parallel_degree       IN NUMBER DEFAULT 4
    ) RETURN CLOB;
    
    FUNCTION generate_online_subpartitioning_ddl(
        p_table_name            IN VARCHAR2,
        p_subpartition_column   IN VARCHAR2,
        p_subpartition_type     IN VARCHAR2 DEFAULT 'HASH',
        p_tablespace_list       IN VARCHAR2, -- Comma-separated: 'TS1,TS2,TS3'
        p_subpartition_count    IN NUMBER DEFAULT NULL, -- If NULL, uses tablespace count
        p_parallel_degree       IN NUMBER DEFAULT 4
    ) RETURN CLOB;
    
    -- Simple migration utilities
    PROCEDURE copy_table_structure(
        p_source_table    IN VARCHAR2,
        p_target_table    IN VARCHAR2,
        p_include_indexes IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE copy_partition_data(
        p_source_table    IN VARCHAR2,
        p_target_table    IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    );
    
    -- Oracle 19c Statistics Integration
    PROCEDURE collect_partition_stats_after_maintenance(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_auto_configure  IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE configure_table_stats_optimal(
        p_table_name      IN VARCHAR2,
        p_enable_incremental IN BOOLEAN DEFAULT TRUE
    );
    
END table_ops_pkg;
/