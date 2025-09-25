-- =====================================================
-- Oracle Partition Management Package
-- Comprehensive partition handling with autonomous logging
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Specification
CREATE OR REPLACE PACKAGE partition_management_pkg
AUTHID DEFINER
AS
    -- Types for partition operations
    TYPE partition_info_rec IS RECORD (
        table_name        VARCHAR2(128),
        partition_name    VARCHAR2(128),
        partition_type    VARCHAR2(20),
        high_value        VARCHAR2(4000),
        tablespace_name   VARCHAR2(30),
        num_rows          NUMBER,
        blocks            NUMBER,
        last_analyzed     DATE
    );
    
    TYPE partition_info_tab IS TABLE OF partition_info_rec;
    
    TYPE strategy_config_rec IS RECORD (
        table_name        VARCHAR2(128),
        partition_type    VARCHAR2(20),
        partition_column  VARCHAR2(128),
        interval_value    VARCHAR2(50),
        tablespace_prefix VARCHAR2(30),
        retention_days    NUMBER,
        auto_maintenance  BOOLEAN
    );
    
    -- Core partition management procedures
    PROCEDURE create_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_high_value      IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE drop_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_update_indexes  IN BOOLEAN DEFAULT TRUE,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE split_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_split_value     IN VARCHAR2,
        p_new_partition1  IN VARCHAR2,
        p_new_partition2  IN VARCHAR2,
        p_tablespace1     IN VARCHAR2 DEFAULT NULL,
        p_tablespace2     IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE merge_partitions(
        p_table_name      IN VARCHAR2,
        p_partition1      IN VARCHAR2,
        p_partition2      IN VARCHAR2,
        p_new_partition   IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE move_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_tablespace      IN VARCHAR2,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE truncate_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_update_indexes  IN BOOLEAN DEFAULT TRUE
    );
    
    -- Data movement procedures
    PROCEDURE move_data_to_partition(
        p_source_table    IN VARCHAR2,
        p_target_table    IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_where_clause    IN VARCHAR2 DEFAULT NULL,
        p_batch_size      IN NUMBER DEFAULT 10000
    );
    
    PROCEDURE exchange_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_exchange_table  IN VARCHAR2,
        p_validation      IN BOOLEAN DEFAULT TRUE
    );
    
    -- Strategy management procedures
    PROCEDURE change_partition_strategy(
        p_table_name      IN VARCHAR2,
        p_new_strategy    IN strategy_config_rec,
        p_migrate_data    IN BOOLEAN DEFAULT TRUE,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE convert_to_interval_partitioning(
        p_table_name      IN VARCHAR2,
        p_interval_value  IN VARCHAR2,
        p_column_name     IN VARCHAR2,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    -- Partition conversion procedures (delegated to partition_strategy_pkg)
    PROCEDURE convert_table_to_partitioned(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_partition_count   IN NUMBER DEFAULT NULL,
        p_parent_table      IN VARCHAR2 DEFAULT NULL,
        p_foreign_key       IN VARCHAR2 DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE convert_table_to_partitioned_with_subpartitions(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_subpartition_type IN VARCHAR2,
        p_subpartition_column IN VARCHAR2,
        p_partition_definitions IN VARCHAR2 DEFAULT NULL,
        p_subpartition_definitions IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_partition_count   IN NUMBER DEFAULT NULL,
        p_subpartition_count IN NUMBER DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE convert_partitioned_to_non_partitioned(
        p_table_name        IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE add_subpartitioning_to_table(
        p_table_name        IN VARCHAR2,
        p_subpartition_type IN VARCHAR2,
        p_subpartition_column IN VARCHAR2,
        p_subpartition_definitions IN VARCHAR2 DEFAULT NULL,
        p_subpartition_count IN NUMBER DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    -- Oracle 19c specific online operations
    PROCEDURE online_reorganize_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2 DEFAULT NULL,
        p_compress        IN BOOLEAN DEFAULT FALSE
    );
    
    PROCEDURE online_split_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_split_value     IN VARCHAR2,
        p_new_partition1  IN VARCHAR2,
        p_new_partition2  IN VARCHAR2,
        p_tablespace1     IN VARCHAR2 DEFAULT NULL,
        p_tablespace2     IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE online_merge_partitions(
        p_table_name      IN VARCHAR2,
        p_partition1      IN VARCHAR2,
        p_partition2      IN VARCHAR2,
        p_new_partition   IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE online_coalesce_partition(
        p_table_name      IN VARCHAR2
    );
    
    -- Maintenance procedures
    PROCEDURE maintain_old_partitions(
        p_table_name      IN VARCHAR2,
        p_retention_days  IN NUMBER,
        p_action          IN VARCHAR2 DEFAULT 'DROP' -- DROP, ARCHIVE, COMPRESS
    );
    
    PROCEDURE rebuild_partition_indexes(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    );
    
    -- Analysis and monitoring functions
    FUNCTION get_partition_info(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL
    ) RETURN partition_info_tab PIPELINED;
    
    FUNCTION analyze_partition_usage(
        p_table_name      IN VARCHAR2,
        p_days_back       IN NUMBER DEFAULT 30
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_partition_size_info(
        p_table_name      IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- Utility procedures
    PROCEDURE validate_partition_operation(
        p_table_name      IN VARCHAR2,
        p_operation       IN VARCHAR2,
        p_parameters      IN VARCHAR2
    );
    
    PROCEDURE generate_partition_ddl(
        p_table_name      IN VARCHAR2,
        p_operation       IN VARCHAR2,
        p_parameters      IN VARCHAR2
    );
    
    -- Configuration procedures
    PROCEDURE set_partition_strategy(
        p_table_name      IN VARCHAR2,
        p_config          IN strategy_config_rec
    );
    
    FUNCTION get_partition_strategy(
        p_table_name      IN VARCHAR2
    ) RETURN strategy_config_rec;
    
    -- Resource management and monitoring procedures
    PROCEDURE monitor_operation_resources(
        p_operation_type IN VARCHAR2,
        p_table_name IN VARCHAR2
    );
    
    PROCEDURE optimize_operation_settings(
        p_table_name IN VARCHAR2,
        p_operation_type IN VARCHAR2
    );
    
    FUNCTION get_table_size_mb(
        p_table_name IN VARCHAR2
    ) RETURN NUMBER;
    
END partition_management_pkg;
/
