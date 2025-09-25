-- =====================================================
-- Oracle Partition Strategy Management Package Specification
-- Advanced partition strategy handling and migration
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Specification
CREATE OR REPLACE PACKAGE partition_strategy_pkg
AUTHID DEFINER
AS
    -- Types for strategy management
    TYPE strategy_config_rec IS RECORD (
        config_id          NUMBER,
        table_name         VARCHAR2(128),
        strategy_type      VARCHAR2(20),
        partition_column   VARCHAR2(128),
        interval_value     VARCHAR2(50),
        tablespace_prefix  VARCHAR2(30),
        retention_days     NUMBER,
        auto_maintenance   BOOLEAN,
        created_date       DATE,
        created_by         VARCHAR2(30),
        last_modified      DATE,
        last_modified_by   VARCHAR2(30),
        is_active          BOOLEAN
    );
    
    TYPE partition_analysis_rec IS RECORD (
        table_name         VARCHAR2(128),
        current_strategy   VARCHAR2(20),
        partition_count    NUMBER,
        total_size_mb      NUMBER,
        avg_partition_size_mb NUMBER,
        max_partition_size_mb NUMBER,
        min_partition_size_mb NUMBER,
        last_analyzed      DATE,
        recommended_strategy VARCHAR2(20),
        migration_complexity VARCHAR2(20)
    );
    
    TYPE partition_analysis_tab IS TABLE OF partition_analysis_rec;
    
    -- Strategy configuration procedures
    PROCEDURE create_strategy_config(
        p_table_name        IN VARCHAR2,
        p_strategy_type     IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_tablespace_prefix IN VARCHAR2 DEFAULT NULL,
        p_retention_days    IN NUMBER DEFAULT 90,
        p_auto_maintenance  IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE update_strategy_config(
        p_table_name        IN VARCHAR2,
        p_strategy_type     IN VARCHAR2 DEFAULT NULL,
        p_partition_column  IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_tablespace_prefix IN VARCHAR2 DEFAULT NULL,
        p_retention_days    IN NUMBER DEFAULT NULL,
        p_auto_maintenance  IN BOOLEAN DEFAULT NULL
    );
    
    PROCEDURE deactivate_strategy_config(
        p_table_name IN VARCHAR2
    );
    
    FUNCTION get_strategy_config(
        p_table_name IN VARCHAR2
    ) RETURN strategy_config_rec;
    
    -- Strategy migration procedures
    PROCEDURE migrate_to_range_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE migrate_to_list_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE migrate_to_hash_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_count   IN NUMBER,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE migrate_to_interval_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_interval_value    IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE migrate_to_reference_partitioning(
        p_table_name        IN VARCHAR2,
        p_parent_table      IN VARCHAR2,
        p_foreign_key       IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    -- Advanced migration procedures for complex scenarios
    PROCEDURE convert_to_partitioned(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2, -- RANGE, LIST, HASH, INTERVAL, REFERENCE
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_partition_count   IN NUMBER DEFAULT NULL,
        p_parent_table      IN VARCHAR2 DEFAULT NULL,
        p_foreign_key       IN VARCHAR2 DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE convert_to_partitioned_with_subpartitions(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2, -- RANGE, LIST, HASH, INTERVAL
        p_partition_column  IN VARCHAR2,
        p_subpartition_type IN VARCHAR2, -- RANGE, LIST, HASH
        p_subpartition_column IN VARCHAR2,
        p_partition_definitions IN VARCHAR2 DEFAULT NULL,
        p_subpartition_definitions IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_partition_count   IN NUMBER DEFAULT NULL,
        p_subpartition_count IN NUMBER DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE convert_to_non_partitioned(
        p_table_name        IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE add_subpartitioning(
        p_table_name        IN VARCHAR2,
        p_subpartition_type IN VARCHAR2, -- RANGE, LIST, HASH
        p_subpartition_column IN VARCHAR2,
        p_subpartition_definitions IN VARCHAR2 DEFAULT NULL,
        p_subpartition_count IN NUMBER DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    -- Oracle 19c specific partitioning support
    PROCEDURE migrate_to_auto_list_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE migrate_to_auto_range_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_interval_value    IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE migrate_to_hybrid_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2,
        p_interval_value    IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    );
    
    -- Strategy analysis procedures
    FUNCTION analyze_table_for_partitioning(
        p_table_name IN VARCHAR2
    ) RETURN partition_analysis_rec;
    
    FUNCTION get_partitioning_recommendations(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION analyze_partition_effectiveness(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- Strategy optimization procedures
    PROCEDURE optimize_partition_strategy(
        p_table_name IN VARCHAR2,
        p_force_recommendation IN BOOLEAN DEFAULT FALSE
    );
    
    PROCEDURE rebalance_partitions(
        p_table_name IN VARCHAR2,
        p_target_size_mb IN NUMBER DEFAULT 1000
    );
    
    PROCEDURE consolidate_small_partitions(
        p_table_name IN VARCHAR2,
        p_min_size_mb IN NUMBER DEFAULT 100
    );
    
    -- Strategy validation procedures
    FUNCTION validate_partition_strategy(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE validate_partition_columns(
        p_table_name IN VARCHAR2,
        p_partition_column IN VARCHAR2
    );
    
    -- Strategy monitoring procedures
    FUNCTION get_strategy_usage_stats(
        p_table_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE monitor_strategy_performance(
        p_table_name IN VARCHAR2
    );
    
    -- Utility procedures
    PROCEDURE generate_migration_script(
        p_table_name        IN VARCHAR2,
        p_target_strategy   IN VARCHAR2,
        p_script_type       IN VARCHAR2 DEFAULT 'DDL' -- DDL, DML, COMPLETE
    );
    
    PROCEDURE estimate_migration_impact(
        p_table_name        IN VARCHAR2,
        p_target_strategy   IN VARCHAR2
    );
    
    PROCEDURE rollback_strategy_migration(
        p_table_name IN VARCHAR2,
        p_backup_table IN VARCHAR2
    );
    
END partition_strategy_pkg;
/