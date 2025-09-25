-- =====================================================
-- Oracle Partition Utilities Package Specification
-- Advanced partition analysis and monitoring utilities
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Specification
CREATE OR REPLACE PACKAGE partition_utils_pkg
AUTHID DEFINER
AS
    -- Types for analysis
    TYPE partition_health_rec IS RECORD (
        table_name           VARCHAR2(128),
        partition_name       VARCHAR2(128),
        health_score         NUMBER,
        issues_found         NUMBER,
        recommendations      VARCHAR2(4000),
        size_mb             NUMBER,
        num_rows            NUMBER,
        last_analyzed       DATE
    );
    
    TYPE partition_health_tab IS TABLE OF partition_health_rec;
    
    TYPE performance_metrics_rec IS RECORD (
        table_name           VARCHAR2(128),
        partition_name       VARCHAR2(128),
        avg_query_time_ms    NUMBER,
        total_queries        NUMBER,
        cache_hit_ratio      NUMBER,
        io_efficiency        NUMBER
    );
    
    -- Health analysis procedures
    FUNCTION analyze_partition_health(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) RETURN partition_health_tab PIPELINED;
    
    FUNCTION get_partition_health_summary(
        p_table_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE generate_health_report(
        p_table_name IN VARCHAR2 DEFAULT NULL,
        p_output_format IN VARCHAR2 DEFAULT 'TEXT' -- TEXT, HTML, JSON
    );
    
    -- Performance analysis procedures
    FUNCTION analyze_partition_performance(
        p_table_name IN VARCHAR2,
        p_days_back IN NUMBER DEFAULT 7
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_slow_partitions(
        p_table_name IN VARCHAR2,
        p_threshold_ms IN NUMBER DEFAULT 1000
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE identify_performance_bottlenecks(
        p_table_name IN VARCHAR2
    );
    
    -- Size analysis procedures
    FUNCTION get_partition_size_analysis(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION find_oversized_partitions(
        p_table_name IN VARCHAR2,
        p_max_size_mb IN NUMBER DEFAULT 1000
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION find_undersized_partitions(
        p_table_name IN VARCHAR2,
        p_min_size_mb IN NUMBER DEFAULT 10
    ) RETURN SYS_REFCURSOR;
    
    -- Data distribution analysis
    FUNCTION analyze_data_distribution(
        p_table_name IN VARCHAR2,
        p_partition_column IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION check_partition_pruning(
        p_table_name IN VARCHAR2,
        p_sample_queries IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    -- Index analysis procedures
    FUNCTION analyze_partition_indexes(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION find_unused_indexes(
        p_table_name IN VARCHAR2,
        p_days_back IN NUMBER DEFAULT 30
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION find_missing_indexes(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- Maintenance recommendations
    FUNCTION get_maintenance_recommendations(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE generate_maintenance_plan(
        p_table_name IN VARCHAR2,
        p_priority_level IN VARCHAR2 DEFAULT 'MEDIUM' -- LOW, MEDIUM, HIGH, CRITICAL
    );
    
    -- Monitoring procedures
    PROCEDURE setup_partition_monitoring(
        p_table_name IN VARCHAR2,
        p_monitoring_level IN VARCHAR2 DEFAULT 'STANDARD' -- BASIC, STANDARD, DETAILED
    );
    
    PROCEDURE collect_partition_metrics(
        p_table_name IN VARCHAR2
    );
    
    FUNCTION get_partition_metrics(
        p_table_name IN VARCHAR2,
        p_metric_type IN VARCHAR2 DEFAULT 'ALL' -- SIZE, PERFORMANCE, HEALTH, ALL
    ) RETURN SYS_REFCURSOR;
    
    -- Utility procedures
    PROCEDURE export_partition_info(
        p_table_name IN VARCHAR2,
        p_output_file IN VARCHAR2,
        p_format IN VARCHAR2 DEFAULT 'CSV' -- CSV, JSON, XML
    );
    
    PROCEDURE compare_partition_strategies(
        p_table_name IN VARCHAR2,
        p_strategy1 IN VARCHAR2,
        p_strategy2 IN VARCHAR2
    );
    
    FUNCTION estimate_partition_benefits(
        p_table_name IN VARCHAR2,
        p_target_strategy IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- Validation procedures
    FUNCTION validate_partition_design(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE check_partition_constraints(
        p_table_name IN VARCHAR2
    );
    
    FUNCTION get_partition_dependencies(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- String and partition utility functions (from manage_partition_pkg)
    FUNCTION parse_delimited_string(
        p_delimited_string IN VARCHAR2,
        p_element_index IN NUMBER,
        p_delimiter IN VARCHAR2
    ) RETURN VARCHAR2;
    
    FUNCTION is_interval_partitioned(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION get_interval_definition(
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2;
    
    FUNCTION partition_exists(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION get_partition_high_value_date(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2
    ) RETURN DATE;
    
    PROCEDURE manage_interval_partitioning(
        p_table_name IN VARCHAR2,
        p_action IN VARCHAR2, -- 'ENABLE', 'DISABLE', 'SET'
        p_interval_definition IN VARCHAR2 DEFAULT NULL
    );
    
    -- Core utility functions (shared across packages)
    FUNCTION table_exists(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION is_partitioned(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION get_partition_type(
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2;
    
END partition_utils_pkg;
/