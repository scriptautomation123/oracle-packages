-- =====================================================
-- Oracle Partition Analysis Package - Simplified
-- Essential monitoring and analysis without complexity
-- Author: Principal Oracle Database Application Engineer
-- Version: 2.0 (Refactored)
-- =====================================================

CREATE OR REPLACE PACKAGE partition_analysis_pkg
AUTHID DEFINER
AS
    -- Simple analysis types
    TYPE partition_summary_rec IS RECORD (
        table_name           VARCHAR2(128),
        partition_count      NUMBER,
        total_size_mb        NUMBER,
        avg_partition_size   NUMBER,
        largest_partition    VARCHAR2(128),
        smallest_partition   VARCHAR2(128),
        last_analyzed        DATE
    );
    
    -- Essential Analysis Functions
    FUNCTION get_partition_summary(
        p_table_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_partition_sizes(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION find_large_partitions(
        p_table_name IN VARCHAR2,
        p_size_threshold_mb IN NUMBER DEFAULT 1000
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION find_empty_partitions(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- Simple Performance Analysis
    FUNCTION analyze_partition_usage(
        p_table_name IN VARCHAR2,
        p_days_back IN NUMBER DEFAULT 7
    ) RETURN SYS_REFCURSOR;
    
    -- Index Analysis
    FUNCTION get_partition_index_status(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- Simple Recommendations (no complex AI)
    FUNCTION get_cleanup_candidates(
        p_table_name IN VARCHAR2,
        p_retention_days IN NUMBER DEFAULT 90
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION check_partition_efficiency(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    -- Utility Functions
    PROCEDURE update_partition_statistics(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    );
    
    FUNCTION generate_partition_report(
        p_table_name IN VARCHAR2
    ) RETURN CLOB;
    
    -- Enhanced monitoring for online operations
    FUNCTION get_tablespace_usage(
        p_table_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION check_move_feasibility(
        p_table_name IN VARCHAR2,
        p_target_tablespace IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION estimate_move_time(
        p_table_name IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    ) RETURN NUMBER; -- Returns estimated minutes
    
    -- Oracle 19c Statistics Analysis
    FUNCTION check_stats_freshness(
        p_table_name IN VARCHAR2,
        p_days_threshold IN NUMBER DEFAULT 7
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION is_incremental_stats_enabled(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION estimate_stats_collection_time(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER; -- Returns estimated minutes
    
    PROCEDURE recommend_stats_strategy(
        p_table_name IN VARCHAR2
    );
    
    -- New functions from review enhancements
    FUNCTION validate_incremental_stats_config(
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2;
    
    FUNCTION get_partition_strategy(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION analyze_partition_compression(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION check_exchange_readiness(
        p_source_table IN VARCHAR2,
        p_target_partition IN VARCHAR2
    ) RETURN SYS_REFCURSOR;
    
END partition_analysis_pkg;
/