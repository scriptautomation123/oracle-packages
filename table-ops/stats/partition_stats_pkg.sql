-- =====================================================
-- Oracle 19c Partition Statistics Management Package
-- Best practices for statistics collection after partition maintenance
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0 (Oracle 19c Optimized)
-- =====================================================

CREATE OR REPLACE PACKAGE partition_stats_pkg AUTHID CURRENT_USER AS
    
    -- Package constants for Oracle 19c best practices
    CONCURRENT_ENABLED CONSTANT BOOLEAN := TRUE;
    AUTO_DEGREE CONSTANT NUMBER := DBMS_STATS.AUTO_DEGREE;
    DEFAULT_ESTIMATE_PERCENT CONSTANT NUMBER := DBMS_STATS.AUTO_SAMPLE_SIZE;
    
    -- Statistics collection strategies
    TYPE stats_strategy_rec IS RECORD (
        strategy_name VARCHAR2(50),
        description VARCHAR2(200),
        use_concurrent BOOLEAN,
        granularity VARCHAR2(20),
        degree NUMBER
    );
    
    -- Performance metrics
    TYPE stats_performance_rec IS RECORD (
        operation_type VARCHAR2(50),
        table_name VARCHAR2(128),
        partition_name VARCHAR2(128),
        duration_seconds NUMBER,
        rows_processed NUMBER,
        strategy_used VARCHAR2(50)
    );
    
    -- =====================================================
    -- Core Statistics Collection Procedures
    -- =====================================================
    
    /**
     * Collect statistics on specific partition after maintenance
     * Uses Oracle 19c best practices with incremental stats
     */
    PROCEDURE collect_partition_stats(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_degree IN NUMBER DEFAULT AUTO_DEGREE,
        p_cascade_indexes IN BOOLEAN DEFAULT TRUE,
        p_estimate_percent IN NUMBER DEFAULT DEFAULT_ESTIMATE_PERCENT
    );
    
    /**
     * Collect statistics on subpartitioned table
     * Hierarchical approach: subpartition -> partition -> global
     */
    PROCEDURE collect_subpartition_stats(
        p_table_name IN VARCHAR2,
        p_subpartition_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_degree IN NUMBER DEFAULT AUTO_DEGREE,
        p_full_hierarchy IN BOOLEAN DEFAULT TRUE
    );
    
    /**
     * Refresh global statistics using incremental approach
     * Fast operation when incremental stats are enabled
     */
    PROCEDURE refresh_global_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_degree IN NUMBER DEFAULT AUTO_DEGREE,
        p_use_concurrent IN BOOLEAN DEFAULT CONCURRENT_ENABLED
    );
    
    /**
     * Comprehensive statistics collection for large partitioned tables
     * Oracle 19c concurrent collection with optimal strategy
     */
    PROCEDURE collect_table_stats_comprehensive(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_affected_partitions IN DBMS_UTILITY.UNCL_ARRAY DEFAULT NULL,
        p_strategy IN VARCHAR2 DEFAULT 'INCREMENTAL_CONCURRENT'
    );
    
    -- =====================================================
    -- Configuration and Setup Procedures
    -- =====================================================
    
    /**
     * Configure table for optimal Oracle 19c statistics collection
     * Sets incremental, publish, and other preferences
     */
    PROCEDURE configure_table_for_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_incremental IN BOOLEAN DEFAULT TRUE,
        p_concurrent IN BOOLEAN DEFAULT TRUE
    );
    
    /**
     * Setup automatic statistics collection job for partition maintenance
     * Integrates with Oracle's automatic stats collection
     */
    PROCEDURE setup_auto_stats_job(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_job_name IN VARCHAR2 DEFAULT NULL
    );
    
    -- =====================================================
    -- Analysis and Monitoring Functions
    -- =====================================================
    
    /**
     * Check if table is properly configured for incremental statistics
     */
    FUNCTION is_incremental_stats_enabled(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) RETURN BOOLEAN;
    
    /**
     * Get recommended statistics collection strategy
     * Based on table size, partition count, and Oracle version
     */
    FUNCTION get_recommended_strategy(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) RETURN stats_strategy_rec;
    
    /**
     * Estimate statistics collection time
     * Based on table size and historical performance
     */
    FUNCTION estimate_stats_collection_time(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) RETURN NUMBER; -- Returns estimated minutes
    
    /**
     * Check statistics freshness for partitioned table
     * Identifies stale partition statistics
     */
    FUNCTION check_stats_freshness(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_days_threshold IN NUMBER DEFAULT 7
    ) RETURN DBMS_UTILITY.UNCL_ARRAY; -- Returns stale partition names
    
    -- =====================================================
    -- Utility Procedures
    -- =====================================================
    
    /**
     * Lock/unlock statistics to prevent automatic collection
     * Useful during maintenance windows
     */
    PROCEDURE lock_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_lock_type IN VARCHAR2 DEFAULT 'ALL' -- ALL, DATA, CACHE
    );
    
    PROCEDURE unlock_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    );
    
    /**
     * Export/import statistics for backup purposes
     * Useful before major partition maintenance
     */
    PROCEDURE export_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_stat_table IN VARCHAR2
    );
    
    PROCEDURE import_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_stat_table IN VARCHAR2
    );
    
    -- =====================================================
    -- Oracle 19c Advanced Features
    -- =====================================================
    
    /**
     * Use Oracle 19c Real-Time Statistics
     * Automatic statistics maintenance during DML
     */
    PROCEDURE enable_realtime_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    );
    
    /**
     * Configure Online Statistics Gathering
     * Collect stats during index/table operations
     */
    PROCEDURE configure_online_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_online_enabled IN BOOLEAN DEFAULT TRUE
    );
    
    /**
     * Use Hybrid Histograms (Oracle 19c)
     * Better histogram quality for skewed data
     */
    PROCEDURE configure_hybrid_histograms(
        p_table_name IN VARCHAR2,
        p_column_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    );

END partition_stats_pkg;
/