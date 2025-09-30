-- =====================================================
-- Oracle 19c Partition Statistics Management Package Body
-- Implementation of best practices for statistics collection
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0 (Oracle 19c Optimized)
-- =====================================================

CREATE OR REPLACE PACKAGE BODY partition_stats_pkg AS
    
    -- Private constants
    C_LARGE_TABLE_THRESHOLD CONSTANT NUMBER := 1000000; -- 1M rows
    C_VERY_LARGE_TABLE_THRESHOLD CONSTANT NUMBER := 100000000; -- 100M rows
    C_MAX_CONCURRENT_DEGREE CONSTANT NUMBER := 16;
    
    -- =====================================================
    -- Private Utility Functions
    -- =====================================================
    
    FUNCTION get_table_row_count(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_count NUMBER;
        v_sql VARCHAR2(1000);
    BEGIN
        IF p_partition_name IS NOT NULL THEN
            v_sql := 'SELECT NVL(num_rows, 0) FROM all_tab_partitions ' ||
                    'WHERE owner = :1 AND table_name = :2 AND partition_name = :3';
            EXECUTE IMMEDIATE v_sql INTO v_count USING p_schema_name, p_table_name, p_partition_name;
        ELSE
            v_sql := 'SELECT NVL(num_rows, 0) FROM all_tables ' ||
                    'WHERE owner = :1 AND table_name = :2';
            EXECUTE IMMEDIATE v_sql INTO v_count USING p_schema_name, p_table_name;
        END IF;
        
        RETURN NVL(v_count, 0);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END get_table_row_count;
    
    FUNCTION get_optimal_degree(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_row_count NUMBER;
        v_degree NUMBER;
    BEGIN
        v_row_count := get_table_row_count(p_table_name, p_schema_name, p_partition_name);
        
        -- Calculate optimal degree based on data size
        CASE
            WHEN v_row_count < 1000000 THEN v_degree := 2;
            WHEN v_row_count < 10000000 THEN v_degree := 4;
            WHEN v_row_count < 100000000 THEN v_degree := 8;
            ELSE v_degree := C_MAX_CONCURRENT_DEGREE;
        END CASE;
        
        RETURN v_degree;
    END get_optimal_degree;
    
    PROCEDURE log_stats_operation(
        p_operation_type IN VARCHAR2,
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_status IN VARCHAR2,
        p_duration_seconds IN NUMBER DEFAULT NULL,
        p_message IN VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        -- Use modern logging if available
        BEGIN
            modern_logging_pkg.log_message(
                p_level => CASE WHEN p_status = 'SUCCESS' THEN 'INFO' ELSE 'ERROR' END,
                p_message => p_operation_type || ' for ' || p_table_name || 
                           CASE WHEN p_partition_name IS NOT NULL THEN '.' || p_partition_name END ||
                           ' - ' || p_status || 
                           CASE WHEN p_message IS NOT NULL THEN ': ' || p_message END,
                p_operation_type => 'STATS_COLLECTION'
            );
        EXCEPTION
            WHEN OTHERS THEN
                -- Fallback to DBMS_OUTPUT if logging package not available
                DBMS_OUTPUT.PUT_LINE(SYSDATE || ' - ' || p_operation_type || ': ' || p_status);
        END;
        COMMIT;
    END log_stats_operation;
    
    -- =====================================================
    -- Core Statistics Collection Procedures
    -- =====================================================
    
    PROCEDURE collect_partition_stats(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_degree IN NUMBER DEFAULT AUTO_DEGREE,
        p_cascade_indexes IN BOOLEAN DEFAULT TRUE,
        p_estimate_percent IN NUMBER DEFAULT DEFAULT_ESTIMATE_PERCENT
    ) IS
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_duration NUMBER;
        v_actual_degree NUMBER;
    BEGIN
        -- Determine optimal degree if AUTO is specified
        v_actual_degree := CASE WHEN p_degree = AUTO_DEGREE 
                               THEN get_optimal_degree(p_table_name, p_schema_name, p_partition_name)
                               ELSE p_degree END;
        
        log_stats_operation('PARTITION_STATS_START', p_table_name, p_partition_name, 'STARTED');
        
        -- Oracle 19c: Collect partition statistics with best practices
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname          => p_schema_name,
            tabname          => p_table_name,
            partname         => p_partition_name,
            estimate_percent => p_estimate_percent,
            degree           => v_actual_degree,
            granularity      => 'PARTITION',
            cascade          => p_cascade_indexes,
            no_invalidate    => FALSE,
            force            => FALSE,
            options          => 'GATHER'
        );
        
        -- If incremental stats enabled, refresh global stats
        IF is_incremental_stats_enabled(p_table_name, p_schema_name) THEN
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname     => p_schema_name,
                tabname     => p_table_name,
                granularity => 'GLOBAL',
                degree      => v_actual_degree,
                force       => FALSE
            );
        END IF;
        
        v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
        log_stats_operation('PARTITION_STATS_COMPLETE', p_table_name, p_partition_name, 
                          'SUCCESS', v_duration);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
            log_stats_operation('PARTITION_STATS_ERROR', p_table_name, p_partition_name, 
                              'ERROR', v_duration, SQLERRM);
            RAISE;
    END collect_partition_stats;
    
    PROCEDURE collect_subpartition_stats(
        p_table_name IN VARCHAR2,
        p_subpartition_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_degree IN NUMBER DEFAULT AUTO_DEGREE,
        p_full_hierarchy IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_duration NUMBER;
        v_actual_degree NUMBER;
        v_partition_name VARCHAR2(128);
    BEGIN
        v_actual_degree := CASE WHEN p_degree = AUTO_DEGREE 
                               THEN get_optimal_degree(p_table_name, p_schema_name)
                               ELSE p_degree END;
        
        log_stats_operation('SUBPARTITION_STATS_START', p_table_name, p_subpartition_name, 'STARTED');
        
        -- Step 1: Collect subpartition statistics
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname     => p_schema_name,
            tabname     => p_table_name,
            partname    => p_subpartition_name,
            granularity => 'SUBPARTITION',
            degree      => LEAST(v_actual_degree, 4), -- Lower degree for subpartitions
            cascade     => TRUE
        );
        
        IF p_full_hierarchy THEN
            -- Get parent partition name
            SELECT partition_name INTO v_partition_name
            FROM all_tab_subpartitions
            WHERE table_owner = p_schema_name 
            AND table_name = p_table_name 
            AND subpartition_name = p_subpartition_name
            AND ROWNUM = 1;
            
            -- Step 2: Aggregate to partition level
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname     => p_schema_name,
                tabname     => p_table_name,
                partname    => v_partition_name,
                granularity => 'PARTITION',
                degree      => v_actual_degree
            );
            
            -- Step 3: Refresh global statistics
            IF is_incremental_stats_enabled(p_table_name, p_schema_name) THEN
                DBMS_STATS.GATHER_TABLE_STATS(
                    ownname     => p_schema_name,
                    tabname     => p_table_name,
                    granularity => 'GLOBAL',
                    degree      => v_actual_degree
                );
            END IF;
        END IF;
        
        v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
        log_stats_operation('SUBPARTITION_STATS_COMPLETE', p_table_name, p_subpartition_name, 
                          'SUCCESS', v_duration);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
            log_stats_operation('SUBPARTITION_STATS_ERROR', p_table_name, p_subpartition_name, 
                              'ERROR', v_duration, SQLERRM);
            RAISE;
    END collect_subpartition_stats;
    
    PROCEDURE refresh_global_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_degree IN NUMBER DEFAULT AUTO_DEGREE,
        p_use_concurrent IN BOOLEAN DEFAULT CONCURRENT_ENABLED
    ) IS
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_duration NUMBER;
        v_actual_degree NUMBER;
    BEGIN
        v_actual_degree := CASE WHEN p_degree = AUTO_DEGREE 
                               THEN get_optimal_degree(p_table_name, p_schema_name)
                               ELSE p_degree END;
        
        log_stats_operation('GLOBAL_STATS_REFRESH_START', p_table_name, NULL, 'STARTED');
        
        -- Oracle 19c: Use concurrent collection if available and requested
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname     => p_schema_name,
            tabname     => p_table_name,
            granularity => 'GLOBAL',
            degree      => v_actual_degree,
            concurrent  => p_use_concurrent,
            no_invalidate => FALSE
        );
        
        v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
        log_stats_operation('GLOBAL_STATS_REFRESH_COMPLETE', p_table_name, NULL, 
                          'SUCCESS', v_duration);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
            log_stats_operation('GLOBAL_STATS_REFRESH_ERROR', p_table_name, NULL, 
                              'ERROR', v_duration, SQLERRM);
            RAISE;
    END refresh_global_stats;
    
    PROCEDURE collect_table_stats_comprehensive(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_affected_partitions IN DBMS_UTILITY.UNCL_ARRAY DEFAULT NULL,
        p_strategy IN VARCHAR2 DEFAULT 'INCREMENTAL_CONCURRENT'
    ) IS
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_duration NUMBER;
        v_degree NUMBER;
        v_use_concurrent BOOLEAN := TRUE;
        v_granularity VARCHAR2(20) := 'ALL';
    BEGIN
        v_degree := get_optimal_degree(p_table_name, p_schema_name);
        
        log_stats_operation('COMPREHENSIVE_STATS_START', p_table_name, NULL, 'STARTED');
        
        -- Configure strategy
        CASE UPPER(p_strategy)
            WHEN 'INCREMENTAL_CONCURRENT' THEN
                v_use_concurrent := TRUE;
                v_granularity := 'ALL';
            WHEN 'PARTITION_ONLY' THEN
                v_use_concurrent := FALSE;
                v_granularity := 'PARTITION';
            WHEN 'GLOBAL_ONLY' THEN
                v_use_concurrent := TRUE;
                v_granularity := 'GLOBAL';
            ELSE
                v_use_concurrent := TRUE;
                v_granularity := 'ALL';
        END CASE;
        
        -- If specific partitions provided, collect them individually
        IF p_affected_partitions IS NOT NULL AND p_affected_partitions.COUNT > 0 THEN
            FOR i IN 1..p_affected_partitions.COUNT LOOP
                collect_partition_stats(
                    p_table_name => p_table_name,
                    p_partition_name => p_affected_partitions(i),
                    p_schema_name => p_schema_name,
                    p_degree => v_degree
                );
            END LOOP;
            
            -- Refresh global stats
            refresh_global_stats(p_table_name, p_schema_name, v_degree, v_use_concurrent);
        ELSE
            -- Comprehensive collection for entire table
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname     => p_schema_name,
                tabname     => p_table_name,
                estimate_percent => DEFAULT_ESTIMATE_PERCENT,
                degree      => v_degree,
                granularity => v_granularity,
                cascade     => TRUE,
                concurrent  => v_use_concurrent,
                no_invalidate => FALSE
            );
        END IF;
        
        v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
        log_stats_operation('COMPREHENSIVE_STATS_COMPLETE', p_table_name, NULL, 
                          'SUCCESS', v_duration);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));
            log_stats_operation('COMPREHENSIVE_STATS_ERROR', p_table_name, NULL, 
                              'ERROR', v_duration, SQLERRM);
            RAISE;
    END collect_table_stats_comprehensive;
    
    -- =====================================================
    -- Configuration and Setup Procedures
    -- =====================================================
    
    PROCEDURE configure_table_for_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_incremental IN BOOLEAN DEFAULT TRUE,
        p_concurrent IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        log_stats_operation('CONFIG_STATS_START', p_table_name, NULL, 'STARTED');
        
        -- Enable incremental statistics
        IF p_incremental THEN
            DBMS_STATS.SET_TABLE_PREFS(
                ownname => p_schema_name,
                tabname => p_table_name,
                pname   => 'INCREMENTAL',
                pvalue  => 'TRUE'
            );
        END IF;
        
        -- Set publish preference
        DBMS_STATS.SET_TABLE_PREFS(
            ownname => p_schema_name,
            tabname => p_table_name,
            pname   => 'PUBLISH',
            pvalue  => 'TRUE'
        );
        
        -- Set estimate percentage to AUTO
        DBMS_STATS.SET_TABLE_PREFS(
            ownname => p_schema_name,
            tabname => p_table_name,
            pname   => 'ESTIMATE_PERCENT',
            pvalue  => 'DBMS_STATS.AUTO_SAMPLE_SIZE'
        );
        
        -- Oracle 19c: Configure concurrent collection
        IF p_concurrent THEN
            BEGIN
                DBMS_STATS.SET_TABLE_PREFS(
                    ownname => p_schema_name,
                    tabname => p_table_name,
                    pname   => 'CONCURRENT',
                    pvalue  => 'TRUE'
                );
            EXCEPTION
                WHEN OTHERS THEN
                    -- Concurrent may not be available in all versions
                    NULL;
            END;
        END IF;
        
        log_stats_operation('CONFIG_STATS_COMPLETE', p_table_name, NULL, 'SUCCESS');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('CONFIG_STATS_ERROR', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END configure_table_for_stats;
    
    FUNCTION is_incremental_stats_enabled(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) RETURN BOOLEAN IS
        v_incremental VARCHAR2(10);
    BEGIN
        v_incremental := DBMS_STATS.GET_PREFS(
            pname   => 'INCREMENTAL',
            ownname => p_schema_name,
            tabname => p_table_name
        );
        
        RETURN UPPER(v_incremental) = 'TRUE';
        
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END is_incremental_stats_enabled;
    
    FUNCTION get_recommended_strategy(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) RETURN stats_strategy_rec IS
        v_strategy stats_strategy_rec;
        v_row_count NUMBER;
        v_partition_count NUMBER;
    BEGIN
        -- Get table metrics
        v_row_count := get_table_row_count(p_table_name, p_schema_name);
        
        SELECT COUNT(*) INTO v_partition_count
        FROM all_tab_partitions
        WHERE owner = p_schema_name AND table_name = p_table_name;
        
        -- Determine optimal strategy
        IF v_row_count > C_VERY_LARGE_TABLE_THRESHOLD THEN
            v_strategy.strategy_name := 'INCREMENTAL_CONCURRENT';
            v_strategy.description := 'Large table with incremental and concurrent stats';
            v_strategy.use_concurrent := TRUE;
            v_strategy.granularity := 'ALL';
            v_strategy.degree := C_MAX_CONCURRENT_DEGREE;
        ELSIF v_row_count > C_LARGE_TABLE_THRESHOLD THEN
            v_strategy.strategy_name := 'INCREMENTAL_STANDARD';
            v_strategy.description := 'Medium table with incremental stats';
            v_strategy.use_concurrent := TRUE;
            v_strategy.granularity := 'ALL';
            v_strategy.degree := 8;
        ELSE
            v_strategy.strategy_name := 'STANDARD';
            v_strategy.description := 'Small table with standard collection';
            v_strategy.use_concurrent := FALSE;
            v_strategy.granularity := 'ALL';
            v_strategy.degree := 4;
        END IF;
        
        RETURN v_strategy;
        
    EXCEPTION
        WHEN OTHERS THEN
            v_strategy.strategy_name := 'STANDARD';
            v_strategy.description := 'Default strategy due to error';
            v_strategy.use_concurrent := FALSE;
            v_strategy.granularity := 'ALL';
            v_strategy.degree := 4;
            RETURN v_strategy;
    END get_recommended_strategy;
    
    FUNCTION estimate_stats_collection_time(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) RETURN NUMBER IS
        v_row_count NUMBER;
        v_estimated_minutes NUMBER;
    BEGIN
        v_row_count := get_table_row_count(p_table_name, p_schema_name, p_partition_name);
        
        -- Rough estimation based on experience
        -- Assumes modern hardware and Oracle 19c optimizations
        CASE
            WHEN v_row_count < 1000000 THEN v_estimated_minutes := 0.5;
            WHEN v_row_count < 10000000 THEN v_estimated_minutes := 2;
            WHEN v_row_count < 100000000 THEN v_estimated_minutes := 10;
            WHEN v_row_count < 1000000000 THEN v_estimated_minutes := 30;
            ELSE v_estimated_minutes := 60;
        END CASE;
        
        -- Adjust for incremental stats (much faster)
        IF is_incremental_stats_enabled(p_table_name, p_schema_name) THEN
            v_estimated_minutes := v_estimated_minutes * 0.3; -- 70% faster
        END IF;
        
        RETURN v_estimated_minutes;
        
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 5; -- Default estimate
    END estimate_stats_collection_time;
    
    FUNCTION check_stats_freshness(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_days_threshold IN NUMBER DEFAULT 7
    ) RETURN DBMS_UTILITY.UNCL_ARRAY IS
        v_stale_partitions DBMS_UTILITY.UNCL_ARRAY := DBMS_UTILITY.UNCL_ARRAY();
        v_counter NUMBER := 0;
    BEGIN
        FOR rec IN (
            SELECT partition_name
            FROM all_tab_partitions
            WHERE owner = p_schema_name
            AND table_name = p_table_name
            AND (last_analyzed IS NULL 
                 OR last_analyzed < SYSDATE - p_days_threshold
                 OR stale_stats = 'YES')
        ) LOOP
            v_counter := v_counter + 1;
            v_stale_partitions.EXTEND;
            v_stale_partitions(v_counter) := rec.partition_name;
        END LOOP;
        
        RETURN v_stale_partitions;
        
    EXCEPTION
        WHEN OTHERS THEN
            RETURN v_stale_partitions;
    END check_stats_freshness;
    
    -- Additional procedures implementation truncated for brevity...
    -- The remaining procedures would follow similar patterns with Oracle 19c optimizations
    
    PROCEDURE lock_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_lock_type IN VARCHAR2 DEFAULT 'ALL'
    ) IS
    BEGIN
        DBMS_STATS.LOCK_TABLE_STATS(
            ownname => p_schema_name,
            tabname => p_table_name
        );
        
        log_stats_operation('LOCK_STATS', p_table_name, NULL, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('LOCK_STATS', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END lock_table_stats;
    
    PROCEDURE unlock_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) IS
    BEGIN
        DBMS_STATS.UNLOCK_TABLE_STATS(
            ownname => p_schema_name,
            tabname => p_table_name
        );
        
        log_stats_operation('UNLOCK_STATS', p_table_name, NULL, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('UNLOCK_STATS', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END unlock_table_stats;
    
    PROCEDURE export_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_stat_table IN VARCHAR2
    ) IS
    BEGIN
        DBMS_STATS.EXPORT_TABLE_STATS(
            ownname  => p_schema_name,
            tabname  => p_table_name,
            stattab  => p_stat_table
        );
        
        log_stats_operation('EXPORT_STATS', p_table_name, NULL, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('EXPORT_STATS', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END export_table_stats;
    
    PROCEDURE import_table_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_stat_table IN VARCHAR2
    ) IS
    BEGIN
        DBMS_STATS.IMPORT_TABLE_STATS(
            ownname  => p_schema_name,
            tabname  => p_table_name,
            stattab  => p_stat_table
        );
        
        log_stats_operation('IMPORT_STATS', p_table_name, NULL, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('IMPORT_STATS', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END import_table_stats;
    
    PROCEDURE enable_realtime_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) IS
    BEGIN
        -- Oracle 19c Real-Time Statistics
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_schema_name || '.' || p_table_name || 
                         ' ENABLE ROW MOVEMENT';
        
        DBMS_STATS.SET_TABLE_PREFS(
            ownname => p_schema_name,
            tabname => p_table_name,
            pname   => 'REALTIME_STATS',
            pvalue  => 'TRUE'
        );
        
        log_stats_operation('ENABLE_REALTIME_STATS', p_table_name, NULL, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('ENABLE_REALTIME_STATS', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END enable_realtime_stats;
    
    PROCEDURE configure_online_stats(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_online_enabled IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        DBMS_STATS.SET_TABLE_PREFS(
            ownname => p_schema_name,
            tabname => p_table_name,
            pname   => 'ONLINE',
            pvalue  => CASE WHEN p_online_enabled THEN 'TRUE' ELSE 'FALSE' END
        );
        
        log_stats_operation('CONFIGURE_ONLINE_STATS', p_table_name, NULL, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('CONFIGURE_ONLINE_STATS', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END configure_online_stats;
    
    PROCEDURE configure_hybrid_histograms(
        p_table_name IN VARCHAR2,
        p_column_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER
    ) IS
    BEGIN
        -- Oracle 19c Hybrid Histograms
        DBMS_STATS.SET_COLUMN_STATS(
            ownname => p_schema_name,
            tabname => p_table_name,
            colname => p_column_name,
            distcnt => NULL,
            density => NULL,
            nullcnt => NULL,
            force   => TRUE
        );
        
        log_stats_operation('CONFIGURE_HYBRID_HISTOGRAMS', p_table_name, p_column_name, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('CONFIGURE_HYBRID_HISTOGRAMS', p_table_name, p_column_name, 'ERROR', NULL, SQLERRM);
            RAISE;
    END configure_hybrid_histograms;
    
    PROCEDURE setup_auto_stats_job(
        p_table_name IN VARCHAR2,
        p_schema_name IN VARCHAR2 DEFAULT USER,
        p_job_name IN VARCHAR2 DEFAULT NULL
    ) IS
        v_job_name VARCHAR2(128);
    BEGIN
        v_job_name := NVL(p_job_name, 'AUTO_STATS_' || p_table_name);
        
        -- Create scheduler job for automatic statistics collection
        DBMS_SCHEDULER.CREATE_JOB(
            job_name        => v_job_name,
            job_type        => 'PLSQL_BLOCK',
            job_action      => 'BEGIN partition_stats_pkg.collect_table_stats_comprehensive(''' || 
                               p_table_name || ''', ''' || p_schema_name || '''); END;',
            start_date      => SYSTIMESTAMP,
            repeat_interval => 'FREQ=WEEKLY;BYDAY=SUN;BYHOUR=2',
            enabled         => TRUE,
            comments        => 'Automatic statistics collection for ' || p_table_name
        );
        
        log_stats_operation('SETUP_AUTO_STATS_JOB', p_table_name, NULL, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            log_stats_operation('SETUP_AUTO_STATS_JOB', p_table_name, NULL, 'ERROR', NULL, SQLERRM);
            RAISE;
    END setup_auto_stats_job;

END partition_stats_pkg;
/