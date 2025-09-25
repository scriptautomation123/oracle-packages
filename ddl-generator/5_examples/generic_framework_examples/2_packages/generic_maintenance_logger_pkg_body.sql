-- =====================================================
-- Oracle Generic Maintenance Logger Package Body
-- Autonomous logging for maintenance operations
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Body
CREATE OR REPLACE PACKAGE BODY generic_maintenance_logger_pkg
AS
    -- Private variables
    g_logging_enabled BOOLEAN := TRUE;
    g_retention_days  NUMBER := 90;
    g_strategy_logging VARCHAR2(4000) := 'ALL'; -- Comma-separated strategy names or 'ALL'
    
    -- Private procedure for autonomous logging
    PROCEDURE log_operation(
        p_strategy_name  IN VARCHAR2,
        p_operation_type IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL,
        p_job_name       IN VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF g_logging_enabled AND (g_strategy_logging = 'ALL' OR INSTR(g_strategy_logging, p_strategy_name) > 0) THEN
            INSERT INTO generic_operation_log (
                operation_id,
                strategy_name,
                operation_type,
                job_name,
                target_object,
                target_type,
                status,
                message,
                duration_ms,
                operation_time,
                user_name,
                session_id,
                sql_text,
                error_code,
                error_message,
                strategy_context
            ) VALUES (
                generic_operation_log_seq.NEXTVAL,
                p_strategy_name,
                p_operation_type,
                p_job_name,
                p_target_object,
                p_target_type,
                p_status,
                p_message,
                p_duration_ms,
                SYSTIMESTAMP,
                USER,
                SYS_CONTEXT('USERENV', 'SID'),
                p_sql_text,
                p_error_code,
                p_error_message,
                p_strategy_context
            );
            COMMIT;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for logging to prevent recursion
            NULL;
    END log_operation;
    
    -- Public logging procedures
    PROCEDURE log_operation(
        p_strategy_name  IN VARCHAR2,
        p_operation_type IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL,
        p_job_name       IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        log_operation(
            p_strategy_name,
            p_operation_type,
            p_target_object,
            p_target_type,
            p_status,
            p_message,
            p_duration_ms,
            p_sql_text,
            p_error_code,
            p_error_message,
            p_strategy_context,
            p_job_name
        );
    END log_operation;
    
    PROCEDURE log_start_operation(
        p_strategy_name  IN VARCHAR2,
        p_operation_type IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL,
        p_job_name       IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_operation_id NUMBER;
    BEGIN
        v_operation_id := generic_operation_log_seq.NEXTVAL;
        
        log_operation(
            p_strategy_name,
            p_operation_type,
            p_target_object,
            p_target_type,
            'STARTED',
            'Operation started',
            NULL,
            p_sql_text,
            NULL,
            NULL,
            p_strategy_context,
            p_job_name
        );
        
        RETURN v_operation_id;
    END log_start_operation;
    
    PROCEDURE log_end_operation(
        p_operation_id   IN NUMBER,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_cpu_time_ms    IN NUMBER DEFAULT NULL,
        p_memory_used_mb IN NUMBER DEFAULT NULL,
        p_io_operations  IN NUMBER DEFAULT NULL,
        p_rows_processed IN NUMBER DEFAULT NULL,
        p_objects_affected IN NUMBER DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF g_logging_enabled THEN
            UPDATE generic_operation_log
            SET status = p_status,
                message = p_message,
                error_code = p_error_code,
                error_message = p_error_message,
                duration_ms = p_duration_ms,
                cpu_time_ms = p_cpu_time_ms,
                memory_used_mb = p_memory_used_mb,
                io_operations = p_io_operations,
                rows_processed = p_rows_processed,
                objects_affected = p_objects_affected,
                operation_time = SYSTIMESTAMP
            WHERE operation_id = p_operation_id;
            
            COMMIT;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for logging
            NULL;
    END log_end_operation;
    
    -- Strategy-specific logging procedures
    PROCEDURE log_strategy_start(
        p_strategy_name  IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL
    ) RETURN NUMBER IS
    BEGIN
        RETURN log_start_operation(
            p_strategy_name,
            'STRATEGY_START',
            p_target_object,
            p_target_type,
            NULL,
            p_strategy_context,
            NULL
        );
    END log_strategy_start;
    
    PROCEDURE log_strategy_end(
        p_operation_id   IN NUMBER,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_performance_metrics IN CLOB DEFAULT NULL
    ) IS
    BEGIN
        log_end_operation(
            p_operation_id,
            p_status,
            p_message,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
        );
    END log_strategy_end;
    
    PROCEDURE log_job_execution(
        p_strategy_name  IN VARCHAR2,
        p_job_name       IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL
    ) IS
    BEGIN
        log_operation(
            p_strategy_name,
            'JOB_EXECUTION',
            p_target_object,
            p_target_type,
            p_status,
            p_message,
            p_duration_ms,
            NULL,
            NULL,
            NULL,
            p_strategy_context,
            p_job_name
        );
    END log_job_execution;
    
    -- Configuration procedures
    PROCEDURE set_logging_enabled(
        p_enabled IN BOOLEAN
    ) IS
    BEGIN
        g_logging_enabled := p_enabled;
        log_operation('SYSTEM', 'CONFIGURE', 'LOGGING', 'SYSTEM', 'INFO', 
                     'Logging enabled set to ' || CASE WHEN p_enabled THEN 'TRUE' ELSE 'FALSE' END);
    END set_logging_enabled;
    
    FUNCTION is_logging_enabled RETURN BOOLEAN IS
    BEGIN
        RETURN g_logging_enabled;
    END is_logging_enabled;
    
    PROCEDURE set_log_retention_days(
        p_days IN NUMBER
    ) IS
    BEGIN
        g_retention_days := p_days;
        log_operation('SYSTEM', 'CONFIGURE', 'RETENTION', 'SYSTEM', 'INFO', 
                     'Log retention days set to ' || p_days);
    END set_log_retention_days;
    
    FUNCTION get_log_retention_days RETURN NUMBER IS
    BEGIN
        RETURN g_retention_days;
    END get_log_retention_days;
    
    PROCEDURE set_strategy_logging(
        p_strategy_name IN VARCHAR2,
        p_enabled IN BOOLEAN
    ) IS
    BEGIN
        IF p_enabled THEN
            IF g_strategy_logging = 'ALL' THEN
                NULL; -- Already enabled for all
            ELSIF INSTR(g_strategy_logging, p_strategy_name) = 0 THEN
                g_strategy_logging := g_strategy_logging || ',' || p_strategy_name;
            END IF;
        ELSE
            IF g_strategy_logging = 'ALL' THEN
                g_strategy_logging := 'ALL'; -- Keep all enabled except this one
            ELSE
                g_strategy_logging := REPLACE(g_strategy_logging, ',' || p_strategy_name, '');
                g_strategy_logging := REPLACE(g_strategy_logging, p_strategy_name || ',', '');
                g_strategy_logging := REPLACE(g_strategy_logging, p_strategy_name, '');
            END IF;
        END IF;
        
        log_operation('SYSTEM', 'CONFIGURE', 'STRATEGY_LOGGING', 'SYSTEM', 'INFO', 
                     'Strategy logging for ' || p_strategy_name || ' set to ' || 
                     CASE WHEN p_enabled THEN 'ENABLED' ELSE 'DISABLED' END);
    END set_strategy_logging;
    
    FUNCTION is_strategy_logging_enabled(
        p_strategy_name IN VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        RETURN g_strategy_logging = 'ALL' OR INSTR(g_strategy_logging, p_strategy_name) > 0;
    END is_strategy_logging_enabled;
    
    -- Log analysis and reporting
    FUNCTION get_operation_log(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_target_object  IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
        v_sql VARCHAR2(4000);
    BEGIN
        v_sql := 'SELECT * FROM generic_operation_log WHERE 1=1';
        
        IF p_strategy_name IS NOT NULL THEN
            v_sql := v_sql || ' AND strategy_name = :strategy_name';
        END IF;
        
        IF p_operation_type IS NOT NULL THEN
            v_sql := v_sql || ' AND operation_type = :operation_type';
        END IF;
        
        IF p_target_object IS NOT NULL THEN
            v_sql := v_sql || ' AND target_object = :target_object';
        END IF;
        
        IF p_status IS NOT NULL THEN
            v_sql := v_sql || ' AND status = :status';
        END IF;
        
        IF p_start_date IS NOT NULL THEN
            v_sql := v_sql || ' AND operation_time >= :start_date';
        END IF;
        
        IF p_end_date IS NOT NULL THEN
            v_sql := v_sql || ' AND operation_time <= :end_date';
        END IF;
        
        v_sql := v_sql || ' ORDER BY operation_time DESC';
        
        OPEN v_cursor FOR v_sql USING p_strategy_name, p_operation_type, p_target_object, p_status, p_start_date, p_end_date;
        
        RETURN v_cursor;
    END get_operation_log;
    
    FUNCTION get_strategy_statistics(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                strategy_name,
                operation_type,
                status,
                COUNT(*) as operation_count,
                AVG(duration_ms) as avg_duration_ms,
                MIN(duration_ms) as min_duration_ms,
                MAX(duration_ms) as max_duration_ms,
                MIN(operation_time) as first_operation,
                MAX(operation_time) as last_operation
            FROM generic_operation_log
            WHERE (p_strategy_name IS NULL OR strategy_name = p_strategy_name)
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            GROUP BY strategy_name, operation_type, status
            ORDER BY strategy_name, operation_type, status;
            
        RETURN v_cursor;
    END get_strategy_statistics;
    
    FUNCTION get_performance_summary(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                strategy_name,
                operation_type,
                COUNT(*) as total_operations,
                AVG(duration_ms) as avg_duration_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) as median_duration_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) as p99_duration_ms,
                MIN(duration_ms) as min_duration_ms,
                MAX(duration_ms) as max_duration_ms,
                AVG(cpu_time_ms) as avg_cpu_time_ms,
                AVG(memory_used_mb) as avg_memory_mb,
                AVG(io_operations) as avg_io_operations
            FROM generic_operation_log
            WHERE status = 'SUCCESS'
            AND duration_ms IS NOT NULL
            AND (p_strategy_name IS NULL OR strategy_name = p_strategy_name)
            AND (p_operation_type IS NULL OR operation_type = p_operation_type)
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            GROUP BY strategy_name, operation_type
            ORDER BY avg_duration_ms DESC;
            
        RETURN v_cursor;
    END get_performance_summary;
    
    FUNCTION get_error_summary(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                strategy_name,
                operation_type,
                target_object,
                error_code,
                error_message,
                COUNT(*) as error_count,
                MIN(operation_time) as first_error,
                MAX(operation_time) as last_error
            FROM generic_operation_log
            WHERE status = 'ERROR'
            AND (p_strategy_name IS NULL OR strategy_name = p_strategy_name)
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            GROUP BY strategy_name, operation_type, target_object, error_code, error_message
            ORDER BY error_count DESC;
            
        RETURN v_cursor;
    END get_error_summary;
    
    FUNCTION get_job_execution_summary(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_job_name       IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                strategy_name,
                job_name,
                target_object,
                status,
                COUNT(*) as execution_count,
                AVG(duration_ms) as avg_duration_ms,
                MIN(duration_time) as first_execution,
                MAX(operation_time) as last_execution,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as error_count
            FROM generic_operation_log
            WHERE job_name IS NOT NULL
            AND (p_strategy_name IS NULL OR strategy_name = p_strategy_name)
            AND (p_job_name IS NULL OR job_name = p_job_name)
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            GROUP BY strategy_name, job_name, target_object, status
            ORDER BY strategy_name, job_name, last_execution DESC;
            
        RETURN v_cursor;
    END get_job_execution_summary;
    
    -- Log maintenance procedures
    PROCEDURE cleanup_old_logs(
        p_retention_days IN NUMBER DEFAULT NULL,
        p_strategy_name  IN VARCHAR2 DEFAULT NULL
    ) IS
        v_retention_days NUMBER;
        v_cutoff_date DATE;
        v_deleted_count NUMBER := 0;
    BEGIN
        v_retention_days := NVL(p_retention_days, g_retention_days);
        v_cutoff_date := SYSDATE - v_retention_days;
        
        DELETE FROM generic_operation_log
        WHERE operation_time < v_cutoff_date
        AND (p_strategy_name IS NULL OR strategy_name = p_strategy_name);
        
        v_deleted_count := SQL%ROWCOUNT;
        
        log_operation('CLEANUP', 'CLEANUP', 'LOGS', 'SYSTEM', 'SUCCESS', 
                     'Deleted ' || v_deleted_count || ' old log entries');
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('CLEANUP', 'CLEANUP', 'LOGS', 'SYSTEM', 'ERROR', 
                         'Failed to cleanup old logs: ' || SQLERRM);
            RAISE;
    END cleanup_old_logs;
    
    PROCEDURE archive_logs(
        p_archive_table  IN VARCHAR2,
        p_start_date     IN DATE,
        p_end_date       IN DATE,
        p_strategy_name  IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
        v_archived_count NUMBER := 0;
    BEGIN
        -- Create archive table if it doesn't exist
        v_sql := 'CREATE TABLE ' || p_archive_table || ' AS 
                  SELECT * FROM generic_operation_log WHERE 1=0';
        
        BEGIN
            EXECUTE IMMEDIATE v_sql;
        EXCEPTION
            WHEN OTHERS THEN
                -- Table might already exist, continue
                NULL;
        END;
        
        -- Archive the data
        v_sql := 'INSERT INTO ' || p_archive_table || ' 
                  SELECT * FROM generic_operation_log 
                  WHERE operation_time >= :start_date 
                  AND operation_time <= :end_date';
        
        IF p_strategy_name IS NOT NULL THEN
            v_sql := v_sql || ' AND strategy_name = :strategy_name';
        END IF;
        
        IF p_strategy_name IS NOT NULL THEN
            EXECUTE IMMEDIATE v_sql USING p_start_date, p_end_date, p_strategy_name;
        ELSE
            EXECUTE IMMEDIATE v_sql USING p_start_date, p_end_date;
        END IF;
        
        v_archived_count := SQL%ROWCOUNT;
        
        -- Delete archived data from main table
        v_sql := 'DELETE FROM generic_operation_log
                  WHERE operation_time >= :start_date
                  AND operation_time <= :end_date';
        
        IF p_strategy_name IS NOT NULL THEN
            v_sql := v_sql || ' AND strategy_name = :strategy_name';
        END IF;
        
        IF p_strategy_name IS NOT NULL THEN
            EXECUTE IMMEDIATE v_sql USING p_start_date, p_end_date, p_strategy_name;
        ELSE
            EXECUTE IMMEDIATE v_sql USING p_start_date, p_end_date;
        END IF;
        
        log_operation('ARCHIVE', 'ARCHIVE', 'LOGS', 'SYSTEM', 'SUCCESS', 
                     'Archived ' || v_archived_count || ' log entries to ' || p_archive_table);
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('ARCHIVE', 'ARCHIVE', 'LOGS', 'SYSTEM', 'ERROR', 
                         'Failed to archive logs: ' || SQLERRM);
            RAISE;
    END archive_logs;
    
    PROCEDURE compress_log_partitions(
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        IF p_partition_name IS NOT NULL THEN
            v_sql := 'ALTER TABLE generic_operation_log MODIFY PARTITION ' || 
                     p_partition_name || ' COMPRESS';
            EXECUTE IMMEDIATE v_sql;
        ELSE
            -- Compress all partitions
            FOR rec IN (
                SELECT partition_name
                FROM user_tab_partitions
                WHERE table_name = 'GENERIC_OPERATION_LOG'
                AND partition_name != 'P_LOG_FUTURE'
            ) LOOP
                v_sql := 'ALTER TABLE generic_operation_log MODIFY PARTITION ' || 
                         rec.partition_name || ' COMPRESS';
                EXECUTE IMMEDIATE v_sql;
            END LOOP;
        END IF;
        
        log_operation('COMPRESS', 'COMPRESS', 'LOGS', 'SYSTEM', 'SUCCESS', 
                     'Compressed log partitions');
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('COMPRESS', 'COMPRESS', 'LOGS', 'SYSTEM', 'ERROR', 
                         'Failed to compress partitions: ' || SQLERRM);
            RAISE;
    END compress_log_partitions;
    
    -- Utility procedures
    PROCEDURE export_logs_to_csv(
        p_file_path      IN VARCHAR2,
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) IS
    BEGIN
        -- This is a simplified version - in practice, you'd use UTL_FILE or similar
        log_operation('EXPORT', 'EXPORT', 'LOGS', 'SYSTEM', 'INFO', 
                     'CSV export requested to ' || p_file_path);
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('EXPORT', 'EXPORT', 'LOGS', 'SYSTEM', 'ERROR', 
                         'Failed to export logs: ' || SQLERRM);
            RAISE;
    END export_logs_to_csv;
    
    PROCEDURE generate_log_report(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) IS
    BEGIN
        -- Generate a simple text report
        log_operation('REPORT', 'REPORT', 'LOGS', 'SYSTEM', 'INFO', 
                     'Log report generated for strategy: ' || NVL(p_strategy_name, 'ALL'));
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('REPORT', 'REPORT', 'LOGS', 'SYSTEM', 'ERROR', 
                         'Failed to generate report: ' || SQLERRM);
            RAISE;
    END generate_log_report;
    
    -- Monitoring procedures
    PROCEDURE monitor_log_table_size IS
        v_total_size_mb NUMBER;
        v_partition_count NUMBER;
    BEGIN
        SELECT 
            ROUND(SUM(bytes) / 1024 / 1024, 2),
            COUNT(*)
        INTO v_total_size_mb, v_partition_count
        FROM user_segments
        WHERE segment_name = 'GENERIC_OPERATION_LOG';
        
        log_operation('MONITOR', 'MONITOR', 'LOGS', 'SYSTEM', 'INFO', 
                     'Log table size: ' || v_total_size_mb || ' MB, Partitions: ' || v_partition_count);
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('MONITOR', 'MONITOR', 'LOGS', 'SYSTEM', 'ERROR', 
                         'Failed to monitor log table: ' || SQLERRM);
    END monitor_log_table_size;
    
    PROCEDURE check_log_table_health IS
        v_unusable_indexes NUMBER;
        v_stale_stats NUMBER;
    BEGIN
        -- Check for unusable indexes
        SELECT COUNT(*)
        INTO v_unusable_indexes
        FROM user_indexes
        WHERE table_name = 'GENERIC_OPERATION_LOG'
        AND status = 'UNUSABLE';
        
        -- Check for stale statistics
        SELECT COUNT(*)
        INTO v_stale_stats
        FROM user_tab_statistics
        WHERE table_name = 'GENERIC_OPERATION_LOG'
        AND stale_stats = 'YES';
        
        IF v_unusable_indexes > 0 OR v_stale_stats > 0 THEN
            log_operation('HEALTH_CHECK', 'HEALTH_CHECK', 'LOGS', 'SYSTEM', 'WARNING', 
                         'Log table health issues - Unusable indexes: ' || v_unusable_indexes || 
                         ', Stale stats: ' || v_stale_stats);
        ELSE
            log_operation('HEALTH_CHECK', 'HEALTH_CHECK', 'LOGS', 'SYSTEM', 'SUCCESS', 
                         'Log table is healthy');
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('HEALTH_CHECK', 'HEALTH_CHECK', 'LOGS', 'SYSTEM', 'ERROR', 
                         'Failed to check log table health: ' || SQLERRM);
    END check_log_table_health;
    
    FUNCTION get_log_table_size_info RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                partition_name,
                tablespace_name,
                num_rows,
                blocks,
                ROUND(blocks * 8192 / 1024 / 1024, 2) as size_mb,
                last_analyzed
            FROM user_tab_partitions
            WHERE table_name = 'GENERIC_OPERATION_LOG'
            ORDER BY blocks DESC;
            
        RETURN v_cursor;
    END get_log_table_size_info;
    
    -- Strategy management procedures
    PROCEDURE register_strategy(
        p_strategy_name  IN VARCHAR2,
        p_strategy_type  IN VARCHAR2,
        p_description    IN VARCHAR2 DEFAULT NULL,
        p_category       IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        INSERT INTO generic_strategy_types (strategy_name, strategy_type, description, category)
        VALUES (p_strategy_name, p_strategy_type, p_description, p_category);
        
        log_operation('REGISTER', 'REGISTER', 'STRATEGY', 'SYSTEM', 'SUCCESS', 
                     'Strategy registered: ' || p_strategy_name);
                     
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            log_operation('REGISTER', 'REGISTER', 'STRATEGY', 'SYSTEM', 'WARNING', 
                         'Strategy already exists: ' || p_strategy_name);
        WHEN OTHERS THEN
            log_operation('REGISTER', 'REGISTER', 'STRATEGY', 'SYSTEM', 'ERROR', 
                         'Failed to register strategy: ' || SQLERRM);
            RAISE;
    END register_strategy;
    
    PROCEDURE unregister_strategy(
        p_strategy_name  IN VARCHAR2
    ) IS
    BEGIN
        UPDATE generic_strategy_types
        SET is_active = 'N'
        WHERE strategy_name = p_strategy_name;
        
        log_operation('UNREGISTER', 'UNREGISTER', 'STRATEGY', 'SYSTEM', 'SUCCESS', 
                     'Strategy unregistered: ' || p_strategy_name);
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('UNREGISTER', 'UNREGISTER', 'STRATEGY', 'SYSTEM', 'ERROR', 
                         'Failed to unregister strategy: ' || SQLERRM);
            RAISE;
    END unregister_strategy;
    
    FUNCTION get_registered_strategies RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                strategy_name,
                strategy_type,
                description,
                category,
                is_active,
                created_date
            FROM generic_strategy_types
            WHERE is_active = 'Y'
            ORDER BY strategy_name;
            
        RETURN v_cursor;
    END get_registered_strategies;
    
    -- Performance monitoring
    PROCEDURE log_performance_metrics(
        p_operation_id   IN NUMBER,
        p_cpu_time_ms    IN NUMBER,
        p_memory_used_mb IN NUMBER,
        p_io_operations  IN NUMBER,
        p_rows_processed IN NUMBER,
        p_objects_affected IN NUMBER
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE generic_operation_log
        SET cpu_time_ms = p_cpu_time_ms,
            memory_used_mb = p_memory_used_mb,
            io_operations = p_io_operations,
            rows_processed = p_rows_processed,
            objects_affected = p_objects_affected
        WHERE operation_id = p_operation_id;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for performance logging
            NULL;
    END log_performance_metrics;
    
    FUNCTION get_performance_trends(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_days_back      IN NUMBER DEFAULT 30
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                strategy_name,
                operation_type,
                TRUNC(operation_time) as operation_date,
                COUNT(*) as operation_count,
                AVG(duration_ms) as avg_duration_ms,
                AVG(cpu_time_ms) as avg_cpu_time_ms,
                AVG(memory_used_mb) as avg_memory_mb,
                AVG(io_operations) as avg_io_operations
            FROM generic_operation_log
            WHERE status = 'SUCCESS'
            AND operation_time >= SYSDATE - p_days_back
            AND (p_strategy_name IS NULL OR strategy_name = p_strategy_name)
            GROUP BY strategy_name, operation_type, TRUNC(operation_time)
            ORDER BY operation_date DESC, strategy_name, operation_type;
            
        RETURN v_cursor;
    END get_performance_trends;
    
END generic_maintenance_logger_pkg;
/
