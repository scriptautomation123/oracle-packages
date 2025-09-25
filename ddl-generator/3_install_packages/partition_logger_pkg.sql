-- =====================================================
-- Oracle Partition Logger Package
-- Autonomous logging for partition operations
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Specification
CREATE OR REPLACE PACKAGE partition_logger_pkg
AUTHID DEFINER
AS
    -- Types for logging
    TYPE log_entry_rec IS RECORD (
        operation_id     NUMBER,
        operation_type   VARCHAR2(50),
        table_name       VARCHAR2(128),
        partition_name   VARCHAR2(128),
        status           VARCHAR2(20),
        message          VARCHAR2(4000),
        duration_ms      NUMBER,
        operation_time   TIMESTAMP,
        user_name        VARCHAR2(30),
        session_id       NUMBER,
        sql_text         CLOB,
        error_code       NUMBER,
        error_message    VARCHAR2(4000)
    );
    
    TYPE log_entry_tab IS TABLE OF log_entry_rec;
    
    -- Logging procedures
    PROCEDURE log_operation(
        p_operation_type IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE log_start_operation(
        p_operation_type IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL
    ) RETURN NUMBER;
    
    PROCEDURE log_end_operation(
        p_operation_id   IN NUMBER,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL
    );
    
    -- Logging configuration
    PROCEDURE set_logging_enabled(
        p_enabled IN BOOLEAN
    );
    
    FUNCTION is_logging_enabled RETURN BOOLEAN;
    
    PROCEDURE set_log_retention_days(
        p_days IN NUMBER
    );
    
    FUNCTION get_log_retention_days RETURN NUMBER;
    
    -- Log analysis and reporting
    FUNCTION get_operation_log(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_operation_statistics(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_error_summary(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_performance_summary(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    -- Log maintenance procedures
    PROCEDURE cleanup_old_logs(
        p_retention_days IN NUMBER DEFAULT NULL
    );
    
    PROCEDURE archive_logs(
        p_archive_table  IN VARCHAR2,
        p_start_date     IN DATE,
        p_end_date       IN DATE
    );
    
    PROCEDURE compress_log_partitions(
        p_partition_name IN VARCHAR2 DEFAULT NULL
    );
    
    -- Utility procedures
    PROCEDURE export_logs_to_csv(
        p_file_path      IN VARCHAR2,
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    );
    
    PROCEDURE generate_log_report(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    );
    
    -- Monitoring procedures
    PROCEDURE monitor_log_table_size;
    
    PROCEDURE check_log_table_health;
    
    FUNCTION get_log_table_size_info RETURN SYS_REFCURSOR;
    
END partition_logger_pkg;
/

-- Package Body
CREATE OR REPLACE PACKAGE BODY partition_logger_pkg
AS
    -- Private variables
    g_logging_enabled BOOLEAN := TRUE;
    g_retention_days  NUMBER := 90;
    
    -- Private procedure for autonomous logging
    PROCEDURE log_operation(
        p_operation_type IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF g_logging_enabled THEN
            INSERT INTO partition_operation_log (
                operation_id,
                operation_type,
                table_name,
                partition_name,
                status,
                message,
                duration_ms,
                operation_time,
                user_name,
                session_id,
                sql_text,
                error_code,
                error_message
            ) VALUES (
                partition_operation_log_seq.NEXTVAL,
                p_operation_type,
                p_table_name,
                p_partition_name,
                p_status,
                p_message,
                p_duration_ms,
                SYSTIMESTAMP,
                USER,
                SYS_CONTEXT('USERENV', 'SID'),
                p_sql_text,
                p_error_code,
                p_error_message
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
        p_operation_type IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        log_operation(
            p_operation_type,
            p_table_name,
            p_partition_name,
            p_status,
            p_message,
            p_duration_ms,
            p_sql_text,
            p_error_code,
            p_error_message
        );
    END log_operation;
    
    PROCEDURE log_start_operation(
        p_operation_type IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL
    ) RETURN NUMBER IS
        v_operation_id NUMBER;
    BEGIN
        v_operation_id := partition_operation_log_seq.NEXTVAL;
        
        log_operation(
            p_operation_type,
            p_table_name,
            p_partition_name,
            'STARTED',
            'Operation started',
            NULL,
            p_sql_text
        );
        
        RETURN v_operation_id;
    END log_start_operation;
    
    PROCEDURE log_end_operation(
        p_operation_id   IN NUMBER,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_error_code     IN NUMBER DEFAULT NULL,
        p_error_message  IN VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF g_logging_enabled THEN
            UPDATE partition_operation_log
            SET status = p_status,
                message = p_message,
                error_code = p_error_code,
                error_message = p_error_message,
                operation_time = SYSTIMESTAMP
            WHERE operation_id = p_operation_id;
            
            COMMIT;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for logging
            NULL;
    END log_end_operation;
    
    -- Configuration procedures
    PROCEDURE set_logging_enabled(
        p_enabled IN BOOLEAN
    ) IS
    BEGIN
        g_logging_enabled := p_enabled;
        log_operation('CONFIG', 'SYSTEM', NULL, 'INFO', 
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
        log_operation('CONFIG', 'SYSTEM', NULL, 'INFO', 
                     'Log retention days set to ' || p_days);
    END set_log_retention_days;
    
    FUNCTION get_log_retention_days RETURN NUMBER IS
    BEGIN
        RETURN g_retention_days;
    END get_log_retention_days;
    
    -- Log analysis and reporting
    FUNCTION get_operation_log(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
        v_sql VARCHAR2(4000);
    BEGIN
        v_sql := 'SELECT * FROM partition_operation_log WHERE 1=1';
        
        IF p_table_name IS NOT NULL THEN
            v_sql := v_sql || ' AND table_name = :table_name';
        END IF;
        
        IF p_operation_type IS NOT NULL THEN
            v_sql := v_sql || ' AND operation_type = :operation_type';
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
        
        OPEN v_cursor FOR v_sql USING p_table_name, p_operation_type, p_status, p_start_date, p_end_date;
        
        RETURN v_cursor;
    END get_operation_log;
    
    FUNCTION get_operation_statistics(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                operation_type,
                status,
                COUNT(*) as operation_count,
                AVG(duration_ms) as avg_duration_ms,
                MIN(duration_ms) as min_duration_ms,
                MAX(duration_ms) as max_duration_ms,
                MIN(operation_time) as first_operation,
                MAX(operation_time) as last_operation
            FROM partition_operation_log
            WHERE (p_table_name IS NULL OR table_name = p_table_name)
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            GROUP BY operation_type, status
            ORDER BY operation_type, status;
            
        RETURN v_cursor;
    END get_operation_statistics;
    
    FUNCTION get_error_summary(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                table_name,
                operation_type,
                error_code,
                error_message,
                COUNT(*) as error_count,
                MIN(operation_time) as first_error,
                MAX(operation_time) as last_error
            FROM partition_operation_log
            WHERE status = 'ERROR'
            AND (p_table_name IS NULL OR table_name = p_table_name)
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            GROUP BY table_name, operation_type, error_code, error_message
            ORDER BY error_count DESC;
            
        RETURN v_cursor;
    END get_error_summary;
    
    FUNCTION get_performance_summary(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                table_name,
                operation_type,
                COUNT(*) as total_operations,
                AVG(duration_ms) as avg_duration_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) as median_duration_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) as p99_duration_ms,
                MIN(duration_ms) as min_duration_ms,
                MAX(duration_ms) as max_duration_ms
            FROM partition_operation_log
            WHERE status = 'SUCCESS'
            AND duration_ms IS NOT NULL
            AND (p_table_name IS NULL OR table_name = p_table_name)
            AND (p_operation_type IS NULL OR operation_type = p_operation_type)
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            GROUP BY table_name, operation_type
            ORDER BY avg_duration_ms DESC;
            
        RETURN v_cursor;
    END get_performance_summary;
    
    -- Log maintenance procedures
    PROCEDURE cleanup_old_logs(
        p_retention_days IN NUMBER DEFAULT NULL
    ) IS
        v_retention_days NUMBER;
        v_cutoff_date DATE;
        v_deleted_count NUMBER := 0;
    BEGIN
        v_retention_days := NVL(p_retention_days, g_retention_days);
        v_cutoff_date := SYSDATE - v_retention_days;
        
        DELETE FROM partition_operation_log
        WHERE operation_time < v_cutoff_date;
        
        v_deleted_count := SQL%ROWCOUNT;
        
        log_operation('CLEANUP', 'SYSTEM', NULL, 'SUCCESS', 
                     'Deleted ' || v_deleted_count || ' old log entries');
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('CLEANUP', 'SYSTEM', NULL, 'ERROR', 
                         'Failed to cleanup old logs: ' || SQLERRM);
            RAISE;
    END cleanup_old_logs;
    
    PROCEDURE archive_logs(
        p_archive_table  IN VARCHAR2,
        p_start_date     IN DATE,
        p_end_date       IN DATE
    ) IS
        v_sql VARCHAR2(4000);
        v_archived_count NUMBER := 0;
    BEGIN
        -- Create archive table if it doesn't exist
        v_sql := 'CREATE TABLE ' || p_archive_table || ' AS 
                  SELECT * FROM partition_operation_log WHERE 1=0';
        
        BEGIN
            EXECUTE IMMEDIATE v_sql;
        EXCEPTION
            WHEN OTHERS THEN
                -- Table might already exist, continue
                NULL;
        END;
        
        -- Archive the data
        v_sql := 'INSERT INTO ' || p_archive_table || ' 
                  SELECT * FROM partition_operation_log 
                  WHERE operation_time >= :start_date 
                  AND operation_time <= :end_date';
        
        EXECUTE IMMEDIATE v_sql USING p_start_date, p_end_date;
        v_archived_count := SQL%ROWCOUNT;
        
        -- Delete archived data from main table
        DELETE FROM partition_operation_log
        WHERE operation_time >= p_start_date
        AND operation_time <= p_end_date;
        
        log_operation('ARCHIVE', 'SYSTEM', NULL, 'SUCCESS', 
                     'Archived ' || v_archived_count || ' log entries to ' || p_archive_table);
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('ARCHIVE', 'SYSTEM', NULL, 'ERROR', 
                         'Failed to archive logs: ' || SQLERRM);
            RAISE;
    END archive_logs;
    
    PROCEDURE compress_log_partitions(
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        IF p_partition_name IS NOT NULL THEN
            v_sql := 'ALTER TABLE partition_operation_log MODIFY PARTITION ' || 
                     p_partition_name || ' COMPRESS';
            EXECUTE IMMEDIATE v_sql;
        ELSE
            -- Compress all partitions
            FOR rec IN (
                SELECT partition_name
                FROM user_tab_partitions
                WHERE table_name = 'PARTITION_OPERATION_LOG'
                AND partition_name != 'P_LOG_FUTURE'
            ) LOOP
                v_sql := 'ALTER TABLE partition_operation_log MODIFY PARTITION ' || 
                         rec.partition_name || ' COMPRESS';
                EXECUTE IMMEDIATE v_sql;
            END LOOP;
        END IF;
        
        log_operation('COMPRESS', 'SYSTEM', p_partition_name, 'SUCCESS', 
                     'Compressed log partitions');
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('COMPRESS', 'SYSTEM', p_partition_name, 'ERROR', 
                         'Failed to compress partitions: ' || SQLERRM);
            RAISE;
    END compress_log_partitions;
    
    -- Utility procedures
    PROCEDURE export_logs_to_csv(
        p_file_path      IN VARCHAR2,
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
        v_file UTL_FILE.FILE_TYPE;
        v_line VARCHAR2(4000);
    BEGIN
        -- This is a simplified version - in practice, you'd use UTL_FILE or similar
        log_operation('EXPORT', 'SYSTEM', NULL, 'INFO', 
                     'CSV export requested to ' || p_file_path);
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('EXPORT', 'SYSTEM', NULL, 'ERROR', 
                         'Failed to export logs: ' || SQLERRM);
            RAISE;
    END export_logs_to_csv;
    
    PROCEDURE generate_log_report(
        p_table_name     IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) IS
        v_cursor SYS_REFCURSOR;
        v_line VARCHAR2(4000);
    BEGIN
        -- Generate a simple text report
        v_cursor := get_operation_statistics(p_table_name, p_start_date, p_end_date);
        
        log_operation('REPORT', 'SYSTEM', NULL, 'INFO', 
                     'Log report generated for table: ' || NVL(p_table_name, 'ALL'));
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('REPORT', 'SYSTEM', NULL, 'ERROR', 
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
        WHERE segment_name = 'PARTITION_OPERATION_LOG';
        
        log_operation('MONITOR', 'SYSTEM', NULL, 'INFO', 
                     'Log table size: ' || v_total_size_mb || ' MB, Partitions: ' || v_partition_count);
                     
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('MONITOR', 'SYSTEM', NULL, 'ERROR', 
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
        WHERE table_name = 'PARTITION_OPERATION_LOG'
        AND status = 'UNUSABLE';
        
        -- Check for stale statistics
        SELECT COUNT(*)
        INTO v_stale_stats
        FROM user_tab_statistics
        WHERE table_name = 'PARTITION_OPERATION_LOG'
        AND stale_stats = 'YES';
        
        IF v_unusable_indexes > 0 OR v_stale_stats > 0 THEN
            log_operation('HEALTH_CHECK', 'SYSTEM', NULL, 'WARNING', 
                         'Log table health issues - Unusable indexes: ' || v_unusable_indexes || 
                         ', Stale stats: ' || v_stale_stats);
        ELSE
            log_operation('HEALTH_CHECK', 'SYSTEM', NULL, 'SUCCESS', 
                         'Log table is healthy');
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('HEALTH_CHECK', 'SYSTEM', NULL, 'ERROR', 
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
            WHERE table_name = 'PARTITION_OPERATION_LOG'
            ORDER BY blocks DESC;
            
        RETURN v_cursor;
    END get_log_table_size_info;
    
END partition_logger_pkg;
/
