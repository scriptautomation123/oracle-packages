-- =====================================================
-- Modern Logging Package Body - Implementation
-- High-performance autonomous logging with JSON
-- Author: Principal Oracle Database Application Engineer
-- Version: 3.0 (Modern)
-- =====================================================

CREATE OR REPLACE PACKAGE BODY modern_logging_pkg
AS
    -- Private variables
    g_log_level VARCHAR2(10) := c_info;
    g_logging_enabled BOOLEAN := TRUE;
    
    -- Private level hierarchy for filtering
    FUNCTION get_level_priority(p_level VARCHAR2) RETURN NUMBER IS
    BEGIN
        CASE UPPER(p_level)
            WHEN c_debug THEN RETURN 1;
            WHEN c_info THEN RETURN 2;
            WHEN c_warn THEN RETURN 3;
            WHEN c_error THEN RETURN 4;
            WHEN c_fatal THEN RETURN 5;
            ELSE RETURN 2; -- Default to INFO
        END CASE;
    END get_level_priority;
    
    -- Core autonomous logging procedure
    PROCEDURE write_log_entry(
        p_log_level       IN VARCHAR2,
        p_log_type        IN VARCHAR2,
        p_operation_id    IN NUMBER,
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_operation_type  IN VARCHAR2,
        p_operation_status IN VARCHAR2,
        p_message         IN VARCHAR2,
        p_error_code      IN NUMBER,
        p_error_message   IN VARCHAR2,
        p_duration_ms     IN NUMBER,
        p_rows_processed  IN NUMBER,
        p_attributes      IN JSON
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO partition_operations_log (
            log_level,
            log_type,
            operation_id,
            table_name,
            partition_name,
            operation_type,
            operation_status,
            message,
            error_code,
            error_message,
            duration_ms,
            rows_processed,
            attributes
        ) VALUES (
            p_log_level,
            p_log_type,
            p_operation_id,
            p_table_name,
            p_partition_name,
            p_operation_type,
            p_operation_status,
            p_message,
            p_error_code,
            p_error_message,
            p_duration_ms,
            p_rows_processed,
            p_attributes
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for logging - never break the main operation
            ROLLBACK;
    END write_log_entry;
    
    -- Public procedures
    PROCEDURE log_message(
        p_level         IN VARCHAR2,
        p_message       IN VARCHAR2,
        p_operation_id  IN NUMBER DEFAULT NULL,
        p_table_name    IN VARCHAR2 DEFAULT NULL,
        p_error_code    IN NUMBER DEFAULT NULL
    ) IS
    BEGIN
        -- Check if logging is enabled for this level
        IF NOT is_logging_enabled(p_level) THEN
            RETURN;
        END IF;
        
        write_log_entry(
            p_log_level => p_level,
            p_log_type => CASE WHEN p_error_code IS NOT NULL THEN c_error_type ELSE c_operation END,
            p_operation_id => p_operation_id,
            p_table_name => p_table_name,
            p_partition_name => NULL,
            p_operation_type => NULL,
            p_operation_status => NULL,
            p_message => p_message,
            p_error_code => p_error_code,
            p_error_message => NULL,
            p_duration_ms => NULL,
            p_rows_processed => NULL,
            p_attributes => NULL
        );
    END log_message;
    
    PROCEDURE log_operation_start(
        p_operation_type  IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_attributes      IN JSON DEFAULT NULL,
        p_operation_id    OUT NUMBER
    ) IS
    BEGIN
        -- Get next operation ID
        p_operation_id := get_next_operation_id();
        
        -- Log operation start
        write_log_entry(
            p_log_level => c_info,
            p_log_type => c_operation,
            p_operation_id => p_operation_id,
            p_table_name => p_table_name,
            p_partition_name => p_partition_name,
            p_operation_type => p_operation_type,
            p_operation_status => c_started,
            p_message => 'Operation started: ' || p_operation_type || 
                        CASE WHEN p_partition_name IS NOT NULL THEN ' on partition ' || p_partition_name ELSE '' END,
            p_error_code => NULL,
            p_error_message => NULL,
            p_duration_ms => NULL,
            p_rows_processed => NULL,
            p_attributes => p_attributes
        );
    END log_operation_start;
    
    PROCEDURE log_operation_end(
        p_operation_id    IN NUMBER,
        p_status          IN VARCHAR2,
        p_message         IN VARCHAR2 DEFAULT NULL,
        p_error_code      IN NUMBER DEFAULT NULL,
        p_error_message   IN VARCHAR2 DEFAULT NULL,
        p_duration_ms     IN NUMBER DEFAULT NULL,
        p_rows_processed  IN NUMBER DEFAULT NULL
    ) IS
        v_log_level VARCHAR2(10);
        v_final_message VARCHAR2(4000);
    BEGIN
        -- Determine log level based on status
        v_log_level := CASE 
            WHEN p_status = c_success THEN c_info
            WHEN p_status = c_failed THEN c_error
            WHEN p_status = c_cancelled THEN c_warn
            ELSE c_info
        END;
        
        -- Build comprehensive message
        v_final_message := NVL(p_message, 'Operation completed with status: ' || p_status);
        IF p_duration_ms IS NOT NULL THEN
            v_final_message := v_final_message || ' (Duration: ' || 
                              CASE 
                                  WHEN p_duration_ms >= 60000 THEN ROUND(p_duration_ms/60000, 1) || ' min'
                                  WHEN p_duration_ms >= 1000 THEN ROUND(p_duration_ms/1000, 1) || ' sec'
                                  ELSE p_duration_ms || ' ms'
                              END || ')';
        END IF;
        
        write_log_entry(
            p_log_level => v_log_level,
            p_log_type => c_operation,
            p_operation_id => p_operation_id,
            p_table_name => NULL, -- Will be filled from context
            p_partition_name => NULL,
            p_operation_type => NULL,
            p_operation_status => p_status,
            p_message => v_final_message,
            p_error_code => p_error_code,
            p_error_message => p_error_message,
            p_duration_ms => p_duration_ms,
            p_rows_processed => p_rows_processed,
            p_attributes => NULL
        );
    END log_operation_end;
    
    PROCEDURE log_performance(
        p_operation_id    IN NUMBER,
        p_duration_ms     IN NUMBER,
        p_rows_processed  IN NUMBER DEFAULT NULL,
        p_attributes      IN JSON DEFAULT NULL
    ) IS
        v_message VARCHAR2(4000);
    BEGIN
        -- Build performance message
        v_message := 'Performance metrics - Duration: ' || p_duration_ms || 'ms';
        IF p_rows_processed IS NOT NULL THEN
            v_message := v_message || ', Rows: ' || p_rows_processed;
            IF p_duration_ms > 0 THEN
                v_message := v_message || ', Rate: ' || ROUND(p_rows_processed / (p_duration_ms/1000), 0) || ' rows/sec';
            END IF;
        END IF;
        
        write_log_entry(
            p_log_level => c_info,
            p_log_type => c_performance,
            p_operation_id => p_operation_id,
            p_table_name => NULL,
            p_partition_name => NULL,
            p_operation_type => NULL,
            p_operation_status => NULL,
            p_message => v_message,
            p_error_code => NULL,
            p_error_message => NULL,
            p_duration_ms => p_duration_ms,
            p_rows_processed => p_rows_processed,
            p_attributes => p_attributes
        );
    END log_performance;
    
    PROCEDURE log_error(
        p_error_code      IN NUMBER,
        p_error_message   IN VARCHAR2,
        p_operation_id    IN NUMBER DEFAULT NULL,
        p_table_name      IN VARCHAR2 DEFAULT NULL,
        p_context         IN VARCHAR2 DEFAULT NULL
    ) IS
        v_message VARCHAR2(4000);
    BEGIN
        v_message := 'Error occurred';
        IF p_context IS NOT NULL THEN
            v_message := v_message || ' in ' || p_context;
        END IF;
        v_message := v_message || ': ' || p_error_message;
        
        write_log_entry(
            p_log_level => c_error,
            p_log_type => c_error_type,
            p_operation_id => p_operation_id,
            p_table_name => p_table_name,
            p_partition_name => NULL,
            p_operation_type => NULL,
            p_operation_status => c_failed,
            p_message => v_message,
            p_error_code => p_error_code,
            p_error_message => p_error_message,
            p_duration_ms => NULL,
            p_rows_processed => NULL,
            p_attributes => NULL
        );
    END log_error;
    
    -- Utility functions
    FUNCTION get_next_operation_id RETURN NUMBER IS
        v_operation_id NUMBER;
    BEGIN
        SELECT partition_operation_seq.NEXTVAL INTO v_operation_id FROM DUAL;
        RETURN v_operation_id;
    END get_next_operation_id;
    
    FUNCTION create_attributes_json(
        p_parallel_degree IN NUMBER DEFAULT NULL,
        p_tablespace      IN VARCHAR2 DEFAULT NULL,
        p_batch_size      IN NUMBER DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT NULL,
        p_compression     IN VARCHAR2 DEFAULT NULL
    ) RETURN JSON IS
        v_json_obj JSON_OBJECT_T;
    BEGIN
        v_json_obj := JSON_OBJECT_T();
        
        IF p_parallel_degree IS NOT NULL THEN
            v_json_obj.put('parallel_degree', p_parallel_degree);
        END IF;
        
        IF p_tablespace IS NOT NULL THEN
            v_json_obj.put('tablespace', p_tablespace);
        END IF;
        
        IF p_batch_size IS NOT NULL THEN
            v_json_obj.put('batch_size', p_batch_size);
        END IF;
        
        IF p_online IS NOT NULL THEN
            v_json_obj.put('online_operation', CASE WHEN p_online THEN 'true' ELSE 'false' END);
        END IF;
        
        IF p_compression IS NOT NULL THEN
            v_json_obj.put('compression', p_compression);
        END IF;
        
        RETURN TREAT(v_json_obj AS JSON);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL; -- Return NULL if JSON creation fails
    END create_attributes_json;
    
    FUNCTION is_logging_enabled(p_level IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN g_logging_enabled AND 
               get_level_priority(p_level) >= get_level_priority(g_log_level);
    END is_logging_enabled;
    
    -- Maintenance procedures
    PROCEDURE cleanup_old_logs(p_days_old IN NUMBER DEFAULT 90) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_rows_deleted NUMBER;
        v_cutoff_date DATE;
    BEGIN
        v_cutoff_date := SYSDATE - p_days_old;
        
        DELETE FROM partition_operations_log
        WHERE log_timestamp < v_cutoff_date;
        
        v_rows_deleted := SQL%ROWCOUNT;
        COMMIT;
        
        log_message(c_info, 'Cleanup completed: ' || v_rows_deleted || ' old log entries removed');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_error(SQLCODE, SQLERRM, NULL, NULL, 'cleanup_old_logs');
    END cleanup_old_logs;
    
    PROCEDURE archive_logs(p_days_old IN NUMBER DEFAULT 30) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_rows_archived NUMBER;
        v_cutoff_date DATE;
    BEGIN
        v_cutoff_date := SYSDATE - p_days_old;
        
        -- In real implementation, this would copy to archive table/location
        -- For now, just mark as demonstration
        UPDATE partition_operations_log
        SET attributes = JSON_OBJECT('archived' VALUE 'true' RETURNING JSON)
        WHERE log_timestamp < v_cutoff_date
          AND JSON_VALUE(attributes, '$.archived') IS NULL;
        
        v_rows_archived := SQL%ROWCOUNT;
        COMMIT;
        
        log_message(c_info, 'Archive completed: ' || v_rows_archived || ' log entries marked for archival');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_error(SQLCODE, SQLERRM, NULL, NULL, 'archive_logs');
    END archive_logs;
    
    -- Configuration
    PROCEDURE set_log_level(p_level IN VARCHAR2) IS
    BEGIN
        IF p_level IN (c_debug, c_info, c_warn, c_error, c_fatal) THEN
            g_log_level := p_level;
            log_message(c_info, 'Log level changed to: ' || p_level);
        ELSE
            RAISE_APPLICATION_ERROR(-20401, 'Invalid log level: ' || p_level);
        END IF;
    END set_log_level;
    
    FUNCTION get_log_level RETURN VARCHAR2 IS
    BEGIN
        RETURN g_log_level;
    END get_log_level;
    
END modern_logging_pkg;
/