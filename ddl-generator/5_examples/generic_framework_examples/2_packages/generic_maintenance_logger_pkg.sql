-- =====================================================
-- Oracle Generic Maintenance Logger Package
-- Autonomous logging for maintenance operations
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Specification
CREATE OR REPLACE PACKAGE generic_maintenance_logger_pkg
AUTHID DEFINER
AS
    -- Types for logging
    TYPE log_entry_rec IS RECORD (
        operation_id     NUMBER,
        strategy_name    VARCHAR2(50),
        operation_type   VARCHAR2(50),
        job_name         VARCHAR2(128),
        target_object    VARCHAR2(128),
        target_type      VARCHAR2(30),
        status           VARCHAR2(20),
        message          VARCHAR2(4000),
        duration_ms      NUMBER,
        operation_time   TIMESTAMP,
        user_name        VARCHAR2(30),
        session_id       NUMBER,
        sql_text         CLOB,
        error_code       NUMBER,
        error_message    VARCHAR2(4000),
        strategy_context CLOB,
        cpu_time_ms      NUMBER,
        memory_used_mb   NUMBER,
        io_operations    NUMBER,
        rows_processed   NUMBER,
        objects_affected NUMBER
    );
    
    TYPE log_entry_tab IS TABLE OF log_entry_rec;
    
    -- Core logging procedures
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
    );
    
    PROCEDURE log_start_operation(
        p_strategy_name  IN VARCHAR2,
        p_operation_type IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_sql_text       IN CLOB DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL,
        p_job_name       IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;
    
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
    );
    
    -- Strategy-specific logging procedures
    PROCEDURE log_strategy_start(
        p_strategy_name  IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL
    ) RETURN NUMBER;
    
    PROCEDURE log_strategy_end(
        p_operation_id   IN NUMBER,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_performance_metrics IN CLOB DEFAULT NULL
    );
    
    PROCEDURE log_job_execution(
        p_strategy_name  IN VARCHAR2,
        p_job_name       IN VARCHAR2,
        p_target_object  IN VARCHAR2,
        p_target_type    IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL,
        p_strategy_context IN CLOB DEFAULT NULL
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
    
    PROCEDURE set_strategy_logging(
        p_strategy_name IN VARCHAR2,
        p_enabled IN BOOLEAN
    );
    
    FUNCTION is_strategy_logging_enabled(
        p_strategy_name IN VARCHAR2
    ) RETURN BOOLEAN;
    
    -- Log analysis and reporting
    FUNCTION get_operation_log(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_target_object  IN VARCHAR2 DEFAULT NULL,
        p_status         IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_strategy_statistics(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_performance_summary(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_operation_type IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_error_summary(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_job_execution_summary(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_job_name       IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    -- Log maintenance procedures
    PROCEDURE cleanup_old_logs(
        p_retention_days IN NUMBER DEFAULT NULL,
        p_strategy_name  IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE archive_logs(
        p_archive_table  IN VARCHAR2,
        p_start_date     IN DATE,
        p_end_date       IN DATE,
        p_strategy_name  IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE compress_log_partitions(
        p_partition_name IN VARCHAR2 DEFAULT NULL
    );
    
    -- Utility procedures
    PROCEDURE export_logs_to_csv(
        p_file_path      IN VARCHAR2,
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    );
    
    PROCEDURE generate_log_report(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_start_date     IN DATE DEFAULT NULL,
        p_end_date       IN DATE DEFAULT NULL
    );
    
    -- Monitoring procedures
    PROCEDURE monitor_log_table_size;
    
    PROCEDURE check_log_table_health;
    
    FUNCTION get_log_table_size_info RETURN SYS_REFCURSOR;
    
    -- Strategy management procedures
    PROCEDURE register_strategy(
        p_strategy_name  IN VARCHAR2,
        p_strategy_type  IN VARCHAR2,
        p_description    IN VARCHAR2 DEFAULT NULL,
        p_category       IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE unregister_strategy(
        p_strategy_name  IN VARCHAR2
    );
    
    FUNCTION get_registered_strategies RETURN SYS_REFCURSOR;
    
    -- Performance monitoring
    PROCEDURE log_performance_metrics(
        p_operation_id   IN NUMBER,
        p_cpu_time_ms    IN NUMBER,
        p_memory_used_mb IN NUMBER,
        p_io_operations  IN NUMBER,
        p_rows_processed IN NUMBER,
        p_objects_affected IN NUMBER
    );
    
    FUNCTION get_performance_trends(
        p_strategy_name  IN VARCHAR2 DEFAULT NULL,
        p_days_back      IN NUMBER DEFAULT 30
    ) RETURN SYS_REFCURSOR;
    
END generic_maintenance_logger_pkg;
/
