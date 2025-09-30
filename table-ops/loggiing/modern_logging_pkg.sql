-- =====================================================
-- Modern Logging Package - Lightweight and Efficient
-- Autonomous transactions with JSON support
-- Author: Principal Oracle Database Application Engineer
-- Version: 3.0 (Modern)
-- =====================================================

CREATE OR REPLACE PACKAGE modern_logging_pkg
AUTHID DEFINER
AS
    -- Modern logging levels
    c_debug   CONSTANT VARCHAR2(10) := 'DEBUG';
    c_info    CONSTANT VARCHAR2(10) := 'INFO';
    c_warn    CONSTANT VARCHAR2(10) := 'WARN';
    c_error   CONSTANT VARCHAR2(10) := 'ERROR';
    c_fatal   CONSTANT VARCHAR2(10) := 'FATAL';
    
    -- Log types
    c_operation   CONSTANT VARCHAR2(20) := 'OPERATION';
    c_performance CONSTANT VARCHAR2(20) := 'PERFORMANCE';
    c_error_type  CONSTANT VARCHAR2(20) := 'ERROR';
    c_audit       CONSTANT VARCHAR2(20) := 'AUDIT';
    
    -- Operation statuses
    c_started   CONSTANT VARCHAR2(20) := 'STARTED';
    c_running   CONSTANT VARCHAR2(20) := 'RUNNING';
    c_success   CONSTANT VARCHAR2(20) := 'SUCCESS';
    c_failed    CONSTANT VARCHAR2(20) := 'FAILED';
    c_cancelled CONSTANT VARCHAR2(20) := 'CANCELLED';
    
    -- Core logging procedures
    PROCEDURE log_message(
        p_level         IN VARCHAR2,
        p_message       IN VARCHAR2,
        p_operation_id  IN NUMBER DEFAULT NULL,
        p_table_name    IN VARCHAR2 DEFAULT NULL,
        p_error_code    IN NUMBER DEFAULT NULL
    );
    
    PROCEDURE log_operation_start(
        p_operation_type  IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_attributes      IN JSON DEFAULT NULL,
        p_operation_id    OUT NUMBER
    );
    
    PROCEDURE log_operation_end(
        p_operation_id    IN NUMBER,
        p_status          IN VARCHAR2,
        p_message         IN VARCHAR2 DEFAULT NULL,
        p_error_code      IN NUMBER DEFAULT NULL,
        p_error_message   IN VARCHAR2 DEFAULT NULL,
        p_duration_ms     IN NUMBER DEFAULT NULL,
        p_rows_processed  IN NUMBER DEFAULT NULL
    );
    
    PROCEDURE log_performance(
        p_operation_id    IN NUMBER,
        p_duration_ms     IN NUMBER,
        p_rows_processed  IN NUMBER DEFAULT NULL,
        p_attributes      IN JSON DEFAULT NULL
    );
    
    PROCEDURE log_error(
        p_error_code      IN NUMBER,
        p_error_message   IN VARCHAR2,
        p_operation_id    IN NUMBER DEFAULT NULL,
        p_table_name      IN VARCHAR2 DEFAULT NULL,
        p_context         IN VARCHAR2 DEFAULT NULL
    );
    
    -- Utility functions
    FUNCTION get_next_operation_id RETURN NUMBER;
    
    FUNCTION create_attributes_json(
        p_parallel_degree IN NUMBER DEFAULT NULL,
        p_tablespace      IN VARCHAR2 DEFAULT NULL,
        p_batch_size      IN NUMBER DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT NULL,
        p_compression     IN VARCHAR2 DEFAULT NULL
    ) RETURN JSON;
    
    FUNCTION is_logging_enabled(p_level IN VARCHAR2) RETURN BOOLEAN;
    
    -- Maintenance procedures
    PROCEDURE cleanup_old_logs(p_days_old IN NUMBER DEFAULT 90);
    PROCEDURE archive_logs(p_days_old IN NUMBER DEFAULT 30);
    
    -- Configuration
    PROCEDURE set_log_level(p_level IN VARCHAR2);
    FUNCTION get_log_level RETURN VARCHAR2;
    
END modern_logging_pkg;
/