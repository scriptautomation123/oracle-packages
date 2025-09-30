-- =====================================================
-- Logging Views for Different Use Cases
-- Specialized views over the single logging table
-- Author: Principal Oracle Database Application Engineer
-- Version: 3.0 (Modern)
-- =====================================================

-- 1. Operation Log View - High-level operation tracking
CREATE OR REPLACE VIEW v_partition_operations AS
SELECT 
    log_id,
    log_timestamp,
    operation_id,
    table_name,
    partition_name,
    operation_type,
    operation_status,
    message,
    duration_ms,
    rows_processed,
    username,
    -- JSON attributes extraction
    JSON_VALUE(attributes, '$.parallel_degree') as parallel_degree,
    JSON_VALUE(attributes, '$.tablespace') as tablespace,
    JSON_VALUE(attributes, '$.high_value') as high_value,
    -- Calculated fields
    CASE 
        WHEN duration_ms IS NOT NULL THEN ROUND(duration_ms/1000, 2)
        ELSE NULL 
    END as duration_seconds,
    CASE 
        WHEN operation_status = 'SUCCESS' THEN 'COMPLETED'
        WHEN operation_status = 'FAILED' THEN 'ERROR'
        ELSE operation_status
    END as status_description
FROM partition_operations_log
WHERE log_type = 'OPERATION'
  AND log_level IN ('INFO', 'WARN', 'ERROR')
ORDER BY log_timestamp DESC;

-- 2. Error Log View - Focus on errors and warnings
CREATE OR REPLACE VIEW v_partition_errors AS
SELECT 
    log_id,
    log_timestamp,
    operation_id,
    log_level,
    table_name,
    partition_name,
    operation_type,
    message,
    error_code,
    error_message,
    username,
    session_id,
    -- Context information
    module_name,
    action_name,
    client_info
FROM partition_operations_log
WHERE log_level IN ('WARN', 'ERROR', 'FATAL')
   OR error_code IS NOT NULL
ORDER BY log_timestamp DESC;

-- 3. Performance Log View - Focus on performance metrics
CREATE OR REPLACE VIEW v_partition_performance AS
SELECT 
    log_id,
    log_timestamp,
    operation_id,
    table_name,
    partition_name,
    operation_type,
    operation_status,
    duration_ms,
    rows_processed,
    -- Performance calculations
    CASE 
        WHEN duration_ms > 0 AND rows_processed > 0 THEN 
            ROUND(rows_processed / (duration_ms/1000), 0)
        ELSE NULL 
    END as rows_per_second,
    CASE 
        WHEN duration_ms >= 60000 THEN ROUND(duration_ms/60000, 1) || ' min'
        WHEN duration_ms >= 1000 THEN ROUND(duration_ms/1000, 1) || ' sec'
        ELSE duration_ms || ' ms'
    END as duration_formatted,
    -- JSON performance attributes
    JSON_VALUE(attributes, '$.parallel_degree') as parallel_degree,
    JSON_VALUE(attributes, '$.batch_size') as batch_size,
    JSON_VALUE(attributes, '$.compression') as compression_used,
    JSON_VALUE(attributes, '$.online_operation') as online_operation
FROM partition_operations_log
WHERE log_type IN ('OPERATION', 'PERFORMANCE')
  AND (duration_ms IS NOT NULL OR rows_processed IS NOT NULL)
ORDER BY log_timestamp DESC;

-- 4. Audit Log View - Security and compliance tracking
CREATE OR REPLACE VIEW v_partition_audit AS
SELECT 
    log_id,
    log_timestamp,
    operation_id,
    log_type,
    table_name,
    partition_name,
    operation_type,
    operation_status,
    username,
    session_id,
    client_info,
    module_name,
    action_name,
    message,
    -- Sensitive operation flags
    CASE 
        WHEN operation_type IN ('DROP', 'TRUNCATE') THEN 'DESTRUCTIVE'
        WHEN operation_type IN ('MOVE', 'SPLIT', 'MERGE') THEN 'STRUCTURAL'
        WHEN operation_type IN ('CREATE', 'ADD') THEN 'ADDITIVE'
        ELSE 'OTHER'
    END as operation_category,
    CASE 
        WHEN operation_status = 'SUCCESS' AND operation_type IN ('DROP', 'TRUNCATE') THEN 'Y'
        ELSE 'N'
    END as data_loss_risk
FROM partition_operations_log
WHERE log_type IN ('OPERATION', 'AUDIT')
ORDER BY log_timestamp DESC;

-- 5. Recent Activity View - Dashboard view for monitoring
CREATE OR REPLACE VIEW v_partition_recent_activity AS
SELECT 
    log_id,
    log_timestamp,
    operation_id,
    log_level,
    table_name,
    operation_type,
    operation_status,
    duration_ms,
    message,
    username,
    -- Recent time indicators
    CASE 
        WHEN log_timestamp >= SYSTIMESTAMP - INTERVAL '1' HOUR THEN 'LAST_HOUR'
        WHEN log_timestamp >= SYSTIMESTAMP - INTERVAL '1' DAY THEN 'LAST_DAY'
        WHEN log_timestamp >= SYSTIMESTAMP - INTERVAL '7' DAY THEN 'LAST_WEEK'
        ELSE 'OLDER'
    END as time_category,
    -- Status indicators
    CASE operation_status
        WHEN 'SUCCESS' THEN '✓'
        WHEN 'FAILED' THEN '✗'
        WHEN 'RUNNING' THEN '⟳'
        WHEN 'STARTED' THEN '▶'
        ELSE '○'
    END as status_icon
FROM partition_operations_log
WHERE log_timestamp >= SYSTIMESTAMP - INTERVAL '30' DAY
  AND log_type = 'OPERATION'
ORDER BY log_timestamp DESC;

-- 6. Summary Statistics View - Aggregated metrics
CREATE OR REPLACE VIEW v_partition_statistics AS
SELECT 
    table_name,
    operation_type,
    COUNT(*) as total_operations,
    SUM(CASE WHEN operation_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_operations,
    SUM(CASE WHEN operation_status = 'FAILED' THEN 1 ELSE 0 END) as failed_operations,
    ROUND(AVG(duration_ms), 0) as avg_duration_ms,
    ROUND(MAX(duration_ms), 0) as max_duration_ms,
    SUM(NVL(rows_processed, 0)) as total_rows_processed,
    MIN(log_timestamp) as first_operation,
    MAX(log_timestamp) as last_operation,
    -- Success rate
    ROUND((SUM(CASE WHEN operation_status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 1) as success_rate_pct
FROM partition_operations_log
WHERE log_type = 'OPERATION'
  AND log_timestamp >= SYSTIMESTAMP - INTERVAL '30' DAY
GROUP BY table_name, operation_type
HAVING COUNT(*) > 0
ORDER BY table_name, operation_type;

-- Comments on views
COMMENT ON VIEW v_partition_operations IS 'High-level view of partition operations with key metrics';
COMMENT ON VIEW v_partition_errors IS 'Error-focused view for troubleshooting and alerting';
COMMENT ON VIEW v_partition_performance IS 'Performance metrics view for optimization analysis';
COMMENT ON VIEW v_partition_audit IS 'Audit trail view for compliance and security monitoring';
COMMENT ON VIEW v_partition_recent_activity IS 'Recent activity dashboard view';
COMMENT ON VIEW v_partition_statistics IS 'Aggregated statistics for reporting and analysis';