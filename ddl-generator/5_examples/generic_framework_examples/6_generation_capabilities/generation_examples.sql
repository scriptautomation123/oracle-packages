-- =====================================================
-- Generic Framework Generation Examples
-- Demonstrates what can be generated from the generic framework
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- =====================================================
-- EXAMPLE 1: DATABASE MAINTENANCE STRATEGIES
-- =====================================================

-- A. Index Maintenance Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'INDEX_MAINTENANCE', 
    'MAINTENANCE', 
    'Automated index maintenance strategy',
    'DATABASE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode, parallel_degree
) VALUES (
    'INDEX_MAINTENANCE', 'ALL_INDEXES', 'INDEX', 'MAINTENANCE',
    '{"maintenance_type": "REBUILD", "online": true, "parallel_degree": 4}',
    '0 2 * * 0', 'AUTOMATIC', 4
);

INSERT INTO generic_maintenance_jobs (
    strategy_name, job_name, job_type, schedule_type, target_object, target_type,
    job_parameters, max_parallel_degree
) VALUES (
    'INDEX_MAINTENANCE', 'REBUILD_UNUSABLE_INDEXES', 'MAINTENANCE', 'WEEKLY',
    'ALL_INDEXES', 'INDEX', '{"operation": "REBUILD", "condition": "UNUSABLE"}', 4
);

-- B. Statistics Maintenance Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'STATISTICS_MAINTENANCE', 
    'MAINTENANCE', 
    'Database statistics maintenance strategy',
    'DATABASE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'STATISTICS_MAINTENANCE', 'ALL_TABLES', 'TABLE', 'MAINTENANCE',
    '{"estimate_percent": 10, "cascade": true, "degree": 4}',
    '0 3 * * 0', 'AUTOMATIC'
);

-- C. Cleanup Maintenance Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'CLEANUP_MAINTENANCE', 
    'MAINTENANCE', 
    'Data cleanup maintenance strategy',
    'DATABASE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'CLEANUP_MAINTENANCE', 'ALL_TABLES', 'TABLE', 'MAINTENANCE',
    '{"cleanup_type": "OLD_DATA", "retention_days": 90, "batch_size": 1000}',
    '0 4 * * 0', 'AUTOMATIC'
);

-- =====================================================
-- EXAMPLE 2: APPLICATION MAINTENANCE STRATEGIES
-- =====================================================

-- A. Log Rotation Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'LOG_ROTATION', 
    'MAINTENANCE', 
    'Application log rotation strategy',
    'APPLICATION'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'LOG_ROTATION', 'APPLICATION_LOGS', 'FILE', 'MAINTENANCE',
    '{"rotation_type": "SIZE_BASED", "max_size_mb": 100, "retention_days": 30}',
    '0 */6 * * *', 'AUTOMATIC'
);

-- B. Cache Cleanup Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'CACHE_CLEANUP', 
    'MAINTENANCE', 
    'Application cache cleanup strategy',
    'APPLICATION'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'CACHE_CLEANUP', 'APPLICATION_CACHE', 'APPLICATION', 'MAINTENANCE',
    '{"cleanup_type": "EXPIRED", "max_age_hours": 24, "batch_size": 100}',
    '0 1 * * *', 'AUTOMATIC'
);

-- =====================================================
-- EXAMPLE 3: INFRASTRUCTURE MAINTENANCE STRATEGIES
-- =====================================================

-- A. Disk Cleanup Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'DISK_CLEANUP', 
    'MAINTENANCE', 
    'Disk space cleanup strategy',
    'INFRASTRUCTURE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'DISK_CLEANUP', 'TEMP_FILES', 'FILE', 'MAINTENANCE',
    '{"cleanup_type": "TEMP_FILES", "max_age_days": 7, "min_free_space_gb": 10}',
    '0 2 * * *', 'AUTOMATIC'
);

-- B. System Monitoring Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'SYSTEM_MONITORING', 
    'MAINTENANCE', 
    'System resource monitoring strategy',
    'INFRASTRUCTURE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'SYSTEM_MONITORING', 'SYSTEM_RESOURCES', 'SYSTEM', 'MAINTENANCE',
    '{"monitor_cpu": true, "monitor_memory": true, "monitor_disk": true, "alert_threshold": 80}',
    '*/5 * * * *', 'AUTOMATIC'
);

-- =====================================================
-- EXAMPLE 4: COMPLIANCE MAINTENANCE STRATEGIES
-- =====================================================

-- A. Audit Trail Maintenance
EXEC generic_maintenance_logger_pkg.register_strategy(
    'AUDIT_MAINTENANCE', 
    'MAINTENANCE', 
    'Audit trail maintenance strategy',
    'COMPLIANCE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'AUDIT_MAINTENANCE', 'AUDIT_TRAILS', 'TABLE', 'MAINTENANCE',
    '{"retention_years": 7, "archive_older_than": "5_YEARS", "compress": true}',
    '0 3 * * 0', 'AUTOMATIC'
);

-- B. Data Retention Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'DATA_RETENTION', 
    'MAINTENANCE', 
    'Data retention compliance strategy',
    'COMPLIANCE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'DATA_RETENTION', 'PERSONAL_DATA', 'TABLE', 'MAINTENANCE',
    '{"retention_years": 3, "anonymize_before_delete": true, "compliance_standard": "GDPR"}',
    '0 4 * * 0', 'AUTOMATIC'
);

-- =====================================================
-- EXAMPLE 5: PERFORMANCE MAINTENANCE STRATEGIES
-- =====================================================

-- A. Query Optimization Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'QUERY_OPTIMIZATION', 
    'MAINTENANCE', 
    'Query performance optimization strategy',
    'PERFORMANCE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'QUERY_OPTIMIZATION', 'SLOW_QUERIES', 'QUERY', 'MAINTENANCE',
    '{"optimize_threshold_ms": 1000, "analyze_plan": true, "suggest_indexes": true}',
    '0 5 * * *', 'AUTOMATIC'
);

-- B. Memory Optimization Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'MEMORY_OPTIMIZATION', 
    'MAINTENANCE', 
    'Memory usage optimization strategy',
    'PERFORMANCE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'MEMORY_OPTIMIZATION', 'MEMORY_POOLS', 'MEMORY', 'MAINTENANCE',
    '{"optimize_buffer_pools": true, "tune_shared_pool": true, "monitor_usage": true}',
    '0 6 * * *', 'AUTOMATIC'
);

-- =====================================================
-- EXAMPLE 6: SECURITY MAINTENANCE STRATEGIES
-- =====================================================

-- A. Access Review Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'ACCESS_REVIEW', 
    'MAINTENANCE', 
    'User access review strategy',
    'SECURITY'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'ACCESS_REVIEW', 'USER_ACCESS', 'USER', 'MAINTENANCE',
    '{"review_frequency": "QUARTERLY", "check_inactive_users": true, "check_excessive_privileges": true}',
    '0 7 1 */3 *', 'AUTOMATIC'
);

-- B. Key Rotation Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'KEY_ROTATION', 
    'MAINTENANCE', 
    'Encryption key rotation strategy',
    'SECURITY'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'KEY_ROTATION', 'ENCRYPTION_KEYS', 'KEY', 'MAINTENANCE',
    '{"rotation_frequency": "MONTHLY", "key_length": 256, "algorithm": "AES"}',
    '0 8 1 * *', 'AUTOMATIC'
);

-- =====================================================
-- EXAMPLE 7: MONITORING MAINTENANCE STRATEGIES
-- =====================================================

-- A. Health Check Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'HEALTH_CHECK', 
    'MAINTENANCE', 
    'System health monitoring strategy',
    'MONITORING'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'HEALTH_CHECK', 'SYSTEM_HEALTH', 'SYSTEM', 'MAINTENANCE',
    '{"check_database": true, "check_connectivity": true, "check_performance": true}',
    '*/15 * * * *', 'AUTOMATIC'
);

-- B. Alert Management Strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'ALERT_MANAGEMENT', 
    'MAINTENANCE', 
    'Alert management and notification strategy',
    'MONITORING'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode
) VALUES (
    'ALERT_MANAGEMENT', 'SYSTEM_ALERTS', 'ALERT', 'MAINTENANCE',
    '{"escalation_rules": true, "notification_channels": ["EMAIL", "SMS"], "alert_thresholds": "CUSTOM"}',
    '*/5 * * * *', 'AUTOMATIC'
);

-- =====================================================
-- EXAMPLE 8: USING STRATEGY IMPLEMENTATION GENERATOR
-- =====================================================

-- Generate a complete backup strategy implementation
EXEC strategy_implementation_generator_pkg.generate_strategy_implementation(
    p_strategy_name     => 'BACKUP_STRATEGY',
    p_strategy_type     => 'MAINTENANCE',
    p_description       => 'Automated backup strategy for database and application data',
    p_category          => 'DATABASE',
    p_target_types      => 'DATABASE,FILE,APPLICATION',
    p_operation_types   => 'BACKUP,VERIFY,CLEANUP',
    p_job_types         => 'BACKUP,VALIDATION,MAINTENANCE',
    p_output_directory  => '/tmp/backup_strategy'
);

-- Generate a complete archive strategy implementation
EXEC strategy_implementation_generator_pkg.generate_strategy_implementation(
    p_strategy_name     => 'ARCHIVE_STRATEGY',
    p_strategy_type     => 'MAINTENANCE',
    p_description       => 'Data archival strategy for long-term storage',
    p_category          => 'DATABASE',
    p_target_types      => 'TABLE,FILE',
    p_operation_types   => 'ARCHIVE,COMPRESS,VERIFY',
    p_job_types         => 'ARCHIVE,COMPRESSION,VALIDATION',
    p_output_directory  => '/tmp/archive_strategy'
);

-- =====================================================
-- EXAMPLE 9: COMPREHENSIVE STRATEGY CONFIGURATION
-- =====================================================

-- Create a comprehensive maintenance strategy with all features
EXEC generic_maintenance_logger_pkg.register_strategy(
    'COMPREHENSIVE_MAINTENANCE', 
    'MAINTENANCE', 
    'Comprehensive maintenance strategy covering all aspects',
    'DATABASE'
);

INSERT INTO generic_strategy_config (
    strategy_name, target_object, target_type, strategy_type,
    strategy_config, schedule_expression, execution_mode, parallel_degree,
    batch_size, timeout_seconds, resource_limits, priority_level,
    depends_on_strategies, prerequisite_checks, description, tags,
    monitoring_enabled, alert_thresholds, notification_config
) VALUES (
    'COMPREHENSIVE_MAINTENANCE', 'ALL_OBJECTS', 'DATABASE', 'MAINTENANCE',
    '{"maintenance_type": "COMPREHENSIVE", "include_indexes": true, "include_statistics": true, "include_cleanup": true}',
    '0 1 * * 0', 'AUTOMATIC', 8, 5000, 7200,
    '{"max_cpu_percent": 80, "max_memory_mb": 4096, "max_io_ops": 1000}',
    9, 'HEALTH_CHECK,SYSTEM_MONITORING', '{"check_disk_space": true, "check_memory": true}',
    'Comprehensive maintenance covering all database objects',
    'COMPREHENSIVE,MAINTENANCE,DATABASE,PERFORMANCE',
    'Y', '{"cpu_threshold": 80, "memory_threshold": 85, "disk_threshold": 90}',
    '{"email": ["admin@company.com"], "sms": ["+1234567890"], "webhook": ["https://alerts.company.com"]}'
);

-- Create multiple jobs for the comprehensive strategy
INSERT INTO generic_maintenance_jobs (
    strategy_name, job_name, job_type, schedule_type, schedule_value,
    target_object, target_type, job_parameters, max_parallel_degree,
    depends_on_jobs, prerequisite_checks, notify_on_success, notify_on_failure,
    notification_recipients
) VALUES (
    'COMPREHENSIVE_MAINTENANCE', 'INDEX_MAINTENANCE_JOB', 'MAINTENANCE', 'WEEKLY', 'SUNDAY 01:00',
    'ALL_INDEXES', 'INDEX', '{"operation": "REBUILD", "online": true, "parallel": 4}', 4,
    NULL, '{"check_disk_space": true}', 'N', 'Y',
    'admin@company.com'
);

INSERT INTO generic_maintenance_jobs (
    strategy_name, job_name, job_type, schedule_type, schedule_value,
    target_object, target_type, job_parameters, max_parallel_degree,
    depends_on_jobs, prerequisite_checks, notify_on_success, notify_on_failure,
    notification_recipients
) VALUES (
    'COMPREHENSIVE_MAINTENANCE', 'STATISTICS_MAINTENANCE_JOB', 'ANALYSIS', 'WEEKLY', 'SUNDAY 02:00',
    'ALL_TABLES', 'TABLE', '{"operation": "ANALYZE", "estimate_percent": 10, "cascade": true}', 2,
    'INDEX_MAINTENANCE_JOB', '{"check_indexes_healthy": true}', 'N', 'Y',
    'admin@company.com'
);

INSERT INTO generic_maintenance_jobs (
    strategy_name, job_name, job_type, schedule_type, schedule_value,
    target_object, target_type, job_parameters, max_parallel_degree,
    depends_on_jobs, prerequisite_checks, notify_on_success, notify_on_failure,
    notification_recipients
) VALUES (
    'COMPREHENSIVE_MAINTENANCE', 'CLEANUP_MAINTENANCE_JOB', 'CLEANUP', 'WEEKLY', 'SUNDAY 03:00',
    'ALL_TABLES', 'TABLE', '{"operation": "CLEANUP", "retention_days": 90, "batch_size": 1000}', 1,
    'STATISTICS_MAINTENANCE_JOB', '{"check_statistics_current": true}', 'N', 'Y',
    'admin@company.com'
);

-- =====================================================
-- EXAMPLE 10: MONITORING AND REPORTING
-- =====================================================

-- Get comprehensive maintenance statistics
SELECT * FROM TABLE(
    generic_maintenance_logger_pkg.get_strategy_statistics(
        p_strategy_name => 'COMPREHENSIVE_MAINTENANCE',
        p_start_date    => SYSDATE - 30,
        p_end_date      => SYSDATE
    )
);

-- Get performance summary
SELECT * FROM TABLE(
    generic_maintenance_logger_pkg.get_performance_summary(
        p_strategy_name => 'COMPREHENSIVE_MAINTENANCE',
        p_start_date    => SYSDATE - 30,
        p_end_date      => SYSDATE
    )
);

-- Get error summary
SELECT * FROM TABLE(
    generic_maintenance_logger_pkg.get_error_summary(
        p_strategy_name => 'COMPREHENSIVE_MAINTENANCE',
        p_start_date    => SYSDATE - 7,
        p_end_date      => SYSDATE
    )
);

-- Get job execution summary
SELECT * FROM TABLE(
    generic_maintenance_logger_pkg.get_job_execution_summary(
        p_strategy_name => 'COMPREHENSIVE_MAINTENANCE',
        p_start_date    => SYSDATE - 30,
        p_end_date      => SYSDATE
    )
);

PROMPT Generic framework generation examples completed
PROMPT 
PROMPT The generic framework can generate:
PROMPT - Database maintenance strategies (indexes, statistics, cleanup)
PROMPT - Application maintenance strategies (logs, cache, cleanup)
PROMPT - Infrastructure maintenance strategies (disk, system monitoring)
PROMPT - Compliance maintenance strategies (audit, data retention)
PROMPT - Performance maintenance strategies (optimization, tuning)
PROMPT - Security maintenance strategies (access review, key rotation)
PROMPT - Monitoring maintenance strategies (health checks, alerts)
PROMPT - Comprehensive maintenance strategies (all-in-one)
PROMPT - Complete strategy implementations with code generation
PROMPT - Flexible configuration and job management
PROMPT - Comprehensive monitoring and reporting
