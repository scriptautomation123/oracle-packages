-- =====================================================
-- Generate Partition Management Tables from Generic Framework
-- Demonstrates how the generic framework can generate partition-specific tables
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Step 1: Register partition maintenance as a strategy in the generic framework
EXEC generic_maintenance_logger_pkg.register_strategy(
    'PARTITION_MAINTENANCE', 
    'MAINTENANCE', 
    'Database partition maintenance strategy',
    'DATABASE'
);

-- Step 2: Create partition-specific strategy configuration
INSERT INTO generic_strategy_config (
    strategy_name,
    target_object,
    target_type,
    strategy_type,
    strategy_config,
    schedule_expression,
    execution_mode,
    parallel_degree,
    priority_level,
    description,
    tags
) VALUES (
    'PARTITION_MAINTENANCE',
    'ALL_PARTITIONED_TABLES',
    'TABLE',
    'MAINTENANCE',
    '{"partition_type": "RANGE", "maintenance_operations": ["CREATE", "DROP", "SPLIT", "MERGE"], "retention_days": 90}',
    '0 1 * * *', -- Daily at 1 AM
    'AUTOMATIC',
    4,
    8,
    'Automated partition maintenance for all partitioned tables',
    'PARTITION,MAINTENANCE,DATABASE'
);

-- Step 3: Create partition-specific maintenance jobs
INSERT INTO generic_maintenance_jobs (
    strategy_name,
    job_name,
    job_type,
    schedule_type,
    schedule_value,
    target_object,
    target_type,
    job_parameters,
    max_parallel_degree,
    notify_on_failure,
    depends_on_jobs
) VALUES (
    'PARTITION_MAINTENANCE',
    'CREATE_PARTITION_JOB',
    'MAINTENANCE',
    'DAILY',
    '01:00',
    'ALL_PARTITIONED_TABLES',
    'TABLE',
    '{"operation": "CREATE_PARTITION", "interval": "DAILY", "online": true}',
    2,
    'Y',
    NULL
);

INSERT INTO generic_maintenance_jobs (
    strategy_name,
    job_name,
    job_type,
    schedule_type,
    schedule_value,
    target_object,
    target_type,
    job_parameters,
    max_parallel_degree,
    notify_on_failure,
    depends_on_jobs
) VALUES (
    'PARTITION_MAINTENANCE',
    'DROP_OLD_PARTITIONS_JOB',
    'CLEANUP',
    'WEEKLY',
    'SUNDAY 02:00',
    'ALL_PARTITIONED_TABLES',
    'TABLE',
    '{"operation": "DROP_PARTITION", "retention_days": 90, "dry_run": false}',
    1,
    'Y',
    'CREATE_PARTITION_JOB'
);

INSERT INTO generic_maintenance_jobs (
    strategy_name,
    job_name,
    job_type,
    schedule_type,
    schedule_value,
    target_object,
    target_type,
    job_parameters,
    max_parallel_degree,
    notify_on_failure,
    depends_on_jobs
) VALUES (
    'PARTITION_MAINTENANCE',
    'ANALYZE_PARTITIONS_JOB',
    'ANALYSIS',
    'WEEKLY',
    'SUNDAY 03:00',
    'ALL_PARTITIONED_TABLES',
    'TABLE',
    '{"operation": "ANALYZE", "estimate_percent": 10, "cascade": true}',
    2,
    'Y',
    'DROP_OLD_PARTITIONS_JOB'
);

-- Step 4: Create partition-specific operation types in the generic framework
INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'CREATE_PARTITION', 'Create new partition', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'CREATE_PARTITION');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'DROP_PARTITION', 'Drop existing partition', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'DROP_PARTITION');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'SPLIT_PARTITION', 'Split partition into multiple partitions', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'SPLIT_PARTITION');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'MERGE_PARTITIONS', 'Merge multiple partitions', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'MERGE_PARTITIONS');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'MOVE_PARTITION', 'Move partition to different tablespace', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'MOVE_PARTITION');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'TRUNCATE_PARTITION', 'Truncate partition data', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'TRUNCATE_PARTITION');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'EXCHANGE_PARTITION', 'Exchange partition with table', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'EXCHANGE_PARTITION');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'REBUILD_INDEXES', 'Rebuild partition indexes', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'REBUILD_INDEXES');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'ANALYZE', 'Analyze partition statistics', 'ANALYSIS' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'ANALYZE');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'COMPRESS', 'Compress partition', 'OPTIMIZATION' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'COMPRESS');

INSERT INTO generic_operation_types (operation_type, description, category) 
SELECT 'CLEANUP', 'Cleanup old partitions', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_operation_types WHERE operation_type = 'CLEANUP');

-- Step 5: Create partition-specific job types
INSERT INTO generic_job_types (job_type, description, category) 
SELECT 'CLEANUP', 'Cleanup old partitions', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_job_types WHERE job_type = 'CLEANUP');

INSERT INTO generic_job_types (job_type, description, category) 
SELECT 'REBUILD_INDEXES', 'Rebuild partition indexes', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_job_types WHERE job_type = 'REBUILD_INDEXES');

INSERT INTO generic_job_types (job_type, description, category) 
SELECT 'ANALYZE', 'Analyze partition statistics', 'ANALYSIS' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_job_types WHERE job_type = 'ANALYZE');

INSERT INTO generic_job_types (job_type, description, category) 
SELECT 'COMPRESS', 'Compress partitions', 'OPTIMIZATION' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_job_types WHERE job_type = 'COMPRESS');

INSERT INTO generic_job_types (job_type, description, category) 
SELECT 'MOVE', 'Move partitions', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_job_types WHERE job_type = 'MOVE');

INSERT INTO generic_job_types (job_type, description, category) 
SELECT 'SPLIT', 'Split partitions', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_job_types WHERE job_type = 'SPLIT');

INSERT INTO generic_job_types (job_type, description, category) 
SELECT 'MERGE', 'Merge partitions', 'MAINTENANCE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_job_types WHERE job_type = 'MERGE');

-- Step 6: Create partition-specific target types
INSERT INTO generic_target_types (target_type, description, category) 
SELECT 'PARTITION', 'Table partition', 'DATABASE' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM generic_target_types WHERE target_type = 'PARTITION');

-- Step 7: Create views that map the generic framework to partition-specific concepts
CREATE OR REPLACE VIEW partition_maintenance_jobs AS
SELECT 
    job_id,
    'PARTITION_MAINTENANCE' as strategy_name,
    job_name,
    job_type,
    schedule_type,
    schedule_value,
    target_object,
    target_type,
    is_active,
    last_run,
    next_run,
    created_date,
    created_by,
    last_modified,
    last_modified_by,
    execution_count,
    avg_duration_ms,
    last_duration_ms,
    last_status,
    error_count,
    last_error,
    job_parameters,
    depends_on_jobs,
    prerequisite_checks,
    max_parallel_degree,
    resource_limits,
    notify_on_success,
    notify_on_failure,
    notification_recipients
FROM generic_maintenance_jobs
WHERE strategy_name = 'PARTITION_MAINTENANCE';

CREATE OR REPLACE VIEW partition_operation_log AS
SELECT 
    operation_id,
    operation_type,
    target_object as table_name,
    target_type as partition_name,
    status,
    message,
    duration_ms,
    operation_time,
    user_name,
    session_id,
    sql_text,
    error_code,
    error_message
FROM generic_operation_log
WHERE strategy_name = 'PARTITION_MAINTENANCE';

CREATE OR REPLACE VIEW partition_strategy_config AS
SELECT 
    config_id,
    target_object as table_name,
    strategy_type,
    target_type as partition_column,
    strategy_config as interval_value,
    NULL as tablespace_prefix,
    JSON_VALUE(strategy_config, '$.retention_days') as retention_days,
    CASE WHEN execution_mode = 'AUTOMATIC' THEN 'Y' ELSE 'N' END as auto_maintenance,
    created_date,
    created_by,
    last_modified,
    last_modified_by,
    is_active,
    version,
    description,
    tags,
    execution_count,
    success_count,
    failure_count,
    avg_duration_ms,
    last_execution,
    last_success,
    last_failure,
    deployment_status,
    deployment_started,
    deployment_completed,
    rollback_available,
    monitoring_enabled,
    alert_thresholds,
    notification_config
FROM generic_strategy_config
WHERE strategy_name = 'PARTITION_MAINTENANCE';

-- Step 8: Create partition-specific lookup tables as views
CREATE OR REPLACE VIEW partition_operation_types AS
SELECT 
    operation_type_id,
    operation_type,
    description,
    is_active,
    created_date
FROM generic_operation_types
WHERE operation_type IN (
    'CREATE_PARTITION', 'DROP_PARTITION', 'SPLIT_PARTITION', 'MERGE_PARTITIONS',
    'MOVE_PARTITION', 'TRUNCATE_PARTITION', 'EXCHANGE_PARTITION', 'REBUILD_INDEXES',
    'ANALYZE', 'COMPRESS', 'CLEANUP'
);

CREATE OR REPLACE VIEW partition_operation_status AS
SELECT 
    status_id,
    status,
    description,
    is_active,
    created_date
FROM generic_operation_status;

CREATE OR REPLACE VIEW partition_job_types AS
SELECT 
    job_type_id,
    job_type,
    description,
    is_active,
    created_date
FROM generic_job_types
WHERE job_type IN (
    'CLEANUP', 'REBUILD_INDEXES', 'ANALYZE', 'COMPRESS', 'MOVE', 'SPLIT', 'MERGE'
);

CREATE OR REPLACE VIEW partition_schedule_types AS
SELECT 
    schedule_type_id,
    schedule_type,
    description,
    is_active,
    created_date
FROM generic_schedule_types;

CREATE OR REPLACE VIEW partition_strategy_types AS
SELECT 
    strategy_type_id,
    'RANGE' as strategy_type,
    'Range partitioning' as description,
    'Y' as is_active,
    SYSDATE as created_date
FROM dual
UNION ALL
SELECT 
    strategy_type_id + 1,
    'LIST' as strategy_type,
    'List partitioning' as description,
    'Y' as is_active,
    SYSDATE as created_date
FROM dual
UNION ALL
SELECT 
    strategy_type_id + 2,
    'HASH' as strategy_type,
    'Hash partitioning' as description,
    'Y' as is_active,
    SYSDATE as created_date
FROM dual
UNION ALL
SELECT 
    strategy_type_id + 3,
    'INTERVAL' as strategy_type,
    'Interval partitioning' as description,
    'Y' as is_active,
    SYSDATE as created_date
FROM dual
UNION ALL
SELECT 
    strategy_type_id + 4,
    'REFERENCE' as strategy_type,
    'Reference partitioning' as description,
    'Y' as is_active,
    SYSDATE as created_date
FROM dual;

CREATE OR REPLACE VIEW partition_yes_no AS
SELECT 
    value_id,
    value,
    description,
    created_date
FROM generic_yes_no;

-- Step 9: Create a procedure to generate the original partition tables from the generic framework
CREATE OR REPLACE PROCEDURE generate_partition_tables_from_generic AS
BEGIN
    -- This procedure would generate the original partition-specific tables
    -- by extracting data from the generic framework and creating the specific tables
    
    -- Generate partition_maintenance_jobs table
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_maintenance_jobs AS
    SELECT * FROM partition_maintenance_jobs';
    
    -- Generate partition_operation_log table  
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_operation_log AS
    SELECT * FROM partition_operation_log';
    
    -- Generate partition_strategy_config table
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_strategy_config AS
    SELECT * FROM partition_strategy_config';
    
    -- Generate lookup tables
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_operation_types AS
    SELECT * FROM partition_operation_types';
    
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_operation_status AS
    SELECT * FROM partition_operation_status';
    
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_job_types AS
    SELECT * FROM partition_job_types';
    
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_schedule_types AS
    SELECT * FROM partition_schedule_types';
    
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_strategy_types AS
    SELECT * FROM partition_strategy_types';
    
    EXECUTE IMMEDIATE '
    CREATE TABLE partition_yes_no AS
    SELECT * FROM partition_yes_no';
    
    DBMS_OUTPUT.PUT_LINE('Partition tables generated successfully from generic framework');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error generating partition tables: ' || SQLERRM);
        RAISE;
END generate_partition_tables_from_generic;
/

-- Step 10: Demonstrate how to use the generic framework to manage partition maintenance
CREATE OR REPLACE PROCEDURE demonstrate_partition_maintenance_with_generic AS
    v_operation_id NUMBER;
BEGIN
    -- Start partition maintenance using generic framework
    v_operation_id := generic_maintenance_logger_pkg.log_strategy_start(
        'PARTITION_MAINTENANCE',
        'ALL_PARTITIONED_TABLES',
        'TABLE'
    );
    
    -- Log partition operations using generic framework
    generic_maintenance_logger_pkg.log_operation(
        'PARTITION_MAINTENANCE',
        'CREATE_PARTITION',
        'SALES_DATA',
        'TABLE',
        'SUCCESS',
        'Created new partition for current month',
        NULL,
        'ALTER TABLE SALES_DATA ADD PARTITION p_202412 VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD''))',
        NULL,
        NULL,
        '{"partition_name": "p_202412", "high_value": "2025-01-01"}',
        'CREATE_PARTITION_JOB'
    );
    
    -- End partition maintenance
    generic_maintenance_logger_pkg.log_strategy_end(
        v_operation_id,
        'SUCCESS',
        'Partition maintenance completed successfully'
    );
    
    DBMS_OUTPUT.PUT_LINE('Partition maintenance demonstrated using generic framework');
    
EXCEPTION
    WHEN OTHERS THEN
        generic_maintenance_logger_pkg.log_strategy_end(
            v_operation_id,
            'ERROR',
            'Partition maintenance failed: ' || SQLERRM
        );
        RAISE;
END demonstrate_partition_maintenance_with_generic;
/

-- Step 11: Create a migration script to convert existing partition tables to generic framework
CREATE OR REPLACE PROCEDURE migrate_partition_tables_to_generic AS
BEGIN
    -- Migrate existing partition_maintenance_jobs to generic_maintenance_jobs
    INSERT INTO generic_maintenance_jobs (
        strategy_name, job_name, job_type, schedule_type, schedule_value,
        target_object, target_type, is_active, last_run, next_run,
        created_date, created_by, last_modified, last_modified_by,
        execution_count, avg_duration_ms, last_duration_ms, last_status,
        error_count, last_error, job_parameters, depends_on_jobs,
        prerequisite_checks, max_parallel_degree, resource_limits,
        notify_on_success, notify_on_failure, notification_recipients
    )
    SELECT 
        'PARTITION_MAINTENANCE',
        job_name,
        job_type,
        schedule_type,
        schedule_value,
        table_name,
        'TABLE',
        is_active,
        last_run,
        next_run,
        created_date,
        created_by,
        last_modified,
        last_modified_by,
        execution_count,
        avg_duration_ms,
        last_duration_ms,
        last_status,
        error_count,
        last_error,
        NULL,
        NULL,
        NULL,
        1,
        NULL,
        'N',
        'Y',
        NULL
    FROM partition_maintenance_jobs;
    
    -- Migrate existing partition_operation_log to generic_operation_log
    INSERT INTO generic_operation_log (
        operation_id, strategy_name, operation_type, job_name, target_object,
        target_type, status, message, duration_ms, operation_time,
        user_name, session_id, sql_text, error_code, error_message,
        strategy_context, cpu_time_ms, memory_used_mb, io_operations,
        rows_processed, objects_affected
    )
    SELECT 
        operation_id,
        'PARTITION_MAINTENANCE',
        operation_type,
        NULL,
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
        error_message,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    FROM partition_operation_log;
    
    -- Migrate existing partition_strategy_config to generic_strategy_config
    INSERT INTO generic_strategy_config (
        strategy_name, target_object, target_type, strategy_type,
        strategy_config, execution_mode, parallel_degree, batch_size,
        timeout_seconds, resource_limits, priority_level, schedule_expression,
        schedule_timezone, depends_on_strategies, prerequisite_checks,
        created_date, created_by, last_modified, last_modified_by,
        is_active, version, description, tags, execution_count,
        success_count, failure_count, avg_duration_ms, last_execution,
        last_success, last_failure, deployment_status, deployment_started,
        deployment_completed, rollback_available, monitoring_enabled,
        alert_thresholds, notification_config
    )
    SELECT 
        'PARTITION_MAINTENANCE',
        table_name,
        'TABLE',
        strategy_type,
        '{"partition_column": "' || partition_column || '", "interval_value": "' || interval_value || '", "retention_days": ' || retention_days || '}',
        'AUTOMATIC',
        1,
        1000,
        3600,
        NULL,
        5,
        NULL,
        'UTC',
        NULL,
        NULL,
        created_date,
        created_by,
        last_modified,
        last_modified_by,
        is_active,
        '1.0',
        'Partition maintenance strategy for ' || table_name,
        'PARTITION,MAINTENANCE',
        0,
        0,
        0,
        NULL,
        NULL,
        NULL,
        NULL,
        'PENDING',
        NULL,
        NULL,
        'N',
        'Y',
        NULL,
        NULL
    FROM partition_strategy_config;
    
    DBMS_OUTPUT.PUT_LINE('Partition tables migrated to generic framework successfully');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error migrating partition tables: ' || SQLERRM);
        RAISE;
END migrate_partition_tables_to_generic;
/

PROMPT Generic framework configured for partition maintenance
PROMPT 
PROMPT To generate partition tables from generic framework:
PROMPT EXEC generate_partition_tables_from_generic;
PROMPT 
PROMPT To demonstrate partition maintenance using generic framework:
PROMPT EXEC demonstrate_partition_maintenance_with_generic;
PROMPT 
PROMPT To migrate existing partition tables to generic framework:
PROMPT EXEC migrate_partition_tables_to_generic;
