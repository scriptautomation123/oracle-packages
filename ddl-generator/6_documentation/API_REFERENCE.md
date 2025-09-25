# Oracle Table Management Suite - API Reference

## 📚 **Complete API Reference**

This document provides a comprehensive reference for all packages, procedures, and functions in the Oracle Table Management Suite.

## 🎯 **Partition Management Packages**

### **partition_management_pkg**

Core partition management operations including create, drop, split, merge, move, and truncate operations.

#### **Procedures**

##### **create_partition**
```sql
PROCEDURE create_partition(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2,
    p_high_value IN VARCHAR2,
    p_tablespace IN VARCHAR2 DEFAULT NULL,
    p_operation_id OUT NUMBER
);
```
Creates a new partition for the specified table.

**Parameters:**
- `p_table_name` - Name of the partitioned table
- `p_partition_name` - Name of the new partition
- `p_high_value` - High value for the partition (e.g., 'TO_DATE(''2024-01-01'', ''YYYY-MM-DD'')')
- `p_tablespace` - Tablespace for the partition (optional)
- `p_operation_id` - Returns the operation ID for logging

##### **drop_partition**
```sql
PROCEDURE drop_partition(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2,
    p_update_indexes IN BOOLEAN DEFAULT TRUE,
    p_operation_id OUT NUMBER
);
```
Drops an existing partition from the specified table.

**Parameters:**
- `p_table_name` - Name of the partitioned table
- `p_partition_name` - Name of the partition to drop
- `p_update_indexes` - Whether to update indexes after drop
- `p_operation_id` - Returns the operation ID for logging

##### **split_partition**
```sql
PROCEDURE split_partition(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2,
    p_split_value IN VARCHAR2,
    p_new_partition1 IN VARCHAR2,
    p_new_partition2 IN VARCHAR2,
    p_operation_id OUT NUMBER
);
```
Splits an existing partition into two new partitions.

**Parameters:**
- `p_table_name` - Name of the partitioned table
- `p_partition_name` - Name of the partition to split
- `p_split_value` - Value to split the partition at
- `p_new_partition1` - Name of the first new partition
- `p_new_partition2` - Name of the second new partition
- `p_operation_id` - Returns the operation ID for logging

##### **merge_partitions**
```sql
PROCEDURE merge_partitions(
    p_table_name IN VARCHAR2,
    p_partition1 IN VARCHAR2,
    p_partition2 IN VARCHAR2,
    p_new_partition IN VARCHAR2,
    p_operation_id OUT NUMBER
);
```
Merges two partitions into a single partition.

**Parameters:**
- `p_table_name` - Name of the partitioned table
- `p_partition1` - Name of the first partition to merge
- `p_partition2` - Name of the second partition to merge
- `p_new_partition` - Name of the merged partition
- `p_operation_id` - Returns the operation ID for logging

##### **move_partition**
```sql
PROCEDURE move_partition(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2,
    p_new_tablespace IN VARCHAR2,
    p_operation_id OUT NUMBER
);
```
Moves a partition to a new tablespace.

**Parameters:**
- `p_table_name` - Name of the partitioned table
- `p_partition_name` - Name of the partition to move
- `p_new_tablespace` - New tablespace for the partition
- `p_operation_id` - Returns the operation ID for logging

##### **truncate_partition**
```sql
PROCEDURE truncate_partition(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2,
    p_operation_id OUT NUMBER
);
```
Truncates a partition, removing all data.

**Parameters:**
- `p_table_name` - Name of the partitioned table
- `p_partition_name` - Name of the partition to truncate
- `p_operation_id` - Returns the operation ID for logging

### **partition_logger_pkg**

Autonomous logging system for all partition operations with performance monitoring and reporting.

#### **Procedures**

##### **log_operation**
```sql
PROCEDURE log_operation(
    p_operation_type IN VARCHAR2,
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2 DEFAULT NULL,
    p_status IN VARCHAR2,
    p_message IN VARCHAR2 DEFAULT NULL,
    p_duration_ms IN NUMBER DEFAULT NULL,
    p_sql_text IN CLOB DEFAULT NULL,
    p_error_code IN VARCHAR2 DEFAULT NULL,
    p_error_message IN VARCHAR2 DEFAULT NULL
);
```
Logs a partition operation with all relevant details.

**Parameters:**
- `p_operation_type` - Type of operation (CREATE_PARTITION, DROP_PARTITION, etc.)
- `p_table_name` - Name of the table
- `p_partition_name` - Name of the partition (optional)
- `p_status` - Status of the operation (SUCCESS, ERROR, WARNING)
- `p_message` - Additional message (optional)
- `p_duration_ms` - Duration in milliseconds (optional)
- `p_sql_text` - SQL text executed (optional)
- `p_error_code` - Error code if failed (optional)
- `p_error_message` - Error message if failed (optional)

##### **set_logging_enabled**
```sql
PROCEDURE set_logging_enabled(p_enabled IN BOOLEAN);
```
Enables or disables logging for partition operations.

**Parameters:**
- `p_enabled` - TRUE to enable logging, FALSE to disable

##### **set_log_retention_days**
```sql
PROCEDURE set_log_retention_days(p_days IN NUMBER);
```
Sets the number of days to retain log entries.

**Parameters:**
- `p_days` - Number of days to retain logs

##### **cleanup_old_logs**
```sql
PROCEDURE cleanup_old_logs(p_days IN NUMBER DEFAULT 30);
```
Cleans up log entries older than the specified number of days.

**Parameters:**
- `p_days` - Number of days to retain logs (default 30)

#### **Functions**

##### **get_operation_history**
```sql
FUNCTION get_operation_history(
    p_table_name IN VARCHAR2,
    p_days IN NUMBER DEFAULT 7
) RETURN operation_history_tab PIPELINED;
```
Returns the operation history for a specific table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_days` - Number of days to look back (default 7)

**Returns:** Table of operation history records

##### **get_performance_summary**
```sql
FUNCTION get_performance_summary(
    p_table_name IN VARCHAR2,
    p_operation_type IN VARCHAR2 DEFAULT NULL
) RETURN performance_summary_tab PIPELINED;
```
Returns performance summary for operations on a specific table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_operation_type` - Type of operation to filter by (optional)

**Returns:** Table of performance summary records

### **partition_strategy_pkg**

Advanced partition strategy management with migration capabilities between different partitioning types.

#### **Procedures**

##### **create_strategy_config**
```sql
PROCEDURE create_strategy_config(
    p_table_name IN VARCHAR2,
    p_strategy_type IN VARCHAR2,
    p_partition_column IN VARCHAR2,
    p_interval_value IN VARCHAR2,
    p_tablespace_prefix IN VARCHAR2 DEFAULT 'DATA',
    p_retention_days IN NUMBER DEFAULT 90,
    p_auto_maintenance IN BOOLEAN DEFAULT TRUE
);
```
Creates a partition strategy configuration for a table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_strategy_type` - Type of partitioning strategy (RANGE, LIST, HASH, INTERVAL)
- `p_partition_column` - Column to partition by
- `p_interval_value` - Interval value for interval partitioning
- `p_tablespace_prefix` - Prefix for tablespace names
- `p_retention_days` - Number of days to retain partitions
- `p_auto_maintenance` - Whether to enable automatic maintenance

##### **migrate_to_interval_partitioning**
```sql
PROCEDURE migrate_to_interval_partitioning(
    p_table_name IN VARCHAR2,
    p_partition_column IN VARCHAR2,
    p_interval_value IN VARCHAR2,
    p_preserve_data IN BOOLEAN DEFAULT TRUE
);
```
Migrates a table to interval partitioning.

**Parameters:**
- `p_table_name` - Name of the table to migrate
- `p_partition_column` - Column to partition by
- `p_interval_value` - Interval value for partitioning
- `p_preserve_data` - Whether to preserve existing data

### **partition_maintenance_pkg**

Automated maintenance with job scheduling and execution management.

#### **Procedures**

##### **create_maintenance_job**
```sql
PROCEDURE create_maintenance_job(
    p_table_name IN VARCHAR2,
    p_job_type IN VARCHAR2,
    p_schedule_type IN VARCHAR2,
    p_schedule_value IN VARCHAR2 DEFAULT NULL
);
```
Creates a maintenance job for a table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_job_type` - Type of maintenance job (CLEANUP, ANALYZE, COMPRESS, REBUILD_INDEXES)
- `p_schedule_type` - Schedule type (DAILY, WEEKLY, MONTHLY)
- `p_schedule_value` - Schedule value (e.g., '1' for first day of month)

##### **execute_all_maintenance_jobs**
```sql
PROCEDURE execute_all_maintenance_jobs;
```
Executes all scheduled maintenance jobs.

### **partition_utils_pkg**

Comprehensive analysis and monitoring utilities for partition health and performance.

#### **Functions**

##### **analyze_partition_health**
```sql
FUNCTION analyze_partition_health(
    p_table_name IN VARCHAR2
) RETURN health_rec;
```
Analyzes the health of partitions for a table.

**Parameters:**
- `p_table_name` - Name of the table

**Returns:** Health analysis record

##### **analyze_partition_performance**
```sql
FUNCTION analyze_partition_performance(
    p_table_name IN VARCHAR2,
    p_days IN NUMBER DEFAULT 7
) RETURN performance_tab PIPELINED;
```
Analyzes partition performance for a table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_days` - Number of days to analyze

**Returns:** Table of performance records

##### **get_maintenance_recommendations**
```sql
FUNCTION get_maintenance_recommendations(
    p_table_name IN VARCHAR2
) RETURN maintenance_recommendations_tab PIPELINED;
```
Gets maintenance recommendations for a table.

**Parameters:**
- `p_table_name` - Name of the table

**Returns:** Table of maintenance recommendations

## 🎯 **Online Operations Package**

### **online_table_operations_pkg**

Comprehensive Oracle 19c package for performing online table operations including partition/subpartition moves, parallel data migration, and safe column removal.

#### **Procedures**

##### **move_table_online**
```sql
PROCEDURE move_table_online(
    p_table_name IN VARCHAR2,
    p_new_tablespace IN VARCHAR2,
    p_parallel_degree IN NUMBER DEFAULT 4,
    p_include_indexes IN BOOLEAN DEFAULT TRUE,
    p_include_constraints IN BOOLEAN DEFAULT TRUE,
    p_include_statistics IN BOOLEAN DEFAULT TRUE,
    p_operation_id OUT NUMBER
);
```
Moves an entire table online to a new tablespace.

**Parameters:**
- `p_table_name` - Name of the table to move
- `p_new_tablespace` - New tablespace for the table
- `p_parallel_degree` - Parallel degree for the operation
- `p_include_indexes` - Whether to include indexes
- `p_include_constraints` - Whether to include constraints
- `p_include_statistics` - Whether to include statistics
- `p_operation_id` - Returns the operation ID for logging

##### **move_partition_online**
```sql
PROCEDURE move_partition_online(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2,
    p_new_tablespace IN VARCHAR2,
    p_parallel_degree IN NUMBER DEFAULT 2,
    p_operation_id OUT NUMBER
);
```
Moves a partition online to a new tablespace.

**Parameters:**
- `p_table_name` - Name of the partitioned table
- `p_partition_name` - Name of the partition to move
- `p_new_tablespace` - New tablespace for the partition
- `p_parallel_degree` - Parallel degree for the operation
- `p_operation_id` - Returns the operation ID for logging

##### **remove_columns_safe**
```sql
PROCEDURE remove_columns_safe(
    p_table_name IN VARCHAR2,
    p_columns_to_remove IN VARCHAR2,
    p_parallel_degree IN NUMBER DEFAULT 4,
    p_batch_size IN NUMBER DEFAULT 10000,
    p_operation_id OUT NUMBER
);
```
Safely removes columns from a table without downtime.

**Parameters:**
- `p_table_name` - Name of the table
- `p_columns_to_remove` - Comma-separated list of columns to remove
- `p_parallel_degree` - Parallel degree for the operation
- `p_batch_size` - Batch size for data processing
- `p_operation_id` - Returns the operation ID for logging

##### **migrate_table_parallel**
```sql
PROCEDURE migrate_table_parallel(
    p_source_table IN VARCHAR2,
    p_target_table IN VARCHAR2,
    p_parallel_degree IN NUMBER DEFAULT 4,
    p_batch_size IN NUMBER DEFAULT 10000,
    p_where_clause IN VARCHAR2 DEFAULT NULL,
    p_operation_id OUT NUMBER
);
```
Migrates data from one table to another using parallel processing.

**Parameters:**
- `p_source_table` - Name of the source table
- `p_target_table` - Name of the target table
- `p_parallel_degree` - Parallel degree for the operation
- `p_batch_size` - Batch size for data processing
- `p_where_clause` - WHERE clause to filter data (optional)
- `p_operation_id` - Returns the operation ID for logging

#### **DDL Generation Functions**

##### **generate_move_table_ddl**
```sql
FUNCTION generate_move_table_ddl(
    p_table_name IN VARCHAR2,
    p_new_tablespace IN VARCHAR2,
    p_parallel_degree IN NUMBER DEFAULT 4,
    p_include_indexes IN BOOLEAN DEFAULT TRUE,
    p_include_constraints IN BOOLEAN DEFAULT TRUE,
    p_include_statistics IN BOOLEAN DEFAULT TRUE
) RETURN CLOB;
```
Generates DDL for moving a table online.

**Parameters:**
- `p_table_name` - Name of the table
- `p_new_tablespace` - New tablespace for the table
- `p_parallel_degree` - Parallel degree for the operation
- `p_include_indexes` - Whether to include indexes
- `p_include_constraints` - Whether to include constraints
- `p_include_statistics` - Whether to include statistics

**Returns:** CLOB containing the generated DDL

##### **generate_remove_columns_ddl**
```sql
FUNCTION generate_remove_columns_ddl(
    p_table_name IN VARCHAR2,
    p_columns_to_remove IN VARCHAR2,
    p_parallel_degree IN NUMBER DEFAULT 4,
    p_batch_size IN NUMBER DEFAULT 10000
) RETURN CLOB;
```
Generates DDL for safely removing columns.

**Parameters:**
- `p_table_name` - Name of the table
- `p_columns_to_remove` - Comma-separated list of columns to remove
- `p_parallel_degree` - Parallel degree for the operation
- `p_batch_size` - Batch size for data processing

**Returns:** CLOB containing the generated DDL

#### **DDL Management Functions**

##### **print_ddl_script**
```sql
PROCEDURE print_ddl_script(p_ddl IN CLOB);
```
Prints DDL script to output.

**Parameters:**
- `p_ddl` - DDL script to print

##### **save_ddl_to_file**
```sql
PROCEDURE save_ddl_to_file(
    p_ddl IN CLOB,
    p_file_path IN VARCHAR2
);
```
Saves DDL script to a file.

**Parameters:**
- `p_ddl` - DDL script to save
- `p_file_path` - Path to save the file

##### **get_ddl_summary**
```sql
FUNCTION get_ddl_summary(p_ddl IN CLOB) RETURN ddl_tab;
```
Gets a summary of DDL steps.

**Parameters:**
- `p_ddl` - DDL script to analyze

**Returns:** Table of DDL step records

## 🎯 **Table Creation Package**

### **create_table_pkg**

Comprehensive table creation package with DDL generation capabilities for various table types.

#### **Procedures**

##### **create_heap_table**
```sql
PROCEDURE create_heap_table(
    p_table_name IN VARCHAR2,
    p_columns IN VARCHAR2,
    p_tablespace IN VARCHAR2 DEFAULT 'USERS',
    p_storage_clause IN VARCHAR2 DEFAULT NULL,
    p_logging IN BOOLEAN DEFAULT TRUE
);
```
Creates a heap table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_columns` - Column definitions
- `p_tablespace` - Tablespace for the table
- `p_storage_clause` - Storage clause (optional)
- `p_logging` - Whether to enable logging

##### **create_partitioned_table**
```sql
PROCEDURE create_partitioned_table(
    p_table_name IN VARCHAR2,
    p_columns IN VARCHAR2,
    p_partition_column IN VARCHAR2,
    p_partition_type IN VARCHAR2,
    p_interval IN VARCHAR2 DEFAULT NULL,
    p_tablespace IN VARCHAR2 DEFAULT 'USERS'
);
```
Creates a partitioned table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_columns` - Column definitions
- `p_partition_column` - Column to partition by
- `p_partition_type` - Type of partitioning (RANGE, LIST, HASH, INTERVAL)
- `p_interval` - Interval for interval partitioning (optional)
- `p_tablespace` - Tablespace for the table

#### **DDL Generation Functions**

##### **generate_heap_table_ddl**
```sql
FUNCTION generate_heap_table_ddl(
    p_table_name IN VARCHAR2,
    p_columns IN VARCHAR2,
    p_tablespace IN VARCHAR2 DEFAULT 'USERS',
    p_storage_clause IN VARCHAR2 DEFAULT NULL,
    p_logging IN BOOLEAN DEFAULT TRUE
) RETURN CLOB;
```
Generates DDL for creating a heap table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_columns` - Column definitions
- `p_tablespace` - Tablespace for the table
- `p_storage_clause` - Storage clause (optional)
- `p_logging` - Whether to enable logging

**Returns:** CLOB containing the generated DDL

##### **generate_partitioned_table_ddl**
```sql
FUNCTION generate_partitioned_table_ddl(
    p_table_name IN VARCHAR2,
    p_columns IN VARCHAR2,
    p_partition_column IN VARCHAR2,
    p_partition_type IN VARCHAR2,
    p_interval IN VARCHAR2 DEFAULT NULL,
    p_tablespace IN VARCHAR2 DEFAULT 'USERS'
) RETURN CLOB;
```
Generates DDL for creating a partitioned table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_columns` - Column definitions
- `p_partition_column` - Column to partition by
- `p_partition_type` - Type of partitioning (RANGE, LIST, HASH, INTERVAL)
- `p_interval` - Interval for interval partitioning (optional)
- `p_tablespace` - Tablespace for the table

**Returns:** CLOB containing the generated DDL

##### **generate_iot_table_ddl**
```sql
FUNCTION generate_iot_table_ddl(
    p_table_name IN VARCHAR2,
    p_columns IN VARCHAR2,
    p_primary_key IN VARCHAR2,
    p_tablespace IN VARCHAR2 DEFAULT 'USERS'
) RETURN CLOB;
```
Generates DDL for creating an Index-Organized Table (IOT).

**Parameters:**
- `p_table_name` - Name of the table
- `p_columns` - Column definitions
- `p_primary_key` - Primary key columns
- `p_tablespace` - Tablespace for the table

**Returns:** CLOB containing the generated DDL

##### **generate_json_table_ddl**
```sql
FUNCTION generate_json_table_ddl(
    p_table_name IN VARCHAR2,
    p_json_column IN VARCHAR2,
    p_columns IN VARCHAR2,
    p_tablespace IN VARCHAR2 DEFAULT 'USERS'
) RETURN CLOB;
```
Generates DDL for creating a JSON table.

**Parameters:**
- `p_table_name` - Name of the table
- `p_json_column` - JSON column name
- `p_columns` - Column definitions
- `p_tablespace` - Tablespace for the table

**Returns:** CLOB containing the generated DDL

#### **DDL Management Functions**

##### **print_ddl_script**
```sql
PROCEDURE print_ddl_script(p_ddl IN CLOB);
```
Prints DDL script to output.

**Parameters:**
- `p_ddl` - DDL script to print

##### **save_ddl_to_file**
```sql
PROCEDURE save_ddl_to_file(
    p_ddl IN CLOB,
    p_file_path IN VARCHAR2
);
```
Saves DDL script to a file.

**Parameters:**
- `p_ddl` - DDL script to save
- `p_file_path` - Path to save the file

##### **validate_ddl_syntax**
```sql
PROCEDURE validate_ddl_syntax(
    p_ddl IN CLOB,
    p_is_valid OUT BOOLEAN,
    p_error_message OUT VARCHAR2
);
```
Validates DDL syntax.

**Parameters:**
- `p_ddl` - DDL script to validate
- `p_is_valid` - Returns TRUE if valid, FALSE otherwise
- `p_error_message` - Error message if invalid

## 🎯 **Generic Framework Packages**

### **generic_maintenance_logger_pkg**

Generic logging system for any maintenance strategy.

#### **Procedures**

##### **log_operation**
```sql
PROCEDURE log_operation(
    p_strategy_name IN VARCHAR2,
    p_job_name IN VARCHAR2,
    p_target_object IN VARCHAR2,
    p_target_type IN VARCHAR2,
    p_operation_type IN VARCHAR2,
    p_status IN VARCHAR2,
    p_message IN VARCHAR2 DEFAULT NULL,
    p_duration_ms IN NUMBER DEFAULT NULL,
    p_rows_processed IN NUMBER DEFAULT NULL,
    p_objects_affected IN NUMBER DEFAULT NULL
);
```
Logs a generic maintenance operation.

**Parameters:**
- `p_strategy_name` - Name of the strategy
- `p_job_name` - Name of the job
- `p_target_object` - Target object name
- `p_target_type` - Type of target object
- `p_operation_type` - Type of operation
- `p_status` - Status of the operation
- `p_message` - Additional message (optional)
- `p_duration_ms` - Duration in milliseconds (optional)
- `p_rows_processed` - Number of rows processed (optional)
- `p_objects_affected` - Number of objects affected (optional)

##### **register_strategy**
```sql
PROCEDURE register_strategy(
    p_strategy_name IN VARCHAR2,
    p_strategy_type IN VARCHAR2,
    p_description IN VARCHAR2,
    p_target_type IN VARCHAR2 DEFAULT 'TABLE',
    p_execution_mode IN VARCHAR2 DEFAULT 'AUTOMATED',
    p_parallel_degree IN NUMBER DEFAULT 1,
    p_priority_level IN VARCHAR2 DEFAULT 'MEDIUM'
);
```
Registers a new maintenance strategy.

**Parameters:**
- `p_strategy_name` - Name of the strategy
- `p_strategy_type` - Type of strategy (DATABASE, APPLICATION, INFRASTRUCTURE, etc.)
- `p_description` - Description of the strategy
- `p_target_type` - Type of target object
- `p_execution_mode` - Execution mode (AUTOMATED, MANUAL)
- `p_parallel_degree` - Parallel degree for execution
- `p_priority_level` - Priority level (HIGH, MEDIUM, LOW)

### **strategy_implementation_generator**

Strategy generation utilities for creating new maintenance strategies.

#### **Procedures**

##### **generate_strategy_implementation**
```sql
PROCEDURE generate_strategy_implementation(
    p_strategy_name IN VARCHAR2,
    p_package_spec OUT CLOB,
    p_package_body OUT CLOB,
    p_test_script OUT CLOB,
    p_documentation OUT CLOB
);
```
Generates a complete strategy implementation.

**Parameters:**
- `p_strategy_name` - Name of the strategy to generate
- `p_package_spec` - Generated package specification
- `p_package_body` - Generated package body
- `p_test_script` - Generated test script
- `p_documentation` - Generated documentation

## 📊 **Data Types**

### **Common Types**

#### **ddl_rec**
```sql
TYPE ddl_rec IS RECORD (
    step_number NUMBER,
    step_name VARCHAR2(100),
    ddl_statement CLOB,
    estimated_duration_ms NUMBER,
    dependencies VARCHAR2(4000)
);
```

#### **ddl_tab**
```sql
TYPE ddl_tab IS TABLE OF ddl_rec;
```

#### **health_rec**
```sql
TYPE health_rec IS RECORD (
    overall_score NUMBER,
    status VARCHAR2(20),
    recommendations CLOB,
    last_analyzed DATE
);
```

#### **performance_rec**
```sql
TYPE performance_rec IS RECORD (
    operation_type VARCHAR2(50),
    total_executions NUMBER,
    average_duration_ms NUMBER,
    success_rate NUMBER,
    last_execution DATE
);
```

#### **maintenance_recommendation_rec**
```sql
TYPE maintenance_recommendation_rec IS RECORD (
    priority VARCHAR2(20),
    action VARCHAR2(100),
    reason CLOB,
    impact VARCHAR2(50),
    estimated_duration_ms NUMBER
);
```

## 🔧 **Error Handling**

All packages include comprehensive error handling with:
- **Autonomous Transactions**: Logging is independent of main operations
- **Error Codes**: Standardized error codes for troubleshooting
- **Error Messages**: Detailed error messages with context
- **Recovery Procedures**: Built-in recovery mechanisms
- **Status Tracking**: Real-time status updates for long-running operations

## 📈 **Performance Considerations**

- **Parallel Processing**: Configurable parallel degrees for all operations
- **Batch Processing**: Optimized batch sizes for large data sets
- **Resource Management**: CPU and memory optimization
- **I/O Optimization**: Parallel operations for better performance
- **Statistics Gathering**: Automatic statistics collection
- **Index Management**: Automatic index rebuilding and optimization

## 🛡️ **Security Features**

- **NO ANY privileges required** - Maximum security design
- **NO DDL privileges required for application users** - Maximum security design
- **Self-contained schema architecture** - Each schema owner manages their own system
- **Comprehensive logging** - All operations are logged
- **Autonomous transactions** - Logging is independent of main operations
- **Input validation** - All inputs are validated for security
- **SQL injection protection** - Built-in protection against SQL injection

This comprehensive API reference provides all the information needed to use the Oracle Table Management Suite effectively and safely.
