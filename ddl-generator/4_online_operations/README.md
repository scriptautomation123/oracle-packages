# Oracle 19c Online Table Operations Package

A comprehensive Oracle 19c package for performing online table operations including partition/subpartition moves, parallel data migration, and safe column removal.

## 🎯 **Key Features**

### **Online Table Operations**
- **Table Moves**: Move entire tables online to new tablespaces
- **Partition Moves**: Move individual partitions online
- **Subpartition Moves**: Move subpartitions online
- **Parallel Processing**: All operations support parallel execution
- **Index Management**: Automatic index rebuilding and optimization
- **Statistics Gathering**: Automatic statistics collection

### **Parallel Data Migration**
- **Parallel SELECT/INSERT**: High-performance data migration
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Table Synchronization**: Keep tables in sync during operations
- **Safe Renaming**: Atomic table rename operations

### **Safe Column Removal**
- **Zero Downtime**: Remove columns without table locks
- **Data Preservation**: Maintain data integrity during operations
- **Constraint Management**: Handle constraints safely
- **Index Management**: Rebuild indexes after column removal

## 📁 **File Structure**

```
alter-table-online/
├── online_table_operations_pkg.sql          # Package specification
├── online_table_operations_pkg_body.sql     # Package body implementation
├── operation_log_table.sql                 # Operation logging table
├── example_usage.sql                       # Usage examples
└── README.md                               # This documentation
```

## 🚀 **Installation**

### **1. Create Operation Log Table**
```sql
@operation_log_table.sql
```

### **2. Install Package**
```sql
@online_table_operations_pkg.sql
@online_table_operations_pkg_body.sql
```

### **3. Grant Permissions**
```sql
GRANT EXECUTE ON online_table_operations_pkg TO your_users;
```

## 📖 **Usage Examples**

### **1. Move Table Online**
```sql
DECLARE
    v_operation_id NUMBER;
BEGIN
    online_table_operations_pkg.move_table_online(
        p_table_name => 'SALES_DATA',
        p_new_tablespace => 'DATA_TS',
        p_parallel_degree => 4,
        p_include_indexes => TRUE,
        p_include_constraints => TRUE,
        p_include_statistics => TRUE,
        p_operation_id => v_operation_id
    );
    
    DBMS_OUTPUT.PUT_LINE('Operation ID: ' || v_operation_id);
END;
/
```

### **2. Move Partition Online**
```sql
DECLARE
    v_operation_id NUMBER;
BEGIN
    online_table_operations_pkg.move_partition_online(
        p_table_name => 'SALES_DATA',
        p_partition_name => 'P_2024_Q1',
        p_new_tablespace => 'ARCHIVE_TS',
        p_parallel_degree => 2,
        p_operation_id => v_operation_id
    );
END;
/
```

### **3. Parallel Table Migration**
```sql
DECLARE
    v_operation_id NUMBER;
BEGIN
    online_table_operations_pkg.migrate_table_parallel(
        p_source_table => 'SALES_DATA',
        p_target_table => 'SALES_DATA_NEW',
        p_parallel_degree => 4,
        p_batch_size => 10000,
        p_where_clause => 'created_date >= SYSDATE - 30',
        p_operation_id => v_operation_id
    );
END;
/
```

### **4. Safe Column Removal**
```sql
DECLARE
    v_operation_id NUMBER;
BEGIN
    online_table_operations_pkg.remove_columns_safe(
        p_table_name => 'SALES_DATA',
        p_columns_to_remove => 'OLD_COLUMN1,OLD_COLUMN2',
        p_parallel_degree => 4,
        p_batch_size => 10000,
        p_operation_id => v_operation_id
    );
END;
/
```

### **5. Table Synchronization and Rename**
```sql
DECLARE
    v_operation_id NUMBER;
BEGIN
    online_table_operations_pkg.sync_and_rename_tables(
        p_old_table => 'SALES_DATA',
        p_new_table => 'SALES_DATA_NEW',
        p_parallel_degree => 4,
        p_batch_size => 10000,
        p_operation_id => v_operation_id
    );
END;
/
```

## 🔧 **Key Procedures**

### **Main Operations**
- `move_table_online()` - Move entire table online
- `move_partition_online()` - Move partition online
- `move_subpartition_online()` - Move subpartition online
- `migrate_table_parallel()` - Parallel table migration
- `remove_columns_safe()` - Safe column removal
- `sync_and_rename_tables()` - Table sync and rename

### **Utility Procedures**
- `create_table_copy()` - Create table copy with structure
- `copy_table_data_parallel()` - Parallel data copying
- `disable_constraints()` - Disable table constraints
- `enable_constraints()` - Enable table constraints
- `create_indexes_parallel()` - Parallel index creation
- `gather_statistics()` - Gather table statistics

### **Monitoring Procedures**
- `get_operation_status()` - Get operation status
- `get_operation_history()` - Get operation history
- `monitor_operation()` - Monitor specific operation
- `cancel_operation()` - Cancel running operation

## 📊 **Operation Logging**

All operations are logged in the `operation_log` table with:
- Operation ID and type
- Table name and status
- Start/end times and duration
- Rows processed and error messages
- Performance metrics

### **Query Operation History**
```sql
SELECT * FROM operation_log 
WHERE table_name = 'SALES_DATA' 
ORDER BY operation_time DESC;
```

### **Monitor Specific Operation**
```sql
EXEC online_table_operations_pkg.monitor_operation(123);
```

## ⚡ **Performance Features**

### **Parallel Processing**
- Configurable parallel degree for all operations
- Parallel index rebuilding and statistics gathering
- Batch processing for large data sets
- Optimized for Oracle 19c features

### **Resource Management**
- CPU and memory optimization
- I/O optimization with parallel operations
- Configurable batch sizes
- Automatic cleanup of old operations

### **Safety Features**
- Comprehensive error handling and recovery
- Transaction management with autonomous transactions
- Constraint handling during operations
- Data integrity preservation

## 🛡️ **Safety Features**

### **Zero Downtime Operations**
- Online table moves without locks
- Parallel data migration
- Safe table renaming with atomic operations
- Constraint management during operations

### **Error Handling**
- Comprehensive error logging
- Operation status tracking
- Automatic cleanup of failed operations
- Recovery procedures

### **Data Integrity**
- Constraint validation
- Index rebuilding
- Statistics gathering
- Data consistency checks

## 🔍 **Monitoring and Maintenance**

### **Operation Monitoring**
```sql
-- Get operation history
SELECT * FROM TABLE(
    online_table_operations_pkg.get_operation_history('SALES_DATA', 7)
);

-- Monitor specific operation
EXEC online_table_operations_pkg.monitor_operation(123);
```

### **Cleanup Operations**
```sql
-- Cleanup failed operations
EXEC online_table_operations_pkg.cleanup_failed_operations;

-- Cleanup old operations (30 days)
EXEC online_table_operations_pkg.cleanup_old_operations(30);
```

## 📈 **Best Practices**

### **1. Planning Operations**
- Test operations on non-production environments
- Plan for sufficient disk space and resources
- Consider parallel degree based on system capacity
- Schedule operations during maintenance windows

### **2. Performance Optimization**
- Use appropriate parallel degrees
- Configure batch sizes for optimal performance
- Monitor resource usage during operations
- Use appropriate tablespace configurations

### **3. Safety Considerations**
- Always backup critical data before operations
- Test column removal operations carefully
- Monitor operation logs for errors
- Have rollback procedures ready

### **4. Maintenance**
- Regular cleanup of operation logs
- Monitor system performance during operations
- Keep statistics up to date
- Regular testing of procedures

## 🎯 **Use Cases**

### **1. Table Maintenance**
- Move tables to new tablespaces
- Reorganize table storage
- Optimize table performance
- Manage table growth

### **2. Partition Management**
- Move partitions to different tablespaces
- Archive old partitions
- Optimize partition storage
- Manage partition lifecycle

### **3. Schema Evolution**
- Remove unused columns safely
- Add new columns without downtime
- Modify table structure
- Handle schema changes

### **4. Data Migration**
- Migrate data between environments
- Copy tables with modifications
- Synchronize table structures
- Handle data transformations

## 🏆 **Benefits**

### **Operational Benefits**
- Zero downtime operations
- Parallel processing for performance
- Comprehensive error handling
- Easy monitoring and management

### **Technical Benefits**
- Oracle 19c optimized features
- Parallel processing support
- Resource management
- Safety and reliability

### **Business Benefits**
- Reduced maintenance windows
- Improved system performance
- Better resource utilization
- Enhanced data management

## 📚 **Documentation**

- Complete package specification and body
- Comprehensive usage examples
- Operation logging and monitoring
- Best practices and guidelines
- Troubleshooting and maintenance

This package provides a comprehensive solution for online table operations in Oracle 19c, enabling zero-downtime maintenance and schema evolution with parallel processing and safety features.
