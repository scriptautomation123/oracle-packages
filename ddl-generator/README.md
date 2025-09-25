# Oracle Table Management Suite

A comprehensive Oracle PL/SQL package suite for advanced table management, partition handling, online operations, and maintenance with autonomous logging and data movement capabilities.

## 🎯 **Overview**

This unified suite combines the power of:
- **Partition Management**: Advanced partition operations, strategy management, and automated maintenance
- **Online Table Operations**: Zero-downtime table moves, parallel data migration, and safe column removal
- **Table Creation**: Comprehensive table creation with DDL generation capabilities
- **Generic Maintenance Framework**: Extensible framework for any maintenance strategy

## 📁 **Directory Structure**

```
oracle-table-management/
├── 1_schema-owner-privileges/          # Privilege management scripts
├── 2_config-tables/                    # Configuration and lookup tables
│   ├── partition_*_table.sql          # Original partition-specific tables
│   ├── generic_*_table.sql            # Generic maintenance framework tables
│   └── lookup_tables.sql              # Reference data tables
├── 3_install_packages/                 # Core package installations
│   ├── partition_*_pkg.sql            # Partition management packages
│   ├── online_table_operations_pkg.sql # Online operations package
│   └── create_table_pkg.sql           # Table creation package
├── 4_online_operations/                # Online operations and DDL generation
│   ├── online_table_operations_pkg.sql # Online table operations
│   ├── create_table_pkg.sql           # Table creation with DDL generation
│   └── operation_log_table.sql        # Operation logging
├── 5_examples/                         # Comprehensive usage examples
│   └── comprehensive_usage_examples.sql # Complete examples for all features
└── 6_documentation/                    # Comprehensive documentation
```

## 🚀 **Key Features**

### **Partition Management**
- **Core Operations**: Create, drop, split, merge, move, truncate partitions
- **Strategy Management**: Dynamic strategy changes and migrations
- **Automated Maintenance**: Scheduled maintenance with job management
- **Health Monitoring**: Comprehensive partition analysis and recommendations
- **Data Movement**: Advanced data movement and partition exchange

### **Online Table Operations**
- **Zero Downtime**: Online table moves without locks
- **Parallel Processing**: High-performance parallel data migration
- **Safe Operations**: Safe column removal and table modifications
- **Table Synchronization**: Keep tables in sync during operations
- **Atomic Renaming**: Safe table rename operations

### **Table Creation**
- **Multiple Table Types**: Heap, partitioned, IOT, temporary, blockchain, JSON, spatial, in-memory
- **DDL Generation**: Generate DDL without execution
- **Bulk Operations**: Create multiple tables efficiently
- **Table Cloning**: Create tables like existing ones
- **Validation**: Syntax validation and dependency checking

### **Generic Maintenance Framework**
- **Extensible Architecture**: Support for any maintenance strategy
- **Strategy Generation**: Automated strategy implementation
- **Comprehensive Logging**: Autonomous transaction logging
- **Performance Monitoring**: Resource usage and optimization
- **Production Examples**: Ready-to-use maintenance strategies

## 📦 **Core Packages**

### **1. Partition Management Packages**
- `partition_management_pkg` - Core partition operations
- `partition_logger_pkg` - Autonomous logging system
- `partition_strategy_pkg` - Strategy management and migration
- `partition_maintenance_pkg` - Automated maintenance jobs
- `partition_utils_pkg` - Analysis and monitoring utilities

### **2. Online Operations Package**
- `online_table_operations_pkg` - Online table operations with DDL generation
- `create_table_pkg` - Table creation with DDL generation
- `operation_log_table` - Operation logging and monitoring

### **3. Generic Maintenance Framework**
- `generic_maintenance_logger_pkg` - Generic logging system
- `strategy_implementation_generator` - Strategy generation utilities
- Production-ready examples for various maintenance strategies

## 🛠️ **Installation**

### **1. Prerequisites**
- Oracle Database 19c or later
- Schema owner with CREATE privileges
- Application user with EXECUTE and SELECT privileges

### **2. Install Configuration Tables**
```sql
-- Install partition-specific tables
@2_config-tables/partition_*_table.sql
@2_config-tables/lookup_tables.sql

-- Install generic framework tables
@2_config-tables/generic_*_table.sql
```

### **3. Install Core Packages**
```sql
-- Install partition management packages
@3_install_packages/partition_*_pkg.sql

-- Install online operations packages
@4_online_operations/online_table_operations_pkg.sql
@4_online_operations/create_table_pkg.sql
@4_online_operations/operation_log_table.sql
```

### **4. Grant Permissions**
```sql
-- Grant privileges to application users
@1_schema-owner-privileges/grant_privileges.sql
```

## 📖 **Usage Examples**

### **Partition Management**
```sql
-- Create partition
EXEC partition_management_pkg.create_partition(
    p_table_name => 'SALES',
    p_partition_name => 'P_2024_Q1',
    p_high_value => 'TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')',
    p_tablespace => 'DATA_TS'
);

-- Migrate to interval partitioning
EXEC partition_strategy_pkg.migrate_to_interval_partitioning(
    p_table_name => 'SALES',
    p_partition_column => 'SALE_DATE',
    p_interval_value => 'NUMTODSINTERVAL(1, ''DAY'')',
    p_preserve_data => TRUE
);
```

### **Online Table Operations**
```sql
-- Move table online
EXEC online_table_operations_pkg.move_table_online(
    p_table_name => 'SALES_DATA',
    p_new_tablespace => 'DATA_TS',
    p_parallel_degree => 4,
    p_include_indexes => TRUE
);

-- Safe column removal
EXEC online_table_operations_pkg.remove_columns_safe(
    p_table_name => 'SALES_DATA',
    p_columns_to_remove => 'OLD_COLUMN1,OLD_COLUMN2',
    p_parallel_degree => 4
);
```

### **Table Creation with DDL Generation**
```sql
-- Generate DDL for partitioned table
DECLARE
    v_ddl CLOB;
BEGIN
    v_ddl := create_table_pkg.generate_partitioned_table_ddl(
        p_table_name => 'SALES_NEW',
        p_columns => 'id NUMBER, sale_date DATE, amount NUMBER',
        p_partition_column => 'sale_date',
        p_partition_type => 'RANGE',
        p_interval => 'NUMTODSINTERVAL(1, ''DAY'')'
    );
    
    -- Print or save DDL
    create_table_pkg.print_ddl_script(v_ddl);
END;
/
```

### **Generic Maintenance Framework**
```sql
-- Register new strategy
EXEC strategy_implementation_generator.register_strategy(
    p_strategy_name => 'index_maintenance',
    p_strategy_type => 'DATABASE',
    p_description => 'Index maintenance strategy'
);

-- Generate strategy implementation
EXEC strategy_implementation_generator.generate_strategy_implementation(
    p_strategy_name => 'index_maintenance'
);
```

## 🔧 **Advanced Features**

### **DDL Generation**
- Generate DDL without execution
- Review and modify before applying
- Save DDL to files
- Validate syntax and dependencies
- Execute DDL in steps

### **Parallel Processing**
- Configurable parallel degrees
- Batch processing for large operations
- Resource optimization
- Performance monitoring

### **Safety Features**
- Zero downtime operations
- Comprehensive error handling
- Transaction management
- Data integrity preservation
- Rollback capabilities

### **Monitoring and Logging**
- Autonomous transaction logging
- Performance metrics collection
- Health monitoring
- Error tracking and reporting
- Log retention management

## 📊 **Production Examples**

The suite includes production-ready examples for:

### **Database Maintenance**
- Index maintenance strategy
- Statistics maintenance strategy
- Data cleanup strategy
- Partition maintenance strategy

### **Application Maintenance**
- Log rotation strategies
- Cache cleanup strategies
- Session management
- Configuration management

### **Infrastructure Maintenance**
- Disk cleanup strategies
- System monitoring
- Resource optimization
- Network maintenance

### **Compliance Maintenance**
- Audit trail maintenance
- Data retention strategies
- Access review processes
- Key rotation strategies

## 🎯 **Benefits**

### **Operational Benefits**
- Zero downtime operations
- Parallel processing for performance
- Comprehensive error handling
- Easy monitoring and management
- Automated maintenance

### **Technical Benefits**
- Oracle 19c optimized features
- Parallel processing support
- Resource management
- Safety and reliability
- Extensible architecture

### **Business Benefits**
- Reduced maintenance windows
- Improved system performance
- Better resource utilization
- Enhanced data management
- Cost-effective operations

## 📚 **Documentation**

- **Complete API Reference**: All packages with detailed procedures and functions
- **Comprehensive Examples**: Real-world usage scenarios
- **Best Practices**: Production-ready guidelines
- **Troubleshooting**: Common issues and solutions
- **Performance Tuning**: Optimization recommendations

## 🏆 **Use Cases**

### **1. Table Maintenance**
- Move tables to new tablespaces
- Reorganize table storage
- Optimize table performance
- Manage table growth
- Safe schema evolution

### **2. Partition Management**
- Move partitions to different tablespaces
- Archive old partitions
- Optimize partition storage
- Manage partition lifecycle
- Strategy migrations

### **3. Data Migration**
- Migrate data between environments
- Copy tables with modifications
- Synchronize table structures
- Handle data transformations
- Zero-downtime migrations

### **4. Maintenance Automation**
- Automated cleanup operations
- Scheduled maintenance jobs
- Health monitoring
- Performance optimization
- Compliance management

## 🔍 **Monitoring and Maintenance**

### **Health Monitoring**
```sql
-- Analyze partition health
SELECT * FROM TABLE(partition_utils_pkg.analyze_partition_health('SALES'));

-- Get operation history
SELECT * FROM operation_log 
WHERE table_name = 'SALES_DATA' 
ORDER BY operation_time DESC;
```

### **Performance Monitoring**
```sql
-- Analyze performance
SELECT * FROM TABLE(partition_utils_pkg.analyze_partition_performance('SALES', 7));

-- Monitor specific operation
EXEC online_table_operations_pkg.monitor_operation(123);
```

## 🛡️ **Security Features**

- **NO ANY privileges required** - Maximum security design
- **NO DDL privileges required for application users** - Maximum security design
- **Self-contained schema architecture** - Each schema owner manages their own system
- **Comprehensive logging** - All operations are logged
- **Autonomous transactions** - Logging is independent of main operations

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
- Test operations carefully
- Monitor operation logs for errors
- Have rollback procedures ready

### **4. Maintenance**
- Regular cleanup of operation logs
- Monitor system performance during operations
- Keep statistics up to date
- Regular testing of procedures

## 🎯 **Getting Started**

1. **Install the suite** following the installation guide
2. **Configure your environment** with appropriate privileges
3. **Test with sample data** to understand the capabilities
4. **Implement in production** following best practices
5. **Monitor and maintain** using the built-in tools

This comprehensive suite provides everything needed for advanced Oracle table management, from basic operations to complex maintenance strategies, all with zero downtime and maximum safety.

## 📄 **License**

This software is provided as-is for educational and development purposes. Use in production environments at your own risk.
