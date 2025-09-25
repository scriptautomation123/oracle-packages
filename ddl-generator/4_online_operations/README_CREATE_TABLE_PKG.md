# CREATE TABLE PACKAGE - Oracle 19c+ Enterprise Edition

## Overview

The `CREATE_TABLE_PKG` is a comprehensive, enterprise-grade Oracle package designed for Oracle Database 19c and above. It provides a modern, programmatic approach to table creation with support for all Oracle table types and modern features.

## Features

### 🚀 **Modern Oracle 19c+ Features**
- **JSON Columns**: Native JSON data type support
- **Spatial Columns**: SDO_GEOMETRY support with SRID configuration
- **In-Memory Optimization**: Automatic in-memory table configuration
- **Blockchain Tables**: Immutable, append-only tables
- **Identity Columns**: Auto-incrementing primary keys
- **Invisible Columns**: Hidden columns for internal use

### 📊 **Table Types Supported**
- **Heap Tables**: Standard relational tables
- **Index-Organized Tables (IOT)**: Optimized for primary key access
- **Partitioned Tables**: Range, List, Hash, and Interval partitioning
- **Temporary Tables**: Session and transaction-scoped
- **External Tables**: Read-only access to external data
- **Blockchain Tables**: Immutable, tamper-proof tables

### 🔧 **Enterprise Features**
- **Bulk Operations**: Create multiple tables in one operation
- **Validation**: Comprehensive table definition validation
- **Testing**: Built-in test framework with rollback capability
- **DDL Generation**: Generate DDL without execution
- **Performance Monitoring**: Built-in performance metrics

## Installation

### Prerequisites
- Oracle Database 19c or higher
- DBA privileges for table creation
- Access to required tablespaces

### Installation Steps

1. **Create the package specification:**
```sql
@create_table_pkg.sql
```

2. **Create the package body:**
```sql
@create_table_pkg_body.sql
```

3. **Run examples (optional):**
```sql
@create_table_examples.sql
```

4. **Run tests (optional):**
```sql
@create_table_tests.sql
```

## Usage Examples

### Basic Heap Table Creation

```sql
DECLARE
    v_columns create_table_pkg.column_def_array := create_table_pkg.column_def_array();
    v_constraints create_table_pkg.constraint_def_array := create_table_pkg.constraint_def_array();
    v_properties create_table_pkg.table_properties;
BEGIN
    -- Define columns
    v_columns.EXTEND(3);
    v_columns(1) := create_table_pkg.column_def(
        column_name => 'ID',
        data_type => 'NUMBER',
        data_precision => 10,
        nullable => FALSE,
        identity_gen => TRUE,
        comment_text => 'Primary key identifier'
    );
    
    v_columns(2) := create_table_pkg.column_def(
        column_name => 'NAME',
        data_type => 'VARCHAR2',
        data_length => 100,
        nullable => FALSE,
        comment_text => 'Customer name'
    );
    
    v_columns(3) := create_table_pkg.column_def(
        column_name => 'EMAIL',
        data_type => 'VARCHAR2',
        data_length => 255,
        nullable => TRUE,
        comment_text => 'Customer email address'
    );
    
    -- Define constraints
    v_constraints.EXTEND(1);
    v_constraints(1) := create_table_pkg.constraint_def(
        constraint_name => 'PK_CUSTOMERS',
        constraint_type => 'PRIMARY',
        column_list => 'ID'
    );
    
    -- Define table properties
    v_properties := create_table_pkg.table_properties(
        tablespace => 'USERS',
        compression => 'BASIC',
        inmemory => TRUE,
        inmemory_priority => 'HIGH'
    );
    
    -- Create the table
    create_table_pkg.create_heap_table(
        p_table_name => 'CUSTOMERS',
        p_columns => v_columns,
        p_constraints => v_constraints,
        p_properties => v_properties
    );
END;
/
```

### Partitioned Table Creation

```sql
DECLARE
    v_columns create_table_pkg.column_def_array := create_table_pkg.column_def_array();
    v_partitions create_table_pkg.partition_def_array := create_table_pkg.partition_def_array();
    v_properties create_table_pkg.table_properties;
BEGIN
    -- Define columns
    v_columns.EXTEND(4);
    v_columns(1) := create_table_pkg.column_def('SALE_ID', 'NUMBER', 0, 10, 0, FALSE, NULL, TRUE, FALSE, 'Sale identifier');
    v_columns(2) := create_table_pkg.column_def('CUSTOMER_ID', 'NUMBER', 0, 10, 0, FALSE, NULL, FALSE, FALSE, 'Customer identifier');
    v_columns(3) := create_table_pkg.column_def('SALE_DATE', 'DATE', 0, 0, 0, FALSE, NULL, FALSE, FALSE, 'Sale date');
    v_columns(4) := create_table_pkg.column_def('AMOUNT', 'NUMBER', 0, 10, 2, FALSE, NULL, FALSE, FALSE, 'Sale amount');
    
    -- Define partitions
    v_partitions.EXTEND(4);
    v_partitions(1) := create_table_pkg.partition_def(
        'P_2023_Q1', 'RANGE', 'SALE_DATE', 
        'VALUES LESS THAN (TO_DATE(''2023-04-01'', ''YYYY-MM-DD''))', 'USERS', NULL
    );
    v_partitions(2) := create_table_pkg.partition_def(
        'P_2023_Q2', 'RANGE', 'SALE_DATE', 
        'VALUES LESS THAN (TO_DATE(''2023-07-01'', ''YYYY-MM-DD''))', 'USERS', NULL
    );
    v_partitions(3) := create_table_pkg.partition_def(
        'P_2023_Q3', 'RANGE', 'SALE_DATE', 
        'VALUES LESS THAN (TO_DATE(''2023-10-01'', ''YYYY-MM-DD''))', 'USERS', NULL
    );
    v_partitions(4) := create_table_pkg.partition_def(
        'P_2023_Q4', 'RANGE', 'SALE_DATE', 
        'VALUES LESS THAN (TO_DATE(''2024-01-01'', ''YYYY-MM-DD''))', 'USERS', NULL
    );
    
    -- Create partitioned table
    create_table_pkg.create_partitioned_table(
        'SALES',
        v_columns,
        NULL, -- No constraints
        v_partitions,
        create_table_pkg.table_properties('USERS', 'HEAP', 'OLTP', TRUE, 'HIGH', 4, TRUE, FALSE, NULL, FALSE, FALSE)
    );
END;
/
```

### JSON Table Creation

```sql
DECLARE
    v_columns create_table_pkg.column_def_array := create_table_pkg.column_def_array();
    v_json_columns VARCHAR2_ARRAY := VARCHAR2_ARRAY('PROFILE_DATA', 'PREFERENCES', 'METADATA');
    v_constraints create_table_pkg.constraint_def_array := create_table_pkg.constraint_def_array();
BEGIN
    -- Define regular columns
    v_columns.EXTEND(3);
    v_columns(1) := create_table_pkg.column_def('USER_ID', 'NUMBER', 0, 10, 0, FALSE, NULL, TRUE, FALSE, 'User identifier');
    v_columns(2) := create_table_pkg.column_def('USERNAME', 'VARCHAR2', 50, 0, 0, FALSE, NULL, FALSE, FALSE, 'Username');
    v_columns(3) := create_table_pkg.column_def('EMAIL', 'VARCHAR2', 255, 0, 0, FALSE, NULL, FALSE, FALSE, 'Email address');
    
    -- Define constraints
    v_constraints.EXTEND(1);
    v_constraints(1) := create_table_pkg.constraint_def('PK_USERS_JSON', 'PRIMARY', 'USER_ID', NULL, NULL, NULL, FALSE, FALSE);
    
    -- Create JSON table
    create_table_pkg.create_json_table(
        'USERS_JSON',
        v_columns,
        v_json_columns,
        v_constraints,
        create_table_pkg.table_properties('USERS', 'HEAP', 'BASIC', FALSE, NULL, 1, TRUE, FALSE, NULL, FALSE, FALSE)
    );
END;
/
```

### Blockchain Table Creation

```sql
DECLARE
    v_columns create_table_pkg.column_def_array := create_table_pkg.column_def_array();
    v_constraints create_table_pkg.constraint_def_array := create_table_pkg.constraint_def_array();
BEGIN
    -- Define columns for blockchain transactions
    v_columns.EXTEND(5);
    v_columns(1) := create_table_pkg.column_def('TX_ID', 'VARCHAR2', 64, 0, 0, FALSE, NULL, FALSE, FALSE, 'Transaction ID');
    v_columns(2) := create_table_pkg.column_def('FROM_ADDRESS', 'VARCHAR2', 100, 0, 0, FALSE, NULL, FALSE, FALSE, 'Sender address');
    v_columns(3) := create_table_pkg.column_def('TO_ADDRESS', 'VARCHAR2', 100, 0, 0, FALSE, NULL, FALSE, FALSE, 'Receiver address');
    v_columns(4) := create_table_pkg.column_def('AMOUNT', 'NUMBER', 0, 20, 8, FALSE, NULL, FALSE, FALSE, 'Transaction amount');
    v_columns(5) := create_table_pkg.column_def('TIMESTAMP', 'TIMESTAMP', 0, 0, 0, FALSE, NULL, FALSE, FALSE, 'Transaction timestamp');
    
    -- Define constraints
    v_constraints.EXTEND(1);
    v_constraints(1) := create_table_pkg.constraint_def('PK_BLOCKCHAIN_TX', 'PRIMARY', 'TX_ID', NULL, NULL, NULL, FALSE, FALSE);
    
    -- Create blockchain table
    create_table_pkg.create_blockchain_table(
        'BLOCKCHAIN_TRANSACTIONS',
        v_columns,
        v_constraints,
        create_table_pkg.table_properties('USERS', 'HEAP', 'BASIC', FALSE, NULL, 1, TRUE, FALSE, NULL, TRUE, TRUE)
    );
END;
/
```

## API Reference

### Main Procedures

#### `create_heap_table`
Creates a standard heap-organized table.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table to create
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_properties` (table_properties): Table properties (optional)
- `p_schema` (VARCHAR2): Schema name (default: USER)

#### `create_partitioned_table`
Creates a partitioned table.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table to create
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_partitions` (partition_def_array): Array of partition definitions
- `p_properties` (table_properties): Table properties (optional)
- `p_schema` (VARCHAR2): Schema name (default: USER)

#### `create_iot_table`
Creates an Index-Organized Table (IOT).

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table to create
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_primary_key` (VARCHAR2): Primary key column(s)
- `p_overflow` (VARCHAR2): Overflow tablespace (optional)
- `p_properties` (table_properties): Table properties (optional)
- `p_schema` (VARCHAR2): Schema name (default: USER)

#### `create_temp_table`
Creates a temporary table.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table to create
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_scope` (VARCHAR2): Table scope ('SESSION' or 'TRANSACTION')
- `p_properties` (table_properties): Table properties (optional)
- `p_schema` (VARCHAR2): Schema name (default: USER)

#### `create_blockchain_table`
Creates a blockchain table.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table to create
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_properties` (table_properties): Table properties (optional)
- `p_schema` (VARCHAR2): Schema name (default: USER)

#### `create_json_table`
Creates a table with JSON columns.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table to create
- `p_columns` (column_def_array): Array of column definitions
- `p_json_columns` (VARCHAR2_ARRAY): Array of JSON column names
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_properties` (table_properties): Table properties (optional)
- `p_schema` (VARCHAR2): Schema name (default: USER)

### Utility Functions

#### `validate_table_definition`
Validates a table definition before creation.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)

**Returns:** BOOLEAN

#### `generate_create_ddl`
Generates DDL for table creation without executing it.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_partitions` (partition_def_array): Array of partition definitions (optional)
- `p_properties` (table_properties): Table properties (optional)
- `p_table_type` (VARCHAR2): Type of table ('HEAP', 'IOT', 'PARTITIONED', etc.)

**Returns:** CLOB

#### `table_exists`
Checks if a table exists.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table
- `p_schema` (VARCHAR2): Schema name (default: USER)

**Returns:** BOOLEAN

#### `test_table_creation`
Tests table creation with rollback.

**Parameters:**
- `p_table_name` (VARCHAR2): Name of the table
- `p_columns` (column_def_array): Array of column definitions
- `p_constraints` (constraint_def_array): Array of constraint definitions (optional)
- `p_properties` (table_properties): Table properties (optional)

**Returns:** BOOLEAN

### Bulk Operations

#### `create_tables_bulk`
Creates multiple tables in one operation.

**Parameters:**
- `p_table_definitions` (table_definition_array): Array of table definitions

## Data Types

### `column_def`
Record type for column definitions.

**Fields:**
- `column_name` (VARCHAR2): Column name
- `data_type` (VARCHAR2): Data type
- `data_length` (NUMBER): Length for VARCHAR2, CHAR, etc.
- `data_precision` (NUMBER): Precision for NUMBER
- `data_scale` (NUMBER): Scale for NUMBER
- `nullable` (BOOLEAN): Whether column allows NULL
- `default_value` (VARCHAR2): Default value
- `identity_gen` (BOOLEAN): Whether column is IDENTITY
- `invisible` (BOOLEAN): Whether column is invisible
- `comment_text` (VARCHAR2): Column comment

### `constraint_def`
Record type for constraint definitions.

**Fields:**
- `constraint_name` (VARCHAR2): Constraint name
- `constraint_type` (VARCHAR2): Constraint type ('PRIMARY', 'UNIQUE', 'FOREIGN', 'CHECK')
- `column_list` (VARCHAR2): Column list for constraint
- `references_table` (VARCHAR2): Referenced table (for foreign keys)
- `references_column` (VARCHAR2): Referenced column (for foreign keys)
- `check_condition` (VARCHAR2): Check condition (for check constraints)
- `deferrable` (BOOLEAN): Whether constraint is deferrable
- `initially_deferred` (BOOLEAN): Whether constraint is initially deferred

### `partition_def`
Record type for partition definitions.

**Fields:**
- `partition_name` (VARCHAR2): Partition name
- `partition_type` (VARCHAR2): Partition type ('RANGE', 'LIST', 'HASH', 'INTERVAL')
- `column_list` (VARCHAR2): Partition column list
- `values_clause` (VARCHAR2): Values clause for partition
- `tablespace` (VARCHAR2): Tablespace for partition
- `interval_expr` (VARCHAR2): Interval expression (for interval partitions)

### `table_properties`
Record type for table properties.

**Fields:**
- `tablespace` (VARCHAR2): Default tablespace
- `organization` (VARCHAR2): Table organization ('HEAP', 'INDEX', 'EXTERNAL')
- `compression` (VARCHAR2): Compression type ('BASIC', 'OLTP', 'QUERY HIGH', etc.)
- `inmemory` (BOOLEAN): Whether table is in-memory
- `inmemory_priority` (VARCHAR2): In-memory priority ('NONE', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL')
- `parallel_degree` (NUMBER): Parallel degree
- `logging` (BOOLEAN): Whether table is logged
- `row_movement` (BOOLEAN): Whether row movement is enabled
- `flashback_archive` (VARCHAR2): Flashback archive name
- `blockchain` (BOOLEAN): Whether table is blockchain
- `immutable` (BOOLEAN): Whether table is immutable

## Error Handling

The package includes comprehensive error handling with custom exceptions:

- `invalid_table_name`: Invalid table name
- `invalid_column_definition`: Invalid column definition
- `invalid_constraint_definition`: Invalid constraint definition
- `table_already_exists`: Table already exists
- `insufficient_privileges`: Insufficient privileges
- `invalid_data_type`: Invalid data type

## Testing

The package includes a comprehensive test suite:

```sql
-- Run all tests
@create_table_tests.sql
```

The test suite includes:
- Basic table creation tests
- Partitioned table tests
- IOT table tests
- Temporary table tests
- JSON table tests
- Validation tests
- DDL generation tests
- Bulk operation tests
- Performance tests

## Performance Considerations

- **Bulk Operations**: Use `create_tables_bulk` for multiple tables
- **Validation**: Use `validate_table_definition` before creation
- **Testing**: Use `test_table_creation` for safe testing
- **In-Memory**: Configure in-memory tables for frequently accessed data
- **Partitioning**: Use partitioning for large tables
- **Compression**: Use appropriate compression for storage optimization

## Best Practices

1. **Always validate** table definitions before creation
2. **Use appropriate data types** for optimal storage and performance
3. **Configure constraints** properly for data integrity
4. **Use partitioning** for large tables
5. **Configure in-memory** for frequently accessed tables
6. **Use compression** for storage optimization
7. **Test table creation** before production deployment
8. **Use bulk operations** for multiple tables
9. **Monitor performance** and adjust accordingly
10. **Document table structures** with comments

## Troubleshooting

### Common Issues

1. **Insufficient Privileges**
   - Ensure user has CREATE TABLE privilege
   - Check tablespace quotas

2. **Invalid Table Name**
   - Use valid Oracle identifiers
   - Avoid reserved words

3. **Invalid Column Definition**
   - Check data type syntax
   - Verify length/precision/scale values

4. **Constraint Errors**
   - Verify referenced tables exist
   - Check column names in constraints

5. **Partition Errors**
   - Verify partition syntax
   - Check tablespace availability

### Debug Mode

Enable debug mode for detailed logging:

```sql
-- Enable debug mode (requires package modification)
-- Set g_debug_mode := TRUE in package body
```

## Support

For issues and questions:
1. Check the test suite for examples
2. Review the error messages
3. Validate table definitions
4. Check Oracle documentation for specific features

## Version History

- **Version 1.0**: Initial release with Oracle 19c+ support
  - Basic table creation
  - Partitioned tables
  - IOT tables
  - Temporary tables
  - Blockchain tables
  - JSON support
  - Comprehensive testing
  - Performance optimization

## License

This package is provided as-is for educational and enterprise use. Please ensure compliance with your Oracle licensing terms.
