# Oracle 19c Partition Support - table_ddl_pkg

## Comprehensive Partitioning Coverage

The `table_ddl_pkg` now supports **all Oracle 19c partitioning strategies** with a unified, low-complexity API.

### Single-Level Partitioning (6 Types)

| Type | Usage | Example |
|------|-------|---------|
| **RANGE** | Time-series, sequential data | `partition_type='RANGE'` |
| **LIST** | Discrete values, categories | `partition_type='LIST'` |
| **HASH** | Even distribution, load balancing | `partition_type='HASH'` |
| **INTERVAL** | Automatic range partition creation | `partition_type='INTERVAL'` + `interval_expr` |
| **REFERENCE** | Parent-child table relationships | `partition_type='REFERENCE'` + `reference_constraint` |
| **AUTO_LIST** | Automatic list partition (19c) | `partition_type='AUTO_LIST'` |

### Composite Partitioning (9 Combinations)

All two-level combinations supported via `subpartition_type`:

#### Range-Based Primary
- **RANGE-RANGE** - Time windows with sub-ranges
- **RANGE-HASH** - Time windows with distribution (most common)
- **RANGE-LIST** - Time windows with categories

#### List-Based Primary
- **LIST-RANGE** - Categories with time sub-ranges
- **LIST-HASH** - Categories with distribution
- **LIST-LIST** - Categories with sub-categories

#### Hash-Based Primary (19c Enhancement)
- **HASH-RANGE** - Distribution with time sub-ranges
- **HASH-HASH** - Two-level distribution
- **HASH-LIST** - Distribution with categories

### Advanced Features

- ✅ **Multi-Column LIST** - Partition on multiple columns
- ✅ **Interval-Reference** - Combining automatic and reference
- ✅ **Virtual Column Partitioning** - Partition on expressions
- ✅ **Partition-Level Tablespaces** - Different tablespace per partition

## API Usage

### Basic Structure

```sql
DECLARE
    v_partitions partition_def_array;
BEGIN
    v_partitions := partition_def_array(
        partition_def(
            p_partition_name => 'p_name',
            p_partition_type => 'RANGE|LIST|HASH|INTERVAL|REFERENCE|AUTO_LIST',
            p_column_list => 'column_name',
            p_values_clause => 'partition values',
            p_tablespace => 'USERS',
            p_interval_expr => 'for INTERVAL only',
            p_subpartition_type => 'RANGE|LIST|HASH for composite',
            p_subpartition_column => 'subpartition column',
            p_subpartition_count => 4, -- for HASH
            p_reference_table => 'parent_table',
            p_reference_constraint => 'fk_name',
            p_auto_list_enabled => TRUE|FALSE,
            p_interval_reference => TRUE|FALSE
        )
    );
END;
```

### Example: Range Partitioning

```sql
v_partitions := partition_def_array(
    partition_def('p_2024_q1', 'RANGE', 'order_date', 
                  'TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')', 'USERS'),
    partition_def('p_2024_q2', 'RANGE', 'order_date', 
                  'TO_DATE(''2024-07-01'', ''YYYY-MM-DD'')', 'USERS')
);
```

### Example: Range-Hash Composite

```sql
v_partitions := partition_def_array(
    partition_def('p_2024', 'RANGE', 'sale_date', 
                  'TO_DATE(''2025-01-01'', ''YYYY-MM-DD'')', 'USERS',
                  NULL, 'HASH', 'customer_id', 4)
);
```

### Example: Interval Partitioning

```sql
v_partitions := partition_def_array(
    partition_def('p_initial', 'INTERVAL', 'log_date',
                  'TO_DATE(''2025-01-01'', ''YYYY-MM-DD'')', 'USERS',
                  'NUMTODSINTERVAL(1, ''DAY'')')
);
```

### Example: Auto List (19c)

```sql
v_partitions := partition_def_array(
    partition_def('p_auto', 'AUTO_LIST', 'category', NULL, 'USERS',
                  NULL, NULL, NULL, NULL, NULL, NULL, TRUE, FALSE)
);
```

### Example: Multi-Column LIST

```sql
v_partitions := partition_def_array(
    partition_def('p_us_east', 'LIST', 'country, state',
                  '(''USA'', ''NY''), (''USA'', ''NJ'')', 'USERS')
);
```

## Implementation Details

### Build Process

1. **Partition Type Detection** - Identifies primary partition strategy
2. **Subpartition Addition** - Adds composite partitioning if specified
3. **Values Clause** - Formats based on partition type:
   - RANGE: `VALUES LESS THAN (...)`
   - LIST: `VALUES (...)`
   - HASH: No values clause
4. **Tablespace Assignment** - Per-partition tablespace support

### Validation

- Partition type must be valid Oracle 19c type
- Composite combinations must be compatible
- Required fields validated per partition type
- Column list syntax checked

## Files Modified

- `table_ddl_pkg.sql` - Enhanced partition_def record type
- `table_ddl_pkg_body.sql` - Added `build_partition_clause()` function
- `partition_types_examples.sql` - Complete examples for all types

## Testing

Run comprehensive examples:
```sql
@examples/partition_types_examples.sql
```

## Production Recommendations

**Most Common Patterns:**

1. **INTERVAL** - Hands-off time-series (logs, events, transactions)
2. **RANGE-HASH** - Time windows + distribution (high-volume transactional)
3. **LIST** - Static categories (regions, status codes)
4. **HASH** - Even distribution (user data, random IDs)
5. **REFERENCE** - Parent-child relationships (orders-order_items)

**Oracle 19c New Features:**

- **AUTO_LIST** - Use when list values unknown upfront
- **HASH-HASH** - Extreme distribution for massive datasets
- **Multi-column LIST** - Complex categorization schemes

## Performance Notes

- INTERVAL partitions created on-demand (zero maintenance)
- HASH provides best load distribution
- RANGE-HASH combines time-based management + performance
- AUTO_LIST eliminates manual partition creation
- Reference partitioning maintains consistency automatically

---

*All Oracle 19c Enterprise Edition partition types supported.*

