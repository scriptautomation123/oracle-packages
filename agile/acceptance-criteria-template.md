`# Acceptance Criteria Template

## Format
Given [precondition/context]  
When [action/trigger]  
Then [expected outcome]

## Categories for Oracle Partition Management

### Functional Criteria
- Core partition operations
- Data integrity validation
- Error handling behavior
- Performance requirements

### Non-Functional Criteria
- Response time limits
- Concurrent operation support
- Resource usage constraints
- Security requirements

## Template Patterns

### Basic Operation Pattern
```
Given a partitioned table [table_name] exists
When I execute [operation] with parameters [params]
Then the operation completes successfully
And the result matches expected [outcome]
And the operation is logged autonomously
```

### Error Handling Pattern
```
Given [error condition exists]
When I attempt [operation]
Then the operation fails gracefully
And a descriptive error message is returned
And the error is logged with details
And no partial changes remain
```

### Performance Pattern
```
Given a table with [data_volume] records
When I execute [operation]
Then the operation completes within [time_limit]
And resource usage stays below [threshold]
And concurrent operations are not blocked
```

## Acceptance Criteria by Feature

### Partition Creation
```
Scenario: Create new partition successfully
Given a partitioned table "SALES" exists
When I create partition "P_2024_Q2" with high value "TO_DATE('2024-07-01', 'YYYY-MM-DD')"
Then the partition is created successfully
And the partition appears in USER_TAB_PARTITIONS
And the operation is logged in partition_operation_log
And indexes are updated if specified

Scenario: Prevent duplicate partition creation
Given a partition "P_2024_Q1" already exists on table "SALES"
When I attempt to create partition "P_2024_Q1"
Then the operation fails with "Partition already exists" error
And no changes are made to the table
And the error is logged with details

Scenario: Validate tablespace existence
Given tablespace "INVALID_TS" does not exist
When I create partition with tablespace "INVALID_TS"
Then the operation fails with "Tablespace not found" error
And no partition is created
```

### Partition Maintenance
```
Scenario: Schedule maintenance job successfully
Given table "SALES" exists and is partitioned
When I create maintenance job with type "CLEANUP" and schedule "MONTHLY"
Then the job is created in DBA_SCHEDULER_JOBS
And the job status is "ENABLED"
And the next run time is calculated correctly

Scenario: Execute maintenance operations
Given maintenance job "SALES_CLEANUP" exists and is enabled
When the scheduled time arrives
Then old partitions are identified based on retention policy
And partitions are dropped successfully
And statistics are updated
And the operation is logged with performance metrics

Scenario: Handle maintenance job failures
Given maintenance job encounters an error
When the job execution fails
Then the error is logged with full details
And the job is rescheduled with backoff
And notification is sent if configured
```

### Strategy Migration
```
Scenario: Migrate to interval partitioning
Given table "SALES" uses range partitioning
When I migrate to interval partitioning with interval "1 DAY"
Then existing partitions are preserved
And new partitions are created automatically
And data remains accessible throughout migration
And the migration is logged with timing

Scenario: Validate migration compatibility
Given table has foreign key constraints
When I attempt strategy migration
Then compatibility is checked first
And migration proceeds only if safe
And rollback plan is available if needed
```

### Performance Analysis
```
Scenario: Analyze partition performance
Given table "SALES" has been active for 7 days
When I request performance analysis
Then metrics are collected from V$SQL_PLAN
And slow partitions are identified
And recommendations are generated
And results are returned within 30 seconds

Scenario: Generate health report
Given table "SALES" with multiple partitions
When I generate health report
Then all partitions are analyzed
And health scores are calculated
And issues are prioritized by severity
And report is formatted as requested (HTML/JSON)
```

## Acceptance Criteria Checklist

### Completeness
- [ ] Covers all happy path scenarios
- [ ] Includes error conditions
- [ ] Addresses edge cases
- [ ] Specifies performance requirements
- [ ] Defines security constraints

### Clarity
- [ ] Uses specific, measurable terms
- [ ] Avoids ambiguous language
- [ ] Includes concrete examples
- [ ] Specifies expected data/outcomes
- [ ] Clear pass/fail criteria

### Testability
- [ ] Can be automated
- [ ] Repeatable results
- [ ] Observable outcomes
- [ ] Measurable criteria
- [ ] Independent scenarios

## Common Acceptance Criteria Patterns

### Data Validation
```
Then the data integrity is maintained
And no orphaned records exist
And referential constraints are satisfied
And partition pruning works correctly
```

### Logging Requirements
```
And the operation is logged autonomously
And performance metrics are captured
And error details include stack trace
And log entries have correlation ID
```

### Performance Requirements
```
And the operation completes within [X] seconds
And memory usage stays below [Y] MB
And CPU utilization remains under [Z]%
And concurrent operations are not blocked
```

### Security Requirements
```
And only authorized users can execute
And sensitive data is not logged
And audit trail is maintained
And privilege escalation is prevented
```

### Recovery Requirements
```
And the operation can be rolled back
And partial failures are cleaned up
And system remains in consistent state
And recovery procedures are documented
```
