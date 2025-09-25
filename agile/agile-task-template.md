# Task Template

## Task Format
**Task:** [Specific deliverable]  
**Story:** [Link to parent user story]  
**Estimate:** [Time/effort estimate]  
**Assignee:** [Team member]

## Task Categories

### Development Tasks
- Package development
- Function implementation
- Error handling
- Performance optimization

### Testing Tasks
- Unit testing
- Integration testing
- Performance testing
- Security testing

### Documentation Tasks
- Code documentation
- User guides
- API documentation
- Troubleshooting guides

### DevOps Tasks
- Deployment scripts
- Environment setup
- Monitoring configuration
- Backup procedures

## Task Templates by Type

### Package Development Task
```
Task: Implement [package_name] with [specific_functionality]
Story: [Story ID/Title]
Estimate: [Hours/Days]
Assignee: [Developer]

Technical Details:
- Package specification with public interface
- Package body with implementation
- Error handling using autonomous transactions
- Logging integration
- Parameter validation

Acceptance Criteria:
- [ ] Package compiles without errors
- [ ] All public procedures/functions implemented
- [ ] Error handling covers edge cases
- [ ] Logging captures all operations
- [ ] Unit tests pass
```

### Testing Task
```
Task: Create test suite for [component/feature]
Story: [Story ID/Title]
Estimate: [Hours/Days]
Assignee: [Tester/Developer]

Test Scope:
- Positive test cases
- Negative test cases
- Edge cases
- Performance benchmarks

Acceptance Criteria:
- [ ] Test cases cover all acceptance criteria
- [ ] Tests execute successfully
- [ ] Test data setup/teardown automated
- [ ] Performance baselines established
- [ ] Test documentation complete
```

### Documentation Task
```
Task: Document [feature/component] usage and examples
Story: [Story ID/Title]
Estimate: [Hours/Days]
Assignee: [Technical Writer/Developer]

Documentation Scope:
- API reference
- Usage examples
- Configuration options
- Troubleshooting guide

Acceptance Criteria:
- [ ] All public interfaces documented
- [ ] Working examples provided
- [ ] Common issues addressed
- [ ] Review completed
- [ ] Published to documentation site
```

## Example Tasks

### Task 1: Partition Creation Function
**Task:** Implement create_partition procedure with validation and logging  
**Story:** US001 - Partition Creation with Automated Logging  
**Estimate:** 8 hours  
**Assignee:** Senior Developer

**Technical Details:**
- Input parameter validation
- Tablespace existence check
- Partition name uniqueness validation
- DDL execution with error handling
- Autonomous logging of operation

**Acceptance Criteria:**
- [ ] Procedure accepts all required parameters
- [ ] Validates partition doesn't already exist
- [ ] Creates partition with specified high value
- [ ] Updates global indexes if requested
- [ ] Logs operation with performance metrics
- [ ] Handles all Oracle exceptions gracefully

### Task 2: Performance Test Suite
**Task:** Create performance test suite for partition operations  
**Story:** US003 - Performance Analysis and Monitoring  
**Estimate:** 12 hours  
**Assignee:** QA Engineer

**Technical Details:**
- Load test data generation
- Operation timing measurements
- Memory usage monitoring
- Concurrent operation testing

**Acceptance Criteria:**
- [ ] Tests cover all partition operations
- [ ] Performance baselines established
- [ ] Concurrent operation safety verified
- [ ] Memory leak detection implemented
- [ ] Results exported for analysis

### Task 3: Migration Documentation
**Task:** Document strategy migration procedures and examples  
**Story:** US002 - Strategy Migration Between Types  
**Estimate:** 6 hours  
**Assignee:** Technical Writer

**Technical Details:**
- Migration paths for each strategy type
- Data preservation requirements
- Rollback procedures
- Performance impact analysis

**Acceptance Criteria:**
- [ ] All migration paths documented
- [ ] Step-by-step procedures provided
- [ ] Prerequisites clearly listed
- [ ] Risk mitigation strategies included
- [ ] Examples for common scenarios

## Task Breakdown Guidelines

### Break Down When:
- Task exceeds 16 hours
- Multiple skill sets required
- Dependencies on external systems
- High complexity or risk

### Keep Together When:
- Single logical unit of work
- Same developer/skill set
- No external dependencies
- Low complexity

## Definition of Done for Tasks

### Development Tasks
- [ ] Code implemented and tested
- [ ] Code review completed
- [ ] Documentation updated
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] No critical security issues
- [ ] Performance requirements met

### Testing Tasks
- [ ] Test cases executed
- [ ] Results documented
- [ ] Defects logged and triaged
- [ ] Test data cleaned up
- [ ] Test automation updated

### Documentation Tasks
- [ ] Content written and reviewed
- [ ] Examples tested and verified
- [ ] Formatting and style consistent
- [ ] Published to appropriate location
- [ ] Stakeholder approval obtained

## Task Status Workflow
1. **To Do** - Ready for work
2. **In Progress** - Actively being worked
3. **In Review** - Peer/code review
4. **Testing** - QA validation
5. **Done** - Meets definition of done
