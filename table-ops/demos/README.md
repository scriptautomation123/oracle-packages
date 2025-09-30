# Oracle Partition Management Demos

This directory contains organized demonstrations and examples for the Oracle Partition Management suite.

## Directory Structure

### 📚 getting-started/
Quick start guides and basic usage examples:
- `quick-start.sql` - Basic partition management operations
- `stats-demo-usage.sql` - Oracle 19c statistics demonstrations
- Clean, simple examples for new users

### 🔧 partitioning/
Advanced partitioning examples and conversions:
- `ONLINE_CONVERSION_EXAMPLES.sql` - Oracle 19c zero-downtime conversions
- `ONLINE_CONVERSION_SUMMARY.md` - Complete implementation guide
- `PARTITION_SUPPORT.md` - All 15 Oracle 19c partition types supported
- Subpartitioning examples with templates
- Table conversion scenarios (DBMS_REDEFINITION)
- Comprehensive partition type demonstrations
- Real-world usage patterns

### ✅ validation-tests/
Test scripts for validating functionality:
- `test_validate_table_ops_pkg.sql` - Oracle 19c online conversion tests
- `test_validate_oracle19c_partition_support.sql` - All 15 partition types
- `test_validate_partition_analysis_pkg.sql` - Statistics and analysis
- `test_validate_table_ddl_pkg.sql` - DDL generation functions
- `test_validate_logging_infrastructure.sql` - Logging validation
- Verification scripts for all package functions
- Quality assurance demonstrations

### 📄 ddl-templates/
DDL generation templates and examples:
- `simple_*.sql` - Basic DDL templates
- Template patterns for common scenarios
- Ready-to-use DDL generation examples

### 📋 Developer Resources
- `IMPLEMENTATION_NOTES.md` - Functions needing implementation
- Package specifications and implementation status
- Next steps for completing the suite

## Usage

1. **Start Here**: Begin with `getting-started/quick-start.sql`
2. **Explore Features**: Review examples in `partitioning/`
3. **Validate Setup**: Run tests from `validation-tests/`
4. **Generate DDL**: Use templates from `ddl-templates/`

## Prerequisites

- Oracle Database 19c or later
- Installed partition management packages (see main README)
- Appropriate schema privileges

## Support

All demos reference the core packages in the parent directory:
- `partition_management_pkg`
- `partition_strategy_pkg`
- `partition_utils_pkg`
- `table_ops_pkg`
- `partition_analysis_pkg`

For implementation details, see the package specifications and bodies in the main directory.