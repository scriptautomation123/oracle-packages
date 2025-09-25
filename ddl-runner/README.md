# DDL Execution Tool

Enterprise-grade DDL script execution tool with configuration file support, error handling, and interactive execution control.

## Features

- **Configuration File**: Simple configuration management
- **No Database Objects**: Pure execution without creating temporary objects
- **Interactive Control**: Wait between scripts with user confirmation
- **Error Handling**: Continue or stop on errors
- **Comprehensive Logging**: Detailed execution logs with timestamps
- **Connection Testing**: Validates database connectivity before execution
- **Dry Run Mode**: Preview execution without running scripts

## Files

- `run_ddl_simple.sh` - Main execution script (recommended)
- `run_ddl.sh` - Alternative execution script with YAML support
- `ddl_config.conf` - Configuration file (simple format)
- `ddl_config.yaml` - Alternative configuration file (YAML format)
- `sqlplus_wrapper.sql` - Simple SQL*Plus wrapper
- `README.md` - This documentation

## Quick Start

1. **Set Environment Variables**:
   ```bash
   export DB_USER=your_username
   export DB_PASS=your_password
   ```

2. **Configure Scripts**:
   Edit `ddl_config.conf` to specify your scripts and settings.

3. **Run Execution**:
   ```bash
   ./run_ddl_simple.sh
   ```

## Configuration

### Database Settings
```conf
DB_HOST=localhost
DB_PORT=1521
DB_SERVICE=ORCL
DB_USERNAME=${DB_USER}
DB_PASSWORD=${DB_PASS}
```

### Execution Settings
```conf
SCRIPT_DIRECTORY=./scripts
TARGET_SCHEMA=${DB_USER}
LOG_FILE=ddl_execution_$(date +%Y%m%d_%H%M%S).log
CONTINUE_ON_ERROR=true
WAIT_BETWEEN_SCRIPTS=true
VERBOSE=true
```

### Script Configuration
```conf
SCRIPTS=partition_logger_pkg.sql:Partition logging package
SCRIPTS=partition_management_pkg.sql:Main partition management package
SCRIPTS=partition_management_pkg_body.sql:Partition management package body
SCRIPTS=partition_strategy_pkg.sql:Partition strategy management package
SCRIPTS=partition_maintenance_pkg.sql:Partition maintenance package
SCRIPTS=partition_utils_pkg.sql:Partition utilities package
```

## Usage Examples

### Basic Execution
```bash
# Using default configuration
DB_USER=hr DB_PASS=hr123 ./run_ddl_simple.sh
```

### Custom Configuration
```bash
# Using custom config file
./run_ddl_simple.sh -c my_config.conf
```

### Dry Run Mode
```bash
# Preview what would be executed
./run_ddl_simple.sh -d
```

### Verbose Output
```bash
# Enable detailed logging
./run_ddl_simple.sh -v
```

## Command Line Options

- `-c, --config FILE` - Configuration file (default: ddl_config.conf)
- `-h, --help` - Show help message
- `-v, --verbose` - Enable verbose output
- `-d, --dry-run` - Show what would be executed without running

## Environment Variables

- `DB_USER` - Database username (required)
- `DB_PASS` - Database password (required)

## Script Requirements

1. **Script Directory**: All SQL scripts must be in the configured directory
2. **File Format**: Scripts must be valid SQL*Plus files
3. **Permissions**: Script files must be readable
4. **Database Access**: User must have appropriate privileges

## Error Handling

- **Connection Errors**: Tool validates database connectivity before execution
- **Script Errors**: Configurable to continue or stop on errors
- **File Errors**: Validates script existence before execution
- **Logging**: All errors are logged with timestamps

## Logging

- **Console Output**: Colored output with timestamps
- **Log File**: Detailed execution log saved to file
- **Error Details**: Comprehensive error information
- **Performance**: Execution time tracking

## Best Practices

1. **Test First**: Use dry-run mode to validate configuration
2. **Backup**: Always backup before running DDL scripts
3. **Review Logs**: Check log files for any issues
4. **Incremental**: Use wait-between-scripts for critical deployments
5. **Validation**: Verify script execution results

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify database credentials
   - Check network connectivity
   - Validate service name

2. **Script Not Found**
   - Check script directory path
   - Verify file permissions
   - Ensure file exists

3. **Permission Denied**
   - Check database privileges
   - Verify schema access
   - Review user permissions

### Debug Mode

Enable verbose logging for detailed information:
```bash
./run_ddl.sh -v
```

## Security Notes

- Never hardcode passwords in configuration files
- Use environment variables for sensitive data
- Restrict file permissions on configuration files
- Review logs for sensitive information

## License

This tool is provided as-is for educational and development purposes. Use in production environments at your own risk.
