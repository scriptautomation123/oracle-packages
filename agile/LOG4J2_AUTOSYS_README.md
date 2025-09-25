# Log4j2 Configuration for Autosys Applications

This guide explains how to properly configure log4j2.xml for applications running under Autosys, including path specification, environment handling, and best practices.

## Table of Contents

- [Overview](#overview)
- [Configuration Methods](#configuration-methods)
- [Complete Examples](#complete-examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

When calling your application from Autosys, you need to handle log file paths carefully since Autosys runs jobs in different working directories and environments. This guide provides multiple approaches for specifying log file paths in log4j2.xml.

## Configuration Methods

### 1. System Properties (Recommended)

#### In log4j2.xml:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
    <Properties>
        <!-- Use system property with fallback -->
        <Property name="logPath">${sys:log.directory:-/app/logs}</Property>
        <Property name="appName">${sys:app.name:-myapp}</Property>
    </Properties>
    
    <Appenders>
        <RollingFile name="FileAppender"
                     fileName="${logPath}/${appName}.log"
                     filePattern="${logPath}/${appName}-%d{yyyy-MM-dd}-%i.log.gz">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
            <Policies>
                <TimeBasedTriggeringPolicy/>
                <SizeBasedTriggeringPolicy size="100MB"/>
            </Policies>
            <DefaultRolloverStrategy max="30"/>
        </RollingFile>
    </Appenders>
    
    <Loggers>
        <Root level="INFO">
            <AppenderRef ref="FileAppender"/>
        </Root>
    </Loggers>
</Configuration>
```

#### In Autosys job definition:
```bash
# Set system properties before running Java
export LOG_DIRECTORY="/app/logs/myapp"
export APP_NAME="myapp"

# Run your Java application
java -Dlog.directory=$LOG_DIRECTORY -Dapp.name=$APP_NAME -jar myapp.jar
```

### 2. Environment Variables

#### In log4j2.xml:
```xml
<Configuration status="WARN">
    <Properties>
        <!-- Use environment variable with fallback -->
        <Property name="logPath">${env:LOG_DIRECTORY:-/app/logs}</Property>
        <Property name="appName">${env:APP_NAME:-myapp}</Property>
        <Property name="jobName">${env:AUTOSYS_JOB_NAME:-unknown}</Property>
    </Properties>
    
    <Appenders>
        <RollingFile name="FileAppender"
                     fileName="${logPath}/${jobName}/${appName}.log"
                     filePattern="${logPath}/${jobName}/${appName}-%d{yyyy-MM-dd}-%i.log.gz">
            <!-- ... rest of configuration ... -->
        </RollingFile>
    </Appenders>
</Configuration>
```

#### In Autosys job definition:
```bash
# Set environment variables
export LOG_DIRECTORY="/app/logs"
export APP_NAME="myapp"
export AUTOSYS_JOB_NAME="MY_APP_JOB"

# Run application
java -jar myapp.jar
```

### 3. Autosys-Specific Properties

#### In log4j2.xml:
```xml
<Configuration status="WARN">
    <Properties>
        <!-- Use Autosys-specific properties -->
        <Property name="logPath">${sys:autosys.log.directory:-/app/logs}</Property>
        <Property name="jobName">${sys:autosys.job.name:-unknown}</Property>
        <Property name="runId">${sys:autosys.run.id:-0}</Property>
    </Properties>
    
    <Appenders>
        <RollingFile name="FileAppender"
                     fileName="${logPath}/${jobName}/run_${runId}/app.log"
                     filePattern="${logPath}/${jobName}/run_${runId}/app-%d{yyyy-MM-dd}-%i.log.gz">
            <!-- ... rest of configuration ... -->
        </RollingFile>
    </Appenders>
</Configuration>
```

#### In Autosys job definition:
```bash
# Get Autosys job information
JOB_NAME=$AUTOSYS_JOB_NAME
RUN_ID=$AUTOSYS_RUN_ID
LOG_DIR="/app/logs"

# Run with Autosys-specific properties
java -Dautosys.log.directory=$LOG_DIR \
     -Dautosys.job.name=$JOB_NAME \
     -Dautosys.run.id=$RUN_ID \
     -jar myapp.jar
```

### 4. Dynamic Path Resolution

#### In log4j2.xml:
```xml
<Configuration status="WARN">
    <Properties>
        <!-- Dynamic path resolution -->
        <Property name="logPath">${sys:log.directory:-${env:LOG_DIRECTORY:-/app/logs}}</Property>
        <Property name="appName">${sys:app.name:-${env:APP_NAME:-myapp}}</Property>
        <Property name="timestamp">${date:yyyy-MM-dd_HH-mm-ss}</Property>
    </Properties>
    
    <Appenders>
        <RollingFile name="FileAppender"
                     fileName="${logPath}/${appName}_${timestamp}.log"
                     filePattern="${logPath}/${appName}_${timestamp}-%d{yyyy-MM-dd}-%i.log.gz">
            <!-- ... rest of configuration ... -->
        </RollingFile>
    </Appenders>
</Configuration>
```

## Complete Examples

### Autosys Job Definition (JIL)

```bash
insert_job: MY_APP_JOB
job_type: c
command: /app/scripts/run_myapp.sh
machine: server01
owner: appuser
permission: gx
description: "My Application Job"
```

### Shell Script (run_myapp.sh)

```bash
#!/bin/bash

# Set up logging environment
export LOG_DIRECTORY="/app/logs/myapp"
export APP_NAME="myapp"
export JOB_NAME="$AUTOSYS_JOB_NAME"
export RUN_ID="$AUTOSYS_RUN_ID"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIRECTORY/$JOB_NAME/run_$RUN_ID"

# Set Java options
JAVA_OPTS="-Xmx2g -Xms1g"
LOG_OPTS="-Dlog.directory=$LOG_DIRECTORY/$JOB_NAME/run_$RUN_ID"
LOG_OPTS="$LOG_OPTS -Dapp.name=$APP_NAME"
LOG_OPTS="$LOG_OPTS -Dautosys.job.name=$JOB_NAME"
LOG_OPTS="$LOG_OPTS -Dautosys.run.id=$RUN_ID"

# Run the application
java $JAVA_OPTS $LOG_OPTS -jar /app/jars/myapp.jar "$@"

# Check exit status
if [ $? -eq 0 ]; then
    echo "Application completed successfully"
else
    echo "Application failed with exit code $?"
    exit 1
fi
```

### Advanced Configuration with Multiple Appenders

```xml
<Configuration status="WARN">
    <Properties>
        <Property name="logPath">${sys:log.directory:-/app/logs}</Property>
        <Property name="appName">${sys:app.name:-myapp}</Property>
        <Property name="jobName">${sys:autosys.job.name:-unknown}</Property>
    </Properties>
    
    <Appenders>
        <!-- Main application log -->
        <RollingFile name="AppFileAppender"
                     fileName="${logPath}/${jobName}/${appName}.log"
                     filePattern="${logPath}/${jobName}/${appName}-%d{yyyy-MM-dd}-%i.log.gz">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
            <Policies>
                <TimeBasedTriggeringPolicy/>
                <SizeBasedTriggeringPolicy size="100MB"/>
            </Policies>
            <DefaultRolloverStrategy max="30"/>
        </RollingFile>
        
        <!-- Error log -->
        <RollingFile name="ErrorFileAppender"
                     fileName="${logPath}/${jobName}/${appName}_error.log"
                     filePattern="${logPath}/${jobName}/${appName}_error-%d{yyyy-MM-dd}-%i.log.gz">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
            <ThresholdFilter level="ERROR" onMatch="ACCEPT" onMismatch="DENY"/>
            <Policies>
                <TimeBasedTriggeringPolicy/>
                <SizeBasedTriggeringPolicy size="50MB"/>
            </Policies>
            <DefaultRolloverStrategy max="30"/>
        </RollingFile>
        
        <!-- Console appender for Autosys -->
        <Console name="ConsoleAppender" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    
    <Loggers>
        <Root level="INFO">
            <AppenderRef ref="AppFileAppender"/>
            <AppenderRef ref="ErrorFileAppender"/>
            <AppenderRef ref="ConsoleAppender"/>
        </Root>
    </Loggers>
</Configuration>
```

## Best Practices

### 1. Path Management
- **Always use absolute paths** - Don't rely on relative paths
- **Create log directories** - Ensure directories exist before logging
- **Use system properties** - More reliable than environment variables
- **Include job identification** - Use Autosys job name and run ID

### 2. Error Handling
- **Handle failures gracefully** - Check exit codes and log errors
- **Validate paths** - Ensure log directories are writable
- **Monitor disk space** - Prevent log files from filling up disk

### 3. Log Management
- **Rotate logs** - Use RollingFile appender to manage log size
- **Compress old logs** - Use gzip compression for archived logs
- **Set retention policies** - Automatically clean up old log files
- **Separate error logs** - Use different appenders for different log levels

### 4. Performance
- **Use asynchronous logging** - Consider AsyncAppender for high-volume applications
- **Optimize patterns** - Use efficient pattern layouts
- **Monitor performance** - Watch for logging overhead

### 5. Security
- **Set proper permissions** - Ensure log files have appropriate access controls
- **Avoid sensitive data** - Don't log passwords or sensitive information
- **Use secure paths** - Avoid world-writable directories

## Troubleshooting

### Common Issues

#### 1. Log Files Not Created
**Problem**: Log files are not being created in the expected location.

**Solutions**:
- Check if the log directory exists and is writable
- Verify system properties are being passed correctly
- Check log4j2.xml syntax and configuration
- Ensure the application has permissions to create files

```bash
# Check directory permissions
ls -la /app/logs/
# Check if directory is writable
touch /app/logs/test.log
# Verify system properties
java -Dlog.directory=/app/logs -Dapp.name=myapp -jar myapp.jar -Dlog4j.debug=true
```

#### 2. Environment Variables Not Recognized
**Problem**: Environment variables are not being picked up by log4j2.

**Solutions**:
- Use system properties instead of environment variables
- Ensure variables are exported in the shell script
- Check variable names and case sensitivity

```bash
# Debug environment variables
env | grep LOG
# Use system properties instead
java -Dlog.directory=$LOG_DIRECTORY -jar myapp.jar
```

#### 3. Permission Denied Errors
**Problem**: Application cannot write to log files due to permission issues.

**Solutions**:
- Check file and directory permissions
- Ensure the application user has write access
- Create log directories with proper permissions

```bash
# Set proper permissions
chmod 755 /app/logs
chown appuser:appgroup /app/logs
# Create log directory in script
mkdir -p "$LOG_DIRECTORY"
chmod 755 "$LOG_DIRECTORY"
```

#### 4. Log Rotation Issues
**Problem**: Log files are not rotating or are growing too large.

**Solutions**:
- Check RollingFile configuration
- Verify disk space availability
- Review retention policies
- Test log rotation manually

```xml
<!-- Ensure proper rotation configuration -->
<Policies>
    <TimeBasedTriggeringPolicy/>
    <SizeBasedTriggeringPolicy size="100MB"/>
</Policies>
<DefaultRolloverStrategy max="30"/>
```

### Debugging Steps

1. **Enable Log4j2 debug logging**:
   ```bash
   java -Dlog4j.debug=true -jar myapp.jar
   ```

2. **Check system properties**:
   ```bash
   java -Dlog4j.debug=true -Dlog.directory=/app/logs -jar myapp.jar
   ```

3. **Verify file creation**:
   ```bash
   # Check if log files are being created
   ls -la /app/logs/
   # Monitor log file creation
   tail -f /app/logs/myapp.log
   ```

4. **Test configuration**:
   ```bash
   # Test log4j2.xml syntax
   java -Dlog4j.configurationFile=log4j2.xml -Dlog4j.debug=true -jar myapp.jar
   ```

### Monitoring and Maintenance

#### Log File Monitoring
```bash
# Monitor log file sizes
find /app/logs -name "*.log" -exec ls -lh {} \;

# Check disk usage
du -sh /app/logs/*

# Monitor log rotation
ls -la /app/logs/*.log.*
```

#### Automated Cleanup
```bash
# Clean up old log files (older than 30 days)
find /app/logs -name "*.log.*" -mtime +30 -delete

# Compress old log files
find /app/logs -name "*.log" -mtime +7 -exec gzip {} \;
```

## Additional Resources

- [Log4j2 Configuration Documentation](https://logging.apache.org/log4j/2.x/manual/configuration.html)
- [Autosys User Guide](https://docs.broadcom.com/doc/autosys-user-guide)
- [Java System Properties](https://docs.oracle.com/javase/tutorial/essential/environment/sysprop.html)

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review log4j2.xml configuration syntax
3. Verify Autosys job configuration
4. Test with minimal configuration first
5. Check application permissions and directory access
