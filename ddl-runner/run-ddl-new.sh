#!/bin/bash
# =====================================================
# DDL Execution Script Runner (Enhanced Version)
# Enterprise-grade DDL script runner with comprehensive file-based tracking
# Author: Principal Database Engineer/DBA
# Version: 4.0
# Security: Hardened against common vulnerabilities
# =====================================================

# Security: Enable strict mode for better error handling
set -euo pipefail
IFS=$'\n\t'

# Security: Set secure umask
umask 077

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_CONFIG_FILE="${SCRIPT_DIR}/ddl_config.conf"

# Tracking directories
TRACKING_DIR="${SCRIPT_DIR}/.ddl_tracking"
HISTORY_DIR="${TRACKING_DIR}/history"
METRICS_DIR="${TRACKING_DIR}/metrics"
LOGS_DIR="${TRACKING_DIR}/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Global variables
declare -A CONFIG
declare -a SCRIPT_LIST
declare -a SCRIPT_ORDER
declare -A SCRIPT_DESCRIPTIONS

# Initialize tracking directories
init_tracking() {
    mkdir -p "$TRACKING_DIR" "$HISTORY_DIR" "$METRICS_DIR" "$LOGS_DIR"
    
    # Create tracking files if they don't exist
    [[ ! -f "$TRACKING_DIR/execution_history.json" ]] && echo "[]" > "$TRACKING_DIR/execution_history.json"
    [[ ! -f "$TRACKING_DIR/script_metrics.json" ]] && echo "{}" > "$TRACKING_DIR/script_metrics.json"
    [[ ! -f "$TRACKING_DIR/error_codes.json" ]] && echo "{}" > "$TRACKING_DIR/error_codes.json"
}

# Function to log messages with proper formatting
log_message() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        "DEBUG")
            if [[ "${CONFIG[logging_level]:-${CONFIG[LOG_LEVEL]}}" == "DEBUG" ]]; then
                echo -e "${CYAN}[$timestamp] [DEBUG] $message${NC}"
            fi
            ;;
        "INFO")
            echo -e "${GREEN}[$timestamp] [INFO] $message${NC}"
            ;;
        "WARN")
            echo -e "${YELLOW}[$timestamp] [WARN] $message${NC}"
            ;;
        "ERROR")
            echo -e "${RED}[$timestamp] [ERROR] $message${NC}"
            ;;
    esac
}

# Function to show usage information
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

DDL Execution Script Runner - Enhanced Version

OPTIONS:
    -c, --config FILE     Configuration file (CONF format)
    -h, --help           Show this help message
    -v, --verbose        Enable verbose output
    -d, --dry-run        Show what would be executed without running
    -r, --report TYPE    Generate report (summary|detailed|metrics|history|errors|all)

CONFIGURATION:
    The script uses CONF configuration format:
    - CONF: $CONF_CONFIG_FILE
    
    If no config file is specified, the script will look for:
    ddl_config.conf in the script directory

EXAMPLES:
    $0                                    # Use default configuration
    $0 -c my_config.conf                 # Use specific CONF config
    $0 -d                                # Dry run mode
    $0 -v -r detailed                    # Verbose with detailed report

EOF
}


# Security: Validate configuration file content
validate_config_file() {
    local config_file="$1"
    
    # Check file permissions (should not be world-writable)
    if [[ -w "$config_file" ]] && [[ "$(stat -c '%a' "$config_file")" =~ ^[0-7][0-7][0-7]$ ]]; then
        local perms
        perms=$(stat -c '%a' "$config_file")
        if [[ "$perms" =~ ^[0-7][0-7][4-7]$ ]]; then
            log_message "WARN" "Configuration file is world-readable: $config_file"
        fi
    fi
    
    # Security: Validate file is not a symlink to prevent symlink attacks
    if [[ -L "$config_file" ]]; then
        log_message "ERROR" "Configuration file cannot be a symbolic link: $config_file"
        return 1
    fi
    
    # Security: Check for dangerous patterns in config file
    if grep -qE '\$\(|\`|exec|eval|source|\$\(\$\)' "$config_file" 2>/dev/null; then
        log_message "ERROR" "Configuration file contains potentially dangerous content: $config_file"
        return 1
    fi
    
    return 0
}

# Function to load CONF configuration
load_conf_config() {
    local config_file="$1"
    
    if [[ ! -f "$config_file" ]]; then
        log_message "ERROR" "CONF configuration file not found: $config_file"
        return 1
    fi
    
    # Security: Validate configuration file before sourcing
    if ! validate_config_file "$config_file"; then
        log_message "ERROR" "Configuration file validation failed"
        return 1
    fi
    
    # Security: Use safer sourcing method with restricted environment
    # Create a restricted environment for sourcing
    local temp_env
    temp_env="/tmp/ddl_config_$$.env"
    
    # Extract only safe variables from config file
    grep -E '^[A-Z_][A-Z0-9_]*=' "$config_file" > "$temp_env" 2>/dev/null || true
    
    # Security: Source only the extracted variables
    # shellcheck source=/dev/null
    source "$temp_env"
    
    # Cleanup temporary file
    rm -f "$temp_env"
    
    # Security: Validate and sanitize configuration values
    CONFIG[DB_HOST]="${DB_HOST:-}"
    CONFIG[DB_PORT]="${DB_PORT:-}"
    CONFIG[DB_SERVICE]="${DB_SERVICE:-}"
    CONFIG[DB_USERNAME]="${DB_USERNAME:-}"
    CONFIG[DB_PASSWORD]="${DB_PASSWORD:-}"
    
    # Security: Validate host format (basic IP/hostname validation)
    if [[ -n "${CONFIG[DB_HOST]}" ]] && ! [[ "${CONFIG[DB_HOST]}" =~ ^[a-zA-Z0-9.-]+$ ]]; then
        log_message "ERROR" "Invalid database host format: ${CONFIG[DB_HOST]}"
        return 1
    fi
    
    # Security: Validate port is numeric
    if [[ -n "${CONFIG[DB_PORT]}" ]] && ! [[ "${CONFIG[DB_PORT]}" =~ ^[0-9]+$ ]]; then
        log_message "ERROR" "Invalid database port format: ${CONFIG[DB_PORT]}"
        return 1
    fi
    
    # Security: Validate service name
    if [[ -n "${CONFIG[DB_SERVICE]}" ]] && ! [[ "${CONFIG[DB_SERVICE]}" =~ ^[a-zA-Z0-9._-]+$ ]]; then
        log_message "ERROR" "Invalid database service format: ${CONFIG[DB_SERVICE]}"
        return 1
    fi
    
    # Security: Validate username
    if [[ -n "${CONFIG[DB_USERNAME]}" ]] && ! [[ "${CONFIG[DB_USERNAME]}" =~ ^[a-zA-Z0-9._-]+$ ]]; then
        log_message "ERROR" "Invalid database username format: ${CONFIG[DB_USERNAME]}"
        return 1
    fi
    CONFIG[SCRIPT_DIRECTORY]="$SCRIPT_DIRECTORY"
    CONFIG[TARGET_SCHEMA]="${TARGET_SCHEMA:-}"
    CONFIG[LOG_FILE]="${LOG_FILE:-}"
    CONFIG[CONTINUE_ON_ERROR]="${CONTINUE_ON_ERROR:-}"
    CONFIG[WAIT_BETWEEN_SCRIPTS]="$WAIT_BETWEEN_SCRIPTS"
    CONFIG[VERBOSE]="${VERBOSE:-}"
    CONFIG[LOG_LEVEL]="$LOG_LEVEL"
    CONFIG[INCLUDE_TIMESTAMPS]="$INCLUDE_TIMESTAMPS"
    CONFIG[MAX_BACKUP_FILES]="$MAX_BACKUP_FILES"
    
    # Parse scripts from CONF format
    SCRIPT_LIST=()
    SCRIPT_ORDER=()
    SCRIPT_DESCRIPTIONS=()
    
    # Extract scripts from SCRIPTS array (if it exists)
    if [[ -n "${SCRIPTS[*]}" ]]; then
        for script_entry in "${SCRIPTS[@]}"; do
            local script_name
            local description
            script_name=$(echo "$script_entry" | cut -d':' -f1)
            description=$(echo "$script_entry" | cut -d':' -f2-)
            
            SCRIPT_LIST+=("$script_name")
            SCRIPT_ORDER+=("$script_name")
            SCRIPT_DESCRIPTIONS["$script_name"]="$description"
        done
    fi
    
    log_message "INFO" "CONF configuration loaded from: $config_file"
    return 0
}

# Function to load configuration
load_configuration() {
    local config_file="$1"
    
    # If no config file specified, use default CONF file
    if [[ -z "$config_file" ]]; then
        config_file="$CONF_CONFIG_FILE"
    fi
    
    # Load CONF configuration
    load_conf_config "$config_file"
}

# Function to validate configuration
validate_config() {
    local errors=0
    
    # Check required database settings
    if [[ -z "${CONFIG[database_host]:-${CONFIG[DB_HOST]}}" ]]; then
        log_message "ERROR" "Database host not configured"
        ((errors++))
    fi
    
    if [[ -z "${CONFIG[database_port]:-${CONFIG[DB_PORT]}}" ]]; then
        log_message "ERROR" "Database port not configured"
        ((errors++))
    fi
    
    if [[ -z "${CONFIG[database_service]:-${CONFIG[DB_SERVICE]}}" ]]; then
        log_message "ERROR" "Database service not configured"
        ((errors++))
    fi
    
    if [[ -z "${CONFIG[database_username]:-${CONFIG[DB_USERNAME]}}" ]]; then
        log_message "ERROR" "Database username not configured"
        ((errors++))
    fi
    
    if [[ -z "${CONFIG[database_password]:-${CONFIG[DB_PASSWORD]}}" ]]; then
        log_message "ERROR" "Database password not configured"
        ((errors++))
    fi
    
    # Check script directory
    local script_dir="${CONFIG[execution_script_directory]:-${CONFIG[SCRIPT_DIRECTORY]}}"
    if [[ -z "$script_dir" ]]; then
        log_message "ERROR" "Script directory not configured"
        ((errors++))
    elif [[ ! -d "$script_dir" ]]; then
        log_message "ERROR" "Script directory does not exist: $script_dir"
        ((errors++))
    fi
    
    # Check if any scripts are configured
    if [[ ${#SCRIPT_ORDER[@]} -eq 0 ]]; then
        log_message "ERROR" "No scripts configured for execution"
        ((errors++))
    fi
    
    return $errors
}

# Function to show current configuration
show_config() {
    echo "====================================================="
    echo "CURRENT CONFIGURATION"
    echo "====================================================="
    echo "Database Host: ${CONFIG[database_host]:-${CONFIG[DB_HOST]}}"
    echo "Database Port: ${CONFIG[database_port]:-${CONFIG[DB_PORT]}}"
    echo "Database Service: ${CONFIG[database_service]:-${CONFIG[DB_SERVICE]}}"
    # Security: Mask username in display
    local masked_username="${CONFIG[database_username]:-${CONFIG[DB_USERNAME]}}"
    if [[ -n "$masked_username" ]]; then
        masked_username="${masked_username:0:2}***${masked_username: -1}"
    fi
    echo "Database Username: $masked_username"
    echo "Target Schema: ${CONFIG[execution_target_schema]:-${CONFIG[TARGET_SCHEMA]}}"
    echo "Script Directory: ${CONFIG[execution_script_directory]:-${CONFIG[SCRIPT_DIRECTORY]}}"
    echo "Continue on Error: ${CONFIG[execution_continue_on_error]:-${CONFIG[CONTINUE_ON_ERROR]}}"
    echo "Wait Between Scripts: ${CONFIG[execution_wait_between_scripts]:-${CONFIG[WAIT_BETWEEN_SCRIPTS]}}"
    echo "Verbose Mode: ${CONFIG[execution_verbose]:-${CONFIG[VERBOSE]}}"
    echo "Log Level: ${CONFIG[logging_level]:-${CONFIG[LOG_LEVEL]}}"
    echo "Scripts to Execute: ${#SCRIPT_ORDER[@]}"
    for script in "${SCRIPT_ORDER[@]}"; do
        echo "  - $script: ${SCRIPT_DESCRIPTIONS[$script]}"
    done
    echo "====================================================="
}

# Function to test database connection
test_connection() {
    local host="${CONFIG[database_host]:-${CONFIG[DB_HOST]}}"
    local port="${CONFIG[database_port]:-${CONFIG[DB_PORT]}}"
    local service="${CONFIG[database_service]:-${CONFIG[DB_SERVICE]}}"
    local username="${CONFIG[database_username]:-${CONFIG[DB_USERNAME]}}"
    local password="${CONFIG[database_password]:-${CONFIG[DB_PASSWORD]}}"
    
    log_message "INFO" "Testing database connection to $host:$port/$service"
    
    # Security: Create secure temporary file for connection test
    local temp_conn="/tmp/ddl_conn_test_$$.sql"
    local temp_log="/tmp/ddl_conn_log_$$.log"
    
    # Security: Set secure permissions on temp files
    touch "$temp_conn" "$temp_log"
    chmod 600 "$temp_conn" "$temp_log"
    
    # Create connection test SQL
    cat > "$temp_conn" << 'EOF'
SELECT 1 FROM DUAL;
EXIT;
EOF
    
    # Security: Use environment variables to avoid password in process list
    export ORACLE_USERNAME="$username"
    export ORACLE_PASSWORD="$password"
    export ORACLE_CONNECTION="$host:$port/$service"
    
    # Test connection using sqlplus with secure method
    if sqlplus -S "$username/$password@$host:$port/$service" @"$temp_conn" > "$temp_log" 2>&1; then
        log_message "INFO" "Database connection successful"
        # Security: Clear environment variables
        unset ORACLE_USERNAME ORACLE_PASSWORD ORACLE_CONNECTION
        rm -f "$temp_conn" "$temp_log"
        return 0
    else
        log_message "ERROR" "Database connection failed"
        # Security: Clear environment variables
        unset ORACLE_USERNAME ORACLE_PASSWORD ORACLE_CONNECTION
        rm -f "$temp_conn" "$temp_log"
        return 1
    fi
}

# Function to generate execution ID
generate_execution_id() {
    echo "DDL_$(date +%Y%m%d_%H%M%S)_$$"
}

# Function to log execution start
log_execution_start() {
    local exec_id="$1"
    local config_file="$2"
    local script_count="$3"
    
    local start_time
    local start_timestamp
    local execution_record
    
    start_time=$(date -Iseconds)
    start_timestamp=$(date +%s)
    
    # Create execution record
    execution_record=$(cat << EOF
{
    "execution_id": "$exec_id",
    "start_time": "$start_time",
    "start_timestamp": $start_timestamp,
    "config_file": "$config_file",
    "script_count": $script_count,
    "status": "RUNNING",
    "scripts": []
}
EOF
)
    
    echo "$execution_record" > "$HISTORY_DIR/${exec_id}.json"
    echo "$execution_record" > "$TRACKING_DIR/current_execution.json"
}

# Function to log script execution
log_script_execution() {
    local exec_id="$1"
    local script_name="$2"
    local description="$3"
    local start_time="$4"
    local end_time="$5"
    local status="$6"
    local error_code="$7"
    local error_message="$8"
    local duration="$9"
    local order="${10}"
    
    # Create script record
    local script_record
    script_record=$(cat << EOF
{
    "script_name": "$script_name",
    "description": "$description",
    "start_time": "$start_time",
    "end_time": "$end_time",
    "status": "$status",
    "error_code": $error_code,
    "error_message": "$error_message",
    "duration": $duration,
    "execution_order": $order
}
EOF
)
    
    # Update current execution
    if [[ -f "$TRACKING_DIR/current_execution.json" ]]; then
        local current_exec
        local updated_exec
        
        current_exec=$(cat "$TRACKING_DIR/current_execution.json")
        updated_exec=$(echo "$current_exec" | jq --argjson script "$script_record" '.scripts += [$script]')
        echo "$updated_exec" > "$TRACKING_DIR/current_execution.json"
    fi
    
    # Update script metrics
    update_script_metrics "$script_name" "$status" "$duration" "$error_code"
    
    # Update error codes
    if [[ "$status" == "ERROR" && -n "$error_code" ]]; then
        update_error_codes "$error_code" "$error_message"
    fi
}

# Function to update script metrics
update_script_metrics() {
    local script_name="$1"
    local status="$2"
    local duration="$3"
    local error_code="$4"
    
    local metrics_file="$TRACKING_DIR/script_metrics.json"
    local current_metrics
    local script_metrics
    local total_runs
    local successful_runs
    local failed_runs
    local total_duration
    local min_duration
    local max_duration
    
    current_metrics=$(cat "$metrics_file")
    
    # Get or create script metrics
    script_metrics=$(echo "$current_metrics" | jq -r ".$script_name // {}")
    
    # Update metrics
    total_runs=$(echo "$script_metrics" | jq -r '.total_runs // 0')
    successful_runs=$(echo "$script_metrics" | jq -r '.successful_runs // 0')
    failed_runs=$(echo "$script_metrics" | jq -r '.failed_runs // 0')
    total_duration=$(echo "$script_metrics" | jq -r '.total_duration // 0')
    min_duration=$(echo "$script_metrics" | jq -r '.min_duration // null')
    max_duration=$(echo "$script_metrics" | jq -r '.max_duration // null')
    
    # Update counters
    ((total_runs++))
    if [[ "$status" == "SUCCESS" ]]; then
        ((successful_runs++))
    else
        ((failed_runs++))
    fi
    
    # Update duration metrics
    total_duration=$(echo "$total_duration + $duration" | bc)
    if [[ "$min_duration" == "null" || $(echo "$duration < $min_duration" | bc) -eq 1 ]]; then
        min_duration="$duration"
    fi
    if [[ "$max_duration" == "null" || $(echo "$duration > $max_duration" | bc) -eq 1 ]]; then
        max_duration="$duration"
    fi
    
    # Calculate success rate
    local success_rate
    local avg_duration
    
    success_rate=$(echo "scale=2; $successful_runs * 100 / $total_runs" | bc)
    avg_duration=$(echo "scale=2; $total_duration / $total_runs" | bc)
    
    # Create updated metrics
    local updated_metrics
    updated_metrics=$(cat << EOF
{
    "total_runs": $total_runs,
    "successful_runs": $successful_runs,
    "failed_runs": $failed_runs,
    "success_rate": $success_rate,
    "total_duration": $total_duration,
    "avg_duration": $avg_duration,
    "min_duration": $min_duration,
    "max_duration": $max_duration,
    "last_run": "$(date -Iseconds)",
    "last_status": "$status",
    "last_error_code": $error_code
}
EOF
)
    
    # Update the metrics file
    echo "$current_metrics" | jq --arg script "$script_name" --argjson metrics "$updated_metrics" '.[$script] = $metrics' > "$metrics_file"
}

# Function to update error codes
update_error_codes() {
    local error_code="$1"
    local error_message="$2"
    
    local error_codes_file="$TRACKING_DIR/error_codes.json"
    local current_codes
    local error_entry
    
    current_codes=$(cat "$error_codes_file")
    
    # Get or create error code entry
    error_entry=$(echo "$current_codes" | jq -r ".$error_code // {}")
    
    # Update error code metrics
    local count
    local first_seen
    local last_seen
    
    count=$(echo "$error_entry" | jq -r '.count // 0')
    first_seen=$(echo "$error_entry" | jq -r '.first_seen // null')
    last_seen=$(echo "$error_entry" | jq -r '.last_seen // null')
    
    ((count++))
    if [[ "$first_seen" == "null" ]]; then
        first_seen="$(date -Iseconds)"
    fi
    last_seen="$(date -Iseconds)"
    
    # Create updated error entry
    local updated_entry
    updated_entry=$(cat << EOF
{
    "error_code": $error_code,
    "error_message": "$error_message",
    "count": $count,
    "first_seen": "$first_seen",
    "last_seen": "$last_seen"
}
EOF
)
    
    # Update the error codes file
    echo "$current_codes" | jq --arg code "$error_code" --argjson entry "$updated_entry" '.[$code] = $entry' > "$error_codes_file"
}

# Function to log execution completion
log_execution_completion() {
    local exec_id="$1"
    local status="$2"
    local success_count="$3"
    local error_count="$4"
    local total_duration="$5"
    
    local end_time
    local end_timestamp
    
    end_time=$(date -Iseconds)
    end_timestamp=$(date +%s)
    
    # Update execution record
    if [[ -f "$TRACKING_DIR/current_execution.json" ]]; then
        local current_exec
        local updated_exec
        
        current_exec=$(cat "$TRACKING_DIR/current_execution.json")
        updated_exec=$(echo "$current_exec" | jq --arg end_time "$end_time" --arg end_timestamp "$end_timestamp" --arg status "$status" --arg success_count "$success_count" --arg error_count "$error_count" --arg total_duration "$total_duration" '.end_time = $end_time | .end_timestamp = $end_timestamp | .status = $status | .success_count = $success_count | .error_count = $error_count | .total_duration = $total_duration')
        
        echo "$updated_exec" > "$HISTORY_DIR/${exec_id}.json"
        echo "$updated_exec" > "$TRACKING_DIR/current_execution.json"
        
        # Add to execution history
        local history_file
        local history
        local updated_history
        
        history_file="$TRACKING_DIR/execution_history.json"
        history=$(cat "$history_file")
        updated_history=$(echo "$history" | jq --argjson exec "$updated_exec" '. += [$exec]')
        echo "$updated_history" > "$history_file"
    fi
}

# Function to generate comprehensive report
generate_report() {
    local exec_id="$1"
    local report_type="${2:-summary}"
    
    case "$report_type" in
        "summary")
            generate_summary_report "$exec_id"
            ;;
        "detailed")
            generate_detailed_report "$exec_id"
            ;;
        "metrics")
            generate_metrics_report
            ;;
        "history")
            generate_history_report
            ;;
        "errors")
            generate_error_report
            ;;
        "all")
            generate_summary_report "$exec_id"
            generate_metrics_report
            generate_history_report
            generate_error_report
            ;;
    esac
}

# Function to generate summary report
generate_summary_report() {
    local exec_id="$1"
    
    if [[ -f "$HISTORY_DIR/${exec_id}.json" ]]; then
        local exec_data
        exec_data=$(cat "$HISTORY_DIR/${exec_id}.json")
        
        echo "====================================================="
        echo "EXECUTION SUMMARY REPORT"
        echo "====================================================="
        echo "Execution ID: $exec_id"
        echo "Start Time: $(echo "$exec_data" | jq -r '.start_time')"
        echo "End Time: $(echo "$exec_data" | jq -r '.end_time')"
        echo "Status: $(echo "$exec_data" | jq -r '.status')"
        echo "Total Scripts: $(echo "$exec_data" | jq -r '.script_count')"
        echo "Successful: $(echo "$exec_data" | jq -r '.success_count')"
        echo "Failed: $(echo "$exec_data" | jq -r '.error_count')"
        echo "Success Rate: $(echo "$exec_data" | jq -r '.success_count * 100 / .script_count')%"
        echo "Total Duration: $(echo "$exec_data" | jq -r '.total_duration')s"
        echo "====================================================="
    fi
}

# Function to generate metrics report
generate_metrics_report() {
    local metrics_file="$TRACKING_DIR/script_metrics.json"
    
    if [[ -f "$metrics_file" ]]; then
        echo "====================================================="
        echo "SCRIPT METRICS REPORT"
        echo "====================================================="
        
        # Overall statistics
        local total_scripts
        local total_runs
        local total_successful
        local total_failed
        local overall_success_rate
        
        total_scripts=$(jq -r 'keys | length' "$metrics_file")
        total_runs=$(jq -r '.[] | .total_runs' "$metrics_file" | awk '{sum+=$1} END {print sum}')
        total_successful=$(jq -r '.[] | .successful_runs' "$metrics_file" | awk '{sum+=$1} END {print sum}')
        total_failed=$(jq -r '.[] | .failed_runs' "$metrics_file" | awk '{sum+=$1} END {print sum}')
        overall_success_rate=$(echo "scale=2; $total_successful * 100 / $total_runs" | bc)
        
        echo "Overall Statistics:"
        echo "  Total Scripts: $total_scripts"
        echo "  Total Runs: $total_runs"
        echo "  Successful: $total_successful"
        echo "  Failed: $total_failed"
        echo "  Overall Success Rate: ${overall_success_rate}%"
        echo ""
        
        # Per-script statistics
        echo "Per-Script Statistics:"
        jq -r 'to_entries[] | "\(.key): \(.value.success_rate)% success (\(.value.successful_runs)/\(.value.total_runs)) - Avg: \(.value.avg_duration)s"' "$metrics_file" | sort -k2 -nr
        echo "====================================================="
    fi
}

# Function to generate history report
generate_history_report() {
    local history_file="$TRACKING_DIR/execution_history.json"
    
    if [[ -f "$history_file" ]]; then
        echo "====================================================="
        echo "EXECUTION HISTORY REPORT"
        echo "====================================================="
        
        # Recent executions
        echo "Recent Executions:"
        jq -r '.[-5:] | .[] | "\(.execution_id): \(.start_time) - \(.status) (\(.success_count)/\(.script_count))"' "$history_file"
        echo ""
        
        # Success rate over time
        local total_executions
        local successful_executions
        local success_rate
        
        total_executions=$(jq -r 'length' "$history_file")
        successful_executions=$(jq -r '[.[] | select(.status == "SUCCESS")] | length' "$history_file")
        success_rate=$(echo "scale=2; $successful_executions * 100 / $total_executions" | bc)
        
        echo "Historical Success Rate: ${success_rate}% ($successful_executions/$total_executions)"
        echo "====================================================="
    fi
}

# Function to generate error report
generate_error_report() {
    local error_codes_file="$TRACKING_DIR/error_codes.json"
    
    if [[ -f "$error_codes_file" ]]; then
        echo "====================================================="
        echo "ERROR ANALYSIS REPORT"
        echo "====================================================="
        
        # Most common errors
        echo "Most Common Errors:"
        jq -r 'to_entries[] | "\(.key): \(.value.count) occurrences - \(.value.error_message)"' "$error_codes_file" | sort -k2 -nr | head -10
        echo ""
        
        # Error trends
        echo "Error Trends:"
        jq -r 'to_entries[] | "\(.key): First seen \(.value.first_seen), Last seen \(.value.last_seen)"' "$error_codes_file"
        echo "====================================================="
    fi
}

# Enhanced execute_script function with comprehensive tracking
execute_script() {
    local script_name="$1"
    local exec_id="$2"
    local order="$3"
    local script_dir="${CONFIG[execution_script_directory]:-${CONFIG[SCRIPT_DIRECTORY]}}"
    local script_path="$script_dir/$script_name"
    local host="${CONFIG[database_host]:-${CONFIG[DB_HOST]}}"
    local port="${CONFIG[database_port]:-${CONFIG[DB_PORT]}}"
    local service="${CONFIG[database_service]:-${CONFIG[DB_SERVICE]}}"
    local username="${CONFIG[database_username]:-${CONFIG[DB_USERNAME]}}"
    local password="${CONFIG[database_password]:-${CONFIG[DB_PASSWORD]}}"
    local target_schema="${CONFIG[execution_target_schema]:-${CONFIG[TARGET_SCHEMA]}}"
    
    local start_time
    local start_timestamp
    local description
    
    start_time=$(date -Iseconds)
    start_timestamp=$(date +%s)
    description="${SCRIPT_DESCRIPTIONS[$script_name]}"
    
    log_message "INFO" "Executing script: $script_name"
    
    # Show description if available
    if [[ -n "$description" ]]; then
        log_message "INFO" "Description: $description"
    fi
    
    # Security: Validate script path to prevent path traversal
    local script_dir="${CONFIG[execution_script_directory]:-${CONFIG[SCRIPT_DIRECTORY]}}"
    local script_path="$script_dir/$script_name"
    
    # Security: Resolve and validate script path
    script_path=$(realpath "$script_path" 2>/dev/null || echo "$script_path")
    script_dir=$(realpath "$script_dir" 2>/dev/null || echo "$script_dir")
    
    # Security: Ensure script is within allowed directory
    if [[ ! "$script_path" =~ ^"$script_dir"/ ]]; then
        log_message "ERROR" "Script path traversal detected: $script_path"
        return 1
    fi
    
    # Security: Validate script file exists and is readable
    if [[ ! -f "$script_path" ]] || [[ ! -r "$script_path" ]]; then
        log_message "ERROR" "Script file not found or not readable: $script_path"
        return 1
    fi
    
    # Security: Create secure temporary SQL file
    local temp_sql
    temp_sql="/tmp/ddl_exec_$$_$(date +%s).sql"
    
    # Security: Set secure permissions on temp file
    touch "$temp_sql"
    chmod 600 "$temp_sql"
    
    # Security: Validate and sanitize target schema
    if [[ -n "$target_schema" ]] && ! [[ "$target_schema" =~ ^[a-zA-Z][a-zA-Z0-9_]*$ ]]; then
        log_message "ERROR" "Invalid target schema format: $target_schema"
        rm -f "$temp_sql"
        return 1
    fi
    
    cat > "$temp_sql" << EOF
-- Set environment
SET SERVEROUTPUT ON SIZE 1000000
SET PAGESIZE 0
SET LINESIZE 200
SET FEEDBACK OFF
SET VERIFY OFF
SET ECHO ON

-- Set schema (validated)
ALTER SESSION SET CURRENT_SCHEMA = $target_schema;

-- Execute the script (path validated)
@$script_path

-- Exit
EXIT;
EOF
    
    # Execute the script
    local exit_code=0
    local error_message=""
    local error_code=""
    
    # Security: Use environment variables to avoid password in process list
    export ORACLE_USERNAME="$username"
    export ORACLE_PASSWORD="$password"
    export ORACLE_CONNECTION="$host:$port/$service"
    
    if sqlplus -S "$username/$password@$host:$port/$service" @"$temp_sql" 2>&1; then
        exit_code=0
    else
        exit_code=$?
        error_code="$exit_code"
        error_message="SQL*Plus execution failed with exit code $exit_code"
    fi
    
    local end_time
    local end_timestamp
    local duration
    
    end_time=$(date -Iseconds)
    end_timestamp=$(date +%s)
    duration=$((end_timestamp - start_timestamp))
    
    # Determine status
    local status="SUCCESS"
    if [[ $exit_code -ne 0 ]]; then
        status="ERROR"
    fi
    
    # Log script execution
    log_script_execution "$exec_id" "$script_name" "$description" "$start_time" "$end_time" "$status" "$error_code" "$error_message" "$duration" "$order"
    
    # Security: Clear environment variables and cleanup
    unset ORACLE_USERNAME ORACLE_PASSWORD ORACLE_CONNECTION
    rm -f "$temp_sql"
    
    if [[ $exit_code -eq 0 ]]; then
        log_message "INFO" "Script $script_name completed successfully in ${duration}s"
        return 0
    else
        log_message "ERROR" "Script $script_name failed after ${duration}s (exit code: $exit_code)"
        return 1
    fi
}

# Enhanced main function with comprehensive tracking
main() {
    # Initialize tracking
    init_tracking
    
    local config_file=""
    local dry_run=false
    local verbose=false
    local report_type="summary"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--config)
                config_file="$2"
                shift 2
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            -d|--dry-run)
                dry_run=true
                shift
                ;;
            -r|--report)
                report_type="$2"
                shift 2
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Load configuration
    if ! load_configuration "$config_file"; then
        log_message "ERROR" "Failed to load configuration"
        exit 1
    fi
    
    # Generate execution ID
    local exec_id
    exec_id=$(generate_execution_id)
    
    # Log execution start
    log_execution_start "$exec_id" "$config_file" "${#SCRIPT_ORDER[@]}"
    
    # Override verbose setting if specified
    if [[ "$verbose" == "true" ]]; then
        CONFIG[logging_level]="DEBUG"
        CONFIG[LOG_LEVEL]="DEBUG"
    fi
    
    # Validate configuration
    if ! validate_config; then
        log_message "ERROR" "Configuration validation failed"
        exit 1
    fi
    
    # Show configuration
    show_config
    
    # Test database connection (skip in dry run mode)
    if [[ "$dry_run" != "true" ]]; then
        if ! test_connection; then
            log_message "ERROR" "Cannot proceed without database connection"
            exit 1
        fi
    else
        log_message "INFO" "Skipping database connection test in dry run mode"
    fi
    
    # Dry run mode
    if [[ "$dry_run" == "true" ]]; then
        log_message "INFO" "Dry run mode - no scripts will be executed"
        exit 0
    fi
    
    # Confirm execution
    echo ""
    read -p "Proceed with DDL execution? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_message "INFO" "Execution cancelled by user"
        exit 0
    fi
    
    # Execute scripts
    local success_count=0
    local error_count=0
    local total_count=${#SCRIPT_ORDER[@]}
    local total_start_time
    total_start_time=$(date +%s)
    
    log_message "INFO" "Starting execution of $total_count scripts"
    
    for i in "${!SCRIPT_ORDER[@]}"; do
        local script="${SCRIPT_ORDER[$i]}"
        local order
        order=$((i + 1))
        
        echo ""
        log_message "INFO" "Executing: $script ($order/$total_count)"
        
        if execute_script "$script" "$exec_id" "$order"; then
            ((success_count++))
        else
            ((error_count++))
            local continue_on_error="${CONFIG[execution_continue_on_error]:-${CONFIG[CONTINUE_ON_ERROR]}}"
            if [[ "$continue_on_error" != "true" ]]; then
                log_message "ERROR" "Stopping execution due to error"
                break
            fi
        fi
        
        # Wait for user input if configured
        local wait_between="${CONFIG[execution_wait_between_scripts]:-${CONFIG[WAIT_BETWEEN_SCRIPTS]}}"
        if [[ "$wait_between" == "true" && "$script" != "${SCRIPT_ORDER[-1]}" ]]; then
            echo ""
            read -r -p "Press Enter to continue to next script..."
        fi
    done
    
    # Calculate total duration
    local total_end_time
    local total_duration
    
    total_end_time=$(date +%s)
    total_duration=$((total_end_time - total_start_time))
    
    # Determine overall status
    local overall_status="SUCCESS"
    if [[ $error_count -gt 0 ]]; then
        overall_status="ERROR"
    fi
    
    # Log execution completion
    log_execution_completion "$exec_id" "$overall_status" "$success_count" "$error_count" "$total_duration"
    
    # Generate reports
    generate_report "$exec_id" "$report_type"
    
    # Summary
    echo ""
    log_message "INFO" "Execution Summary:"
    log_message "INFO" "  Total Scripts: $total_count"
    log_message "INFO" "  Successful: $success_count"
    log_message "INFO" "  Failed: $error_count"
    log_message "INFO" "  Success Rate: $(( (success_count * 100) / total_count ))%"
    log_message "INFO" "  Total Duration: ${total_duration}s"
    
    if [[ $error_count -gt 0 ]]; then
        local log_file="${CONFIG[execution_log_file]:-${CONFIG[LOG_FILE]}}"
        log_message "WARN" "Some scripts failed - check log file: $log_file"
        exit 1
    else
        log_message "INFO" "All scripts executed successfully"
        exit 0
    fi
}

# Run main function
main "$@"