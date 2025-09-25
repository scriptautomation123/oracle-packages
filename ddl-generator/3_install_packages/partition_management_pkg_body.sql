-- =====================================================
-- Oracle Partition Management Package Body
-- Implementation with autonomous logging and data movement
-- =====================================================

CREATE OR REPLACE PACKAGE BODY partition_management_pkg
AS
    -- Private variables
    g_logger_enabled BOOLEAN := TRUE;
    
    -- Private procedure for logging using centralized logger
    PROCEDURE log_operation(
        p_operation       IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_status          IN VARCHAR2,
        p_message         IN VARCHAR2 DEFAULT NULL,
        p_duration_ms     IN NUMBER DEFAULT NULL
    ) IS
    BEGIN
        partition_logger_pkg.log_operation(
            p_operation_type => p_operation,
            p_table_name => p_table_name,
            p_partition_name => p_partition_name,
            p_status => p_status,
            p_message => p_message,
            p_duration_ms => p_duration_ms
        );
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for logging
            NULL;
    END log_operation;
    
    -- Private function to validate table exists and is partitioned
    FUNCTION validate_partitioned_table(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        RETURN partition_utils_pkg.is_partitioned(p_table_name);
    END validate_partitioned_table;
    
    -- Private function to get partition type
    FUNCTION get_partition_type(
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2 IS
    BEGIN
        RETURN partition_utils_pkg.get_partition_type(p_table_name);
    END get_partition_type;
    
    -- Security validation functions
    FUNCTION validate_table_name(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        -- Validate table name format and prevent SQL injection
        IF p_table_name IS NULL OR LENGTH(TRIM(p_table_name)) = 0 THEN
            RETURN FALSE;
        END IF;
        
        -- Check for valid Oracle identifier pattern
        IF NOT REGEXP_LIKE(UPPER(TRIM(p_table_name)), '^[A-Z][A-Z0-9_]{0,29}$') THEN
            RETURN FALSE;
        END IF;
        
        -- Check for reserved words and SQL injection patterns
        IF UPPER(TRIM(p_table_name)) IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'EXECUTE', 'UNION', 'OR', 'AND') THEN
            RETURN FALSE;
        END IF;
        
        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END validate_table_name;
    
    FUNCTION validate_partition_name(
        p_partition_name IN VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        -- Validate partition name format
        IF p_partition_name IS NULL OR LENGTH(TRIM(p_partition_name)) = 0 THEN
            RETURN FALSE;
        END IF;
        
        -- Check for valid Oracle identifier pattern
        IF NOT REGEXP_LIKE(UPPER(TRIM(p_partition_name)), '^[A-Z][A-Z0-9_]{0,29}$') THEN
            RETURN FALSE;
        END IF;
        
        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END validate_partition_name;
    
    FUNCTION sanitize_where_clause(
        p_where_clause IN VARCHAR2
    ) RETURN VARCHAR2 IS
        v_sanitized VARCHAR2(4000);
    BEGIN
        IF p_where_clause IS NULL THEN
            RETURN NULL;
        END IF;
        
        v_sanitized := TRIM(p_where_clause);
        
        -- Remove dangerous SQL patterns
        IF REGEXP_LIKE(UPPER(v_sanitized), '(DROP|DELETE|UPDATE|INSERT|EXECUTE|UNION|OR\s+1\s*=\s*1)', 'i') THEN
            RAISE_APPLICATION_ERROR(-20005, 'Potentially dangerous SQL pattern detected in WHERE clause');
        END IF;
        
        -- Limit length to prevent buffer overflow attacks
        IF LENGTH(v_sanitized) > 2000 THEN
            RAISE_APPLICATION_ERROR(-20006, 'WHERE clause too long');
        END IF;
        
        RETURN v_sanitized;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20007, 'Invalid WHERE clause: ' || SQLERRM);
    END sanitize_where_clause;
    
    FUNCTION optimize_batch_size(
        p_table_name IN VARCHAR2,
        p_requested_size IN NUMBER
    ) RETURN NUMBER IS
        v_table_size_mb NUMBER;
        v_optimized_size NUMBER;
    BEGIN
        -- Get table size to optimize batch size
        BEGIN
            SELECT ROUND(SUM(bytes) / 1024 / 1024, 2)
            INTO v_table_size_mb
            FROM user_segments
            WHERE segment_name = UPPER(p_table_name);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_table_size_mb := 0;
        END;
        
        -- Optimize batch size based on table size and system resources
        IF v_table_size_mb > 10000 THEN  -- Large table
            v_optimized_size := LEAST(p_requested_size * 2, 50000);
        ELSIF v_table_size_mb > 1000 THEN  -- Medium table
            v_optimized_size := LEAST(p_requested_size, 25000);
        ELSE  -- Small table
            v_optimized_size := LEAST(p_requested_size, 10000);
        END IF;
        
        -- Ensure minimum viable batch size
        RETURN GREATEST(v_optimized_size, 1000);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN p_requested_size;  -- Fallback to requested size
    END optimize_batch_size;
    
    -- Core partition management procedures
    PROCEDURE create_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_high_value      IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
        v_partition_type VARCHAR2(20);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('CREATE_PARTITION', p_table_name, p_partition_name, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        v_partition_type := get_partition_type(p_table_name);
        
        -- Enhanced input validation
        IF NOT validate_table_name(p_table_name) THEN
            RAISE_APPLICATION_ERROR(-20018, 'Invalid table name: ' || p_table_name);
        END IF;
        
        IF NOT validate_partition_name(p_partition_name) THEN
            RAISE_APPLICATION_ERROR(-20019, 'Invalid partition name: ' || p_partition_name);
        END IF;
        
        -- Build SQL based on partition type with enhanced online support
        v_sql := 'ALTER TABLE ' || p_table_name || ' ADD PARTITION ' || p_partition_name;
        
        IF v_partition_type = 'RANGE' THEN
            v_sql := v_sql || ' VALUES LESS THAN (' || p_high_value || ')';
        ELSIF v_partition_type = 'LIST' THEN
            v_sql := v_sql || ' VALUES (' || p_high_value || ')';
        END IF;
        
        IF p_tablespace IS NOT NULL THEN
            v_sql := v_sql || ' TABLESPACE ' || p_tablespace;
        END IF;
        
        -- Always use online operations for better availability
        v_sql := v_sql || ' ONLINE';
        
        -- Add parallel processing for large operations
        IF v_partition_type IN ('HASH', 'RANGE') THEN
            v_sql := v_sql || ' PARALLEL';
        END IF;
        
        -- Execute the DDL
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('CREATE_PARTITION', p_table_name, p_partition_name, 'SUCCESS', 
                     'Partition created successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('CREATE_PARTITION', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END create_partition;
    
    PROCEDURE drop_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_update_indexes  IN BOOLEAN DEFAULT TRUE,
        p_online          IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('DROP_PARTITION', p_table_name, p_partition_name, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        -- Build SQL
        v_sql := 'ALTER TABLE ' || p_table_name || ' DROP PARTITION ' || p_partition_name;
        
        IF p_update_indexes THEN
            v_sql := v_sql || ' UPDATE INDEXES';
        END IF;
        
        IF p_online THEN
            v_sql := v_sql || ' ONLINE';
        END IF;
        
        -- Execute the DDL
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('DROP_PARTITION', p_table_name, p_partition_name, 'SUCCESS', 
                     'Partition dropped successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('DROP_PARTITION', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END drop_partition;
    
    PROCEDURE split_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_split_value     IN VARCHAR2,
        p_new_partition1  IN VARCHAR2,
        p_new_partition2  IN VARCHAR2,
        p_tablespace1     IN VARCHAR2 DEFAULT NULL,
        p_tablespace2     IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
        v_partition_type VARCHAR2(20);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('SPLIT_PARTITION', p_table_name, p_partition_name, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        v_partition_type := get_partition_type(p_table_name);
        
        -- Build SQL based on partition type
        v_sql := 'ALTER TABLE ' || p_table_name || ' SPLIT PARTITION ' || p_partition_name;
        
        IF v_partition_type = 'RANGE' THEN
            v_sql := v_sql || ' AT (' || p_split_value || ') INTO (';
        ELSIF v_partition_type = 'LIST' THEN
            v_sql := v_sql || ' VALUES (' || p_split_value || ') INTO (';
        END IF;
        
        v_sql := v_sql || 'PARTITION ' || p_new_partition1;
        IF p_tablespace1 IS NOT NULL THEN
            v_sql := v_sql || ' TABLESPACE ' || p_tablespace1;
        END IF;
        
        v_sql := v_sql || ', PARTITION ' || p_new_partition2;
        IF p_tablespace2 IS NOT NULL THEN
            v_sql := v_sql || ' TABLESPACE ' || p_tablespace2;
        END IF;
        
        v_sql := v_sql || ')';
        
        IF p_online THEN
            v_sql := v_sql || ' ONLINE';
        END IF;
        
        -- Execute the DDL
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('SPLIT_PARTITION', p_table_name, p_partition_name, 'SUCCESS', 
                     'Partition split successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('SPLIT_PARTITION', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END split_partition;
    
    PROCEDURE merge_partitions(
        p_table_name      IN VARCHAR2,
        p_partition1      IN VARCHAR2,
        p_partition2      IN VARCHAR2,
        p_new_partition   IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('MERGE_PARTITIONS', p_table_name, p_partition1, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        -- Build SQL
        v_sql := 'ALTER TABLE ' || p_table_name || ' MERGE PARTITIONS ' || 
                 p_partition1 || ', ' || p_partition2 || ' INTO ' || p_new_partition;
        
        IF p_tablespace IS NOT NULL THEN
            v_sql := v_sql || ' TABLESPACE ' || p_tablespace;
        END IF;
        
        IF p_online THEN
            v_sql := v_sql || ' ONLINE';
        END IF;
        
        -- Execute the DDL
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('MERGE_PARTITIONS', p_table_name, p_new_partition, 'SUCCESS', 
                     'Partitions merged successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('MERGE_PARTITIONS', p_table_name, p_new_partition, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END merge_partitions;
    
    PROCEDURE move_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_tablespace      IN VARCHAR2,
        p_online          IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('MOVE_PARTITION', p_table_name, p_partition_name, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        -- Build SQL
        v_sql := 'ALTER TABLE ' || p_table_name || ' MOVE PARTITION ' || 
                 p_partition_name || ' TABLESPACE ' || p_tablespace;
        
        IF p_online THEN
            v_sql := v_sql || ' ONLINE';
        END IF;
        
        -- Execute the DDL
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('MOVE_PARTITION', p_table_name, p_partition_name, 'SUCCESS', 
                     'Partition moved successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('MOVE_PARTITION', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END move_partition;
    
    PROCEDURE truncate_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_update_indexes  IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('TRUNCATE_PARTITION', p_table_name, p_partition_name, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        -- Build SQL
        v_sql := 'ALTER TABLE ' || p_table_name || ' TRUNCATE PARTITION ' || p_partition_name;
        
        IF p_update_indexes THEN
            v_sql := v_sql || ' UPDATE INDEXES';
        END IF;
        
        -- Execute the DDL
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('TRUNCATE_PARTITION', p_table_name, p_partition_name, 'SUCCESS', 
                     'Partition truncated successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('TRUNCATE_PARTITION', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END truncate_partition;
    
    -- Data movement procedures
    PROCEDURE move_data_to_partition(
        p_source_table    IN VARCHAR2,
        p_target_table    IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_where_clause    IN VARCHAR2 DEFAULT NULL,
        p_batch_size      IN NUMBER DEFAULT 10000
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_rows_processed NUMBER := 0;
        v_sql VARCHAR2(4000);
        v_where_clause VARCHAR2(4000);
        v_parallel_degree NUMBER := 4;
        v_bulk_errors EXCEPTION;
        PRAGMA EXCEPTION_INIT(v_bulk_errors, -24381);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Input validation and security checks
        IF NOT validate_table_name(p_source_table) OR NOT validate_table_name(p_target_table) THEN
            RAISE_APPLICATION_ERROR(-20003, 'Invalid table name provided');
        END IF;
        
        IF NOT validate_partition_name(p_partition_name) THEN
            RAISE_APPLICATION_ERROR(-20004, 'Invalid partition name provided');
        END IF;
        
        -- Build secure WHERE clause with proper escaping
        v_where_clause := NVL(sanitize_where_clause(p_where_clause), '1=1');
        
        -- Optimize batch size based on table size and system resources
        v_batch_size := optimize_batch_size(p_source_table, p_batch_size);
        
        -- Process data in optimized batches with parallel processing
        LOOP
            v_sql := 'INSERT /*+ APPEND PARALLEL(' || v_parallel_degree || ') */ INTO ' || 
                     p_target_table || ' PARTITION(' || p_partition_name || ') ' ||
                     'SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || p_source_table || 
                     ' WHERE ' || v_where_clause || ' AND ROWNUM <= ' || v_batch_size;
            
            BEGIN
                EXECUTE IMMEDIATE v_sql;
                v_rows_processed := v_rows_processed + SQL%ROWCOUNT;
                
                -- Exit if no more rows
                EXIT WHEN SQL%ROWCOUNT = 0;
                
                -- Update WHERE clause to exclude processed rows efficiently
                v_where_clause := v_where_clause || ' AND ROWID NOT IN (SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ ROWID FROM ' || 
                                 p_target_table || ' PARTITION(' || p_partition_name || '))';
                
                -- Commit in batches to manage undo space
                IF MOD(v_rows_processed, v_batch_size * 10) = 0 THEN
                    COMMIT;
                END IF;
                
            EXCEPTION
                WHEN v_bulk_errors THEN
                    -- Handle bulk operation errors gracefully
                    log_operation('MOVE_DATA', p_source_table, p_partition_name, 'WARNING', 
                                 'Bulk operation completed with errors: ' || SQLERRM);
                    EXIT;
                WHEN OTHERS THEN
                    -- Log error and re-raise
                    log_operation('MOVE_DATA', p_source_table, p_partition_name, 'ERROR', SQLERRM);
                    RAISE;
            END;
        END LOOP;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('MOVE_DATA', p_source_table, p_partition_name, 'SUCCESS', 
                     'Moved ' || v_rows_processed || ' rows', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('MOVE_DATA', p_source_table, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END move_data_to_partition;
    
    PROCEDURE exchange_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_exchange_table  IN VARCHAR2,
        p_validation      IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('EXCHANGE_PARTITION', p_table_name, p_partition_name, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        -- Build SQL
        v_sql := 'ALTER TABLE ' || p_table_name || ' EXCHANGE PARTITION ' || 
                 p_partition_name || ' WITH TABLE ' || p_exchange_table;
        
        IF p_validation THEN
            v_sql := v_sql || ' WITH VALIDATION';
        END IF;
        
        -- Execute the DDL
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('EXCHANGE_PARTITION', p_table_name, p_partition_name, 'SUCCESS', 
                     'Partition exchanged successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('EXCHANGE_PARTITION', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END exchange_partition;
    
    -- Strategy management procedures
    PROCEDURE change_partition_strategy(
        p_table_name      IN VARCHAR2,
        p_new_strategy    IN strategy_config_rec,
        p_migrate_data    IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_temp_table VARCHAR2(128);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('CHANGE_STRATEGY', p_table_name, NULL, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        -- Create temporary table for data migration
        v_temp_table := p_table_name || '_TEMP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
        
        -- Create temporary table with same structure
        EXECUTE IMMEDIATE 'CREATE TABLE ' || v_temp_table || ' AS SELECT * FROM ' || p_table_name || ' WHERE 1=0';
        
        -- Migrate data if requested
        IF p_migrate_data THEN
            EXECUTE IMMEDIATE 'INSERT INTO ' || v_temp_table || ' SELECT * FROM ' || p_table_name;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Recreate table with new partitioning strategy
        -- This would need to be customized based on the specific strategy
        -- For now, we'll create a basic range partition
        EXECUTE IMMEDIATE 'CREATE TABLE ' || p_table_name || ' PARTITION BY RANGE (' || 
                         p_new_strategy.partition_column || ') (' ||
                         'PARTITION p_default VALUES LESS THAN (MAXVALUE)' ||
                         ') AS SELECT * FROM ' || v_temp_table || ' WHERE 1=0';
        
        -- Migrate data back if requested
        IF p_migrate_data THEN
            EXECUTE IMMEDIATE 'INSERT INTO ' || p_table_name || ' SELECT * FROM ' || v_temp_table;
        END IF;
        
        -- Drop temporary table
        EXECUTE IMMEDIATE 'DROP TABLE ' || v_temp_table;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('CHANGE_STRATEGY', p_table_name, NULL, 'SUCCESS', 
                     'Partition strategy changed successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('CHANGE_STRATEGY', p_table_name, NULL, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END change_partition_strategy;
    
    PROCEDURE convert_to_interval_partitioning(
        p_table_name      IN VARCHAR2,
        p_interval_value  IN VARCHAR2,
        p_column_name     IN VARCHAR2,
        p_online          IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Validate input
        IF NOT validate_partitioned_table(p_table_name) THEN
            log_operation('CONVERT_INTERVAL', p_table_name, NULL, 'ERROR', 'Table is not partitioned');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' is not partitioned');
        END IF;
        
        -- Convert to interval partitioning
        v_sql := 'ALTER TABLE ' || p_table_name || ' SET INTERVAL (' || p_interval_value || ')';
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('CONVERT_INTERVAL', p_table_name, NULL, 'SUCCESS', 
                     'Converted to interval partitioning', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('CONVERT_INTERVAL', p_table_name, NULL, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END convert_to_interval_partitioning;
    
    -- Maintenance procedures
    PROCEDURE maintain_old_partitions(
        p_table_name      IN VARCHAR2,
        p_retention_days  IN NUMBER,
        p_action          IN VARCHAR2 DEFAULT 'DROP'
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_cutoff_date DATE;
        v_partition_name VARCHAR2(128);
        v_sql VARCHAR2(4000);
        v_partitions_processed NUMBER := 0;
    BEGIN
        v_start_time := SYSTIMESTAMP;
        v_cutoff_date := SYSDATE - p_retention_days;
        
        -- Find old partitions based on high value
        FOR rec IN (
            SELECT partition_name, high_value
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            AND high_value IS NOT NULL
            AND TO_DATE(SUBSTR(high_value, 1, 10), 'YYYY-MM-DD') < v_cutoff_date
        ) LOOP
            CASE UPPER(p_action)
                WHEN 'DROP' THEN
                    drop_partition(p_table_name, rec.partition_name);
                WHEN 'ARCHIVE' THEN
                    -- Archive logic would go here
                    NULL;
                WHEN 'COMPRESS' THEN
                    v_sql := 'ALTER TABLE ' || p_table_name || ' MODIFY PARTITION ' || 
                             rec.partition_name || ' COMPRESS';
                    EXECUTE IMMEDIATE v_sql;
            END CASE;
            
            v_partitions_processed := v_partitions_processed + 1;
        END LOOP;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('MAINTAIN_OLD', p_table_name, NULL, 'SUCCESS', 
                     'Processed ' || v_partitions_processed || ' partitions', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('MAINTAIN_OLD', p_table_name, NULL, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END maintain_old_partitions;
    
    PROCEDURE rebuild_partition_indexes(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_online          IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
        v_indexes_processed NUMBER := 0;
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Rebuild indexes for specified partition or all partitions
        FOR rec IN (
            SELECT index_name, partition_name
            FROM user_ind_partitions
            WHERE index_name IN (
                SELECT index_name 
                FROM user_indexes 
                WHERE table_name = UPPER(p_table_name)
            )
            AND (p_partition_name IS NULL OR partition_name = UPPER(p_partition_name))
        ) LOOP
            v_sql := 'ALTER INDEX ' || rec.index_name || ' REBUILD PARTITION ' || rec.partition_name;
            
            IF p_online THEN
                v_sql := v_sql || ' ONLINE';
            END IF;
            
            EXECUTE IMMEDIATE v_sql;
            v_indexes_processed := v_indexes_processed + 1;
        END LOOP;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('REBUILD_INDEXES', p_table_name, p_partition_name, 'SUCCESS', 
                     'Rebuilt ' || v_indexes_processed || ' indexes', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('REBUILD_INDEXES', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END rebuild_partition_indexes;
    
    -- Analysis and monitoring functions
    FUNCTION get_partition_info(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL
    ) RETURN partition_info_tab PIPELINED IS
    BEGIN
        FOR rec IN (
            SELECT 
                t.table_name,
                p.partition_name,
                pt.partition_type,
                p.high_value,
                p.tablespace_name,
                p.num_rows,
                p.blocks,
                p.last_analyzed
            FROM user_tables t
            JOIN user_part_tables pt ON t.table_name = pt.table_name
            JOIN user_tab_partitions p ON t.table_name = p.table_name
            WHERE t.table_name = UPPER(p_table_name)
            AND (p_partition_name IS NULL OR p.partition_name = UPPER(p_partition_name))
        ) LOOP
            PIPE ROW(partition_info_rec(
                rec.table_name,
                rec.partition_name,
                rec.partition_type,
                rec.high_value,
                rec.tablespace_name,
                rec.num_rows,
                rec.blocks,
                rec.last_analyzed
            ));
        END LOOP;
        
        RETURN;
    END get_partition_info;
    
    FUNCTION analyze_partition_usage(
        p_table_name      IN VARCHAR2,
        p_days_back       IN NUMBER DEFAULT 30
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                partition_name,
                num_rows,
                blocks,
                last_analyzed,
                CASE 
                    WHEN last_analyzed IS NULL THEN 'NEVER_ANALYZED'
                    WHEN last_analyzed < SYSDATE - p_days_back THEN 'STALE_STATS'
                    ELSE 'CURRENT'
                END as stats_status
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            ORDER BY partition_name;
            
        RETURN v_cursor;
    END analyze_partition_usage;
    
    FUNCTION get_partition_size_info(
        p_table_name      IN VARCHAR2
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                partition_name,
                tablespace_name,
                num_rows,
                blocks,
                ROUND(blocks * 8192 / 1024 / 1024, 2) as size_mb,
                last_analyzed
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            ORDER BY blocks DESC;
            
        RETURN v_cursor;
    END get_partition_size_info;
    
    -- Utility procedures
    PROCEDURE validate_partition_operation(
        p_table_name      IN VARCHAR2,
        p_operation       IN VARCHAR2,
        p_parameters      IN VARCHAR2
    ) IS
        v_valid BOOLEAN := TRUE;
        v_message VARCHAR2(4000);
    BEGIN
        -- Basic validation
        IF NOT validate_partitioned_table(p_table_name) THEN
            v_valid := FALSE;
            v_message := 'Table is not partitioned';
        END IF;
        
        -- Additional validations based on operation
        CASE UPPER(p_operation)
            WHEN 'CREATE_PARTITION' THEN
                -- Validate partition name format
                IF LENGTH(p_parameters) > 128 THEN
                    v_valid := FALSE;
                    v_message := 'Partition name too long';
                END IF;
            WHEN 'DROP_PARTITION' THEN
                -- Check if partition exists
                SELECT COUNT(*) INTO v_valid
                FROM user_tab_partitions
                WHERE table_name = UPPER(p_table_name)
                AND partition_name = UPPER(p_parameters);
                
                IF v_valid = 0 THEN
                    v_valid := FALSE;
                    v_message := 'Partition does not exist';
                END IF;
        END CASE;
        
        IF NOT v_valid THEN
            log_operation('VALIDATE', p_table_name, p_parameters, 'ERROR', v_message);
            RAISE_APPLICATION_ERROR(-20002, v_message);
        END IF;
        
        log_operation('VALIDATE', p_table_name, p_parameters, 'SUCCESS', 'Validation passed');
    END validate_partition_operation;
    
    PROCEDURE generate_partition_ddl(
        p_table_name      IN VARCHAR2,
        p_operation       IN VARCHAR2,
        p_parameters      IN VARCHAR2
    ) IS
        v_ddl VARCHAR2(4000);
    BEGIN
        -- This would generate DDL based on the operation and parameters
        -- For now, just log the request
        log_operation('GENERATE_DDL', p_table_name, p_parameters, 'INFO', 
                     'DDL generation requested for ' || p_operation);
    END generate_partition_ddl;
    
    -- Configuration procedures
    PROCEDURE set_partition_strategy(
        p_table_name      IN VARCHAR2,
        p_config          IN strategy_config_rec
    ) IS
    BEGIN
        -- Store configuration in a configuration table
        -- This would be implemented based on your specific needs
        log_operation('SET_STRATEGY', p_table_name, NULL, 'SUCCESS', 
                     'Partition strategy configured');
    END set_partition_strategy;
    
    FUNCTION get_partition_strategy(
        p_table_name      IN VARCHAR2
    ) RETURN strategy_config_rec IS
        v_config strategy_config_rec;
    BEGIN
        -- Retrieve configuration from configuration table
        -- This would be implemented based on your specific needs
        v_config.table_name := p_table_name;
        v_config.partition_type := 'RANGE';
        v_config.partition_column := 'CREATED_DATE';
        v_config.interval_value := '1';
        v_config.tablespace_prefix := 'DATA';
        v_config.retention_days := 90;
        v_config.auto_maintenance := TRUE;
        
        RETURN v_config;
    END get_partition_strategy;
    
    -- Partition conversion procedures (delegated to partition_strategy_pkg)
    PROCEDURE convert_table_to_partitioned(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_partition_count   IN NUMBER DEFAULT NULL,
        p_parent_table      IN VARCHAR2 DEFAULT NULL,
        p_foreign_key       IN VARCHAR2 DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        log_operation('CONVERT_TO_PARTITIONED', p_table_name, NULL, 'STARTED', 
                     'Delegating to partition_strategy_pkg');
        
        partition_strategy_pkg.convert_to_partitioned(
            p_table_name, p_partition_type, p_partition_column, 
            p_partition_definitions, p_interval_value, p_partition_count,
            p_parent_table, p_foreign_key, p_preserve_data, p_online
        );
        
        log_operation('CONVERT_TO_PARTITIONED', p_table_name, NULL, 'SUCCESS', 
                     'Conversion delegated successfully');
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('CONVERT_TO_PARTITIONED', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END convert_table_to_partitioned;
    
    PROCEDURE convert_table_to_partitioned_with_subpartitions(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_subpartition_type IN VARCHAR2,
        p_subpartition_column IN VARCHAR2,
        p_partition_definitions IN VARCHAR2 DEFAULT NULL,
        p_subpartition_definitions IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_partition_count   IN NUMBER DEFAULT NULL,
        p_subpartition_count IN NUMBER DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        log_operation('CONVERT_TO_PARTITIONED_SUBPART', p_table_name, NULL, 'STARTED', 
                     'Delegating to partition_strategy_pkg');
        
        partition_strategy_pkg.convert_to_partitioned_with_subpartitions(
            p_table_name, p_partition_type, p_partition_column,
            p_subpartition_type, p_subpartition_column, p_partition_definitions,
            p_subpartition_definitions, p_interval_value, p_partition_count,
            p_subpartition_count, p_preserve_data, p_online
        );
        
        log_operation('CONVERT_TO_PARTITIONED_SUBPART', p_table_name, NULL, 'SUCCESS', 
                     'Conversion delegated successfully');
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('CONVERT_TO_PARTITIONED_SUBPART', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END convert_table_to_partitioned_with_subpartitions;
    
    PROCEDURE convert_partitioned_to_non_partitioned(
        p_table_name        IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        log_operation('CONVERT_TO_NON_PARTITIONED', p_table_name, NULL, 'STARTED', 
                     'Delegating to partition_strategy_pkg');
        
        partition_strategy_pkg.convert_to_non_partitioned(
            p_table_name, p_preserve_data, p_online
        );
        
        log_operation('CONVERT_TO_NON_PARTITIONED', p_table_name, NULL, 'SUCCESS', 
                     'Conversion delegated successfully');
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('CONVERT_TO_NON_PARTITIONED', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END convert_partitioned_to_non_partitioned;
    
    PROCEDURE add_subpartitioning_to_table(
        p_table_name        IN VARCHAR2,
        p_subpartition_type IN VARCHAR2,
        p_subpartition_column IN VARCHAR2,
        p_subpartition_definitions IN VARCHAR2 DEFAULT NULL,
        p_subpartition_count IN NUMBER DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        log_operation('ADD_SUBPARTITIONING', p_table_name, NULL, 'STARTED', 
                     'Delegating to partition_strategy_pkg');
        
        partition_strategy_pkg.add_subpartitioning(
            p_table_name, p_subpartition_type, p_subpartition_column, 
            p_subpartition_definitions, p_subpartition_count, p_preserve_data, p_online
        );
        
        log_operation('ADD_SUBPARTITIONING', p_table_name, NULL, 'SUCCESS', 
                     'Subpartitioning addition delegated successfully');
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('ADD_SUBPARTITIONING', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END add_subpartitioning_to_table;
    
    -- Oracle 19c specific online operations
    PROCEDURE online_reorganize_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2 DEFAULT NULL,
        p_compress        IN BOOLEAN DEFAULT FALSE
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        log_operation('ONLINE_REORGANIZE', p_table_name, p_partition_name, 'STARTED', 
                     'Starting online partition reorganization');
        
        -- Build SQL for online partition reorganization
        v_sql := 'ALTER TABLE ' || p_table_name || ' MOVE PARTITION ' || p_partition_name || ' ONLINE';
        
        IF p_new_tablespace IS NOT NULL THEN
            v_sql := v_sql || ' TABLESPACE ' || p_new_tablespace;
        END IF;
        
        IF p_compress THEN
            v_sql := v_sql || ' COMPRESS';
        END IF;
        
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('ONLINE_REORGANIZE', p_table_name, p_partition_name, 'SUCCESS', 
                     'Online partition reorganization completed successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('ONLINE_REORGANIZE', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END online_reorganize_partition;
    
    PROCEDURE online_split_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_split_value     IN VARCHAR2,
        p_new_partition1  IN VARCHAR2,
        p_new_partition2  IN VARCHAR2,
        p_tablespace1     IN VARCHAR2 DEFAULT NULL,
        p_tablespace2     IN VARCHAR2 DEFAULT NULL
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        log_operation('ONLINE_SPLIT', p_table_name, p_partition_name, 'STARTED', 
                     'Starting online partition split');
        
        -- Use the existing split_partition procedure with online=true
        split_partition(p_table_name, p_partition_name, p_split_value, 
                       p_new_partition1, p_new_partition2, p_tablespace1, p_tablespace2, TRUE);
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('ONLINE_SPLIT', p_table_name, p_partition_name, 'SUCCESS', 
                     'Online partition split completed successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('ONLINE_SPLIT', p_table_name, p_partition_name, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END online_split_partition;
    
    PROCEDURE online_merge_partitions(
        p_table_name      IN VARCHAR2,
        p_partition1      IN VARCHAR2,
        p_partition2      IN VARCHAR2,
        p_new_partition   IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        log_operation('ONLINE_MERGE', p_table_name, p_new_partition, 'STARTED', 
                     'Starting online partition merge');
        
        -- Use the existing merge_partitions procedure with online=true
        merge_partitions(p_table_name, p_partition1, p_partition2, p_new_partition, p_tablespace, TRUE);
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('ONLINE_MERGE', p_table_name, p_new_partition, 'SUCCESS', 
                     'Online partition merge completed successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('ONLINE_MERGE', p_table_name, p_new_partition, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END online_merge_partitions;
    
    PROCEDURE online_coalesce_partition(
        p_table_name      IN VARCHAR2
    ) IS
        v_start_time TIMESTAMP;
        v_duration_ms NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        log_operation('ONLINE_COALESCE', p_table_name, NULL, 'STARTED', 
                     'Starting online partition coalesce');
        
        -- Build SQL for online partition coalesce
        v_sql := 'ALTER TABLE ' || p_table_name || ' COALESCE PARTITION ONLINE';
        
        EXECUTE IMMEDIATE v_sql;
        
        v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                        EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
        
        log_operation('ONLINE_COALESCE', p_table_name, NULL, 'SUCCESS', 
                     'Online partition coalesce completed successfully', v_duration_ms);
                     
    EXCEPTION
        WHEN OTHERS THEN
            v_duration_ms := EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 24 * 60 * 60 * 1000 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 60 * 60 * 1000 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60 * 1000 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;
            
            log_operation('ONLINE_COALESCE', p_table_name, NULL, 'ERROR', 
                         SQLERRM, v_duration_ms);
            RAISE;
    END online_coalesce_partition;
    
    -- Resource management and monitoring procedures
    PROCEDURE monitor_operation_resources(
        p_operation_type IN VARCHAR2,
        p_table_name IN VARCHAR2
    ) IS
        v_undo_usage NUMBER;
        v_temp_usage NUMBER;
        v_session_id NUMBER;
    BEGIN
        -- Get current session information
        SELECT SYS_CONTEXT('USERENV', 'SID') INTO v_session_id FROM DUAL;
        
        -- Monitor undo usage
        BEGIN
            SELECT ROUND(SUM(used_ublk * 8192) / 1024 / 1024, 2)
            INTO v_undo_usage
            FROM v$transaction
            WHERE ses_addr = (SELECT saddr FROM v$session WHERE sid = v_session_id);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_undo_usage := 0;
        END;
        
        -- Monitor temp space usage
        BEGIN
            SELECT ROUND(SUM(blocks * 8192) / 1024 / 1024, 2)
            INTO v_temp_usage
            FROM v$tempseg_usage
            WHERE session_addr = (SELECT saddr FROM v$session WHERE sid = v_session_id);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_temp_usage := 0;
        END;
        
        -- Log resource usage
        log_operation('RESOURCE_MONITOR', p_table_name, NULL, 'INFO', 
                     'Operation: ' || p_operation_type || 
                     ', Undo Usage: ' || v_undo_usage || 'MB' ||
                     ', Temp Usage: ' || v_temp_usage || 'MB');
        
        -- Alert if resource usage is high
        IF v_undo_usage > 1000 THEN  -- 1GB undo usage
            log_operation('RESOURCE_ALERT', p_table_name, NULL, 'WARNING', 
                         'High undo usage detected: ' || v_undo_usage || 'MB');
        END IF;
        
        IF v_temp_usage > 2000 THEN  -- 2GB temp usage
            log_operation('RESOURCE_ALERT', p_table_name, NULL, 'WARNING', 
                         'High temp space usage detected: ' || v_temp_usage || 'MB');
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for monitoring
            NULL;
    END monitor_operation_resources;
    
    PROCEDURE optimize_operation_settings(
        p_table_name IN VARCHAR2,
        p_operation_type IN VARCHAR2
    ) IS
        v_table_size_mb NUMBER;
        v_recommended_settings VARCHAR2(4000);
    BEGIN
        -- Get table size
        v_table_size_mb := get_table_size_mb(p_table_name);
        
        -- Generate recommendations based on table size and operation type
        CASE p_operation_type
            WHEN 'CONVERSION' THEN
                IF v_table_size_mb > 10000 THEN
                    v_recommended_settings := 'RECOMMENDED: Use batch processing, degree=8, commit every 50 batches';
                ELSIF v_table_size_mb > 1000 THEN
                    v_recommended_settings := 'RECOMMENDED: Use parallel processing, degree=4, commit every 25 batches';
                ELSE
                    v_recommended_settings := 'RECOMMENDED: Standard processing, degree=1, commit after completion';
                END IF;
            WHEN 'MAINTENANCE' THEN
                IF v_table_size_mb > 50000 THEN
                    v_recommended_settings := 'RECOMMENDED: Use 1% sampling for stats, degree=8, online operations only';
                ELSIF v_table_size_mb > 10000 THEN
                    v_recommended_settings := 'RECOMMENDED: Use 5% sampling for stats, degree=4, online operations only';
                ELSE
                    v_recommended_settings := 'RECOMMENDED: Use 10% sampling for stats, degree=2, online operations only';
                END IF;
            ELSE
                v_recommended_settings := 'RECOMMENDED: Standard settings, online operations only';
        END CASE;
        
        log_operation('OPTIMIZATION_ADVICE', p_table_name, NULL, 'INFO', v_recommended_settings);
        
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for optimization advice
            NULL;
    END optimize_operation_settings;
    
    FUNCTION get_table_size_mb(
        p_table_name IN VARCHAR2
    ) RETURN NUMBER IS
        v_size_mb NUMBER := 0;
    BEGIN
        BEGIN
            SELECT ROUND(SUM(bytes) / 1024 / 1024, 2)
            INTO v_size_mb
            FROM user_segments
            WHERE segment_name = UPPER(p_table_name);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_size_mb := 0;
        END;
        
        RETURN NVL(v_size_mb, 0);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END get_table_size_mb;
    
END partition_management_pkg;
/
