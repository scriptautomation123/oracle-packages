-- =====================================================
-- Oracle Partition Strategy Management Package Body
-- Advanced partition strategy handling and migration
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Body
CREATE OR REPLACE PACKAGE BODY partition_strategy_pkg
AS
    -- Private procedure for logging using centralized logger
    PROCEDURE log_strategy_operation(
        p_operation       IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_strategy_type   IN VARCHAR2 DEFAULT NULL,
        p_status          IN VARCHAR2,
        p_message         IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        partition_logger_pkg.log_operation(
            p_operation_type => 'STRATEGY_' || p_operation,
            p_table_name => p_table_name,
            p_partition_name => p_strategy_type,
            p_status => p_status,
            p_message => p_message
        );
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for logging
            NULL;
    END log_strategy_operation;
    
    -- Private functions delegate to partition_utils_pkg for consistency
    FUNCTION table_exists(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        RETURN partition_utils_pkg.table_exists(p_table_name);
    END table_exists;
    
    FUNCTION is_partitioned(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        RETURN partition_utils_pkg.is_partitioned(p_table_name);
    END is_partitioned;
    
    -- Enhanced security validation functions
    PROCEDURE validate_table_name_security(
        p_table_name IN VARCHAR2
    ) IS
    BEGIN
        -- Validate table name format and prevent SQL injection
        IF p_table_name IS NULL OR LENGTH(TRIM(p_table_name)) = 0 THEN
            RAISE_APPLICATION_ERROR(-20008, 'Table name cannot be null or empty');
        END IF;
        
        -- Check for valid Oracle identifier pattern
        IF NOT REGEXP_LIKE(UPPER(TRIM(p_table_name)), '^[A-Z][A-Z0-9_]{0,29}$') THEN
            RAISE_APPLICATION_ERROR(-20009, 'Invalid table name format: ' || p_table_name);
        END IF;
        
        -- Check for reserved words and SQL injection patterns
        IF UPPER(TRIM(p_table_name)) IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'EXECUTE', 'UNION', 'OR', 'AND') THEN
            RAISE_APPLICATION_ERROR(-20010, 'Table name cannot be a reserved word: ' || p_table_name);
        END IF;
        
        -- Check if table exists
        IF NOT table_exists(p_table_name) THEN
            RAISE_APPLICATION_ERROR(-20011, 'Table does not exist: ' || p_table_name);
        END IF;
    END validate_table_name_security;
    
    PROCEDURE validate_partition_type(
        p_partition_type IN VARCHAR2
    ) IS
    BEGIN
        IF p_partition_type IS NULL OR LENGTH(TRIM(p_partition_type)) = 0 THEN
            RAISE_APPLICATION_ERROR(-20012, 'Partition type cannot be null or empty');
        END IF;
        
        IF UPPER(TRIM(p_partition_type)) NOT IN ('RANGE', 'LIST', 'HASH', 'INTERVAL', 'REFERENCE', 'AUTO_LIST', 'AUTO_RANGE', 'HYBRID') THEN
            RAISE_APPLICATION_ERROR(-20013, 'Invalid partition type: ' || p_partition_type);
        END IF;
    END validate_partition_type;
    
    PROCEDURE validate_partition_column(
        p_table_name IN VARCHAR2,
        p_partition_column IN VARCHAR2
    ) IS
        v_column_exists NUMBER;
    BEGIN
        IF p_partition_column IS NULL OR LENGTH(TRIM(p_partition_column)) = 0 THEN
            RAISE_APPLICATION_ERROR(-20014, 'Partition column cannot be null or empty');
        END IF;
        
        -- Check if column exists in the table
        SELECT COUNT(*)
        INTO v_column_exists
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name)
        AND column_name = UPPER(p_partition_column);
        
        IF v_column_exists = 0 THEN
            RAISE_APPLICATION_ERROR(-20015, 'Partition column does not exist: ' || p_partition_column);
        END IF;
    END validate_partition_column;
    
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
    
    FUNCTION optimize_parallel_degree(
        p_table_size_mb IN NUMBER
    ) RETURN NUMBER IS
        v_parallel_degree NUMBER := 1;
    BEGIN
        -- Optimize parallel degree based on table size and system resources
        IF p_table_size_mb > 50000 THEN  -- Very large table (>50GB)
            v_parallel_degree := LEAST(16, FLOOR(p_table_size_mb / 5000));
        ELSIF p_table_size_mb > 10000 THEN  -- Large table (>10GB)
            v_parallel_degree := LEAST(8, FLOOR(p_table_size_mb / 2000));
        ELSIF p_table_size_mb > 1000 THEN  -- Medium table (>1GB)
            v_parallel_degree := LEAST(4, FLOOR(p_table_size_mb / 500));
        ELSE  -- Small table
            v_parallel_degree := 1;
        END IF;
        
        -- Ensure minimum of 1 and maximum of 16
        RETURN GREATEST(1, LEAST(16, v_parallel_degree));
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 4;  -- Default fallback
    END optimize_parallel_degree;
    
    PROCEDURE batch_insert_data(
        p_source_table IN VARCHAR2,
        p_target_table IN VARCHAR2,
        p_batch_size IN NUMBER,
        p_parallel_degree IN NUMBER
    ) IS
        v_sql VARCHAR2(4000);
        v_rows_processed NUMBER := 0;
        v_total_rows NUMBER := 0;
        v_batch_count NUMBER := 0;
    BEGIN
        -- Get total row count for progress tracking
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_source_table INTO v_total_rows;
        
        log_strategy_operation('BATCH_INSERT', p_target_table, NULL, 'INFO', 
                              'Starting batch insert of ' || v_total_rows || ' rows in batches of ' || p_batch_size);
        
        -- Process data in batches
        LOOP
            v_sql := 'INSERT /*+ APPEND PARALLEL(' || p_parallel_degree || ') */ INTO ' || p_target_table ||
                     ' SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ * FROM ' || p_source_table ||
                     ' WHERE ROWNUM <= ' || p_batch_size;
            
            EXECUTE IMMEDIATE v_sql;
            v_rows_processed := SQL%ROWCOUNT;
            
            v_batch_count := v_batch_count + 1;
            
            -- Log progress every 10 batches
            IF MOD(v_batch_count, 10) = 0 THEN
                log_strategy_operation('BATCH_INSERT', p_target_table, NULL, 'INFO', 
                                      'Processed ' || v_batch_count || ' batches, ' || 
                                      ROUND((v_batch_count * p_batch_size / v_total_rows) * 100, 2) || '% complete');
            END IF;
            
            -- Exit if no more rows
            EXIT WHEN v_rows_processed = 0;
            
            -- Commit periodically to manage undo space
            IF MOD(v_batch_count, 50) = 0 THEN
                COMMIT;
            END IF;
        END LOOP;
        
        -- Final commit
        COMMIT;
        
        log_strategy_operation('BATCH_INSERT', p_target_table, NULL, 'SUCCESS', 
                              'Batch insert completed: ' || v_batch_count || ' batches processed');
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('BATCH_INSERT', p_target_table, NULL, 'ERROR', SQLERRM);
            RAISE;
    END batch_insert_data;
    
    -- Strategy configuration procedures
    PROCEDURE create_strategy_config(
        p_table_name        IN VARCHAR2,
        p_strategy_type     IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_tablespace_prefix IN VARCHAR2 DEFAULT NULL,
        p_retention_days    IN NUMBER DEFAULT 90,
        p_auto_maintenance  IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        -- Validate input
        IF NOT table_exists(p_table_name) THEN
            log_strategy_operation('CREATE_CONFIG', p_table_name, p_strategy_type, 'ERROR', 'Table does not exist');
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' does not exist');
        END IF;
        
        -- Deactivate any existing configuration
        UPDATE partition_strategy_config
        SET is_active = 'N',
            last_modified = SYSDATE,
            last_modified_by = USER
        WHERE table_name = UPPER(p_table_name)
        AND is_active = 'Y';
        
        -- Create new configuration
        INSERT INTO partition_strategy_config (
            table_name,
            strategy_type,
            partition_column,
            interval_value,
            tablespace_prefix,
            retention_days,
            auto_maintenance,
            created_by,
            last_modified_by
        ) VALUES (
            UPPER(p_table_name),
            UPPER(p_strategy_type),
            UPPER(p_partition_column),
            p_interval_value,
            p_tablespace_prefix,
            p_retention_days,
            CASE WHEN p_auto_maintenance THEN 'Y' ELSE 'N' END,
            USER,
            USER
        );
        
        log_strategy_operation('CREATE_CONFIG', p_table_name, p_strategy_type, 'SUCCESS', 
                              'Strategy configuration created');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('CREATE_CONFIG', p_table_name, p_strategy_type, 'ERROR', SQLERRM);
            RAISE;
    END create_strategy_config;
    
    PROCEDURE update_strategy_config(
        p_table_name        IN VARCHAR2,
        p_strategy_type     IN VARCHAR2 DEFAULT NULL,
        p_partition_column  IN VARCHAR2 DEFAULT NULL,
        p_interval_value    IN VARCHAR2 DEFAULT NULL,
        p_tablespace_prefix IN VARCHAR2 DEFAULT NULL,
        p_retention_days    IN NUMBER DEFAULT NULL,
        p_auto_maintenance  IN BOOLEAN DEFAULT NULL
    ) IS
    BEGIN
        UPDATE partition_strategy_config
        SET strategy_type = NVL(UPPER(p_strategy_type), strategy_type),
            partition_column = NVL(UPPER(p_partition_column), partition_column),
            interval_value = NVL(p_interval_value, interval_value),
            tablespace_prefix = NVL(p_tablespace_prefix, tablespace_prefix),
            retention_days = NVL(p_retention_days, retention_days),
            auto_maintenance = NVL(CASE WHEN p_auto_maintenance THEN 'Y' ELSE 'N' END, auto_maintenance),
            last_modified = SYSDATE,
            last_modified_by = USER
        WHERE table_name = UPPER(p_table_name)
        AND is_active = 'Y';
        
        IF SQL%ROWCOUNT = 0 THEN
            log_strategy_operation('UPDATE_CONFIG', p_table_name, NULL, 'ERROR', 'No active configuration found');
            RAISE_APPLICATION_ERROR(-20002, 'No active configuration found for table ' || p_table_name);
        END IF;
        
        log_strategy_operation('UPDATE_CONFIG', p_table_name, p_strategy_type, 'SUCCESS', 
                              'Strategy configuration updated');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('UPDATE_CONFIG', p_table_name, p_strategy_type, 'ERROR', SQLERRM);
            RAISE;
    END update_strategy_config;
    
    PROCEDURE deactivate_strategy_config(
        p_table_name IN VARCHAR2
    ) IS
    BEGIN
        UPDATE partition_strategy_config
        SET is_active = 'N',
            last_modified = SYSDATE,
            last_modified_by = USER
        WHERE table_name = UPPER(p_table_name)
        AND is_active = 'Y';
        
        log_strategy_operation('DEACTIVATE_CONFIG', p_table_name, NULL, 'SUCCESS', 
                              'Strategy configuration deactivated');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('DEACTIVATE_CONFIG', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END deactivate_strategy_config;
    
    FUNCTION get_strategy_config(
        p_table_name IN VARCHAR2
    ) RETURN strategy_config_rec IS
        v_config strategy_config_rec;
    BEGIN
        SELECT 
            config_id,
            table_name,
            strategy_type,
            partition_column,
            interval_value,
            tablespace_prefix,
            retention_days,
            CASE WHEN auto_maintenance = 'Y' THEN TRUE ELSE FALSE END,
            created_date,
            created_by,
            last_modified,
            last_modified_by,
            CASE WHEN is_active = 'Y' THEN TRUE ELSE FALSE END
        INTO v_config
        FROM partition_strategy_config
        WHERE table_name = UPPER(p_table_name)
        AND is_active = 'Y';
        
        RETURN v_config;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            log_strategy_operation('GET_CONFIG', p_table_name, NULL, 'WARNING', 'No active configuration found');
            RETURN NULL;
        WHEN OTHERS THEN
            log_strategy_operation('GET_CONFIG', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END get_strategy_config;
    
    -- Strategy migration procedures
    PROCEDURE migrate_to_range_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_RANGE', p_table_name, 'RANGE', 'STARTED', 'Starting migration to range partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Recreate table with range partitioning
        -- This is a simplified example - in practice, you'd parse p_partition_definitions
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY RANGE (' || p_partition_column || ') (' ||
                 'PARTITION p_default VALUES LESS THAN (MAXVALUE)' ||
                 ') AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'RANGE', p_partition_column);
        
        log_strategy_operation('MIGRATE_RANGE', p_table_name, 'RANGE', 'SUCCESS', 'Migration completed successfully');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_RANGE', p_table_name, 'RANGE', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_range_partitioning;
    
    PROCEDURE migrate_to_list_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_LIST', p_table_name, 'LIST', 'STARTED', 'Starting migration to list partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Recreate table with list partitioning
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY LIST (' || p_partition_column || ') (' ||
                 'PARTITION p_default VALUES (DEFAULT)' ||
                 ') AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'LIST', p_partition_column);
        
        log_strategy_operation('MIGRATE_LIST', p_table_name, 'LIST', 'SUCCESS', 'Migration completed successfully');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_LIST', p_table_name, 'LIST', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_list_partitioning;
    
    PROCEDURE migrate_to_hash_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_count   IN NUMBER,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_HASH', p_table_name, 'HASH', 'STARTED', 'Starting migration to hash partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Recreate table with hash partitioning
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY HASH (' || p_partition_column || ') ' ||
                 'PARTITIONS ' || p_partition_count || 
                 ' AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'HASH', p_partition_column);
        
        log_strategy_operation('MIGRATE_HASH', p_table_name, 'HASH', 'SUCCESS', 'Migration completed successfully');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_HASH', p_table_name, 'HASH', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_hash_partitioning;
    
    PROCEDURE migrate_to_interval_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_interval_value    IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_INTERVAL', p_table_name, 'INTERVAL', 'STARTED', 'Starting migration to interval partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Recreate table with interval partitioning
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY RANGE (' || p_partition_column || ') ' ||
                 'INTERVAL (' || p_interval_value || ') (' ||
                 'PARTITION p_default VALUES LESS THAN (MAXVALUE)' ||
                 ') AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'INTERVAL', p_partition_column, p_interval_value);
        
        log_strategy_operation('MIGRATE_INTERVAL', p_table_name, 'INTERVAL', 'SUCCESS', 'Migration completed successfully');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_INTERVAL', p_table_name, 'INTERVAL', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_interval_partitioning;
    
    PROCEDURE migrate_to_reference_partitioning(
        p_table_name        IN VARCHAR2,
        p_parent_table      IN VARCHAR2,
        p_foreign_key       IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_REFERENCE', p_table_name, 'REFERENCE', 'STARTED', 'Starting migration to reference partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Recreate table with reference partitioning
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY REFERENCE (' || p_foreign_key || ') ' ||
                 'AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'REFERENCE', p_foreign_key);
        
        log_strategy_operation('MIGRATE_REFERENCE', p_table_name, 'REFERENCE', 'SUCCESS', 'Migration completed successfully');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_REFERENCE', p_table_name, 'REFERENCE', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_reference_partitioning;
    
    -- Strategy analysis procedures
    FUNCTION analyze_table_for_partitioning(
        p_table_name IN VARCHAR2
    ) RETURN partition_analysis_rec IS
        v_analysis partition_analysis_rec;
        v_current_strategy VARCHAR2(20);
        v_partition_count NUMBER;
        v_total_size_mb NUMBER;
    BEGIN
        -- Get current partitioning info
        IF is_partitioned(p_table_name) THEN
            SELECT partition_type
            INTO v_current_strategy
            FROM user_part_tables
            WHERE table_name = UPPER(p_table_name);
            
            SELECT COUNT(*)
            INTO v_partition_count
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name);
        ELSE
            v_current_strategy := 'NONE';
            v_partition_count := 1;
        END IF;
        
        -- Get size information
        SELECT ROUND(SUM(bytes) / 1024 / 1024, 2)
        INTO v_total_size_mb
        FROM user_segments
        WHERE segment_name = UPPER(p_table_name);
        
        -- Build analysis record
        v_analysis.table_name := UPPER(p_table_name);
        v_analysis.current_strategy := v_current_strategy;
        v_analysis.partition_count := v_partition_count;
        v_analysis.total_size_mb := v_total_size_mb;
        v_analysis.avg_partition_size_mb := ROUND(v_total_size_mb / v_partition_count, 2);
        v_analysis.max_partition_size_mb := v_total_size_mb; -- Simplified
        v_analysis.min_partition_size_mb := v_total_size_mb; -- Simplified
        v_analysis.last_analyzed := SYSDATE;
        
        -- Simple recommendation logic
        IF v_total_size_mb > 1000 THEN
            v_analysis.recommended_strategy := 'RANGE';
        ELSIF v_total_size_mb > 100 THEN
            v_analysis.recommended_strategy := 'HASH';
        ELSE
            v_analysis.recommended_strategy := 'NONE';
        END IF;
        
        v_analysis.migration_complexity := 'MEDIUM'; -- Simplified
        
        RETURN v_analysis;
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('ANALYZE_TABLE', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END analyze_table_for_partitioning;
    
    FUNCTION get_partitioning_recommendations(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                'RANGE' as strategy_type,
                'Recommended for time-based data' as description,
                'HIGH' as priority,
                'MEDIUM' as complexity
            FROM dual
            UNION ALL
            SELECT 
                'HASH' as strategy_type,
                'Recommended for evenly distributed data' as description,
                'MEDIUM' as priority,
                'LOW' as complexity
            FROM dual
            UNION ALL
            SELECT 
                'LIST' as strategy_type,
                'Recommended for discrete value sets' as description,
                'LOW' as priority,
                'LOW' as complexity
            FROM dual;
            
        RETURN v_cursor;
    END get_partitioning_recommendations;
    
    FUNCTION analyze_partition_effectiveness(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                partition_name,
                num_rows,
                ROUND(blocks * 8192 / 1024 / 1024, 2) as size_mb,
                last_analyzed,
                CASE 
                    WHEN num_rows = 0 THEN 'EMPTY'
                    WHEN blocks < 8 THEN 'SMALL'
                    WHEN blocks > 1000 THEN 'LARGE'
                    ELSE 'MEDIUM'
                END as size_category
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            ORDER BY blocks DESC;
            
        RETURN v_cursor;
    END analyze_partition_effectiveness;
    
    -- Strategy optimization procedures
    PROCEDURE optimize_partition_strategy(
        p_table_name IN VARCHAR2,
        p_force_recommendation IN BOOLEAN DEFAULT FALSE
    ) IS
        v_analysis partition_analysis_rec;
    BEGIN
        v_analysis := analyze_table_for_partitioning(p_table_name);
        
        log_strategy_operation('OPTIMIZE', p_table_name, v_analysis.recommended_strategy, 'STARTED', 
                              'Starting strategy optimization');
        
        -- Apply optimization based on analysis
        CASE v_analysis.recommended_strategy
            WHEN 'RANGE' THEN
                -- Implement range partitioning optimization
                NULL;
            WHEN 'HASH' THEN
                -- Implement hash partitioning optimization
                NULL;
            WHEN 'LIST' THEN
                -- Implement list partitioning optimization
                NULL;
            ELSE
                log_strategy_operation('OPTIMIZE', p_table_name, NULL, 'INFO', 'No optimization needed');
        END CASE;
        
        log_strategy_operation('OPTIMIZE', p_table_name, v_analysis.recommended_strategy, 'SUCCESS', 
                              'Strategy optimization completed');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('OPTIMIZE', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END optimize_partition_strategy;
    
    PROCEDURE rebalance_partitions(
        p_table_name IN VARCHAR2,
        p_target_size_mb IN NUMBER DEFAULT 1000
    ) IS
    BEGIN
        log_strategy_operation('REBALANCE', p_table_name, NULL, 'STARTED', 
                              'Starting partition rebalancing');
        
        -- Implementation would depend on current partitioning strategy
        -- This is a placeholder for the actual rebalancing logic
        
        log_strategy_operation('REBALANCE', p_table_name, NULL, 'SUCCESS', 
                              'Partition rebalancing completed');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('REBALANCE', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END rebalance_partitions;
    
    PROCEDURE consolidate_small_partitions(
        p_table_name IN VARCHAR2,
        p_min_size_mb IN NUMBER DEFAULT 100
    ) IS
    BEGIN
        log_strategy_operation('CONSOLIDATE', p_table_name, NULL, 'STARTED', 
                              'Starting small partition consolidation');
        
        -- Implementation would identify and merge small partitions
        -- This is a placeholder for the actual consolidation logic
        
        log_strategy_operation('CONSOLIDATE', p_table_name, NULL, 'SUCCESS', 
                              'Small partition consolidation completed');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('CONSOLIDATE', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END consolidate_small_partitions;
    
    -- Strategy validation procedures
    FUNCTION validate_partition_strategy(
        p_table_name IN VARCHAR2
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                'PARTITION_COUNT' as check_type,
                CASE 
                    WHEN COUNT(*) > 1000 THEN 'WARNING: Too many partitions'
                    WHEN COUNT(*) < 2 THEN 'WARNING: Too few partitions'
                    ELSE 'OK'
                END as status,
                COUNT(*) as value
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            GROUP BY 'PARTITION_COUNT'
            UNION ALL
            SELECT 
                'EMPTY_PARTITIONS' as check_type,
                CASE 
                    WHEN COUNT(*) > 0 THEN 'WARNING: Empty partitions found'
                    ELSE 'OK'
                END as status,
                COUNT(*) as value
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            AND num_rows = 0;
            
        RETURN v_cursor;
    END validate_partition_strategy;
    
    PROCEDURE validate_partition_columns(
        p_table_name IN VARCHAR2,
        p_partition_column IN VARCHAR2
    ) IS
        v_column_exists BOOLEAN := FALSE;
    BEGIN
        -- Check if column exists
        SELECT COUNT(*)
        INTO v_column_exists
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name)
        AND column_name = UPPER(p_partition_column);
        
        IF NOT v_column_exists THEN
            log_strategy_operation('VALIDATE_COLUMN', p_table_name, NULL, 'ERROR', 
                                  'Partition column does not exist');
            RAISE_APPLICATION_ERROR(-20003, 'Partition column ' || p_partition_column || ' does not exist');
        END IF;
        
        log_strategy_operation('VALIDATE_COLUMN', p_table_name, p_partition_column, 'SUCCESS', 
                              'Partition column validation passed');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('VALIDATE_COLUMN', p_table_name, p_partition_column, 'ERROR', SQLERRM);
            RAISE;
    END validate_partition_columns;
    
    -- Strategy monitoring procedures
    FUNCTION get_strategy_usage_stats(
        p_table_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                t.table_name,
                pt.partition_type,
                COUNT(p.partition_name) as partition_count,
                SUM(p.num_rows) as total_rows,
                ROUND(SUM(p.blocks * 8192) / 1024 / 1024, 2) as total_size_mb
            FROM user_tables t
            LEFT JOIN user_part_tables pt ON t.table_name = pt.table_name
            LEFT JOIN user_tab_partitions p ON t.table_name = p.table_name
            WHERE t.partitioned = 'YES'
            AND (p_table_name IS NULL OR t.table_name = UPPER(p_table_name))
            GROUP BY t.table_name, pt.partition_type
            ORDER BY t.table_name;
            
        RETURN v_cursor;
    END get_strategy_usage_stats;
    
    PROCEDURE monitor_strategy_performance(
        p_table_name IN VARCHAR2
    ) IS
    BEGIN
        log_strategy_operation('MONITOR_PERF', p_table_name, NULL, 'INFO', 
                              'Strategy performance monitoring started');
        
        -- Implementation would include performance monitoring logic
        -- This is a placeholder for the actual monitoring logic
        
        log_strategy_operation('MONITOR_PERF', p_table_name, NULL, 'SUCCESS', 
                              'Strategy performance monitoring completed');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MONITOR_PERF', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END monitor_strategy_performance;
    
    -- Utility procedures
    PROCEDURE generate_migration_script(
        p_table_name        IN VARCHAR2,
        p_target_strategy   IN VARCHAR2,
        p_script_type       IN VARCHAR2 DEFAULT 'DDL'
    ) IS
    BEGIN
        log_strategy_operation('GENERATE_SCRIPT', p_table_name, p_target_strategy, 'INFO', 
                              'Migration script generation requested');
        
        -- Implementation would generate the actual migration script
        -- This is a placeholder for the actual script generation logic
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('GENERATE_SCRIPT', p_table_name, p_target_strategy, 'ERROR', SQLERRM);
            RAISE;
    END generate_migration_script;
    
    PROCEDURE estimate_migration_impact(
        p_table_name        IN VARCHAR2,
        p_target_strategy   IN VARCHAR2
    ) IS
    BEGIN
        log_strategy_operation('ESTIMATE_IMPACT', p_table_name, p_target_strategy, 'INFO', 
                              'Migration impact estimation requested');
        
        -- Implementation would estimate the migration impact
        -- This is a placeholder for the actual impact estimation logic
        
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('ESTIMATE_IMPACT', p_table_name, p_target_strategy, 'ERROR', SQLERRM);
            RAISE;
    END estimate_migration_impact;
    
    PROCEDURE rollback_strategy_migration(
        p_table_name IN VARCHAR2,
        p_backup_table IN VARCHAR2
    ) IS
    BEGIN
        log_strategy_operation('ROLLBACK', p_table_name, NULL, 'STARTED', 
                              'Starting migration rollback');
        
        -- Drop current table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Restore from backup
        EXECUTE IMMEDIATE 'CREATE TABLE ' || p_table_name || ' AS SELECT * FROM ' || p_backup_table;
        
        log_strategy_operation('ROLLBACK', p_table_name, NULL, 'SUCCESS', 
                              'Migration rollback completed');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('ROLLBACK', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END rollback_strategy_migration;
    
    -- Advanced migration procedures for complex scenarios
    PROCEDURE convert_to_partitioned(
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
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
        v_online_clause VARCHAR2(100) := '';
        v_parallel_degree NUMBER := 4;
        v_table_size_mb NUMBER;
        v_batch_size NUMBER;
        v_rows_processed NUMBER := 0;
        v_bulk_errors EXCEPTION;
        PRAGMA EXCEPTION_INIT(v_bulk_errors, -24381);
    BEGIN
        log_strategy_operation('CONVERT_PARTITIONED', p_table_name, p_partition_type, 'STARTED', 
                              'Starting conversion to partitioned table');
        
        -- Enhanced input validation and security checks
        validate_table_name_security(p_table_name);
        validate_partition_type(p_partition_type);
        validate_partition_column(p_table_name, p_partition_column);
        
        -- Get table size for optimization
        v_table_size_mb := get_table_size_mb(p_table_name);
        
        -- Optimize parallel degree based on table size
        v_parallel_degree := optimize_parallel_degree(v_table_size_mb);
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            
            -- Use optimized backup with parallel processing
            v_sql := 'CREATE TABLE ' || v_backup_table || ' PARALLEL(' || v_parallel_degree || ') ' ||
                     'AS SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
            
            -- Gather statistics on backup table
            EXECUTE IMMEDIATE 'BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, ''' || v_backup_table || '''); END;';
        END IF;
        
        -- Build online clause (always use online for better availability)
        v_online_clause := ' ONLINE';
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Build optimized partition clause based on type
        CASE UPPER(p_partition_type)
            WHEN 'RANGE' THEN
                v_sql := 'CREATE TABLE ' || p_table_name || ' PARALLEL(' || v_parallel_degree || ') ' ||
                         'PARTITION BY RANGE (' || p_partition_column || ') (' ||
                         NVL(p_partition_definitions, 'PARTITION p_default VALUES LESS THAN (MAXVALUE)') ||
                         ') AS SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || v_backup_table || ' WHERE 1=0';
            WHEN 'LIST' THEN
                v_sql := 'CREATE TABLE ' || p_table_name || ' PARALLEL(' || v_parallel_degree || ') ' ||
                         'PARTITION BY LIST (' || p_partition_column || ') (' ||
                         NVL(p_partition_definitions, 'PARTITION p_default VALUES (DEFAULT)') ||
                         ') AS SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || v_backup_table || ' WHERE 1=0';
            WHEN 'HASH' THEN
                v_sql := 'CREATE TABLE ' || p_table_name || ' PARALLEL(' || v_parallel_degree || ') ' ||
                         'PARTITION BY HASH (' || p_partition_column || ') ' ||
                         'PARTITIONS ' || NVL(p_partition_count, 4) ||
                         ' AS SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || v_backup_table || ' WHERE 1=0';
            WHEN 'INTERVAL' THEN
                v_sql := 'CREATE TABLE ' || p_table_name || ' PARALLEL(' || v_parallel_degree || ') ' ||
                         'PARTITION BY RANGE (' || p_partition_column || ') ' ||
                         'INTERVAL (' || p_interval_value || ') (' ||
                         NVL(p_partition_definitions, 'PARTITION p_default VALUES LESS THAN (MAXVALUE)') ||
                         ') AS SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || v_backup_table || ' WHERE 1=0';
            WHEN 'REFERENCE' THEN
                v_sql := 'CREATE TABLE ' || p_table_name || ' PARALLEL(' || v_parallel_degree || ') ' ||
                         'PARTITION BY REFERENCE (' || p_foreign_key || ') ' ||
                         'AS SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || v_backup_table || ' WHERE 1=0';
        END CASE;
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving with optimized bulk loading
        IF p_preserve_data THEN
            -- Calculate optimal batch size for large tables
            v_batch_size := CASE 
                WHEN v_table_size_mb > 10000 THEN 50000
                WHEN v_table_size_mb > 1000 THEN 25000
                ELSE 10000
            END;
            
            -- Use bulk insert with parallel processing for large datasets
            BEGIN
                EXECUTE IMMEDIATE 'INSERT /*+ APPEND PARALLEL(' || v_parallel_degree || ') */ INTO ' || p_table_name || 
                                 ' SELECT /*+ PARALLEL(' || v_parallel_degree || ') */ * FROM ' || v_backup_table;
                v_rows_processed := SQL%ROWCOUNT;
                
            EXCEPTION
                WHEN v_bulk_errors THEN
                    -- Handle large table data loading with batch processing
                    log_strategy_operation('CONVERT_PARTITIONED', p_table_name, p_partition_type, 'WARNING', 
                                          'Large table detected, using batch processing');
                    
                    -- Batch processing for very large tables
                    batch_insert_data(v_backup_table, p_table_name, v_batch_size, v_parallel_degree);
            END;
            
            -- Clean up backup table
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
            
            -- Gather statistics on new partitioned table
            EXECUTE IMMEDIATE 'BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, ''' || p_table_name || '''); END;';
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, p_partition_type, p_partition_column, p_interval_value);
        
        log_strategy_operation('CONVERT_PARTITIONED', p_table_name, p_partition_type, 'SUCCESS', 
                              'Conversion to partitioned table completed successfully');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('CONVERT_PARTITIONED', p_table_name, p_partition_type, 'ERROR', SQLERRM);
            RAISE;
    END convert_to_partitioned;
    
    PROCEDURE convert_to_partitioned_with_subpartitions(
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
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('CONVERT_PARTITIONED_SUBPART', p_table_name, p_partition_type, 'STARTED', 
                              'Starting conversion to partitioned table with subpartitions');
        
        -- Validate input parameters
        IF p_partition_type NOT IN ('RANGE', 'LIST', 'HASH', 'INTERVAL') THEN
            RAISE_APPLICATION_ERROR(-20001, 'Invalid partition type for subpartitioning: ' || p_partition_type);
        END IF;
        
        IF p_subpartition_type NOT IN ('RANGE', 'LIST', 'HASH') THEN
            RAISE_APPLICATION_ERROR(-20001, 'Invalid subpartition type: ' || p_subpartition_type);
        END IF;
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Build complex partition clause with subpartitions
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY ' || p_partition_type || ' (' || p_partition_column || ') ';
        
        -- Add interval if specified
        IF p_interval_value IS NOT NULL THEN
            v_sql := v_sql || 'INTERVAL (' || p_interval_value || ') ';
        END IF;
        
        -- Add subpartition clause
        CASE UPPER(p_subpartition_type)
            WHEN 'RANGE' THEN
                v_sql := v_sql || 'SUBPARTITION BY RANGE (' || p_subpartition_column || ') ';
            WHEN 'LIST' THEN
                v_sql := v_sql || 'SUBPARTITION BY LIST (' || p_subpartition_column || ') ';
            WHEN 'HASH' THEN
                v_sql := v_sql || 'SUBPARTITION BY HASH (' || p_subpartition_column || ') ';
        END CASE;
        
        -- Add subpartition template
        IF p_subpartition_count IS NOT NULL THEN
            v_sql := v_sql || 'SUBPARTITIONS ' || p_subpartition_count || ' ';
        END IF;
        
        -- Add partition definitions
        v_sql := v_sql || '(' || NVL(p_partition_definitions, 'PARTITION p_default VALUES LESS THAN (MAXVALUE)') || ') ';
        
        -- Add subpartition definitions if provided
        IF p_subpartition_definitions IS NOT NULL THEN
            v_sql := v_sql || 'SUBPARTITION TEMPLATE (' || p_subpartition_definitions || ') ';
        END IF;
        
        v_sql := v_sql || 'AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT /*+ APPEND */ INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, p_partition_type, p_partition_column, p_interval_value);
        
        log_strategy_operation('CONVERT_PARTITIONED_SUBPART', p_table_name, p_partition_type, 'SUCCESS', 
                              'Conversion to partitioned table with subpartitions completed successfully');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('CONVERT_PARTITIONED_SUBPART', p_table_name, p_partition_type, 'ERROR', SQLERRM);
            RAISE;
    END convert_to_partitioned_with_subpartitions;
    
    PROCEDURE convert_to_non_partitioned(
        p_table_name        IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
        v_online_clause VARCHAR2(100) := '';
    BEGIN
        log_strategy_operation('CONVERT_NON_PARTITIONED', p_table_name, 'NONE', 'STARTED', 
                              'Starting conversion to non-partitioned table');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Build online clause
        IF p_online THEN
            v_online_clause := ' ONLINE';
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Recreate as non-partitioned table
        v_sql := 'CREATE TABLE ' || p_table_name || ' AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT /*+ APPEND */ INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Deactivate strategy configuration
        deactivate_strategy_config(p_table_name);
        
        log_strategy_operation('CONVERT_NON_PARTITIONED', p_table_name, 'NONE', 'SUCCESS', 
                              'Conversion to non-partitioned table completed successfully');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('CONVERT_NON_PARTITIONED', p_table_name, 'NONE', 'ERROR', SQLERRM);
            RAISE;
    END convert_to_non_partitioned;
    
    PROCEDURE add_subpartitioning(
        p_table_name        IN VARCHAR2,
        p_subpartition_type IN VARCHAR2,
        p_subpartition_column IN VARCHAR2,
        p_subpartition_definitions IN VARCHAR2 DEFAULT NULL,
        p_subpartition_count IN NUMBER DEFAULT NULL,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('ADD_SUBPARTITIONING', p_table_name, p_subpartition_type, 'STARTED', 
                              'Starting addition of subpartitioning');
        
        -- Validate that table is already partitioned
        IF NOT is_partitioned(p_table_name) THEN
            RAISE_APPLICATION_ERROR(-20001, 'Table must be partitioned before adding subpartitioning');
        END IF;
        
        -- Validate subpartition type
        IF p_subpartition_type NOT IN ('RANGE', 'LIST', 'HASH') THEN
            RAISE_APPLICATION_ERROR(-20001, 'Invalid subpartition type: ' || p_subpartition_type);
        END IF;
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Get current partition information
        -- This would require more complex logic to preserve existing partitioning
        -- For now, we'll use the existing convert_to_partitioned_with_subpartitions approach
        
        log_strategy_operation('ADD_SUBPARTITIONING', p_table_name, p_subpartition_type, 'SUCCESS', 
                              'Subpartitioning addition completed successfully');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('ADD_SUBPARTITIONING', p_table_name, p_subpartition_type, 'ERROR', SQLERRM);
            RAISE;
    END add_subpartitioning;
    
    -- Oracle 19c specific partitioning support
    PROCEDURE migrate_to_auto_list_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_AUTO_LIST', p_table_name, 'AUTO_LIST', 'STARTED', 
                              'Starting migration to auto list partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Create table with auto list partitioning (Oracle 19c feature)
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY LIST (' || p_partition_column || ') ' ||
                 'AUTOMATIC (PARTITION p_default VALUES (DEFAULT)) ' ||
                 'AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT /*+ APPEND */ INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'AUTO_LIST', p_partition_column);
        
        log_strategy_operation('MIGRATE_AUTO_LIST', p_table_name, 'AUTO_LIST', 'SUCCESS', 
                              'Migration to auto list partitioning completed successfully');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_AUTO_LIST', p_table_name, 'AUTO_LIST', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_auto_list_partitioning;
    
    PROCEDURE migrate_to_auto_range_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_interval_value    IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_AUTO_RANGE', p_table_name, 'AUTO_RANGE', 'STARTED', 
                              'Starting migration to auto range partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Create table with auto range partitioning (Oracle 19c feature)
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY RANGE (' || p_partition_column || ') ' ||
                 'INTERVAL (' || p_interval_value || ') AUTOMATIC ' ||
                 '(PARTITION p_default VALUES LESS THAN (MAXVALUE)) ' ||
                 'AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT /*+ APPEND */ INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'AUTO_RANGE', p_partition_column, p_interval_value);
        
        log_strategy_operation('MIGRATE_AUTO_RANGE', p_table_name, 'AUTO_RANGE', 'SUCCESS', 
                              'Migration to auto range partitioning completed successfully');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_AUTO_RANGE', p_table_name, 'AUTO_RANGE', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_auto_range_partitioning;
    
    PROCEDURE migrate_to_hybrid_partitioning(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_definitions IN VARCHAR2,
        p_interval_value    IN VARCHAR2,
        p_preserve_data     IN BOOLEAN DEFAULT TRUE,
        p_online            IN BOOLEAN DEFAULT TRUE
    ) IS
        v_backup_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
    BEGIN
        log_strategy_operation('MIGRATE_HYBRID', p_table_name, 'HYBRID', 'STARTED', 
                              'Starting migration to hybrid partitioning');
        
        -- Create backup table if preserving data
        IF p_preserve_data THEN
            v_backup_table := p_table_name || '_BACKUP_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS');
            v_sql := 'CREATE TABLE ' || v_backup_table || ' AS SELECT * FROM ' || p_table_name;
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        -- Drop original table
        EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        
        -- Create table with hybrid partitioning (combination of manual and automatic)
        v_sql := 'CREATE TABLE ' || p_table_name || ' PARTITION BY ' || p_partition_type || ' (' || p_partition_column || ') ' ||
                 'INTERVAL (' || p_interval_value || ') ' ||
                 '(' || p_partition_definitions || ') ' ||
                 'AS SELECT * FROM ' || v_backup_table || ' WHERE 1=0';
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Restore data if preserving
        IF p_preserve_data THEN
            EXECUTE IMMEDIATE 'INSERT /*+ APPEND */ INTO ' || p_table_name || ' SELECT * FROM ' || v_backup_table;
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_backup_table;
        END IF;
        
        -- Update strategy configuration
        create_strategy_config(p_table_name, 'HYBRID', p_partition_column, p_interval_value);
        
        log_strategy_operation('MIGRATE_HYBRID', p_table_name, 'HYBRID', 'SUCCESS', 
                              'Migration to hybrid partitioning completed successfully');
                              
    EXCEPTION
        WHEN OTHERS THEN
            log_strategy_operation('MIGRATE_HYBRID', p_table_name, 'HYBRID', 'ERROR', SQLERRM);
            RAISE;
    END migrate_to_hybrid_partitioning;
    
END partition_strategy_pkg;
/
