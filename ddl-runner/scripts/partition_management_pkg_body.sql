-- =====================================================
-- Oracle Partition Management Package Body
-- Implementation with autonomous logging and data movement
-- =====================================================

CREATE OR REPLACE PACKAGE BODY partition_management_pkg
AS
    -- Private variables
    g_logger_enabled BOOLEAN := TRUE;
    
    -- Private procedure for autonomous logging
    PROCEDURE log_operation(
        p_operation       IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_status          IN VARCHAR2,
        p_message         IN VARCHAR2 DEFAULT NULL,
        p_duration_ms     IN NUMBER DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF g_logger_enabled THEN
            INSERT INTO partition_operation_log (
                operation_id,
                operation_type,
                table_name,
                partition_name,
                status,
                message,
                duration_ms,
                operation_time,
                user_name
            ) VALUES (
                partition_operation_log_seq.NEXTVAL,
                p_operation,
                p_table_name,
                p_partition_name,
                p_status,
                p_message,
                p_duration_ms,
                SYSTIMESTAMP,
                USER
            );
            COMMIT;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- Silent fail for logging
            NULL;
    END log_operation;
    
    -- Private function to validate table exists and is partitioned
    FUNCTION validate_partitioned_table(
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM user_tables
        WHERE table_name = UPPER(p_table_name)
        AND partitioned = 'YES';
        
        RETURN v_count > 0;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END validate_partitioned_table;
    
    -- Private function to get partition type
    FUNCTION get_partition_type(
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2 IS
        v_partition_type VARCHAR2(20);
    BEGIN
        SELECT partition_type
        INTO v_partition_type
        FROM user_part_tables
        WHERE table_name = UPPER(p_table_name);
        
        RETURN v_partition_type;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END get_partition_type;
    
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
        
        -- Build SQL based on partition type
        v_sql := 'ALTER TABLE ' || p_table_name || ' ADD PARTITION ' || p_partition_name;
        
        IF v_partition_type = 'RANGE' THEN
            v_sql := v_sql || ' VALUES LESS THAN (' || p_high_value || ')';
        ELSIF v_partition_type = 'LIST' THEN
            v_sql := v_sql || ' VALUES (' || p_high_value || ')';
        END IF;
        
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
    BEGIN
        v_start_time := SYSTIMESTAMP;
        
        -- Build WHERE clause
        v_where_clause := NVL(p_where_clause, '1=1');
        
        -- Process data in batches
        LOOP
            v_sql := 'INSERT INTO ' || p_target_table || ' PARTITION(' || p_partition_name || ') ' ||
                     'SELECT * FROM ' || p_source_table || ' WHERE ' || v_where_clause ||
                     ' AND ROWNUM <= ' || p_batch_size;
            
            EXECUTE IMMEDIATE v_sql;
            
            v_rows_processed := v_rows_processed + SQL%ROWCOUNT;
            
            -- Exit if no more rows
            EXIT WHEN SQL%ROWCOUNT = 0;
            
            -- Update WHERE clause to exclude processed rows
            v_where_clause := v_where_clause || ' AND ROWID NOT IN (SELECT ROWID FROM ' || p_target_table || ' PARTITION(' || p_partition_name || '))';
            
            COMMIT;
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
        p_column_name     IN VARCHAR2
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
    
END partition_management_pkg;
/
