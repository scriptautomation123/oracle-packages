-- =====================================================
-- Oracle Table Operations Package Body - Simplified
-- Core table and partition operations without complexity overhead
-- Author: Principal Oracle Database Application Engineer
-- Version: 2.0 (Refactored)
-- =====================================================

CREATE OR REPLACE PACKAGE BODY table_ops_pkg
AS
    -- Private constants
    c_valid_partition_types CONSTANT VARCHAR2(100) := 'RANGE,LIST,HASH,INTERVAL,REFERENCE';
    
    -- Private procedure for logging
    PROCEDURE log_operation(
        p_operation_type IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_status         IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_operation_id   IN NUMBER DEFAULT NULL,
        p_duration_ms    IN NUMBER DEFAULT NULL
    ) IS
    BEGIN
        -- Use modern logging if available, otherwise silent fail
        BEGIN
            modern_logging_pkg.log_message(
                p_level => CASE WHEN p_status = 'ERROR' THEN 'ERROR' ELSE 'INFO' END,
                p_message => p_operation_type || ' on ' || p_table_name || ': ' || p_status || 
                           CASE WHEN p_message IS NOT NULL THEN ' - ' || p_message ELSE '' END,
                p_operation_id => p_operation_id,
                p_table_name => p_table_name
            );
        EXCEPTION
            WHEN OTHERS THEN
                NULL; -- Silent fail for logging
        END;
    END log_operation;
    
    -- Private utility functions
    FUNCTION validate_table_name(p_table_name IN VARCHAR2) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        IF p_table_name IS NULL OR LENGTH(TRIM(p_table_name)) = 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Table name cannot be null or empty');
        END IF;
        
        -- Check if table exists
        SELECT COUNT(*)
        INTO v_count
        FROM all_tables
        WHERE table_name = UPPER(TRIM(p_table_name))
        AND owner = USER;
        
        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'Table ' || p_table_name || ' does not exist');
        END IF;
        
        RETURN TRUE;
    END validate_table_name;
    
    FUNCTION validate_partition_name(p_partition_name IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        IF p_partition_name IS NULL OR LENGTH(TRIM(p_partition_name)) = 0 THEN
            RAISE_APPLICATION_ERROR(-20003, 'Partition name cannot be null or empty');
        END IF;
        
        -- Basic SQL injection prevention
        IF REGEXP_LIKE(p_partition_name, '[^A-Za-z0-9_$#]') THEN
            RAISE_APPLICATION_ERROR(-20004, 'Invalid characters in partition name');
        END IF;
        
        RETURN TRUE;
    END validate_partition_name;
    
    -- Core DDL Operations
    PROCEDURE create_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_high_value      IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
        v_tablespace_clause VARCHAR2(100) := '';
        v_operation_id NUMBER;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
    BEGIN
        -- Start operation logging
        BEGIN
            modern_logging_pkg.log_operation_start(
                p_operation_type => 'CREATE_PARTITION',
                p_table_name => p_table_name,
                p_partition_name => p_partition_name,
                p_attributes => modern_logging_pkg.create_attributes_json(
                    p_tablespace => p_tablespace
                ),
                p_operation_id => v_operation_id
            );
        EXCEPTION
            WHEN OTHERS THEN
                v_operation_id := NULL;
        END;
        
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) OR NOT validate_partition_name(p_partition_name) THEN
            RETURN;
        END IF;
        
        IF p_high_value IS NULL THEN
            RAISE_APPLICATION_ERROR(-20005, 'High value cannot be null');
        END IF;
        
        -- Check if partition already exists
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*)
            INTO v_count
            FROM all_tab_partitions
            WHERE table_name = UPPER(TRIM(p_table_name))
            AND partition_name = UPPER(TRIM(p_partition_name))
            AND table_owner = USER;
            
            IF v_count > 0 THEN
                RAISE_APPLICATION_ERROR(-20006, 'Partition ' || p_partition_name || ' already exists');
            END IF;
        END;
        
        -- Prepare tablespace clause
        IF p_tablespace IS NOT NULL THEN
            v_tablespace_clause := ' TABLESPACE ' || p_tablespace;
        END IF;
        
        -- Build and execute DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' ADD PARTITION ' || UPPER(TRIM(p_partition_name)) ||
                 ' VALUES LESS THAN (' || p_high_value || ')' ||
                 v_tablespace_clause;
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Oracle 19c: Automatically collect partition statistics after creation
        BEGIN
            collect_partition_stats_after_maintenance(
                p_table_name => p_table_name,
                p_partition_name => p_partition_name,
                p_auto_configure => TRUE
            );
        EXCEPTION
            WHEN OTHERS THEN
                -- Log warning but don't fail the main operation
                log_operation('STATS_COLLECTION_WARNING', p_table_name, 'WARNING', 
                            'Statistics collection failed: ' || SUBSTR(SQLERRM, 1, 100));
        END;
        
        -- Log successful completion
        IF v_operation_id IS NOT NULL THEN
            modern_logging_pkg.log_operation_end(
                p_operation_id => v_operation_id,
                p_status => 'SUCCESS',
                p_duration_ms => EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 86400000 +
                               EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 3600000 +
                               EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60000 +
                               EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000
            );
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            IF v_operation_id IS NOT NULL THEN
                modern_logging_pkg.log_operation_end(
                    p_operation_id => v_operation_id,
                    p_status => 'FAILED',
                    p_error_code => SQLCODE,
                    p_error_message => SQLERRM
                );
            END IF;
            RAISE_APPLICATION_ERROR(-20100, 'Error creating partition: ' || SQLERRM);
    END create_partition;
    
    PROCEDURE drop_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_update_indexes  IN BOOLEAN DEFAULT TRUE
    ) IS
        v_sql VARCHAR2(4000);
        v_index_clause VARCHAR2(50) := '';
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) OR NOT validate_partition_name(p_partition_name) THEN
            RETURN;
        END IF;
        
        -- Check if partition exists
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*)
            INTO v_count
            FROM all_tab_partitions
            WHERE table_name = UPPER(TRIM(p_table_name))
            AND partition_name = UPPER(TRIM(p_partition_name))
            AND table_owner = USER;
            
            IF v_count = 0 THEN
                RAISE_APPLICATION_ERROR(-20007, 'Partition ' || p_partition_name || ' does not exist');
            END IF;
        END;
        
        -- Prepare index update clause
        IF p_update_indexes THEN
            v_index_clause := ' UPDATE INDEXES';
        END IF;
        
        -- Build and execute DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' DROP PARTITION ' || UPPER(TRIM(p_partition_name)) ||
                 v_index_clause;
        
        EXECUTE IMMEDIATE v_sql;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20101, 'Error dropping partition: ' || SQLERRM);
    END drop_partition;
    
    PROCEDURE split_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_split_value     IN VARCHAR2,
        p_new_partition   IN VARCHAR2,
        p_tablespace      IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
        v_tablespace_clause VARCHAR2(100) := '';
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) OR 
           NOT validate_partition_name(p_partition_name) OR
           NOT validate_partition_name(p_new_partition) THEN
            RETURN;
        END IF;
        
        IF p_split_value IS NULL THEN
            RAISE_APPLICATION_ERROR(-20008, 'Split value cannot be null');
        END IF;
        
        -- Prepare tablespace clause
        IF p_tablespace IS NOT NULL THEN
            v_tablespace_clause := ' TABLESPACE ' || p_tablespace;
        END IF;
        
        -- Build and execute DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' SPLIT PARTITION ' || UPPER(TRIM(p_partition_name)) ||
                 ' AT (' || p_split_value || ')' ||
                 ' INTO (PARTITION ' || UPPER(TRIM(p_partition_name)) ||
                 ', PARTITION ' || UPPER(TRIM(p_new_partition)) ||
                 v_tablespace_clause || ')';
        
        EXECUTE IMMEDIATE v_sql;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20102, 'Error splitting partition: ' || SQLERRM);
    END split_partition;
    
    PROCEDURE merge_partitions(
        p_table_name      IN VARCHAR2,
        p_partition1      IN VARCHAR2,
        p_partition2      IN VARCHAR2,
        p_new_partition   IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
        v_target_partition VARCHAR2(128);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) OR 
           NOT validate_partition_name(p_partition1) OR
           NOT validate_partition_name(p_partition2) THEN
            RETURN;
        END IF;
        
        -- Determine target partition name
        v_target_partition := NVL(p_new_partition, p_partition1);
        
        -- Build and execute DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' MERGE PARTITIONS ' || UPPER(TRIM(p_partition1)) ||
                 ', ' || UPPER(TRIM(p_partition2)) ||
                 ' INTO PARTITION ' || UPPER(TRIM(v_target_partition));
        
        EXECUTE IMMEDIATE v_sql;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20103, 'Error merging partitions: ' || SQLERRM);
    END merge_partitions;
    
    PROCEDURE truncate_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) OR NOT validate_partition_name(p_partition_name) THEN
            RETURN;
        END IF;
        
        -- Build and execute DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' TRUNCATE PARTITION ' || UPPER(TRIM(p_partition_name));
        
        EXECUTE IMMEDIATE v_sql;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20104, 'Error truncating partition: ' || SQLERRM);
    END truncate_partition;
    
    -- Data Movement Operations
    PROCEDURE exchange_partition(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_external_table  IN VARCHAR2,
        p_validate        IN BOOLEAN DEFAULT TRUE
    ) IS
        v_sql VARCHAR2(4000);
        v_validation_clause VARCHAR2(50) := '';
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) OR NOT validate_partition_name(p_partition_name) THEN
            RETURN;
        END IF;
        
        IF p_external_table IS NULL THEN
            RAISE_APPLICATION_ERROR(-20009, 'External table name cannot be null');
        END IF;
        
        -- Prepare validation clause
        IF NOT p_validate THEN
            v_validation_clause := ' WITHOUT VALIDATION';
        END IF;
        
        -- Build and execute DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' EXCHANGE PARTITION ' || UPPER(TRIM(p_partition_name)) ||
                 ' WITH TABLE ' || UPPER(TRIM(p_external_table)) ||
                 v_validation_clause;
        
        EXECUTE IMMEDIATE v_sql;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20105, 'Error exchanging partition: ' || SQLERRM);
    END exchange_partition;
    
    -- Oracle 19c Online Conversion Operations
    PROCEDURE convert_to_partitioned(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_count   IN NUMBER DEFAULT 4,
        p_interval_expr     IN VARCHAR2 DEFAULT NULL,
        p_reference_table   IN VARCHAR2 DEFAULT NULL,
        p_parallel_degree   IN NUMBER DEFAULT 4
    ) IS
        v_sql VARCHAR2(4000);
        v_partition_clause VARCHAR2(2000);
        v_part_type VARCHAR2(20);
        v_index_clause VARCHAR2(1000) := '';
        v_operation_id NUMBER;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
    BEGIN
        -- Start operation logging
        BEGIN
            modern_logging_pkg.log_operation_start(
                p_operation_type => 'CONVERT_TO_PARTITIONED',
                p_table_name => p_table_name,
                p_attributes => modern_logging_pkg.create_attributes_json(
                    p_partition_type => p_partition_type,
                    p_partition_column => p_partition_column
                ),
                p_operation_id => v_operation_id
            );
        EXCEPTION
            WHEN OTHERS THEN
                v_operation_id := NULL;
        END;
        
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RETURN;
        END IF;
        
        v_part_type := UPPER(TRIM(p_partition_type));
        IF v_part_type NOT IN ('RANGE', 'LIST', 'HASH', 'INTERVAL', 'REFERENCE') THEN
            RAISE_APPLICATION_ERROR(-20010, 'Invalid partition type. Valid types: RANGE, LIST, HASH, INTERVAL, REFERENCE');
        END IF;
        
        IF p_partition_column IS NULL AND v_part_type != 'REFERENCE' THEN
            RAISE_APPLICATION_ERROR(-20011, 'Partition column cannot be null');
        END IF;
        
        -- Check if table is already partitioned
        IF is_partitioned(p_table_name) THEN
            RAISE_APPLICATION_ERROR(-20012, 'Table is already partitioned');
        END IF;
        
        -- Build partition clause based on type using Oracle 19c ALTER TABLE MODIFY ONLINE
        CASE v_part_type
            WHEN 'HASH' THEN
                v_partition_clause := 'PARTITION BY HASH (' || p_partition_column || ') PARTITIONS ' || p_partition_count;
                
            WHEN 'RANGE' THEN
                v_partition_clause := 'PARTITION BY RANGE (' || p_partition_column || ') ' ||
                                    '(PARTITION p_default VALUES LESS THAN (MAXVALUE))';
                
            WHEN 'LIST' THEN
                v_partition_clause := 'PARTITION BY LIST (' || p_partition_column || ') ' ||
                                    '(PARTITION p_default VALUES (DEFAULT))';
                
            WHEN 'INTERVAL' THEN
                IF p_interval_expr IS NULL THEN
                    RAISE_APPLICATION_ERROR(-20013, 'Interval expression required for INTERVAL partitioning');
                END IF;
                v_partition_clause := 'PARTITION BY RANGE (' || p_partition_column || ') ' ||
                                    'INTERVAL (' || p_interval_expr || ') ' ||
                                    '(PARTITION p1 VALUES LESS THAN (MAXVALUE))';
                
            WHEN 'REFERENCE' THEN
                IF p_reference_table IS NULL THEN
                    RAISE_APPLICATION_ERROR(-20014, 'Reference table required for REFERENCE partitioning');
                END IF;
                -- Find FK constraint to reference table
                DECLARE
                    v_constraint_name VARCHAR2(128);
                BEGIN
                    SELECT constraint_name INTO v_constraint_name
                    FROM all_constraints
                    WHERE table_name = UPPER(TRIM(p_table_name))
                    AND owner = USER
                    AND constraint_type = 'R'
                    AND r_constraint_name IN (
                        SELECT constraint_name FROM all_constraints
                        WHERE table_name = UPPER(TRIM(p_reference_table))
                        AND owner = USER
                        AND constraint_type IN ('P', 'U')
                    )
                    AND ROWNUM = 1;
                    
                    v_partition_clause := 'PARTITION BY REFERENCE (' || v_constraint_name || ')';
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        RAISE_APPLICATION_ERROR(-20015, 'No foreign key constraint found to reference table: ' || p_reference_table);
                END;
        END CASE;
        
        -- Build index update clause for all indexes
        FOR idx_rec IN (
            SELECT index_name
            FROM all_indexes
            WHERE table_name = UPPER(TRIM(p_table_name))
            AND owner = USER
            AND index_type != 'LOB'
            ORDER BY index_name
        ) LOOP
            IF v_index_clause IS NULL OR LENGTH(v_index_clause) = 0 THEN
                v_index_clause := 'UPDATE INDEXES (';
            ELSE
                v_index_clause := v_index_clause || ', ';
            END IF;
            v_index_clause := v_index_clause || idx_rec.index_name || ' GLOBAL';
        END LOOP;
        
        IF v_index_clause IS NOT NULL AND LENGTH(v_index_clause) > 0 THEN
            v_index_clause := v_index_clause || ')';
        END IF;
        
        -- Build and execute Oracle 19c ALTER TABLE MODIFY ONLINE statement
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' MODIFY ' || v_partition_clause ||
                 ' ONLINE';
        
        IF v_index_clause IS NOT NULL AND LENGTH(v_index_clause) > 0 THEN
            v_sql := v_sql || ' ' || v_index_clause;
        END IF;
        
        IF p_parallel_degree > 1 THEN
            v_sql := v_sql || ' PARALLEL ' || p_parallel_degree;
        END IF;
        
        log_operation('CONVERT_DDL', p_table_name, 'INFO', 'Executing: ' || v_sql);
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Auto-configure statistics for newly partitioned table
        BEGIN
            configure_table_stats_optimal(p_table_name, TRUE);
        EXCEPTION
            WHEN OTHERS THEN
                log_operation('STATS_CONFIG_WARNING', p_table_name, 'WARNING',
                            'Statistics configuration failed: ' || SUBSTR(SQLERRM, 1, 100));
        END;
        
        -- Log successful completion
        IF v_operation_id IS NOT NULL THEN
            modern_logging_pkg.log_operation_end(
                p_operation_id => v_operation_id,
                p_status => 'SUCCESS',
                p_duration_ms => EXTRACT(DAY FROM (SYSTIMESTAMP - v_start_time)) * 86400000 +
                               EXTRACT(HOUR FROM (SYSTIMESTAMP - v_start_time)) * 3600000 +
                               EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) * 60000 +
                               EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000
            );
        END IF;
        
        log_operation('CONVERT_TO_PARTITIONED', p_table_name, 'SUCCESS',
                     'Table converted to ' || v_part_type || ' partitioned online');
        
    EXCEPTION
        WHEN OTHERS THEN
            IF v_operation_id IS NOT NULL THEN
                modern_logging_pkg.log_operation_end(
                    p_operation_id => v_operation_id,
                    p_status => 'FAILED',
                    p_error_code => SQLCODE,
                    p_error_message => SQLERRM
                );
            END IF;
            RAISE_APPLICATION_ERROR(-20106, 'Error converting table to partitioned: ' || SQLERRM);
    END convert_to_partitioned;
    
    PROCEDURE enable_interval_partitioning(
        p_table_name      IN VARCHAR2,
        p_interval_expr   IN VARCHAR2
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RETURN;
        END IF;
        
        IF p_interval_expr IS NULL THEN
            RAISE_APPLICATION_ERROR(-20015, 'Interval expression cannot be null');
        END IF;
        
        -- Check if table is range partitioned
        IF get_partition_type(p_table_name) != 'RANGE' THEN
            RAISE_APPLICATION_ERROR(-20016, 'Table must be range partitioned to enable interval partitioning');
        END IF;
        
        -- Build and execute DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' SET INTERVAL (' || p_interval_expr || ')';
        
        EXECUTE IMMEDIATE v_sql;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20107, 'Error enabling interval partitioning: ' || SQLERRM);
    END enable_interval_partitioning;
    
    -- Essential Utilities
    FUNCTION get_partition_info(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL
    ) RETURN partition_info_tab PIPELINED IS
        v_partition_info partition_info_rec;
    BEGIN
        FOR rec IN (
            SELECT 
                p.table_name,
                p.partition_name,
                pt.partitioning_type as partition_type,
                p.high_value,
                p.tablespace_name,
                p.num_rows,
                ROUND(s.bytes/1024/1024, 2) as size_mb
            FROM all_tab_partitions p
            JOIN all_part_tables pt ON (p.table_name = pt.table_name AND p.table_owner = pt.owner)
            LEFT JOIN (
                SELECT segment_name, partition_name, SUM(bytes) as bytes
                FROM user_segments
                WHERE segment_type = 'TABLE PARTITION'
                GROUP BY segment_name, partition_name
            ) s ON (p.table_name = s.segment_name AND p.partition_name = s.partition_name)
            WHERE p.table_owner = USER
            AND p.table_name = UPPER(TRIM(p_table_name))
            AND (p_partition_name IS NULL OR p.partition_name = UPPER(TRIM(p_partition_name)))
            ORDER BY p.partition_position
        ) LOOP
            v_partition_info.table_name := rec.table_name;
            v_partition_info.partition_name := rec.partition_name;
            v_partition_info.partition_type := rec.partition_type;
            v_partition_info.high_value := rec.high_value;
            v_partition_info.tablespace_name := rec.tablespace_name;
            v_partition_info.num_rows := rec.num_rows;
            v_partition_info.size_mb := rec.size_mb;
            
            PIPE ROW(v_partition_info);
        END LOOP;
        
        RETURN;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20108, 'Error getting partition info: ' || SQLERRM);
    END get_partition_info;
    
    FUNCTION is_partitioned(p_table_name IN VARCHAR2) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM all_part_tables
        WHERE table_name = UPPER(TRIM(p_table_name))
        AND owner = USER;
        
        RETURN (v_count > 0);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END is_partitioned;
    
    FUNCTION get_partition_type(p_table_name IN VARCHAR2) RETURN VARCHAR2 IS
        v_partition_type VARCHAR2(20);
    BEGIN
        SELECT partitioning_type
        INTO v_partition_type
        FROM all_part_tables
        WHERE table_name = UPPER(TRIM(p_table_name))
        AND owner = USER;
        
        RETURN v_partition_type;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20109, 'Error getting partition type: ' || SQLERRM);
    END get_partition_type;
    
    PROCEDURE rebuild_partition_indexes(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RETURN;
        END IF;
        
        FOR idx_rec IN (
            SELECT 
                i.index_name,
                p.partition_name
            FROM all_ind_partitions p
            JOIN all_indexes i ON (p.index_name = i.index_name AND p.index_owner = i.owner)
            JOIN all_part_tables pt ON (i.table_name = pt.table_name AND i.table_owner = pt.owner)
            WHERE i.table_name = UPPER(TRIM(p_table_name))
            AND i.table_owner = USER
            AND (p_partition_name IS NULL OR p.partition_name = UPPER(TRIM(p_partition_name)))
        ) LOOP
            v_sql := 'ALTER INDEX ' || idx_rec.index_name || 
                     ' REBUILD PARTITION ' || idx_rec.partition_name || ' ONLINE';
            
            BEGIN
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    -- Log the error but continue with other indexes
                    DBMS_OUTPUT.PUT_LINE('Warning: Failed to rebuild index ' || 
                                       idx_rec.index_name || ' partition ' || 
                                       idx_rec.partition_name || ': ' || SQLERRM);
            END;
        END LOOP;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20110, 'Error rebuilding partition indexes: ' || SQLERRM);
    END rebuild_partition_indexes;
    
    -- Simple cleanup
    PROCEDURE drop_old_partitions(
        p_table_name      IN VARCHAR2,
        p_retention_days  IN NUMBER
    ) IS
        v_cutoff_date DATE;
        v_sql VARCHAR2(4000);
        v_dropped_count NUMBER := 0;
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RETURN;
        END IF;
        
        IF p_retention_days IS NULL OR p_retention_days <= 0 THEN
            RAISE_APPLICATION_ERROR(-20017, 'Retention days must be a positive number');
        END IF;
        
        v_cutoff_date := SYSDATE - p_retention_days;
        
        -- Only works for date-based range partitions
        FOR part_rec IN (
            SELECT partition_name, high_value
            FROM all_tab_partitions
            WHERE table_name = UPPER(TRIM(p_table_name))
            AND table_owner = USER
            ORDER BY partition_position
        ) LOOP
            -- Simple check for date-based partitions
            -- In production, you'd need more sophisticated date parsing
            BEGIN
                DECLARE
                    v_high_value_date DATE;
                    v_date_sql VARCHAR2(1000);
                BEGIN
                    -- Attempt to convert high_value to date
                    v_date_sql := 'SELECT ' || part_rec.high_value || ' FROM DUAL';
                    EXECUTE IMMEDIATE v_date_sql INTO v_high_value_date;
                    
                    IF v_high_value_date < v_cutoff_date THEN
                        drop_partition(p_table_name, part_rec.partition_name, TRUE);
                        v_dropped_count := v_dropped_count + 1;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Skip non-date partitions
                        NULL;
                END;
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('Dropped ' || v_dropped_count || ' old partitions from ' || p_table_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20111, 'Error dropping old partitions: ' || SQLERRM);
    END drop_old_partitions;
    
    -- Oracle 19c+ Online Operations
    PROCEDURE move_table_online(
        p_table_name      IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RETURN;
        END IF;
        
        IF p_new_tablespace IS NULL THEN
            RAISE_APPLICATION_ERROR(-20018, 'New tablespace cannot be null');
        END IF;
        
        -- Build and execute online move DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' MOVE ONLINE TABLESPACE ' || UPPER(TRIM(p_new_tablespace));
        
        IF p_parallel_degree > 1 THEN
            v_sql := v_sql || ' PARALLEL ' || p_parallel_degree;
        END IF;
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Rebuild indexes online in parallel
        FOR idx_rec IN (
            SELECT index_name
            FROM all_indexes
            WHERE table_name = UPPER(TRIM(p_table_name))
            AND table_owner = USER
            AND index_type != 'LOB'
        ) LOOP
            BEGIN
                v_sql := 'ALTER INDEX ' || idx_rec.index_name || ' REBUILD ONLINE';
                IF p_parallel_degree > 1 THEN
                    v_sql := v_sql || ' PARALLEL ' || p_parallel_degree;
                END IF;
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Warning: Failed to rebuild index ' || 
                                       idx_rec.index_name || ': ' || SQLERRM);
            END;
        END LOOP;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20112, 'Error moving table online: ' || SQLERRM);
    END move_table_online;
    
    PROCEDURE move_partition_online(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) OR NOT validate_partition_name(p_partition_name) THEN
            RETURN;
        END IF;
        
        IF p_new_tablespace IS NULL THEN
            RAISE_APPLICATION_ERROR(-20019, 'New tablespace cannot be null');
        END IF;
        
        -- Build and execute online move partition DDL
        v_sql := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                 ' MOVE PARTITION ' || UPPER(TRIM(p_partition_name)) ||
                 ' ONLINE TABLESPACE ' || UPPER(TRIM(p_new_tablespace));
        
        IF p_parallel_degree > 1 THEN
            v_sql := v_sql || ' PARALLEL ' || p_parallel_degree;
        END IF;
        
        EXECUTE IMMEDIATE v_sql;
        
        -- Rebuild partition indexes online
        FOR idx_rec IN (
            SELECT i.index_name, ip.partition_name
            FROM all_indexes i
            JOIN all_ind_partitions ip ON (i.index_name = ip.index_name AND i.owner = ip.index_owner)
            WHERE i.table_name = UPPER(TRIM(p_table_name))
            AND i.table_owner = USER
            AND ip.partition_name = UPPER(TRIM(p_partition_name))
        ) LOOP
            BEGIN
                v_sql := 'ALTER INDEX ' || idx_rec.index_name || 
                         ' REBUILD PARTITION ' || idx_rec.partition_name || ' ONLINE';
                IF p_parallel_degree > 1 THEN
                    v_sql := v_sql || ' PARALLEL ' || p_parallel_degree;
                END IF;
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Warning: Failed to rebuild index partition ' || 
                                       idx_rec.index_name || '.' || idx_rec.partition_name || 
                                       ': ' || SQLERRM);
            END;
        END LOOP;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20113, 'Error moving partition online: ' || SQLERRM);
    END move_partition_online;
    
    -- DDL Generation Functions
    FUNCTION generate_partition_ddl(
        p_operation_type  IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_parameters      IN VARCHAR2 DEFAULT NULL
    ) RETURN CLOB IS
        v_ddl CLOB;
        v_operation VARCHAR2(20);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RETURN NULL;
        END IF;
        
        v_operation := UPPER(TRIM(p_operation_type));
        
        -- Generate DDL based on operation type
        CASE v_operation
            WHEN 'CREATE' THEN
                v_ddl := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                         ' ADD PARTITION ' || UPPER(TRIM(p_partition_name));
                IF p_parameters IS NOT NULL THEN
                    v_ddl := v_ddl || ' ' || p_parameters;
                END IF;
                
            WHEN 'DROP' THEN
                v_ddl := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                         ' DROP PARTITION ' || UPPER(TRIM(p_partition_name));
                IF p_parameters IS NOT NULL THEN
                    v_ddl := v_ddl || ' ' || p_parameters;
                END IF;
                
            WHEN 'SPLIT' THEN
                v_ddl := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                         ' SPLIT PARTITION ' || UPPER(TRIM(p_partition_name));
                IF p_parameters IS NOT NULL THEN
                    v_ddl := v_ddl || ' ' || p_parameters;
                END IF;
                
            WHEN 'MERGE' THEN
                v_ddl := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                         ' MERGE PARTITIONS ' || UPPER(TRIM(p_partition_name));
                IF p_parameters IS NOT NULL THEN
                    v_ddl := v_ddl || ' ' || p_parameters;
                END IF;
                
            WHEN 'TRUNCATE' THEN
                v_ddl := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                         ' TRUNCATE PARTITION ' || UPPER(TRIM(p_partition_name));
                
            ELSE
                RAISE_APPLICATION_ERROR(-20020, 'Invalid operation type: ' || p_operation_type);
        END CASE;
        
        v_ddl := v_ddl || ';';
        RETURN v_ddl;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20114, 'Error generating partition DDL: ' || SQLERRM);
    END generate_partition_ddl;
    
    FUNCTION generate_move_table_ddl(
        p_table_name      IN VARCHAR2,
        p_new_tablespace  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    ) RETURN CLOB IS
        v_ddl CLOB;
        v_step_ddl VARCHAR2(4000);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_table_name) THEN
            RETURN NULL;
        END IF;
        
        v_ddl := '-- Move Table DDL Script' || CHR(10);
        v_ddl := v_ddl || '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10);
        v_ddl := v_ddl || '-- Table: ' || UPPER(TRIM(p_table_name)) || CHR(10);
        v_ddl := v_ddl || '-- Target Tablespace: ' || UPPER(TRIM(p_new_tablespace)) || CHR(10) || CHR(10);
        
        -- Step 1: Move table
        v_step_ddl := 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) ||
                      ' MOVE ONLINE TABLESPACE ' || UPPER(TRIM(p_new_tablespace));
        IF p_parallel_degree > 1 THEN
            v_step_ddl := v_step_ddl || ' PARALLEL ' || p_parallel_degree;
        END IF;
        v_ddl := v_ddl || '-- Step 1: Move table' || CHR(10) || v_step_ddl || ';' || CHR(10) || CHR(10);
        
        -- Step 2: Rebuild indexes
        v_ddl := v_ddl || '-- Step 2: Rebuild indexes' || CHR(10);
        FOR idx_rec IN (
            SELECT index_name
            FROM all_indexes
            WHERE table_name = UPPER(TRIM(p_table_name))
            AND table_owner = USER
            AND index_type != 'LOB'
        ) LOOP
            v_step_ddl := 'ALTER INDEX ' || idx_rec.index_name || ' REBUILD ONLINE';
            IF p_parallel_degree > 1 THEN
                v_step_ddl := v_step_ddl || ' PARALLEL ' || p_parallel_degree;
            END IF;
            v_ddl := v_ddl || v_step_ddl || ';' || CHR(10);
        END LOOP;
        
        -- Step 3: Gather statistics
        v_ddl := v_ddl || CHR(10) || '-- Step 3: Gather statistics' || CHR(10);
        v_ddl := v_ddl || 'BEGIN' || CHR(10);
        v_ddl := v_ddl || '    DBMS_STATS.GATHER_TABLE_STATS(' || CHR(10);
        v_ddl := v_ddl || '        ownname => USER,' || CHR(10);
        v_ddl := v_ddl || '        tabname => ''' || UPPER(TRIM(p_table_name)) || ''',' || CHR(10);
        v_ddl := v_ddl || '        degree => ' || p_parallel_degree || ',' || CHR(10);
        v_ddl := v_ddl || '        cascade => TRUE' || CHR(10);
        v_ddl := v_ddl || '    );' || CHR(10);
        v_ddl := v_ddl || 'END;' || CHR(10) || '/';
        
        RETURN v_ddl;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20115, 'Error generating move table DDL: ' || SQLERRM);
    END generate_move_table_ddl;
    
    FUNCTION generate_convert_to_partitioned_ddl(
        p_table_name        IN VARCHAR2,
        p_partition_type    IN VARCHAR2,
        p_partition_column  IN VARCHAR2,
        p_partition_count   IN NUMBER DEFAULT 4,
        p_interval_expr     IN VARCHAR2 DEFAULT NULL,
        p_reference_table   IN VARCHAR2 DEFAULT NULL,
        p_parallel_degree   IN NUMBER DEFAULT 4
    ) RETURN CLOB IS
        v_ddl CLOB;
        v_partition_clause VARCHAR2(2000);
        v_part_type VARCHAR2(20);
        v_index_clause VARCHAR2(1000) := '';
    BEGIN
        v_ddl := '-- Convert to Partitioned Table DDL Script' || CHR(10);
        v_ddl := v_ddl || '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10);
        v_ddl := v_ddl || '-- Table: ' || UPPER(TRIM(p_table_name)) || CHR(10);
        v_ddl := v_ddl || '-- Partition Type: ' || UPPER(TRIM(p_partition_type)) || CHR(10);
        v_ddl := v_ddl || '-- Partition Column: ' || p_partition_column || CHR(10) || CHR(10);
        
        v_part_type := UPPER(TRIM(p_partition_type));
        
        -- Build partition clause
        CASE v_part_type
            WHEN 'HASH' THEN
                v_partition_clause := 'PARTITION BY HASH (' || p_partition_column || ') PARTITIONS ' || p_partition_count;
                
            WHEN 'RANGE' THEN
                v_partition_clause := 'PARTITION BY RANGE (' || p_partition_column || ') ' ||
                                    '(PARTITION p_default VALUES LESS THAN (MAXVALUE))';
                
            WHEN 'LIST' THEN
                v_partition_clause := 'PARTITION BY LIST (' || p_partition_column || ') ' ||
                                    '(PARTITION p_default VALUES (DEFAULT))';
                
            WHEN 'INTERVAL' THEN
                v_partition_clause := 'PARTITION BY RANGE (' || p_partition_column || ') ' ||
                                    'INTERVAL (' || NVL(p_interval_expr, 'NUMTOYMINTERVAL(1,''MONTH'')') || ') ' ||
                                    '(PARTITION p1 VALUES LESS THAN (MAXVALUE))';
                
            WHEN 'REFERENCE' THEN
                DECLARE
                    v_constraint_name VARCHAR2(128);
                BEGIN
                    SELECT constraint_name INTO v_constraint_name
                    FROM all_constraints
                    WHERE table_name = UPPER(TRIM(p_table_name))
                    AND owner = USER
                    AND constraint_type = 'R'
                    AND r_constraint_name IN (
                        SELECT constraint_name FROM all_constraints
                        WHERE table_name = UPPER(TRIM(p_reference_table))
                        AND owner = USER
                        AND constraint_type IN ('P', 'U')
                    )
                    AND ROWNUM = 1;
                    
                    v_partition_clause := 'PARTITION BY REFERENCE (' || v_constraint_name || ')';
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_partition_clause := 'PARTITION BY REFERENCE (<FK_CONSTRAINT_NAME>)';
                END;
                
            ELSE
                v_partition_clause := '-- Invalid partition type: ' || v_part_type;
        END CASE;
        
        -- Build index clause
        FOR idx_rec IN (
            SELECT index_name
            FROM all_indexes
            WHERE table_name = UPPER(TRIM(p_table_name))
            AND owner = USER
            AND index_type != 'LOB'
            ORDER BY index_name
        ) LOOP
            IF v_index_clause IS NULL OR LENGTH(v_index_clause) = 0 THEN
                v_index_clause := 'UPDATE INDEXES (';
            ELSE
                v_index_clause := v_index_clause || ', ';
            END IF;
            v_index_clause := v_index_clause || idx_rec.index_name || ' GLOBAL';
        END LOOP;
        
        IF v_index_clause IS NOT NULL AND LENGTH(v_index_clause) > 0 THEN
            v_index_clause := v_index_clause || ')';
        END IF;
        
        -- Generate main DDL
        v_ddl := v_ddl || '-- Step 1: Convert table to partitioned (ONLINE)' || CHR(10);
        v_ddl := v_ddl || 'ALTER TABLE ' || UPPER(TRIM(p_table_name)) || CHR(10);
        v_ddl := v_ddl || '  MODIFY ' || v_partition_clause || CHR(10);
        v_ddl := v_ddl || '  ONLINE' || CHR(10);
        
        IF v_index_clause IS NOT NULL AND LENGTH(v_index_clause) > 0 THEN
            v_ddl := v_ddl || '  ' || v_index_clause || CHR(10);
        END IF;
        
        IF p_parallel_degree > 1 THEN
            v_ddl := v_ddl || '  PARALLEL ' || p_parallel_degree || CHR(10);
        END IF;
        
        v_ddl := v_ddl || ';' || CHR(10) || CHR(10);
        
        -- Add statistics collection
        v_ddl := v_ddl || '-- Step 2: Configure optimal statistics' || CHR(10);
        v_ddl := v_ddl || 'BEGIN' || CHR(10);
        v_ddl := v_ddl || '    table_ops_pkg.configure_table_stats_optimal(' || CHR(10);
        v_ddl := v_ddl || '        p_table_name => ''' || UPPER(TRIM(p_table_name)) || ''',' || CHR(10);
        v_ddl := v_ddl || '        p_enable_incremental => TRUE' || CHR(10);
        v_ddl := v_ddl || '    );' || CHR(10);
        v_ddl := v_ddl || 'END;' || CHR(10);
        v_ddl := v_ddl || '/' || CHR(10);
        
        RETURN v_ddl;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20118, 'Error generating convert to partitioned DDL: ' || SQLERRM);
    END generate_convert_to_partitioned_ddl;
    
    -- Simple migration utilities
    PROCEDURE copy_table_structure(
        p_source_table    IN VARCHAR2,
        p_target_table    IN VARCHAR2,
        p_include_indexes IN BOOLEAN DEFAULT TRUE
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_source_table) THEN
            RETURN;
        END IF;
        
        IF p_target_table IS NULL THEN
            RAISE_APPLICATION_ERROR(-20021, 'Target table name cannot be null');
        END IF;
        
        -- Create table structure
        v_sql := 'CREATE TABLE ' || UPPER(TRIM(p_target_table)) || 
                 ' AS SELECT * FROM ' || UPPER(TRIM(p_source_table)) || 
                 ' WHERE 1=0';
        EXECUTE IMMEDIATE v_sql;
        
        -- Copy indexes if requested
        IF p_include_indexes THEN
            FOR idx_rec IN (
                SELECT index_name, uniqueness, 
                       LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_position) as columns
                FROM all_indexes i
                JOIN all_ind_columns ic USING (index_name, index_owner)
                WHERE i.table_name = UPPER(TRIM(p_source_table))
                AND i.table_owner = USER
                AND i.index_type != 'LOB'
                AND i.generated = 'N'
                GROUP BY index_name, uniqueness
            ) LOOP
                BEGIN
                    v_sql := 'CREATE ';
                    IF idx_rec.uniqueness = 'UNIQUE' THEN
                        v_sql := v_sql || 'UNIQUE ';
                    END IF;
                    v_sql := v_sql || 'INDEX ' || idx_rec.index_name || '_NEW ON ' ||
                             UPPER(TRIM(p_target_table)) || ' (' || idx_rec.columns || ')';
                    EXECUTE IMMEDIATE v_sql;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('Warning: Failed to create index ' || 
                                           idx_rec.index_name || '_NEW: ' || SQLERRM);
                END;
            END LOOP;
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20116, 'Error copying table structure: ' || SQLERRM);
    END copy_table_structure;
    
    PROCEDURE copy_partition_data(
        p_source_table    IN VARCHAR2,
        p_target_table    IN VARCHAR2,
        p_partition_name  IN VARCHAR2,
        p_parallel_degree IN NUMBER DEFAULT 4
    ) IS
        v_sql VARCHAR2(4000);
        v_rows_copied NUMBER;
    BEGIN
        -- Validate inputs
        IF NOT validate_table_name(p_source_table) OR NOT validate_partition_name(p_partition_name) THEN
            RETURN;
        END IF;
        
        IF p_target_table IS NULL THEN
            RAISE_APPLICATION_ERROR(-20022, 'Target table name cannot be null');
        END IF;
        
        -- Copy partition data with parallel hints
        v_sql := 'INSERT /*+ APPEND PARALLEL(' || UPPER(TRIM(p_target_table)) || ', ' || p_parallel_degree || ') */' ||
                 ' INTO ' || UPPER(TRIM(p_target_table)) ||
                 ' SELECT /*+ PARALLEL(' || UPPER(TRIM(p_source_table)) || ', ' || p_parallel_degree || ') */' ||
                 ' * FROM ' || UPPER(TRIM(p_source_table)) ||
                 ' PARTITION (' || UPPER(TRIM(p_partition_name)) || ')';
        
        EXECUTE IMMEDIATE v_sql;
        v_rows_copied := SQL%ROWCOUNT;
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Copied ' || v_rows_copied || ' rows from partition ' || p_partition_name);
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE_APPLICATION_ERROR(-20117, 'Error copying partition data: ' || SQLERRM);
    END copy_partition_data;
    
    -- Oracle 19c Statistics Integration Implementation
    PROCEDURE collect_partition_stats_after_maintenance(
        p_table_name      IN VARCHAR2,
        p_partition_name  IN VARCHAR2 DEFAULT NULL,
        p_auto_configure  IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_degree NUMBER;
    BEGIN
        log_operation('STATS_COLLECTION_START', p_table_name, 'STARTED', 
                     'Collecting statistics for ' || NVL(p_partition_name, 'ALL PARTITIONS'));
        
        -- Auto-configure table for optimal stats if requested
        IF p_auto_configure THEN
            configure_table_stats_optimal(p_table_name, TRUE);
        END IF;
        
        -- Determine optimal degree based on table size
        BEGIN
            SELECT CASE 
                       WHEN NVL(num_rows, 0) < 1000000 THEN 2
                       WHEN NVL(num_rows, 0) < 10000000 THEN 4
                       WHEN NVL(num_rows, 0) < 100000000 THEN 8
                       ELSE 16
                   END INTO v_degree
            FROM all_tables 
            WHERE owner = USER AND table_name = UPPER(p_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                v_degree := 4; -- Default degree
        END;
        
        -- Collect statistics using Oracle 19c best practices
        IF p_partition_name IS NOT NULL THEN
            -- Specific partition statistics
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname          => USER,
                tabname          => p_table_name,
                partname         => p_partition_name,
                estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                degree           => v_degree,
                granularity      => 'PARTITION',
                cascade          => TRUE,
                no_invalidate    => FALSE
            );
            
            -- Refresh global stats (fast with incremental)
            BEGIN
                DBMS_STATS.GATHER_TABLE_STATS(
                    ownname     => USER,
                    tabname     => p_table_name,
                    granularity => 'GLOBAL',
                    degree      => v_degree
                );
            EXCEPTION
                WHEN OTHERS THEN NULL; -- Global refresh might not be needed
            END;
        ELSE
            -- Comprehensive table statistics with Oracle 19c concurrent collection
            BEGIN
                DBMS_STATS.GATHER_TABLE_STATS(
                    ownname     => USER,
                    tabname     => p_table_name,
                    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                    degree      => v_degree,
                    granularity => 'ALL',
                    cascade     => TRUE,
                    concurrent  => TRUE, -- Oracle 19c concurrent collection
                    no_invalidate => FALSE
                );
            EXCEPTION
                WHEN OTHERS THEN
                    -- Fallback without concurrent if not supported
                    DBMS_STATS.GATHER_TABLE_STATS(
                        ownname     => USER,
                        tabname     => p_table_name,
                        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                        degree      => v_degree,
                        granularity => 'ALL',
                        cascade     => TRUE,
                        no_invalidate => FALSE
                    );
            END;
        END IF;
        
        log_operation('STATS_COLLECTION_COMPLETE', p_table_name, 'SUCCESS',
                     'Statistics collected in ' || 
                     EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) || ' seconds');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('STATS_COLLECTION_ERROR', p_table_name, 'ERROR',
                         'Statistics collection failed: ' || SQLERRM);
            -- Don't raise error - statistics failure shouldn't break main operation
            DBMS_OUTPUT.PUT_LINE('Warning: Statistics collection failed: ' || SQLERRM);
    END collect_partition_stats_after_maintenance;
    
    PROCEDURE configure_table_stats_optimal(
        p_table_name      IN VARCHAR2,
        p_enable_incremental IN BOOLEAN DEFAULT TRUE
    ) IS
    BEGIN
        log_operation('CONFIGURE_STATS_START', p_table_name, 'STARTED', 
                     'Configuring table for optimal statistics collection');
        
        -- Enable incremental statistics (Oracle 19c default for partitioned tables)
        IF p_enable_incremental THEN
            BEGIN
                DBMS_STATS.SET_TABLE_PREFS(
                    ownname => USER,
                    tabname => p_table_name,
                    pname   => 'INCREMENTAL',
                    pvalue  => 'TRUE'
                );
            EXCEPTION
                WHEN OTHERS THEN NULL; -- Might not be supported in all versions
            END;
        END IF;
        
        -- Set publish preference
        BEGIN
            DBMS_STATS.SET_TABLE_PREFS(
                ownname => USER,
                tabname => p_table_name,
                pname   => 'PUBLISH',
                pvalue  => 'TRUE'
            );
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
        
        -- Set auto sample size
        BEGIN
            DBMS_STATS.SET_TABLE_PREFS(
                ownname => USER,
                tabname => p_table_name,
                pname   => 'ESTIMATE_PERCENT',
                pvalue  => 'DBMS_STATS.AUTO_SAMPLE_SIZE'
            );
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
        
        -- Oracle 19c: Enable concurrent collection if available
        BEGIN
            DBMS_STATS.SET_TABLE_PREFS(
                ownname => USER,
                tabname => p_table_name,
                pname   => 'CONCURRENT',
                pvalue  => 'TRUE'
            );
        EXCEPTION
            WHEN OTHERS THEN NULL; -- Might not be available in all versions
        END;
        
        log_operation('CONFIGURE_STATS_COMPLETE', p_table_name, 'SUCCESS',
                     'Table configured for optimal statistics collection');
        
    EXCEPTION
        WHEN OTHERS THEN
            log_operation('CONFIGURE_STATS_ERROR', p_table_name, 'ERROR',
                         'Statistics configuration failed: ' || SQLERRM);
            -- Don't raise error - configuration failure shouldn't break main operation
            DBMS_OUTPUT.PUT_LINE('Warning: Statistics configuration failed: ' || SQLERRM);
    END configure_table_stats_optimal;
    
END table_ops_pkg;
/