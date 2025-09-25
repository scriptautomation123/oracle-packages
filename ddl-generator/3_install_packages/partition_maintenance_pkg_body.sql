-- =====================================================
-- Oracle Partition Maintenance Package Body
-- Automated partition maintenance with scheduling
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Body
CREATE OR REPLACE PACKAGE BODY partition_maintenance_pkg
AS
    -- Private procedure for logging using centralized logger
    PROCEDURE log_maintenance_operation(
        p_operation       IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_job_id          IN NUMBER DEFAULT NULL,
        p_status          IN VARCHAR2,
        p_message         IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        partition_logger_pkg.log_operation(
            p_operation_type => 'MAINTENANCE_' || p_operation,
            p_table_name => p_table_name,
            p_partition_name => TO_CHAR(p_job_id),
            p_status => p_status,
            p_message => p_message
        );
    EXCEPTION
        WHEN OTHERS THEN
            NULL;
    END log_maintenance_operation;
    
    -- Job management procedures
    PROCEDURE create_maintenance_job(
        p_table_name     IN VARCHAR2,
        p_job_type       IN VARCHAR2,
        p_schedule_type  IN VARCHAR2,
        p_schedule_value IN VARCHAR2 DEFAULT NULL
    ) IS
        v_next_run DATE;
    BEGIN
        -- Calculate next run time
        CASE p_schedule_type
            WHEN 'DAILY' THEN
                v_next_run := TRUNC(SYSDATE) + 1;
            WHEN 'WEEKLY' THEN
                v_next_run := TRUNC(SYSDATE) + 7;
            WHEN 'MONTHLY' THEN
                v_next_run := ADD_MONTHS(TRUNC(SYSDATE), 1);
            ELSE
                v_next_run := SYSDATE + 1;
        END CASE;
        
        INSERT INTO partition_maintenance_jobs (
            table_name,
            job_type,
            schedule_type,
            schedule_value,
            next_run,
            created_by
        ) VALUES (
            UPPER(p_table_name),
            UPPER(p_job_type),
            UPPER(p_schedule_type),
            p_schedule_value,
            v_next_run,
            USER
        );
        
        log_maintenance_operation('CREATE_JOB', p_table_name, NULL, 'SUCCESS', 
                                 'Maintenance job created');
                                 
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('CREATE_JOB', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END create_maintenance_job;
    
    PROCEDURE update_maintenance_job(
        p_job_id         IN NUMBER,
        p_schedule_type  IN VARCHAR2 DEFAULT NULL,
        p_schedule_value IN VARCHAR2 DEFAULT NULL,
        p_is_active      IN BOOLEAN DEFAULT NULL
    ) IS
        v_next_run DATE;
    BEGIN
        -- Calculate next run time if schedule changed
        IF p_schedule_type IS NOT NULL THEN
            CASE p_schedule_type
                WHEN 'DAILY' THEN
                    v_next_run := TRUNC(SYSDATE) + 1;
                WHEN 'WEEKLY' THEN
                    v_next_run := TRUNC(SYSDATE) + 7;
                WHEN 'MONTHLY' THEN
                    v_next_run := ADD_MONTHS(TRUNC(SYSDATE), 1);
                ELSE
                    v_next_run := SYSDATE + 1;
            END CASE;
        END IF;
        
        UPDATE partition_maintenance_jobs
        SET schedule_type = NVL(p_schedule_type, schedule_type),
            schedule_value = NVL(p_schedule_value, schedule_value),
            is_active = NVL(CASE WHEN p_is_active THEN 'Y' ELSE 'N' END, is_active),
            next_run = NVL(v_next_run, next_run)
        WHERE job_id = p_job_id;
        
        log_maintenance_operation('UPDATE_JOB', 'SYSTEM', p_job_id, 'SUCCESS', 
                                 'Maintenance job updated');
                                 
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('UPDATE_JOB', 'SYSTEM', p_job_id, 'ERROR', SQLERRM);
            RAISE;
    END update_maintenance_job;
    
    PROCEDURE delete_maintenance_job(
        p_job_id IN NUMBER
    ) IS
    BEGIN
        DELETE FROM partition_maintenance_jobs
        WHERE job_id = p_job_id;
        
        log_maintenance_operation('DELETE_JOB', 'SYSTEM', p_job_id, 'SUCCESS', 
                                 'Maintenance job deleted');
                                 
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('DELETE_JOB', 'SYSTEM', p_job_id, 'ERROR', SQLERRM);
            RAISE;
    END delete_maintenance_job;
    
    -- Maintenance execution procedures
    PROCEDURE execute_maintenance_job(
        p_job_id IN NUMBER
    ) IS
        v_job partition_maintenance_jobs%ROWTYPE;
    BEGIN
        SELECT * INTO v_job
        FROM partition_maintenance_jobs
        WHERE job_id = p_job_id
        AND is_active = 'Y';
        
        log_maintenance_operation('EXECUTE_JOB', v_job.table_name, p_job_id, 'STARTED', 
                                 'Executing maintenance job');
        
        -- Execute based on job type
        CASE v_job.job_type
            WHEN 'CLEANUP' THEN
                auto_cleanup_old_partitions(v_job.table_name);
            WHEN 'REBUILD_INDEXES' THEN
                auto_rebuild_indexes(v_job.table_name);
            WHEN 'ANALYZE' THEN
                auto_analyze_partitions(v_job.table_name);
            WHEN 'COMPRESS' THEN
                auto_compress_partitions(v_job.table_name);
        END CASE;
        
        -- Update job status
        UPDATE partition_maintenance_jobs
        SET last_run = SYSDATE,
            next_run = CASE schedule_type
                WHEN 'DAILY' THEN TRUNC(SYSDATE) + 1
                WHEN 'WEEKLY' THEN TRUNC(SYSDATE) + 7
                WHEN 'MONTHLY' THEN ADD_MONTHS(TRUNC(SYSDATE), 1)
                ELSE SYSDATE + 1
            END
        WHERE job_id = p_job_id;
        
        log_maintenance_operation('EXECUTE_JOB', v_job.table_name, p_job_id, 'SUCCESS', 
                                 'Maintenance job completed');
                                 
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('EXECUTE_JOB', 'SYSTEM', p_job_id, 'ERROR', SQLERRM);
            RAISE;
    END execute_maintenance_job;
    
    PROCEDURE execute_all_maintenance_jobs IS
    BEGIN
        FOR rec IN (
            SELECT job_id
            FROM partition_maintenance_jobs
            WHERE is_active = 'Y'
            AND next_run <= SYSDATE
        ) LOOP
            BEGIN
                execute_maintenance_job(rec.job_id);
            EXCEPTION
                WHEN OTHERS THEN
                    log_maintenance_operation('EXECUTE_ALL', 'SYSTEM', rec.job_id, 'ERROR', SQLERRM);
            END;
        END LOOP;
    END execute_all_maintenance_jobs;
    
    PROCEDURE execute_table_maintenance(
        p_table_name IN VARCHAR2,
        p_job_type   IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        FOR rec IN (
            SELECT job_id, job_type
            FROM partition_maintenance_jobs
            WHERE table_name = UPPER(p_table_name)
            AND is_active = 'Y'
            AND (p_job_type IS NULL OR job_type = UPPER(p_job_type))
        ) LOOP
            BEGIN
                execute_maintenance_job(rec.job_id);
            EXCEPTION
                WHEN OTHERS THEN
                    log_maintenance_operation('EXECUTE_TABLE', p_table_name, rec.job_id, 'ERROR', SQLERRM);
            END;
        END LOOP;
    END execute_table_maintenance;
    
    -- Automated maintenance procedures
    PROCEDURE auto_cleanup_old_partitions(
        p_table_name IN VARCHAR2,
        p_retention_days IN NUMBER DEFAULT 90
    ) IS
    BEGIN
        partition_management_pkg.maintain_old_partitions(p_table_name, p_retention_days, 'DROP');
        log_maintenance_operation('CLEANUP', p_table_name, NULL, 'SUCCESS', 
                                 'Old partitions cleaned up');
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('CLEANUP', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END auto_cleanup_old_partitions;
    
    PROCEDURE auto_rebuild_indexes(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        partition_management_pkg.rebuild_partition_indexes(p_table_name, p_partition_name);
        log_maintenance_operation('REBUILD_INDEXES', p_table_name, NULL, 'SUCCESS', 
                                 'Indexes rebuilt');
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('REBUILD_INDEXES', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END auto_rebuild_indexes;
    
    PROCEDURE auto_analyze_partitions(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_estimate_percent IN NUMBER DEFAULT 10,
        p_degree IN NUMBER DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
        v_degree NUMBER;
        v_table_size_mb NUMBER;
        v_estimate_pct NUMBER;
    BEGIN
        -- Input validation
        IF p_table_name IS NULL OR LENGTH(TRIM(p_table_name)) = 0 THEN
            RAISE_APPLICATION_ERROR(-20016, 'Table name cannot be null or empty');
        END IF;
        
        -- Get table size for optimization
        BEGIN
            SELECT ROUND(SUM(bytes) / 1024 / 1024, 2)
            INTO v_table_size_mb
            FROM user_segments
            WHERE segment_name = UPPER(p_table_name);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_table_size_mb := 0;
        END;
        
        -- Optimize degree of parallelism based on table size
        v_degree := NVL(p_degree, CASE 
            WHEN v_table_size_mb > 10000 THEN 8
            WHEN v_table_size_mb > 1000 THEN 4
            ELSE 1
        END);
        
        -- Optimize estimate percent based on table size
        v_estimate_pct := CASE 
            WHEN v_table_size_mb > 50000 THEN 1
            WHEN v_table_size_mb > 10000 THEN 5
            ELSE LEAST(p_estimate_percent, 20)
        END;
        
        -- Use DBMS_STATS instead of ANALYZE for better performance and online capability
        IF p_partition_name IS NOT NULL THEN
            -- Analyze specific partition
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname => USER,
                tabname => UPPER(p_table_name),
                partname => UPPER(p_partition_name),
                estimate_percent => v_estimate_pct,
                degree => v_degree,
                granularity => 'PARTITION',
                cascade => TRUE
            );
        ELSE
            -- Analyze entire table with partition granularity
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname => USER,
                tabname => UPPER(p_table_name),
                estimate_percent => v_estimate_pct,
                degree => v_degree,
                granularity => 'ALL',
                cascade => TRUE
            );
        END IF;
        
        log_maintenance_operation('ANALYZE', p_table_name, p_partition_name, 'SUCCESS', 
                                 'Statistics gathered with ' || v_estimate_pct || '% sampling, degree=' || v_degree);
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('ANALYZE', p_table_name, p_partition_name, 'ERROR', SQLERRM);
            RAISE;
    END auto_analyze_partitions;
    
    PROCEDURE auto_compress_partitions(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    ) IS
        v_sql VARCHAR2(4000);
    BEGIN
        IF p_partition_name IS NOT NULL THEN
            v_sql := 'ALTER TABLE ' || p_table_name || ' MODIFY PARTITION ' || p_partition_name || ' COMPRESS';
        ELSE
            -- Compress all partitions
            FOR rec IN (
                SELECT partition_name
                FROM user_tab_partitions
                WHERE table_name = UPPER(p_table_name)
            ) LOOP
                v_sql := 'ALTER TABLE ' || p_table_name || ' MODIFY PARTITION ' || rec.partition_name || ' COMPRESS';
                EXECUTE IMMEDIATE v_sql;
            END LOOP;
        END IF;
        
        log_maintenance_operation('COMPRESS', p_table_name, NULL, 'SUCCESS', 
                                 'Partitions compressed');
    EXCEPTION
        WHEN OTHERS THEN
            log_maintenance_operation('COMPRESS', p_table_name, NULL, 'ERROR', SQLERRM);
            RAISE;
    END auto_compress_partitions;
    
    -- Monitoring and reporting procedures
    FUNCTION get_maintenance_jobs(
        p_table_name IN VARCHAR2 DEFAULT NULL,
        p_is_active  IN BOOLEAN DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                job_id,
                table_name,
                job_type,
                schedule_type,
                schedule_value,
                CASE WHEN is_active = 'Y' THEN TRUE ELSE FALSE END as is_active,
                last_run,
                next_run
            FROM partition_maintenance_jobs
            WHERE (p_table_name IS NULL OR table_name = UPPER(p_table_name))
            AND (p_is_active IS NULL OR is_active = CASE WHEN p_is_active THEN 'Y' ELSE 'N' END)
            ORDER BY table_name, job_type;
            
        RETURN v_cursor;
    END get_maintenance_jobs;
    
    FUNCTION get_maintenance_history(
        p_table_name IN VARCHAR2 DEFAULT NULL,
        p_start_date IN DATE DEFAULT NULL,
        p_end_date   IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                operation_type,
                table_name,
                status,
                message,
                operation_time,
                user_name
            FROM partition_operation_log
            WHERE operation_type LIKE 'MAINTENANCE_%'
            AND (p_table_name IS NULL OR table_name = UPPER(p_table_name))
            AND (p_start_date IS NULL OR operation_time >= p_start_date)
            AND (p_end_date IS NULL OR operation_time <= p_end_date)
            ORDER BY operation_time DESC;
            
        RETURN v_cursor;
    END get_maintenance_history;
    
    PROCEDURE generate_maintenance_report(
        p_table_name IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        log_maintenance_operation('REPORT', NVL(p_table_name, 'ALL'), NULL, 'INFO', 
                                 'Maintenance report generated');
    END generate_maintenance_report;
    
    -- Utility procedures
    PROCEDURE schedule_next_run(
        p_job_id IN NUMBER
    ) IS
    BEGIN
        UPDATE partition_maintenance_jobs
        SET next_run = CASE schedule_type
            WHEN 'DAILY' THEN TRUNC(SYSDATE) + 1
            WHEN 'WEEKLY' THEN TRUNC(SYSDATE) + 7
            WHEN 'MONTHLY' THEN ADD_MONTHS(TRUNC(SYSDATE), 1)
            ELSE SYSDATE + 1
        END
        WHERE job_id = p_job_id;
        
        log_maintenance_operation('SCHEDULE', 'SYSTEM', p_job_id, 'SUCCESS', 
                                 'Next run scheduled');
    END schedule_next_run;
    
    PROCEDURE validate_maintenance_job(
        p_job_id IN NUMBER
    ) IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM partition_maintenance_jobs
        WHERE job_id = p_job_id;
        
        IF v_count = 0 THEN
            log_maintenance_operation('VALIDATE', 'SYSTEM', p_job_id, 'ERROR', 'Job not found');
            RAISE_APPLICATION_ERROR(-20001, 'Maintenance job not found');
        END IF;
        
        log_maintenance_operation('VALIDATE', 'SYSTEM', p_job_id, 'SUCCESS', 'Job validation passed');
    END validate_maintenance_job;
    
END partition_maintenance_pkg;
/
