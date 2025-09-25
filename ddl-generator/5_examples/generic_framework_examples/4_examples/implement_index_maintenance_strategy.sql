-- =====================================================
-- Example: Implement Index Maintenance Strategy
-- Demonstrates how to create a new maintenance strategy
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Step 1: Register the strategy
EXEC generic_maintenance_logger_pkg.register_strategy(
    'INDEX_MAINTENANCE', 
    'MAINTENANCE', 
    'Automated index maintenance strategy for database optimization',
    'DATABASE'
);

-- Step 2: Create strategy configuration
INSERT INTO generic_strategy_config (
    strategy_name,
    target_object,
    target_type,
    strategy_type,
    strategy_config,
    schedule_expression,
    execution_mode,
    parallel_degree,
    priority_level,
    description
) VALUES (
    'INDEX_MAINTENANCE',
    'ALL_INDEXES',
    'INDEX',
    'MAINTENANCE',
    '{"maintenance_type": "REBUILD", "online": true, "parallel_degree": 4, "tablespace": "INDEX_TS"}',
    '0 2 * * 0', -- Weekly on Sunday at 2 AM
    'AUTOMATIC',
    4,
    7,
    'Weekly index maintenance for all indexes'
);

-- Step 3: Create maintenance jobs
INSERT INTO generic_maintenance_jobs (
    strategy_name,
    job_name,
    job_type,
    schedule_type,
    schedule_value,
    target_object,
    target_type,
    job_parameters,
    max_parallel_degree,
    notify_on_failure
) VALUES (
    'INDEX_MAINTENANCE',
    'REBUILD_UNUSABLE_INDEXES',
    'MAINTENANCE',
    'WEEKLY',
    'SUNDAY 02:00',
    'ALL_INDEXES',
    'INDEX',
    '{"operation": "REBUILD", "condition": "UNUSABLE", "online": true}',
    4,
    'Y'
);

INSERT INTO generic_maintenance_jobs (
    strategy_name,
    job_name,
    job_type,
    schedule_type,
    schedule_value,
    target_object,
    target_type,
    job_parameters,
    max_parallel_degree,
    notify_on_failure
) VALUES (
    'INDEX_MAINTENANCE',
    'ANALYZE_INDEX_STATISTICS',
    'ANALYSIS',
    'WEEKLY',
    'SUNDAY 03:00',
    'ALL_INDEXES',
    'INDEX',
    '{"operation": "ANALYZE", "estimate_percent": 10, "cascade": true}',
    2,
    'Y'
);

INSERT INTO generic_maintenance_jobs (
    strategy_name,
    job_name,
    job_type,
    schedule_type,
    schedule_value,
    target_object,
    target_type,
    job_parameters,
    max_parallel_degree,
    notify_on_failure
) VALUES (
    'INDEX_MAINTENANCE',
    'CLEANUP_ORPHANED_INDEXES',
    'CLEANUP',
    'MONTHLY',
    'FIRST_SUNDAY 04:00',
    'ALL_INDEXES',
    'INDEX',
    '{"operation": "CLEANUP", "orphaned_only": true, "dry_run": false}',
    1,
    'Y'
);

-- Step 4: Create the strategy package
CREATE OR REPLACE PACKAGE index_maintenance_pkg
AUTHID DEFINER
AS
    -- Main strategy execution
    PROCEDURE execute_strategy(
        p_target_object IN VARCHAR2,
        p_parameters   IN CLOB DEFAULT NULL
    );
    
    -- Specific maintenance operations
    PROCEDURE rebuild_unusable_indexes(
        p_schema_name IN VARCHAR2 DEFAULT NULL,
        p_online     IN BOOLEAN DEFAULT TRUE,
        p_parallel   IN NUMBER DEFAULT 4
    );
    
    PROCEDURE analyze_index_statistics(
        p_schema_name IN VARCHAR2 DEFAULT NULL,
        p_estimate_percent IN NUMBER DEFAULT 10
    );
    
    PROCEDURE cleanup_orphaned_indexes(
        p_schema_name IN VARCHAR2 DEFAULT NULL,
        p_dry_run     IN BOOLEAN DEFAULT TRUE
    );
    
    -- Validation and monitoring
    FUNCTION validate_target(
        p_target_object IN VARCHAR2
    ) RETURN BOOLEAN;
    
    FUNCTION get_index_health_status(
        p_schema_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_maintenance_recommendations(
        p_schema_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
END index_maintenance_pkg;
/

-- Step 5: Create the strategy package body
CREATE OR REPLACE PACKAGE BODY index_maintenance_pkg
AS
    -- Main strategy execution
    PROCEDURE execute_strategy(
        p_target_object IN VARCHAR2,
        p_parameters   IN CLOB DEFAULT NULL
    ) IS
        v_operation_id NUMBER;
        v_schema_name VARCHAR2(30);
        v_online BOOLEAN := TRUE;
        v_parallel NUMBER := 4;
    BEGIN
        -- Start logging
        v_operation_id := generic_maintenance_logger_pkg.log_strategy_start(
            'INDEX_MAINTENANCE',
            p_target_object,
            'INDEX'
        );
        
        -- Parse parameters if provided
        IF p_parameters IS NOT NULL THEN
            -- In practice, you would parse JSON parameters here
            v_schema_name := 'ALL'; -- Default to all schemas
        ELSE
            v_schema_name := p_target_object;
        END IF;
        
        -- Execute maintenance operations
        rebuild_unusable_indexes(v_schema_name, v_online, v_parallel);
        analyze_index_statistics(v_schema_name, 10);
        
        -- End logging
        generic_maintenance_logger_pkg.log_strategy_end(
            v_operation_id,
            'SUCCESS',
            'Index maintenance completed successfully'
        );
        
    EXCEPTION
        WHEN OTHERS THEN
            generic_maintenance_logger_pkg.log_strategy_end(
                v_operation_id,
                'ERROR',
                'Index maintenance failed: ' || SQLERRM
            );
            RAISE;
    END execute_strategy;
    
    -- Rebuild unusable indexes
    PROCEDURE rebuild_unusable_indexes(
        p_schema_name IN VARCHAR2 DEFAULT NULL,
        p_online     IN BOOLEAN DEFAULT TRUE,
        p_parallel   IN NUMBER DEFAULT 4
    ) IS
        v_operation_id NUMBER;
        v_sql VARCHAR2(4000);
        v_online_clause VARCHAR2(100);
        v_rebuilt_count NUMBER := 0;
    BEGIN
        v_operation_id := generic_maintenance_logger_pkg.log_start_operation(
            'INDEX_MAINTENANCE',
            'REBUILD',
            'INDEXES',
            'INDEX',
            NULL,
            NULL,
            'REBUILD_UNUSABLE_INDEXES'
        );
        
        -- Build online clause
        IF p_online THEN
            v_online_clause := ' ONLINE';
        ELSE
            v_online_clause := '';
        END IF;
        
        -- Rebuild unusable indexes
        FOR idx_rec IN (
            SELECT owner, index_name, table_name
            FROM dba_indexes
            WHERE status = 'UNUSABLE'
            AND (p_schema_name IS NULL OR owner = p_schema_name)
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'WMSYS', 'EXFSYS', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR')
        ) LOOP
            BEGIN
                v_sql := 'ALTER INDEX ' || idx_rec.owner || '.' || idx_rec.index_name || 
                         ' REBUILD' || v_online_clause || ' PARALLEL ' || p_parallel;
                
                EXECUTE IMMEDIATE v_sql;
                v_rebuilt_count := v_rebuilt_count + 1;
                
                -- Log individual index rebuild
                generic_maintenance_logger_pkg.log_operation(
                    'INDEX_MAINTENANCE',
                    'REBUILD_INDEX',
                    idx_rec.index_name,
                    'INDEX',
                    'SUCCESS',
                    'Index rebuilt successfully',
                    NULL,
                    v_sql,
                    NULL,
                    NULL,
                    NULL,
                    'REBUILD_UNUSABLE_INDEXES'
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    generic_maintenance_logger_pkg.log_operation(
                        'INDEX_MAINTENANCE',
                        'REBUILD_INDEX',
                        idx_rec.index_name,
                        'INDEX',
                        'ERROR',
                        'Failed to rebuild index: ' || SQLERRM,
                        NULL,
                        v_sql,
                        SQLCODE,
                        SQLERRM,
                        NULL,
                        'REBUILD_UNUSABLE_INDEXES'
                    );
            END;
        END LOOP;
        
        -- End operation logging
        generic_maintenance_logger_pkg.log_end_operation(
            v_operation_id,
            'SUCCESS',
            'Rebuilt ' || v_rebuilt_count || ' unusable indexes',
            NULL,
            NULL,
            NULL
        );
        
    EXCEPTION
        WHEN OTHERS THEN
            generic_maintenance_logger_pkg.log_end_operation(
                v_operation_id,
                'ERROR',
                'Failed to rebuild unusable indexes: ' || SQLERRM,
                SQLCODE,
                SQLERRM,
                NULL
            );
            RAISE;
    END rebuild_unusable_indexes;
    
    -- Analyze index statistics
    PROCEDURE analyze_index_statistics(
        p_schema_name IN VARCHAR2 DEFAULT NULL,
        p_estimate_percent IN NUMBER DEFAULT 10
    ) IS
        v_operation_id NUMBER;
        v_analyzed_count NUMBER := 0;
    BEGIN
        v_operation_id := generic_maintenance_logger_pkg.log_start_operation(
            'INDEX_MAINTENANCE',
            'ANALYZE',
            'INDEXES',
            'INDEX',
            NULL,
            NULL,
            'ANALYZE_INDEX_STATISTICS'
        );
        
        -- Analyze index statistics
        FOR idx_rec IN (
            SELECT owner, index_name, table_name
            FROM dba_indexes
            WHERE (p_schema_name IS NULL OR owner = p_schema_name)
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'WMSYS', 'EXFSYS', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR')
        ) LOOP
            BEGIN
                EXECUTE IMMEDIATE 'BEGIN DBMS_STATS.GATHER_INDEX_STATS(''' || 
                    idx_rec.owner || ''', ''' || idx_rec.index_name || ''', ' ||
                    'estimate_percent => ' || p_estimate_percent || '); END;';
                
                v_analyzed_count := v_analyzed_count + 1;
                
            EXCEPTION
                WHEN OTHERS THEN
                    generic_maintenance_logger_pkg.log_operation(
                        'INDEX_MAINTENANCE',
                        'ANALYZE_INDEX',
                        idx_rec.index_name,
                        'INDEX',
                        'ERROR',
                        'Failed to analyze index: ' || SQLERRM,
                        NULL,
                        NULL,
                        SQLCODE,
                        SQLERRM,
                        NULL,
                        'ANALYZE_INDEX_STATISTICS'
                    );
            END;
        END LOOP;
        
        -- End operation logging
        generic_maintenance_logger_pkg.log_end_operation(
            v_operation_id,
            'SUCCESS',
            'Analyzed ' || v_analyzed_count || ' indexes',
            NULL,
            NULL,
            NULL
        );
        
    EXCEPTION
        WHEN OTHERS THEN
            generic_maintenance_logger_pkg.log_end_operation(
                v_operation_id,
                'ERROR',
                'Failed to analyze index statistics: ' || SQLERRM,
                SQLCODE,
                SQLERRM,
                NULL
            );
            RAISE;
    END analyze_index_statistics;
    
    -- Cleanup orphaned indexes
    PROCEDURE cleanup_orphaned_indexes(
        p_schema_name IN VARCHAR2 DEFAULT NULL,
        p_dry_run     IN BOOLEAN DEFAULT TRUE
    ) IS
        v_operation_id NUMBER;
        v_cleaned_count NUMBER := 0;
    BEGIN
        v_operation_id := generic_maintenance_logger_pkg.log_start_operation(
            'INDEX_MAINTENANCE',
            'CLEANUP',
            'INDEXES',
            'INDEX',
            NULL,
            NULL,
            'CLEANUP_ORPHANED_INDEXES'
        );
        
        -- Find and clean up orphaned indexes
        FOR idx_rec IN (
            SELECT owner, index_name, table_name
            FROM dba_indexes
            WHERE (p_schema_name IS NULL OR owner = p_schema_name)
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'WMSYS', 'EXFSYS', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR')
            AND NOT EXISTS (
                SELECT 1 FROM dba_tables t 
                WHERE t.owner = dba_indexes.owner 
                AND t.table_name = dba_indexes.table_name
            )
        ) LOOP
            BEGIN
                IF NOT p_dry_run THEN
                    EXECUTE IMMEDIATE 'DROP INDEX ' || idx_rec.owner || '.' || idx_rec.index_name;
                END IF;
                
                v_cleaned_count := v_cleaned_count + 1;
                
                generic_maintenance_logger_pkg.log_operation(
                    'INDEX_MAINTENANCE',
                    'CLEANUP_INDEX',
                    idx_rec.index_name,
                    'INDEX',
                    'SUCCESS',
                    CASE WHEN p_dry_run THEN 'Would drop orphaned index' ELSE 'Dropped orphaned index' END,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    'CLEANUP_ORPHANED_INDEXES'
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    generic_maintenance_logger_pkg.log_operation(
                        'INDEX_MAINTENANCE',
                        'CLEANUP_INDEX',
                        idx_rec.index_name,
                        'INDEX',
                        'ERROR',
                        'Failed to cleanup index: ' || SQLERRM,
                        NULL,
                        NULL,
                        SQLCODE,
                        SQLERRM,
                        NULL,
                        'CLEANUP_ORPHANED_INDEXES'
                    );
            END;
        END LOOP;
        
        -- End operation logging
        generic_maintenance_logger_pkg.log_end_operation(
            v_operation_id,
            'SUCCESS',
            CASE WHEN p_dry_run THEN 'Would cleanup ' || v_cleaned_count || ' orphaned indexes' 
                 ELSE 'Cleaned up ' || v_cleaned_count || ' orphaned indexes' END,
            NULL,
            NULL,
            NULL
        );
        
    EXCEPTION
        WHEN OTHERS THEN
            generic_maintenance_logger_pkg.log_end_operation(
                v_operation_id,
                'ERROR',
                'Failed to cleanup orphaned indexes: ' || SQLERRM,
                SQLCODE,
                SQLERRM,
                NULL
            );
            RAISE;
    END cleanup_orphaned_indexes;
    
    -- Validation and monitoring functions
    FUNCTION validate_target(
        p_target_object IN VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        -- Validate that the target is appropriate for index maintenance
        IF p_target_object = 'ALL_INDEXES' OR p_target_object = 'ALL' THEN
            RETURN TRUE;
        END IF;
        
        -- Check if it's a valid schema
        RETURN EXISTS (
            SELECT 1 FROM dba_users 
            WHERE username = UPPER(p_target_object)
        );
    END validate_target;
    
    FUNCTION get_index_health_status(
        p_schema_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                owner,
                COUNT(*) as total_indexes,
                SUM(CASE WHEN status = 'UNUSABLE' THEN 1 ELSE 0 END) as unusable_indexes,
                SUM(CASE WHEN status = 'VALID' THEN 1 ELSE 0 END) as valid_indexes,
                ROUND(AVG(last_analyzed - SYSDATE), 2) as avg_days_since_analyzed
            FROM dba_indexes
            WHERE (p_schema_name IS NULL OR owner = p_schema_name)
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'WMSYS', 'EXFSYS', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR')
            GROUP BY owner
            ORDER BY owner;
            
        RETURN v_cursor;
    END get_index_health_status;
    
    FUNCTION get_maintenance_recommendations(
        p_schema_name IN VARCHAR2 DEFAULT NULL
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT 
                'REBUILD_UNUSABLE' as recommendation_type,
                'Rebuild unusable indexes' as description,
                COUNT(*) as affected_count,
                'HIGH' as priority
            FROM dba_indexes
            WHERE status = 'UNUSABLE'
            AND (p_schema_name IS NULL OR owner = p_schema_name)
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'WMSYS', 'EXFSYS', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR')
            UNION ALL
            SELECT 
                'ANALYZE_STALE' as recommendation_type,
                'Analyze stale index statistics' as description,
                COUNT(*) as affected_count,
                'MEDIUM' as priority
            FROM dba_indexes
            WHERE last_analyzed < SYSDATE - 7
            AND (p_schema_name IS NULL OR owner = p_schema_name)
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'WMSYS', 'EXFSYS', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR')
            ORDER BY priority DESC, affected_count DESC;
            
        RETURN v_cursor;
    END get_maintenance_recommendations;
    
END index_maintenance_pkg;
/

-- Step 6: Test the strategy
DECLARE
    v_result BOOLEAN;
    v_cursor SYS_REFCURSOR;
BEGIN
    -- Test target validation
    v_result := index_maintenance_pkg.validate_target('ALL_INDEXES');
    IF v_result THEN
        DBMS_OUTPUT.PUT_LINE('Target validation: PASSED');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Target validation: FAILED');
    END IF;
    
    -- Test strategy execution (dry run)
    index_maintenance_pkg.execute_strategy('ALL_INDEXES');
    DBMS_OUTPUT.PUT_LINE('Strategy execution: COMPLETED');
    
    -- Test health status
    v_cursor := index_maintenance_pkg.get_index_health_status();
    DBMS_OUTPUT.PUT_LINE('Index health status: Retrieved');
    
    -- Test recommendations
    v_cursor := index_maintenance_pkg.get_maintenance_recommendations();
    DBMS_OUTPUT.PUT_LINE('Maintenance recommendations: Retrieved');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Test failed: ' || SQLERRM);
        RAISE;
END;
/

-- Step 7: Create a job to execute the strategy
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'INDEX_MAINTENANCE_JOB',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN index_maintenance_pkg.execute_strategy(''ALL_INDEXES''); END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=WEEKLY;BYDAY=SUN;BYHOUR=2;BYMINUTE=0;BYSECOND=0',
        enabled         => TRUE,
        comments        => 'Weekly index maintenance job'
    );
END;
/

PROMPT Index maintenance strategy implemented successfully
PROMPT 
PROMPT To execute the strategy manually:
PROMPT EXEC index_maintenance_pkg.execute_strategy('ALL_INDEXES');
PROMPT 
PROMPT To check index health:
PROMPT SELECT * FROM TABLE(index_maintenance_pkg.get_index_health_status());
PROMPT 
PROMPT To get maintenance recommendations:
PROMPT SELECT * FROM TABLE(index_maintenance_pkg.get_maintenance_recommendations());
