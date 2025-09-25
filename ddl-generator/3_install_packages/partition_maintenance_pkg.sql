-- =====================================================
-- Oracle Partition Maintenance Package Specification
-- Automated partition maintenance with scheduling
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Package Specification
CREATE OR REPLACE PACKAGE partition_maintenance_pkg
AUTHID DEFINER
AS
    -- Types for maintenance operations
    TYPE maintenance_job_rec IS RECORD (
        job_id            NUMBER,
        table_name        VARCHAR2(128),
        job_type          VARCHAR2(50),
        schedule_type     VARCHAR2(20),
        schedule_value    VARCHAR2(100),
        is_active         BOOLEAN,
        last_run          DATE,
        next_run          DATE
    );
    
    TYPE maintenance_job_tab IS TABLE OF maintenance_job_rec;
    
    -- Job management procedures
    PROCEDURE create_maintenance_job(
        p_table_name     IN VARCHAR2,
        p_job_type       IN VARCHAR2,
        p_schedule_type  IN VARCHAR2,
        p_schedule_value IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE update_maintenance_job(
        p_job_id         IN NUMBER,
        p_schedule_type  IN VARCHAR2 DEFAULT NULL,
        p_schedule_value IN VARCHAR2 DEFAULT NULL,
        p_is_active      IN BOOLEAN DEFAULT NULL
    );
    
    PROCEDURE delete_maintenance_job(
        p_job_id IN NUMBER
    );
    
    -- Maintenance execution procedures
    PROCEDURE execute_maintenance_job(
        p_job_id IN NUMBER
    );
    
    PROCEDURE execute_all_maintenance_jobs;
    
    PROCEDURE execute_table_maintenance(
        p_table_name IN VARCHAR2,
        p_job_type   IN VARCHAR2 DEFAULT NULL
    );
    
    -- Automated maintenance procedures
    PROCEDURE auto_cleanup_old_partitions(
        p_table_name IN VARCHAR2,
        p_retention_days IN NUMBER DEFAULT 90
    );
    
    PROCEDURE auto_rebuild_indexes(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE auto_analyze_partitions(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL,
        p_estimate_percent IN NUMBER DEFAULT 10,
        p_degree IN NUMBER DEFAULT NULL
    );
    
    PROCEDURE auto_compress_partitions(
        p_table_name IN VARCHAR2,
        p_partition_name IN VARCHAR2 DEFAULT NULL
    );
    
    -- Monitoring and reporting procedures
    FUNCTION get_maintenance_jobs(
        p_table_name IN VARCHAR2 DEFAULT NULL,
        p_is_active  IN BOOLEAN DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    FUNCTION get_maintenance_history(
        p_table_name IN VARCHAR2 DEFAULT NULL,
        p_start_date IN DATE DEFAULT NULL,
        p_end_date   IN DATE DEFAULT NULL
    ) RETURN SYS_REFCURSOR;
    
    PROCEDURE generate_maintenance_report(
        p_table_name IN VARCHAR2 DEFAULT NULL
    );
    
    -- Utility procedures
    PROCEDURE schedule_next_run(
        p_job_id IN NUMBER
    );
    
    PROCEDURE validate_maintenance_job(
        p_job_id IN NUMBER
    );
    
END partition_maintenance_pkg;
/