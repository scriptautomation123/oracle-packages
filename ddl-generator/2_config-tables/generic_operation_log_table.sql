-- =====================================================
-- Oracle Generic Operation Log Table
-- Autonomous logging table for maintenance operations
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Create sequence for operation IDs
CREATE SEQUENCE generic_operation_log_seq
START WITH 1
INCREMENT BY 1
NOCACHE
NOMAXVALUE;

-- Create generic logging table
CREATE TABLE generic_operation_log (
    operation_id     NUMBER PRIMARY KEY,
    strategy_name    VARCHAR2(50) NOT NULL,
    operation_type   VARCHAR2(50) NOT NULL,
    job_name         VARCHAR2(128),
    target_object    VARCHAR2(128) NOT NULL,
    target_type      VARCHAR2(30),
    status           VARCHAR2(20) NOT NULL,
    message          VARCHAR2(4000),
    duration_ms      NUMBER,
    operation_time   TIMESTAMP DEFAULT SYSTIMESTAMP,
    user_name        VARCHAR2(30) DEFAULT USER,
    session_id       NUMBER,
    sql_text         CLOB,
    error_code       NUMBER,
    error_message    VARCHAR2(4000),
    -- Strategy-specific context
    strategy_context CLOB, -- JSON or structured data
    -- Resource usage tracking
    cpu_time_ms      NUMBER,
    memory_used_mb   NUMBER,
    io_operations    NUMBER,
    -- Performance metrics
    rows_processed   NUMBER,
    objects_affected NUMBER,
    -- Foreign key constraints
    CONSTRAINT fk_generic_operation_strategy FOREIGN KEY (strategy_name) 
        REFERENCES generic_strategy_types(strategy_name),
    CONSTRAINT fk_generic_operation_type FOREIGN KEY (operation_type) 
        REFERENCES generic_operation_types(operation_type),
    CONSTRAINT fk_generic_operation_status FOREIGN KEY (status) 
        REFERENCES generic_operation_status(status),
    CONSTRAINT fk_generic_operation_target_type FOREIGN KEY (target_type) 
        REFERENCES generic_target_types(target_type)
);

-- Create indexes for performance
CREATE INDEX idx_generic_log_strategy ON generic_operation_log(strategy_name);
CREATE INDEX idx_generic_log_job ON generic_operation_log(job_name);
CREATE INDEX idx_generic_log_target ON generic_operation_log(target_object);
CREATE INDEX idx_generic_log_time ON generic_operation_log(operation_time);
CREATE INDEX idx_generic_log_status ON generic_operation_log(status);
CREATE INDEX idx_generic_log_type ON generic_operation_log(operation_type);
CREATE INDEX idx_generic_log_target_type ON generic_operation_log(target_type);

-- Create interval partitioned table for automatic partition creation
ALTER TABLE generic_operation_log 
PARTITION BY RANGE (operation_time) 
INTERVAL (NUMTODSINTERVAL(1, 'MONTH')) (
    PARTITION p_log_initial VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD'))
);

PROMPT Generic operation log table created successfully
