-- =====================================================
-- Oracle Partition Operation Log Table
-- Autonomous logging table for partition operations
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- Create sequence for operation IDs
CREATE SEQUENCE partition_operation_log_seq
START WITH 1
INCREMENT BY 1
NOCACHE
NOMAXVALUE;

-- Create logging table
CREATE TABLE partition_operation_log (
    operation_id     NUMBER PRIMARY KEY,
    operation_type   VARCHAR2(50) NOT NULL,
    table_name       VARCHAR2(128) NOT NULL,
    partition_name   VARCHAR2(128),
    status           VARCHAR2(20) NOT NULL,
    message          VARCHAR2(4000),
    duration_ms      NUMBER,
    operation_time   TIMESTAMP DEFAULT SYSTIMESTAMP,
    user_name        VARCHAR2(30) DEFAULT USER,
    session_id       NUMBER,
    sql_text         CLOB,
    error_code       NUMBER,
    error_message    VARCHAR2(4000),
    -- Foreign key constraints
    CONSTRAINT fk_operation_type FOREIGN KEY (operation_type) 
        REFERENCES partition_operation_types(operation_type),
    CONSTRAINT fk_operation_status FOREIGN KEY (status) 
        REFERENCES partition_operation_status(status)
);

-- Create indexes for performance
CREATE INDEX idx_part_log_table ON partition_operation_log(table_name);
CREATE INDEX idx_part_log_time ON partition_operation_log(operation_time);
CREATE INDEX idx_part_log_status ON partition_operation_log(status);
CREATE INDEX idx_part_log_type ON partition_operation_log(operation_type);

-- Create interval partitioned table for automatic partition creation
ALTER TABLE partition_operation_log 
PARTITION BY RANGE (operation_time) 
INTERVAL (NUMTODSINTERVAL(1, 'MONTH')) (
    PARTITION p_log_initial VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD'))
);

PROMPT Partition operation log table created successfully
