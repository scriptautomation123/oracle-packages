-- Install tables first
PROMPT Creating partition management tables...
@@config-tables/lookup_tables.sql
@@config-tables/partition_operation_log_table.sql
@@config-tables/partition_maintenance_jobs_table.sql
@@config-tables/partition_strategy_config_table.sql
