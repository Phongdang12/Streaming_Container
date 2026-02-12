CREATE SCHEMA IF NOT EXISTS lakehouse;
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_container_cycle', table_location => 's3://lakehouse/gold/gold_container_cycle');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_container_current_status', table_location => 's3://lakehouse/gold/gold_container_current_status');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_ops_metrics_realtime', table_location => 's3://lakehouse/gold/gold_ops_metrics_realtime');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_backlog_metrics', table_location => 's3://lakehouse/gold/gold_backlog_metrics');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_kpi_shift', table_location => 's3://lakehouse/gold/gold_kpi_shift');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_kpi_daily', table_location => 's3://lakehouse/gold/gold_kpi_daily');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_kpi_peak_hours', table_location => 's3://lakehouse/gold/gold_kpi_peak_hours');
