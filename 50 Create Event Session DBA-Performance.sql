IF NOT EXISTS(
	SELECT *
	FROM master.sys.server_event_sessions s
	WHERE s.name = 'DBA-Performance'
)
BEGIN
	CREATE EVENT SESSION [DBA-Performance] ON SERVER 
	ADD EVENT sqlserver.cursor_execute(
		ACTION(sqlserver.client_app_name,
			sqlserver.client_hostname,
			sqlserver.database_name,
			sqlserver.server_principal_name,
			sqlserver.session_id,
			sqlserver.sql_text)),

	ADD EVENT sqlserver.error_reported(
		ACTION(sqlserver.client_app_name,
			sqlserver.client_hostname,
			sqlserver.database_name,
			sqlserver.server_principal_name,
			sqlserver.session_id,
			sqlserver.sql_text)
		WHERE ([package0].[not_equal_unicode_string]([message],N'''''') AND [severity]>(10))),

	ADD EVENT sqlserver.module_end(
		ACTION(sqlserver.client_app_name,
			sqlserver.client_hostname,
			sqlserver.database_name,
			sqlserver.server_principal_name,
			sqlserver.session_id,
			sqlserver.sql_text)
		WHERE ([duration]>=(250000))),

	ADD EVENT sqlserver.rpc_completed(
		ACTION(sqlserver.client_app_name,
			sqlserver.client_hostname,
			sqlserver.database_name,
			sqlserver.server_principal_name,
			sqlserver.session_id,
			sqlserver.sql_text)
		WHERE ([duration]>=(250000))),

	ADD EVENT sqlserver.sp_statement_completed(
		ACTION(sqlserver.client_app_name,
			sqlserver.client_hostname,
			sqlserver.database_name,
			sqlserver.server_principal_name,
			sqlserver.session_id,
			sqlserver.sql_text)
		WHERE ([duration]>=(250000))),

	ADD EVENT sqlserver.sql_statement_completed(SET collect_statement=(1)
		ACTION(sqlserver.client_app_name,
			sqlserver.client_hostname,
			sqlserver.database_name,
			sqlserver.server_principal_name,
			sqlserver.session_id,
			sqlserver.sql_text)
		WHERE ([duration]>=(250000)))

	WITH (MAX_MEMORY=4096 KB,
		EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
		MAX_DISPATCH_LATENCY=30 SECONDS,
		MAX_EVENT_SIZE=0 KB,
		MEMORY_PARTITION_MODE=NONE,
		TRACK_CAUSALITY=ON,
		STARTUP_STATE=OFF)
END
GO


