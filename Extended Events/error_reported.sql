CREATE EVENT SESSION [DBA-error_reported] ON SERVER 
ADD EVENT sqlserver.error_reported
(
    ACTION
	(
		sqlserver.client_app_name,
		sqlserver.client_hostname,
		sqlserver.database_name,
		sqlserver.server_principal_name,
		sqlserver.sql_text,
		sqlserver.username
	)
	WHERE
	(
		[package0].[greater_than_int64]([severity],(10)) 
		AND [error_number]<>(18456)	--Login failed for user 'XXX'
	)
)
ADD TARGET package0.event_file
(
	SET filename=N'DBA-error_reported',
	max_file_size=(64),
	max_rollover_files=(4)
)
WITH
(
	MAX_MEMORY=4096 KB,
	EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
	MAX_DISPATCH_LATENCY=30 SECONDS,
	MAX_EVENT_SIZE=0 KB,
	MEMORY_PARTITION_MODE=NONE,
	TRACK_CAUSALITY=OFF,
	STARTUP_STATE=OFF
)
GO


