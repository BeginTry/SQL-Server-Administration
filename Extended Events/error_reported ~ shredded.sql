;WITH XEvents AS
(
	SELECT object_name, CAST(event_data AS XML) AS event_data
	FROM sys.fn_xe_file_target_read_file ( 'DBA-error_reported*.xel', NULL, NULL, NULL )  
)
SELECT object_name, event_data,

	event_data.value ('(/event/@timestamp)[1]', 'DATETIME') AS [timestamp],
	event_data.value ('(/event/data[@name=''category'']/text)[1]', 'NVARCHAR(MAX)') AS [category],
	event_data.value ('(/event/action[@name=''client_app_name'']/value)[1]', 'NVARCHAR(MAX)') AS [client_app_name],
	event_data.value ('(/event/action[@name=''client_hostname'']/value)[1]', 'NVARCHAR(MAX)') AS [client_hostname],
	event_data.value ('(/event/action[@name=''database_name'']/value)[1]', 'NVARCHAR(MAX)') AS [database_name],
	event_data.value ('(/event/data[@name=''destination'']/text)[1]', 'NVARCHAR(MAX)') AS [destination],
	event_data.value ('(/event/data[@name=''error'']/value)[1]', 'BIGINT') AS [error],
	event_data.value ('(/event/data[@name=''error_number'']/value)[1]', 'INT') AS [error_number],
	event_data.value ('(/event/data[@name=''file'']/value)[1]', 'VARCHAR(MAX)') AS [file],
	event_data.value ('(/event/data[@name=''function'']/value)[1]', 'VARCHAR(MAX)') AS [function],
	event_data.value ('(/event/data[@name=''is_intercepted'']/value)[1]', 'BIT') AS [is_intercepted],
	event_data.value ('(/event/data[@name=''line'']/value)[1]', 'BIGINT') AS [line],
	event_data.value ('(/event/data[@name=''message'']/value)[1]', 'NVARCHAR(MAX)') AS [message],
	event_data.value ('(/event/action[@name=''server_principal_name'']/value)[1]', 'NVARCHAR(MAX)') AS [server_principal_name],
	event_data.value ('(/event/data[@name=''severity'']/value)[1]', 'INT') AS [severity],
	event_data.value ('(/event/action[@name=''sql_text'']/value)[1]', 'NVARCHAR(MAX)') AS [sql_text],
	event_data.value ('(/event/data[@name=''state'']/value)[1]', 'INT') AS [state],
	event_data.value ('(/event/data[@name=''user_defined'']/value)[1]', 'BIT') AS [user_defined],
	event_data.value ('(/event/action[@name=''username'']/value)[1]', 'NVARCHAR(MAX)') AS [username]
FROM XEvents;
