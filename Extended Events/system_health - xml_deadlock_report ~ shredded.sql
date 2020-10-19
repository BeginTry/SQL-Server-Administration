/*
	Returns information on deadlocks from the [system_health] extended event session.
*/
DECLARE @Utc_Hours_Offset INT = DATEDIFF(HOUR, GETUTCDATE(), GETDATE());

;WITH XEvents AS
(
	SELECT object_name, CAST(event_data AS XML) AS event_data, timestamp_utc
	FROM sys.fn_xe_file_target_read_file ( 'system_health*.xel', NULL, NULL, NULL )  
	WHERE object_name = 'xml_deadlock_report'
)
SELECT object_name, 
	--event_data,
	--Adjust XEvent timestamp from UTC to current server time zone.
	DATEADD(HOUR, @Utc_Hours_Offset, event_data.value ('(/event/@timestamp)[1]', 'DATETIME')) AS [timestamp],
	event_data.query ('(/event/data[@name=''xml_report'']/value/deadlock)[1]') AS [xml_deadlock_report],

	--The deadlock victim/winner data has not been thoroughly tested.
	--For instance, the below assumes only two SPIDs are involved.
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[1]/@spid)[1]', 'BIGINT') AS spid_VIC,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[1]/@clientapp)[1]', 'VARCHAR(255)') AS clientapp_VIC,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[1]/@hostname)[1]', 'VARCHAR(255)') AS hostname_VIC,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[1]/@loginname)[1]', 'VARCHAR(128)') AS loginname_VIC,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[1]/inputbuf)[1]', 'VARCHAR(128)') AS inputbuf_VIC,

	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[2]/@spid)[1]', 'BIGINT') AS spid_Win,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[2]/@clientapp)[1]', 'VARCHAR(255)') AS clientapp_Win,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[2]/@hostname)[1]', 'VARCHAR(255)') AS hostname_Win,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[2]/@loginname)[1]', 'VARCHAR(128)') AS loginname_Win,
	event_data.value ('(/event/data[@name=''xml_report'']/value/deadlock[1]/process-list/process[2]/inputbuf)[1]', 'VARCHAR(128)') AS inputbuf_Win
	
FROM XEvents x
ORDER BY x.timestamp_utc;
