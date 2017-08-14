USE [msdb]
GO

/*********************************************************
	Add alerts for severity levels 17 - 25, and for
	error numbers 823, 824, & 825.
	Only create alerts once (don't drop and re-add).
*********************************************************/
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'17-Insufficient Resources'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'17-Insufficient Resources', 
		@severity=17, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'18-Nonfatal Internal Error'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'18-Nonfatal Internal Error', 
		@severity=18, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'19-Fatal Error in Resource'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'19-Fatal Error in Resource', 
		@severity=19, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'20-Fatal Error in Current Process'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'20-Fatal Error in Current Process', 
		@severity=20, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'21-Fatal Error in Database Processes'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'21-Fatal Error in Database Processes', 
		@severity=21, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'22-Fatal Error: Table Integrity Suspect'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'22-Fatal Error: Table Integrity Suspect', 
		@severity=22, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'23-Fatal Error: Database Integrity Suspect'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'23-Fatal Error: Database Integrity Suspect', 
		@severity=23, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'24-Fatal Error: Hardware Error'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'24-Fatal Error: Hardware Error', 
		@severity=24, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'25-Fatal Error'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'25-Fatal Error', 
		@severity=25, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'Error 823'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'Error 823', 
		@message_id=823, 
		@enabled=1, 
		@delay_between_responses=60, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'Error 824'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'Error 824', 
		@message_id=824, 
		@enabled=1, 
		@delay_between_responses=60, 
		@include_event_description_in=7
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'Error 825'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'Error 825', 
		@message_id=825, 
		@enabled=1, 
		@delay_between_responses=60, 
		@include_event_description_in=7
GO


/****************************************
	Reset/clear alert history as needed.
****************************************/
DECLARE @AlertName SYSNAME
DECLARE curAlerts CURSOR READ_ONLY FAST_FORWARD FOR
	SELECT name
	FROM msdb.dbo.sysalerts
	WHERE name BETWEEN '17' AND '26'
	OR name LIKE 'Error 82_'

OPEN curAlerts
FETCH NEXT FROM curAlerts INTO @AlertName

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC msdb.dbo.sp_update_alert 
		@name=@AlertName, 
		@occurrence_count = 0, 
		@count_reset_date = NULL, 
		@count_reset_time = NULL, 
		@last_occurrence_date = 0, 
		@last_occurrence_time = 0, 
		@last_response_date = 0, 
		@last_response_time = 0
	FETCH NEXT FROM curAlerts INTO @AlertName
END

CLOSE curAlerts
DEALLOCATE curAlerts

/*
	SELECT * FROM msdb.dbo.sysalerts
	SELECT * FROM msdb.dbo.sysnotifications 
*/

