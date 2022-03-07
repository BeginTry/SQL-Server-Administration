USE [master]
GO

CREATE OR ALTER TRIGGER trgDatabase_DDL_Events 
ON ALL SERVER 
FOR DDL_DATABASE_LEVEL_EVENTS 
/*****************************************************************************
* Name     : trgDatabase_DDL_Events
* Purpose  : Logs EVENTDATA() of database DDL events to a table.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	08/11/2020	DMason	Created
*	08/13/2020	DMason	Exclude specific events from logging.
*	09/17/2020	DMason	Exclude all SSISDB events from logging.
*	10/01/2020	DMason	Explicitly SET ANSI_PADDING ON
*		Determine if affected object is a system object.
*	01/19/2021	DMason	Exclude Service Broker-related events.
*	08/13/2021	DMason	Included WITH RESULT SETS for [sp_executesql]. This
*		may help when running OPENQUERY from remote server to local server.
*		CHANGE ROLLED BACK: it caused SQL Agent jobs to fail. Example error:
*			ALTER INDEX [idx_name] ON [dbo].[table] DISABLE; 
*			[SQLSTATE 01000] (Message 0)  EXECUTE statement failed because its 
*			WITH RESULT SETS clause specified 1 result set(s), but the statement 
*			only sent 0 result set(s) at run time. [SQLSTATE 42000] (Error 11536). 
*	02/10/2022	DMason	Exclude database [mssqlsystemresource] from the code
*		that determines if the affected object is a system object.
******************************************************************************/
AS 
BEGIN
	SET NOCOUNT ON;

	/*
		With this trigger enabled, we get the following error. Lets try explicitly setting ANSI_PADDING.

		Replication-Replication Distribution Subsystem: agent <AgentName> failed. 
		SELECT failed because the following SET options have incorrect settings: 'ANSI_PADDING'. 
		Verify that SET options are correct for use with indexed views and/or indexes on computed columns and/or filtered indexes and/or query notifications and/or XML data t 
	*/
	SET ANSI_PADDING ON;	--Default is ON.

	DECLARE @IsLoggedEvent BIT = 1;
	DECLARE @is_ms_shipped BIT = 0;
	DECLARE @EventData XML = EVENTDATA();
	DECLARE @EventName VARCHAR(MAX) = @EventData.value('(/EVENT_INSTANCE/EventType)[1]','VARCHAR(MAX)');
	DECLARE @DBName SYSNAME =  @EventData.value('(/EVENT_INSTANCE/DatabaseName)[1]','VARCHAR(128)');
	DECLARE @SchemaName SYSNAME =  @EventData.value('(/EVENT_INSTANCE/SchemaName)[1]','VARCHAR(128)');
	DECLARE @ObjName SYSNAME =  @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]','VARCHAR(128)');
	DECLARE @Scope VARCHAR(64) =
		CASE
			WHEN @EventData.value('(/EVENT_INSTANCE/TargetObjectType)[1]','VARCHAR(128)') = 'Server' THEN 'Server'
			WHEN @EventData.value('(/EVENT_INSTANCE/DatabaseName)[1]','VARCHAR(128)') <> '' THEN 'Database'
			ELSE NULL
		END
	DECLARE @EventDateTime DATETIME = @EventData.value('(/EVENT_INSTANCE/PostTime)[1]','DATETIME');
	DECLARE @CommandText VARCHAR(MAX) = @EventData.value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]','VARCHAR(MAX)');

	--These events occur during routine index maintenance.
	IF @EventName IN ('CREATE_STATISTICS', 'UPDATE_STATISTICS', 'DROP_STATISTICS') SET @IsLoggedEvent = 0;
	
	--Service Broker events to be excluded.
	IF @EventName IN ('ALTER_QUEUE', 'CREATE_QUEUE', 'DROP_QUEUE',
		'CREATE_CONTRACT', 'DROP_CONTRACT', 
		'CREATE_MESSAGE_TYPE', 'DROP_MESSAGE_TYPE', 
		'CREATE_SERVICE', 'DROP_SERVICE') SET @IsLoggedEvent = 0;

	--Do not log index maintenance operations.
	IF @EventName IN ('ALTER_INDEX') 
		AND (
			@CommandText LIKE '%REBUILD%' 
			OR @CommandText LIKE '%REORGANIZE%' 
			OR @CommandText LIKE '%DISABLE%'
		) SET @IsLoggedEvent = 0;

	--Do not log partition switching operations.
	IF @EventName IN ('ALTER_TABLE') AND @CommandText LIKE '%SWITCH%PARTITION%' 
		SET @IsLoggedEvent = 0;

	--Do not log changes to SSISDB (these are already logged).
	IF @DBName IN ('SSISDB') SET @IsLoggedEvent = 0;
	
	IF @DBName NOT IN ('', 'mssqlsystemresource') AND @SchemaName <> '' AND @ObjName <> ''
	BEGIN
		--Check the [DatabaseName].[SchemaName].[ObjectName]. We don't care about system objects (is_ms_shipped = 1).
		DECLARE @TSql NVARCHAR(MAX) = 
		'SELECT @is_ms_shipped_OUT = o.is_ms_shipped 
		FROM ' + QUOTENAME(@DBName) + '.sys.schemas s 
		JOIN ' + QUOTENAME(@DBName) + '.sys.objects o 
			ON o.schema_id = s.schema_id 
		WHERE s.name = @SchemaName 
		AND o.name = @ObjName;';

		EXEC sp_executesql 
			@TSql, 
			N'@SchemaName SYSNAME, @ObjName SYSNAME, @is_ms_shipped_OUT BIT OUTPUT', 
			@SchemaName, @ObjName, @is_ms_shipped_OUT = @is_ms_shipped OUTPUT;

		IF @is_ms_shipped = 1 SET @IsLoggedEvent = 0
	END

	IF @IsLoggedEvent = 1
	BEGIN
		INSERT INTO DbaMetrics.dbo.DDL_Eventdata
			(EventName, Scope, EventDateTime, [EventData])
		VALUES
			(@EventName, @Scope, @EventDateTime, @EventData);
	END
END
GO

ENABLE TRIGGER [trgDatabase_DDL_Events] ON ALL SERVER
GO
