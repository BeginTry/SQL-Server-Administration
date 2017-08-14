USE DbaData
GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES r
    WHERE r.ROUTINE_SCHEMA = 'dba'
    AND r.ROUTINE_NAME = 'GetXESessionData_ErrorReported'
)
BEGIN
    EXEC('CREATE PROC dba.GetXESessionData_ErrorReported AS PRINT CURRENT_TIMESTAMP;');
END;
GO

ALTER PROC dba.GetXESessionData_ErrorReported
/*
	Purpose: 
	Returns Extended Events Session data.
 
	Inputs: none.

	History:
	09/15/2016	DBA	Created
*/
AS
BEGIN
    --SPID included within session name for isolation.
    DECLARE @XESessionName SYSNAME =
        'Error Handling Session(SPID ' + CAST(@@SPID AS VARCHAR) + ')';

	----<temp>
	--INSERT INTO DbaData.dba.Debug([Message])
	--VALUES('dba.GetXESessionData_ErrorReported @@SPID = ' + CAST(@@SPID AS VARCHAR));
	----</temp>

    IF NOT EXISTS(
        SELECT *
        FROM master.sys.server_event_sessions s
        WHERE s.name = @XESessionName
    )
    BEGIN
        DECLARE @ErrMsg NVARCHAR(MAX) = 'Session ' + @XESessionName + ' does not exist.';
        RAISERROR(@ErrMsg, 16, 1);
        RETURN;
    END

    DECLARE @XEData XML

    SELECT @XEData = CAST(xet.target_data AS XML)
    FROM sys.dm_xe_session_targets AS xet
    JOIN sys.dm_xe_sessions AS xe
        ON (xe.address = xet.event_session_address)
    WHERE xe.name = @XESessionName;

	/*
		Check value of "totalEventsProcessed" to ensure events have been 
		dispatched to event session target (ring_buffer).
		If no events have been processed, delay for a period > MAX_DISPATCH_LATENCY (in seconds).
	*/
    IF @XEData.value('(/RingBufferTarget/@totalEventsProcessed)[1]', 'INT') = 0
    BEGIN
        WAITFOR DELAY '00:00:02';

        SELECT @XEData = CAST(xet.target_data AS XML)
        FROM sys.dm_xe_session_targets AS xet
        JOIN sys.dm_xe_sessions AS xe
            ON (xe.address = xet.event_session_address)
        WHERE xe.name = @XESessionName;
    END

	--SELECT signature must match the [dba].[ErrorReported] table type:
    SELECT 
        x.c.value(N'(action[@name="session_id"]/value)[1]', N'SMALLINT') AS SessionId,
        x.c.value(N'(@name)[1]', N'NVARCHAR(MAX)') AS EventName,
        x.c.value(N'(@timestamp)[1]', N'datetime') AS EventTime,
        x.c.value(N'(data[@name="error_number"]/value)[1]', N'INT') AS ErrorNumber,
        x.c.value(N'(data[@name="severity"]/value)[1]', N'INT') AS Severity,
        x.c.value(N'(data[@name="state"]/value)[1]', N'INT') AS [State],
        x.c.value(N'(data[@name="user_defined"]/value)[1]', N'NVARCHAR(MAX)') AS UserDefined,
        x.c.value(N'(data[@name="category"]/text)[1]', N'NVARCHAR(MAX)') AS Category,
        x.c.value(N'(data[@name="destination"]/text)[1]', N'NVARCHAR(MAX)') AS Destination,
        x.c.value(N'(data[@name="is_intercepted"]/value)[1]', N'NVARCHAR(MAX)') AS IsIntercepted,
        x.c.value(N'(data[@name="message"]/value)[1]', N'NVARCHAR(MAX)') AS [Message],
        x.c.value(N'(action[@name="sql_text"]/value)[1]', N'NVARCHAR(MAX)') AS SqlText
    FROM @XEData.nodes('//RingBufferTarget/event') AS x(c)
END
GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES r
    WHERE r.ROUTINE_SCHEMA = 'dba'
    AND r.ROUTINE_NAME = 'DropXESession_ErrorReported'
)
BEGIN
    EXEC('CREATE PROC dba.DropXESession_ErrorReported AS PRINT CURRENT_TIMESTAMP;');
END;
GO

ALTER PROC dba.DropXESession_ErrorReported
/*
	Purpose: 
	Stops and drops an Extended Events Session that monitor errors.
 
	Inputs: none.

	History:
	09/05/2016	DBA	Created
	08/07/2017	DBA	Check session state before attempting to stop.
*/
AS
BEGIN
    --SPID included within session name for isolation.
    DECLARE @XESessionName SYSNAME =
        'Error Handling Session(SPID ' + CAST(@@SPID AS VARCHAR) + ')';

	----<temp>
	--INSERT INTO DbaData.dba.Debug([Message])
	--VALUES('dba.DropXESession_ErrorReported @@SPID = ' + CAST(@@SPID AS VARCHAR));
	----</temp>

	IF EXISTS (
		SELECT *
		FROM master.sys.dm_xe_sessions s
		WHERE s.name = @XESessionName
		AND s.create_time IS NOT NULL
    )
	BEGIN
        EXEC('ALTER EVENT SESSION [' + @XESessionName + '] ON SERVER STATE=STOP;');
	END

    IF EXISTS(
        SELECT *
        FROM master.sys.server_event_sessions s
        WHERE s.name = @XESessionName
    )
    BEGIN
        EXEC('DROP EVENT SESSION [' + @XESessionName + '] ON SERVER;');
    END
END
GO

IF EXISTS (
    SELECT *
    FROM sys.types
    WHERE name = 'ErrorReported'
    AND schema_id = SCHEMA_ID('dba')
)
	DROP TYPE dba.ErrorReported;
GO

CREATE TYPE dba.ErrorReported AS TABLE (
	SessionId SMALLINT,
	EventName NVARCHAR(MAX),
	EventTime DATETIME,
	ErrorNumber INT,
	Severity INT,
	[State] NVARCHAR(MAX),
	UserDefined NVARCHAR(MAX),
	Category NVARCHAR(MAX),
	Destination NVARCHAR(MAX),
	IsIntercepted NVARCHAR(MAX),
	[Message] NVARCHAR(MAX),
	SqlText NVARCHAR(MAX)
);
GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES r
    WHERE r.ROUTINE_SCHEMA = 'dba'
    AND r.ROUTINE_NAME = 'CreateXESession_ErrorReported'
)
BEGIN
    EXEC('CREATE PROC dba.CreateXESession_ErrorReported AS PRINT CURRENT_TIMESTAMP;');
END;
GO

ALTER PROC dba.CreateXESession_ErrorReported
/*
	Purpose: 
	Creates and starts an Extended Events Session to monitor errors (if any) that occur.
	The session is filtered by the current SPID. 
 
	Inputs: none.

	History:
	09/05/2016	DBA	Created
*/
AS
BEGIN
    DECLARE @XESessionName SYSNAME =
        'Error Handling Session(SPID ' + CAST(@@SPID AS VARCHAR) + ')';

	----<temp>
	--INSERT INTO DbaData.dba.Debug([Message])
	--VALUES('dba.CreateXESession_ErrorReported @@SPID = ' + CAST(@@SPID AS VARCHAR));
	----</temp>

    IF EXISTS(
        SELECT *
        FROM master.sys.server_event_sessions s
        WHERE s.name = @XESessionName
    )
    BEGIN
        --DECLARE @ErrMsg NVARCHAR(MAX) = 'Session ' + @XESessionName + ' already exists.';
        --RAISERROR(@ErrMsg, 16, 1);
        --RETURN;

		/*
			An XEvent session for this SPID already exists, 
			and apparently was never dropped.
			Gather some info and email to DBA.
		*/
		DECLARE @Subject NVARCHAR(255) = @@SERVERNAME + ' -- Orphaned XEvent Session';
		DECLARE @MsgBody NVARCHAR(MAX) = '[dba].[CreateXESession_ErrorReported] invoked at: ' + 
				CONVERT(VARCHAR, CURRENT_TIMESTAMP, 113) + '<br/>';

		--If the session is still running, include session 
		--start time in the email message body.
		SELECT @MsgBody = @MsgBody + 'Orphaned session started at:  ' +
			CONVERT(VARCHAR, r.create_time, 113) + '<br/>' + @MsgBody
		FROM master.sys.dm_xe_sessions r
		WHERE r.name = @XESessionName
		AND r.create_time IS NOT NULL;

		SELECT @MsgBody = @MsgBody + '<br/>';

		DECLARE @ErrData AS dba.ErrorReported;
		INSERT INTO @ErrData 
		EXEC dba.GetXESessionData_ErrorReported;

		IF EXISTS ( SELECT 1 FROM @ErrData)
		BEGIN
			--If the session has gathered any error data, 
			--include it in the email for analysis.
			SET @MsgBody = @MsgBody + 
			(
				SELECT 
					d.SessionId AS td,
					d.EventName AS td,
					d.EventTime AS td,
					d.ErrorNumber AS td,
					d.Severity AS td,
					d.[State] AS td,
					d.UserDefined AS td,
					d.Category AS td,
					d.Destination AS td,
					d.IsIntercepted AS td,
					REPLACE(d.[Message], CHAR(13) + CHAR(10), '<br/>') AS td,
					REPLACE(d.SqlText, CHAR(13) + CHAR(10), '<br/>') AS td
				FROM @ErrData d
				FOR XML RAW ('tr'), ROOT('table'), ELEMENTS
			)
			SET @MsgBody = REPLACE(@MsgBody, '<table>', '<table border="1">
			<tr><th>SessionId</th><th>EventName</th><th>EventTime</th><th>ErrorNumber</th><th>Severity</th><th>State</th><th>UserDefined</th><th>Category</th><th>Destination</th><th>IsIntercepted</th><th>Message</th><th>SqlText</th></tr>');
		END

		EXEC msdb.dbo.sp_send_dbmail 
			@recipients = 'DBA@Domain.com', 
			@profile_name = 'Default',
			@subject = @Subject,
			@body = @MsgBody,
			@body_format = 'HTML';

		--Drop the orphaned XEvent session to clear out the ring buffer target.
        EXEC dba.DropXESession_ErrorReported;
    END

    --Include SPID within session name for isolation.
    DECLARE @TSql NVARCHAR(MAX) = 'CREATE EVENT SESSION [' + @XESessionName + '] 
ON SERVER 
ADD EVENT sqlserver.error_reported
(
    ACTION(
        sqlserver.session_id,
        sqlserver.sql_text
    )
    WHERE [package0].[not_equal_unicode_string]([message],N'''''''''''') 
    AND [severity]>(10) 
    AND [sqlserver].[session_id]=(' + CAST(@@SPID AS VARCHAR) + ')
) 
ADD TARGET package0.ring_buffer
WITH (';

	DECLARE @MajorVersion INT = CAST(SERVERPROPERTY('ProductMajorVersion') AS INT);
	DECLARE @EventRetentionMode NVARCHAR(MAX) = 'NO_EVENT_LOSS';	--Default

	--NO_EVENT_LOSS fails on SQL 2014 SP2 AND SQL2016 SP1
	IF @MajorVersion = 12 OR @MajorVersion = 13
		SET @EventRetentionMode = 'ALLOW_SINGLE_EVENT_LOSS';

	SET @TSql = @TSql + '
	EVENT_RETENTION_MODE=' + @EventRetentionMode + ',
    MAX_MEMORY=4096 KB,
    MAX_DISPATCH_LATENCY=1 SECONDS,
    MAX_EVENT_SIZE=0 KB,
    MEMORY_PARTITION_MODE=NONE,
    TRACK_CAUSALITY=ON,
    STARTUP_STATE=OFF
);'
    --PRINT @TSql;
    EXEC(@TSql);

    SET @TSql = 'ALTER EVENT SESSION [' + @XESessionName + '] ON SERVER STATE=START;';
    EXEC(@TSql);
END
GO

--This PROC is deprecated. Drop if exists.
IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'GetLastErrorMessage'
)
	DROP PROCEDURE dbo.GetLastErrorMessage 
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'BackupDatabases'
)
	DROP PROCEDURE dba.BackupDatabases 
GO

CREATE PROCEDURE dba.BackupDatabases 
	@DifferentialOnly BIT,
	@Path VARCHAR(255),
	@MirrorToPath VARCHAR(255) = NULL,
	@RAISERROR BIT = 1,
	@WithEncryption BIT = 0,
	@ServerCertificate SYSNAME = NULL
AS 
/*
	Purpose:	
	Performs multiple database backups.
		Non-system db's
		Some system db's (see below)
		Online db's
		Db's that are not snapshots.
	
	Inputs:
	@DifferentialOnly - specify 1 for a differential backup, 
		or 0 for a differential backup.
	@Path - the path where the backup files are to be created.
	@MirrorToPath - (optional) the path where a copy of the backup files are to be created.
	@RAISERROR - (optional) indicates if an error should be raised if there is a backup failure.
	@WithEncryption - (optional) create backup with enctyption.
	@ServerCertificate - (optional) name of server certificate.

	History:
	08/03/2009	DBA	Created
	11/03/2009	DBA	Validation of backups is for ONLINE db's only.
	01/29/2010	DBA	Add @MirrorToPath parameter to accomodate backup to multiple locations.
						@Path parameter is now required.
	03/22/2011	DBA	Remove TRY/CATCH block.  Error_Message() only captures the last error, which
						states "BACKUP DATABASE is terminating abnormally."  This tells us nothing!
						Instead, use xp_CmdShell to run sqlcmd.exe and save stdout to a table.
	04/26/2011	DBA	Allow differential backups of msdb.
	05/03/2011	DBA	Make msdb the last db backed up.  We want the backup history of all the other db's
						to be in the backup of msdb for DR purposes.
	09/20/2011	DBA	xp_CmdShell is still not capturing backup failure messages.  
						New strategy:  used a CLR stored proc to call sp dba.BackupDatabase.
						Added optional param @RAISERROR.
	11/15/2011	DBA	msdb sometimes isn't in the cursor select list. (?)
						Specify cursor as CURSOR FORWARD_ONLY READ_ONLY STATIC.
	02/17/2012	DBA	Include [distribution] in the backups.
	07/26/2013	DBA	MS recommendation is to backup databases ReportServer (with FULL recovery model) 
						and ReportServerTempDB (with SIMPLE recovery model).
						Therefore, do not exclude them.
						http://msdn.microsoft.com/en-us/library/ms155814(v=sql.105).aspx
	02/02/2014	DBA	Include distribution in backups.
						Use dba.SendMailByOperator in lieu of msdb.dbo.sp_send_dbmail
	04/17/2014	DBA	Yet another new strategy: use [dba].[GetLastErrorMessage] (which is
						pure tsql) in lieu of CLR stored proc.
						Resume use of [msdb].[dbo].[sp_send_dbmail].
	09/21/2016	DBA	[dba].[GetLastErrorMessage] does not get *all* the error messages.
						Enhance TRY...CATCH error handling with Extended Events.
						http://itsalljustelectrons.blogspot.com/2016/09/Enhanced-TSQL-Error-Handling-With-Extended-Events-Part2.html
	11/04/2016	DBA	Add encryption options.
	11/29/2016	DBA	Change @RAISERROR default to 1.
	07/29/2017	DBA	Exclude DBs in standby mode.
*/
DECLARE @ExcludedDB TABLE (DbName SYSNAME);
DECLARE @BackupType CHAR(1) = 'D';	--Default value:  indicates a FULL backup
--This will be the body of an HTML-formatted email message.
DECLARE @Body NVARCHAR(MAX) = '<table border="1">' +
    '<tr>' +
    '<th>Database Name</th>' +
    '<th>Error Number</th>' +
    '<th>Error Message</th>' +
    '</tr>';
DECLARE @BackupFailures BIT = 0;
DECLARE @ErrData AS dba.ErrorReported;
DECLARE @DbName SYSNAME;

IF @DifferentialOnly = 1
	BEGIN
		--No differential backups for master.  (Full only.)
		INSERT INTO @ExcludedDB (DbName) VALUES ('master')

		SET @BackupType = 'I'
	END 

--These db's are never backed up.
INSERT INTO @ExcludedDB (DbName) VALUES ('tempdb')
INSERT INTO @ExcludedDB (DbName) VALUES ('model')

DECLARE curDBs CURSOR FORWARD_ONLY READ_ONLY STATIC FOR
	SELECT name
	FROM sys.databases
	WHERE state_desc = 'ONLINE'
	AND source_database_id IS NULL
	AND name NOT IN 
		(SELECT DbName FROM @ExcludedDB)
	AND is_in_standby = 0
	ORDER BY CASE WHEN name = 'msdb' THEN 1 ELSE 0 END, name

OPEN curDBs;
FETCH NEXT FROM curDBs INTO @DbName;
EXEC dba.CreateXESession_ErrorReported;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		IF @BackupType = 'D'
		BEGIN
			EXECUTE dba.BackupDatabase_FULL 
				@DBName = @DbName,
				@Path = @Path,
				@MirrorToPath = @MirrorToPath,
				@WithEncryption = @WithEncryption,
				@ServerCertificate = @ServerCertificate
		END
		ELSE IF @BackupType = 'I'
		BEGIN
			EXECUTE dba.BackupDatabase_DIFF
				@DBName = @DbName,
				@Path = @Path,
				@MirrorToPath = @MirrorToPath,
				@WithEncryption = @WithEncryption,
				@ServerCertificate = @ServerCertificate
		END
	END TRY
	BEGIN CATCH
		SET @BackupFailures = 1;

		INSERT INTO @ErrData 
        EXEC dba.GetXESessionData_ErrorReported;
	
		--Add "rows" to the HTML <table>.
        SELECT @Body = @Body + CHAR(13) + CHAR(10) + '<tr>' +
            '<td>' + @DbName + '</td>' +
            '<td>' + CAST(e.ErrorNumber AS VARCHAR) + '</td>' +
            '<td>' + e.[Message] + '</td>' +
            '</tr>'
        FROM @ErrData e

		--Drop & recreate the XEvent session to clear out the ring buffer target.
        EXEC dba.DropXESession_ErrorReported;
        EXEC dba.CreateXESession_ErrorReported;
		DELETE FROM @ErrData;
	END CATCH

	FETCH NEXT FROM curDBs INTO @DbName
END

CLOSE curDBs
DEALLOCATE curDBs
EXEC dba.DropXESession_ErrorReported;

IF @BackupFailures = 1
BEGIN
	DECLARE @Subject NVARCHAR(255) = @@SERVERNAME + ' -- Backup Errors'
	SET @Body = @Body + '</table>'
	
	EXEC msdb.dbo.sp_send_dbmail 
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'Default',
		@subject = @Subject,
		@body = @Body,
		@body_format = 'HTML'
	
	IF @RAISERROR = 1
	BEGIN
		--This will cause any sql job that invokes the sp to fail.
		RAISERROR('One or more backup errors occurred.', 16, 1);
		RETURN
	END
END
GO
 
IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'BackupTransactionLogs'
)
	DROP PROCEDURE dba.BackupTransactionLogs 
GO

CREATE PROCEDURE dba.BackupTransactionLogs
	@Path VARCHAR(255),
	@MirrorToPath VARCHAR(255) = NULL,
	@RAISERROR BIT = 1,
	@WithEncryption BIT = 0,
	@ServerCertificate SYSNAME = NULL
AS 
/******************************************************************************
* Name     : dba.BackupTransactionLogs
* Purpose  : Performs transaction log backups on multiple databases:
*				•Non-system db's 
*				•Online db's
*				•Db's not in standby
*				•Db's set to full recovery model.
* Inputs   : @Path - the path where the transaction log backup files are to be created.
*			 @MirrorToPath - (optional) the path where a copy of the trx log backup file is to be created.
*			 @RAISERROR - (optional) flag to raise an error if backup failures are encountered.
*			 @WithEncryption - (optional) create backup with enctyption.
*			 @ServerCertificate - (optional) name of server certificate.
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	08/03/2009	DBA	Created
*	01/28/2010	DBA	Add @MirrorToPath parameter to accomodate multiple backup sets.
*	02/24/2010	DBA	@Path parameter is no longer optional.
*	07/26/13	DBA	Don't exclude ReportServer and ReportServerTempDB.
*						http://msdn.microsoft.com/en-us/library/ms155814(v=sql.105).aspx
*	02/02/2014	DBA	Use dba.SendMailByOperator in lieu of msdb.dbo.sp_send_dbmail
*	04/21/2014	DBA	Enhance sp for use with [dba].[GetLastErrorMessage]
*	09/21/2016	DBA	[dba].[GetLastErrorMessage] does not get *all* the error messages.
*						Enhance TRY...CATCH error handling with Extended Events.
*						http://itsalljustelectrons.blogspot.com/2016/09/Enhanced-TSQL-Error-Handling-With-Extended-Events-Part2.html
*	11/04/2016	DBA	Add encryption options.
*	11/29/2016	DBA	Change @RAISERROR default to 1.
******************************************************************************/;
DECLARE @DbName SYSNAME;
DECLARE @BackupFailures BIT = 0;
DECLARE @ErrData AS dba.ErrorReported;
--This will be the body of an HTML-formatted email message.
DECLARE @Body NVARCHAR(MAX) = '<table border="1">' +
    '<tr>' +
    '<th>Database Name</th>' +
    '<th>Error Number</th>' +
    '<th>Error Message</th>' +
    '</tr>';
DECLARE curDBs CURSOR FORWARD_ONLY READ_ONLY STATIC FOR
	SELECT name
	FROM sys.databases
	WHERE state_desc = 'ONLINE'
	AND is_in_standby = 0
	AND recovery_model_desc = 'FULL'
	AND is_read_only = 0
	AND name NOT IN 
	(
		'master', 'tempdb', 'model', 'msdb'
	)
	ORDER BY name

IF RIGHT(@Path, 1) != '\'
	SET @Path = @Path + '\'

OPEN curDBs;
FETCH NEXT FROM curDBs INTO @DbName;
EXEC dba.CreateXESession_ErrorReported;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		EXEC dba.BackupTransactionLog 
			@DBName = @DbName, 
			@Path = @Path, 
			@MirrorToPath = @MirrorToPath, 
			@WithEncryption = @WithEncryption, 
			@ServerCertificate = @ServerCertificate;
	END TRY
	BEGIN CATCH
        SET @BackupFailures = 1;

        INSERT INTO @ErrData 
        EXEC dba.GetXESessionData_ErrorReported;

        --Add "rows" to the HTML <table>.
        SELECT @Body = @Body + CHAR(13) + CHAR(10) + '<tr>' +
            '<td>' + @DbName + '</td>' +
            '<td>' + CAST(e.ErrorNumber AS VARCHAR) + '</td>' +
            '<td>' + e.[Message] + '</td>' +
            '</tr>'
        FROM @ErrData e

        --Drop & recreate the XEvent session to clear out the ring buffer target.
        EXEC dba.DropXESession_ErrorReported;
        EXEC dba.CreateXESession_ErrorReported;
		DELETE FROM @ErrData;
    END CATCH

	FETCH NEXT FROM curDBs INTO @DbName
END

CLOSE curDBs
DEALLOCATE curDBs
EXEC dba.DropXESession_ErrorReported;

IF @BackupFailures = 1
BEGIN
	DECLARE @Subject NVARCHAR(255)
	SET @Subject = @@SERVERNAME + ' -- Trx Log Backup Errors'
	SET @Body = @Body + '</table>'
	
	EXEC msdb.dbo.sp_send_dbmail 
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'Default',
		@subject = @Subject,
		@body = @Body,
		@body_format = 'HTML'
	
	IF @RAISERROR = 1
	BEGIN
		--This will cause any sql job that invokes the sp to fail.
		RAISERROR('One or more transaction log backup errors occurred.', 16, 1);
		RETURN
	END
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'BackupDatabasesForArchive'
)
	DROP PROCEDURE dba.BackupDatabasesForArchive 
GO

CREATE PROCEDURE dba.BackupDatabasesForArchive 
	@Path VARCHAR(255),
	@MirrorToPath VARCHAR(255) = NULL,
	@RAISERROR BIT = 0,
	@WithEncryption BIT = 0,
	@ServerCertificate SYSNAME = NULL
AS 
/*
	Purpose:	
	Performs multiple database backups for archive purposes:
		[master] and [msdb] system databases
		User databases that are online and are not snapshots.
	
	Inputs:
	@Path - the path where the backup files are to be created.
	@MirrorToPath - (optional) the path where a copy of the backup files are to be created.
	@RAISERROR - (optional) indicates if an error should be raised if there is a backup failure.
	@WithEncryption - (optional) create backup with enctyption.
	@ServerCertificate - (optional) name of server certificate.

	History:
	07/25/2016	DBA	Created
	09/21/2016	DBA	[dba].[GetLastErrorMessage] does not get *all* the error messages.
						Enhance TRY...CATCH error handling with Extended Events.
						http://itsalljustelectrons.blogspot.com/2016/09/Enhanced-TSQL-Error-Handling-With-Extended-Events-Part2.html
	11/04/2016	DBA	Add encryption options.
	07/29/2017	DBA	Exclude DBs in standby mode.
*/
;
--This will be the body of an HTML-formatted email message.
DECLARE @Body NVARCHAR(MAX) = '<table border="1">' +
    '<tr>' +
    '<th>Database Name</th>' +
    '<th>Error Number</th>' +
    '<th>Error Message</th>' +
    '</tr>';
DECLARE @BackupFailures BIT = 0;
DECLARE @ErrData AS dba.ErrorReported;
DECLARE @DbName SYSNAME;

DECLARE curDBs CURSOR FORWARD_ONLY READ_ONLY STATIC FOR
	SELECT name
	FROM sys.databases
	WHERE state_desc = 'ONLINE'
	AND source_database_id IS NULL
	AND name NOT IN 
		('tempdb', 'model')
	AND is_in_standby = 0
	ORDER BY CASE WHEN name = 'msdb' THEN 1 ELSE 0 END, name

OPEN curDBs
FETCH NEXT FROM curDBs INTO @DbName
EXEC dba.CreateXESession_ErrorReported;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		EXEC DbaData.dba.BackupDatabase_Archive
			@DBName = @DbName,
			@Path = @Path,
			@MirrorToPath = @MirrorToPath,
			@WithEncryption = @WithEncryption,
			@ServerCertificate = @ServerCertificate;
	END TRY
	BEGIN CATCH
        SET @BackupFailures = 1;

        INSERT INTO @ErrData 
        EXEC dba.GetXESessionData_ErrorReported;

        --Add "rows" to the HTML <table>.
        SELECT @Body = @Body + CHAR(13) + CHAR(10) + '<tr>' +
            '<td>' + @DbName + '</td>' +
            '<td>' + CAST(e.ErrorNumber AS VARCHAR) + '</td>' +
            '<td>' + e.[Message] + '</td>' +
            '</tr>'
        FROM @ErrData e

        --Drop & recreate the XEvent session to clear out the ring buffer target.
        EXEC dba.DropXESession_ErrorReported;
        EXEC dba.CreateXESession_ErrorReported;
        DELETE FROM @ErrData;
    END CATCH

	FETCH NEXT FROM curDBs INTO @DbName
END

CLOSE curDBs;
DEALLOCATE curDBs;
EXEC dba.DropXESession_ErrorReported;

IF @BackupFailures = 1
BEGIN
	DECLARE @Subject NVARCHAR(255)
	SET @Subject = @@SERVERNAME + ' -- Backup Errors (Archives)'
	SET @Body = @Body + '</table>'
	
	EXEC msdb.dbo.sp_send_dbmail 
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'Default',
		@subject = @Subject,
		@body = @Body,
		@body_format = 'HTML'
	
	IF @RAISERROR = 1
	BEGIN
		--This will cause any sql job that invokes the sp to fail.
		RAISERROR('One or more backup errors occurred.', 16, 1);
		RETURN
	END
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'DefragmentIndexesByDatabase'
)
	DROP PROCEDURE dba.DefragmentIndexesByDatabase 
GO

CREATE PROCEDURE dba.DefragmentIndexesByDatabase
	@DBName SYSNAME,
	@MinFragmentation FLOAT = 5.0,
	@ReorgVsRebuildPercentThreshold FLOAT = 30.0,
	@DisplayOnly BIT = 0,
	@PrintTsql BIT = 0
AS 
/*
	Purpose:	
	Defragments indexes for a specific database that are at 
	a certain percent fragmentation or above.
	Note: avg_fragmentation_in_percent may not change after a 
	rebuild for some indexes, especially if there are few rows
	in the table.  If the page count for the table is below 100, 
	there's nothing to be concerned about.

	History:
	04/24/2014	DBA	Created
	06/16/2014	DBA	Validate existence of index before attempting to
						rebuild or reorganize.
	09/18/2014	DBA	If ONLINE rebuild fails, catch error and try again
						with ONLINE = OFF
	08/07/2015	DBA	Change recovery model from FULL to BULK_LOGGED (as 
						needed) before defragging.  Revert afterwards.
	08/26/2015	DBA	When a defrag attempt fails:
						Try a 2nd defrag, but only if certain criteria are met.
						Generate a logged error message with debugging details.
*/
DECLARE @Schema SYSNAME
DECLARE @Table SYSNAME
DECLARE @Index SYSNAME
DECLARE @AvgFragPct FLOAT
DECLARE @Tsql NVARCHAR(MAX)
DECLARE @Online NVARCHAR(8)

IF NOT EXISTS ( SELECT 1 FROM master.sys.databases WHERE name = @DBName )
BEGIN
	PRINT 'Database "' + COALESCE(@DBName, '') + '" does not exist.'
	RETURN
END

SELECT @Online = CASE WHEN CAST(SERVERPROPERTY ('edition') AS VARCHAR) LIKE '%Enterprise%' THEN 'ON' ELSE 'OFF' END

SELECT 
	CAST('' AS SYSNAME) SchemaName, 
	object_id TableId,
	CAST('' AS SYSNAME) TableName, 
	index_id IndexId, 
	CAST('' AS SYSNAME) IndexName,
	avg_fragmentation_in_percent [Avg Fragmention %],
	page_count PageCount,
	CAST(0 AS BIT) AS IsReadOnly
INTO #FraggedIndexes
FROM sys.dm_db_index_physical_stats(
	DB_ID(@DBName),		--DBName
	NULL,	--TableName
	NULL, NULL, NULL)
WHERE index_id != 0
AND avg_fragmentation_in_percent >= @MinFragmentation

--Table_Schema + Table_Name (base tables)
SET @Tsql = 
	'UPDATE #FraggedIndexes ' +
	'SET SchemaName = s.name ,' +
	'TableName = t.name ' + 
	'FROM [' + @DBName + '].sys.schemas s ' +
	'JOIN [' + @DBName + '].sys.tables t ' +
		'ON t.schema_id = s.schema_id ' +
	'WHERE t.object_id = TableId '
EXEC (@Tsql)

--View_Schema + View_Name (indexed views)
SET @Tsql = 
	'UPDATE #FraggedIndexes  ' +
	'SET SchemaName = s.name , ' +
	'TableName = v.name   ' +
	'FROM [' + @DBName + '].sys.schemas s  ' +
	'JOIN [' + @DBName + '].sys.views v  ' +
		'ON v.schema_id = s.schema_id  ' +
	'WHERE v.object_id = TableId  ' +
	'AND SchemaName = ''''  ' +
	'AND TableName = ''''  ' 
EXEC (@Tsql)	

SET @Tsql = 
	'UPDATE #FraggedIndexes ' +
	'SET IndexName = i.name, ' +
	'IsReadOnly = fg.is_read_only ' +
	'FROM [' + @DBName + '].sys.indexes i ' +
	'JOIN [' + @DBName + '].sys.filegroups fg ' +
	'    ON fg.data_space_id = i.data_space_id ' + 
	'WHERE i.object_id = TableId ' +
	'AND i.index_id = IndexId '
EXEC (@Tsql)

IF @DisplayOnly = 1
BEGIN
	SELECT * FROM #FraggedIndexes ORDER BY TableName
END
ELSE
BEGIN
	DECLARE curFrags CURSOR FOR
		SELECT IndexName, SchemaName, TableName, [Avg Fragmention %]
		FROM #FraggedIndexes
		WHERE IsReadOnly = 0 
		ORDER BY TableName
	OPEN curFrags
	FETCH NEXT FROM curFrags INTO @Index, @Schema, @Table, @AvgFragPct 

	--Get the recovery model of the db.
	DECLARE @RModel NVARCHAR(60)
	SELECT @RModel = d.recovery_model_desc
	FROM master.sys.databases d
	WHERE d.name = @DBName

	--Change recovery model from FULL to BULK_LOGGED.
	IF @@FETCH_STATUS = 0 AND UPPER(@RModel) = 'FULL'
	BEGIN
		SET @Tsql = 'ALTER DATABASE [' + @DBName + ']
		SET RECOVERY BULK_LOGGED
		WITH NO_WAIT'
		EXEC (@Tsql)
	END

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Tsql = 'IF EXISTS (
			SELECT *
			FROM [' + @DBName + '].sys.indexes i
			WHERE i.name = ''' + @Index + '''
			AND i.object_id = OBJECT_ID(''' + @DBName + '.' + @Schema + '.' + @Table + ''')
		)' + CHAR(13) + CHAR(10)

		IF @AvgFragPct <= @ReorgVsRebuildPercentThreshold
			SET @Tsql = @Tsql + 'ALTER INDEX [' + @Index + '] ON [' + @DBName + '].[' + @Schema + '].[' + @Table + '] REORGANIZE '
		ELSE
			SET @Tsql = @Tsql + 'ALTER INDEX [' + @Index + '] ON [' + @DBName + '].[' + @Schema + '].[' + @Table + '] REBUILD WITH (SORT_IN_TEMPDB = ON, ONLINE = ' + @Online + ')'	
		
		BEGIN TRY
			EXEC (@Tsql)
		END TRY
		BEGIN CATCH
			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;
			DECLARE @ErrDetails VARCHAR(2000);
		    
			SELECT 
				@ErrorMessage = ERROR_MESSAGE(),
				@ErrorSeverity = ERROR_SEVERITY(),
				@ErrorState = ERROR_STATE();

			SET @ErrDetails = 
				'Index Defragmentation Failed.  Index Name: [' + @Index + ']  ' +
				'Indexed Object Name: [' + @DBName + '].[' + @Schema + '].[' + @Table + '] ' +
				'Fragmentation Percent: ' + CAST(@AvgFragPct AS VARCHAR);

			IF @AvgFragPct > @ReorgVsRebuildPercentThreshold AND @Online = 'ON'
			BEGIN
				BEGIN TRY
					--Try to rebuild the index as before, but offline.
					SET @Tsql =  REPLACE(@Tsql, 'ONLINE = ON', 'ONLINE = OFF')
					EXEC (@Tsql)
				END TRY
				BEGIN CATCH
					--If offline index rebuild still fails, throw a custom error
					RAISERROR (@ErrDetails, 16, 1) WITH LOG;
					
					--"Throw" the original error.
					RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
				END CATCH
			END
			ELSE
			BEGIN
				--Throw a custom error.
				RAISERROR (@ErrDetails, 16, 1) WITH LOG;

				--"Throw" the original error.
				RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
			END
		END CATCH
		
		IF @PrintTsql = 1
			PRINT @Tsql

		FETCH NEXT FROM curFrags INTO @Index, @Schema, @Table, @AvgFragPct 
	END

	CLOSE curFrags
	DEALLOCATE curFrags

	INSERT INTO dba.IndexRebuildHistory
		(DatabaseName, SchemaName, TableName, IndexName, FragmentationPct, PageCount)
	SELECT 
		@DBName,
		SchemaName, 
		TableName, 
		IndexName,
		[Avg Fragmention %],
		PageCount
	FROM #FraggedIndexes
	WHERE PageCount >= 100

	--If the recovery model was changed, revert to FULL.
	IF EXISTS (
		SELECT *
		FROM master.sys.databases d
		WHERE d.name = @DBName
		AND d.recovery_model_desc <> 'FULL'
		AND UPPER(@RModel) = 'FULL'
	)
	BEGIN
		SET @Tsql = 'ALTER DATABASE [' + @DBName + ']
			SET RECOVERY FULL
			WITH NO_WAIT'
		EXEC (@Tsql)

		--TODO: backup t-log?
	END
END

DROP TABLE #FraggedIndexes
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'DefragmentIndexes'
)
	DROP PROCEDURE dba.DefragmentIndexes 
GO

CREATE PROCEDURE dba.DefragmentIndexes
	@MinFragmentation FLOAT = 5.0,
	@ReorgVsRebuildPercentThreshold FLOAT = 30.0
AS 
/*
	Purpose:	
	Defragments indexes on [master], [msdb], and user db's that are:
		•Online
		•In MULTI_USER mode
		•Not in standby
		•Not read-only

	History:
	04/23/2014	DBA	Created
*/
DECLARE @DbName SYSNAME

DECLARE curDBs cursor FOR
	SELECT name
	FROM sys.databases
	WHERE user_access_desc = 'MULTI_USER'
	AND state_desc = 'ONLINE'
	AND is_read_only = 0
	AND is_in_standby = 0
	AND name NOT IN 
	(
		'tempdb', 'model'
	)
	ORDER BY name

OPEN curDBs
FETCH NEXT FROM curDBs INTO @DbName

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC dba.DefragmentIndexesByDatabase @DbName, @MinFragmentation, @ReorgVsRebuildPercentThreshold
	FETCH NEXT FROM curDBs INTO @DbName
END

CLOSE curDBs
DEALLOCATE curDBs
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'ReorganizeFullTextCatalogs'
)
	DROP PROCEDURE dba.ReorganizeFullTextCatalogs 
GO

CREATE PROCEDURE dba.ReorganizeFullTextCatalogs
AS 
/*
	Purpose:	
	Reorganizes the FullText Catalogs (as needed) on all user databases.
	
	Inputs: None

	History:
	02/25/2014	DBA	Created
	06/19/2014	DBA	[sys].[fulltext_index_fragments] did not exist until SQL 2008.
						For previous versions, RETURN.
*/
IF NOT @@VERSION > 'Microsoft SQL Server 2008'
	RETURN;

--This is the tsql statement that gets executed on each db.
DECLARE @InnerSql NVARCHAR(MAX) 
SET @InnerSql =
	'DECLARE @Tsql NVARCHAR(MAX)
	DECLARE @FtcName SYSNAME
	DECLARE curFtcName CURSOR FAST_FORWARD READ_ONLY FOR
		SELECT DISTINCT FullTextCatalogName
		FROM (
			SELECT c.Name FullTextCatalogName, COUNT(*) TableIndexFragmentCount
			FROM sys.fulltext_catalogs c
			JOIN  sys.fulltext_indexes i
				ON i.fulltext_catalog_id = c.fulltext_catalog_id
			JOIN sys.fulltext_index_fragments f
				ON f.table_id = i.object_id 
			GROUP BY c.Name, f.table_id 
			HAVING COUNT(*) > 1
		) TableIndexFragments

	OPEN curFtcName
	FETCH NEXT FROM curFtcName INTO @FtcName

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Tsql = ''''ALTER FULLTEXT CATALOG ['''' + @FtcName + ''''] REORGANIZE''''
		PRINT @Tsql
		EXEC (@Tsql)
		FETCH NEXT FROM curFtcName INTO @FtcName
	END

	CLOSE curFtcName
	DEALLOCATE curFtcName'
-------------------------------------------------------
--Iterate through the db's.
DECLARE @Tsql NVARCHAR(MAX)
DECLARE @DB SYSNAME
DECLARE curDB CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT name
	FROM sys.databases
	WHERE User_Access_Desc = 'MULTI_USER'
	AND State_Desc = 'ONLINE'
	AND Is_Read_Only = 0
	AND Is_In_Standby = 0
	AND name NOT IN ('master', 'tempdb', 'model', 'msdb')
	ORDER BY name

OPEN curDB
FETCH NEXT FROM curDB INTO @DB

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @Tsql = '[' + @DB + ']..sp_executesql N''' + @InnerSql + ''''
	EXEC sp_executesql @Tsql;

	FETCH NEXT FROM curDB INTO @DB
END

CLOSE curDB
DEALLOCATE curDB
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'UpdateStatisticsByDatabase'
)
	DROP PROCEDURE dba.UpdateStatisticsByDatabase 
GO

CREATE PROCEDURE dba.UpdateStatisticsByDatabase 
	@DBName SYSNAME,
	@WithFullScan BIT = 0
AS 
/*
	Purpose:	
	Updates statistics for all tables in a specific database.

	Inputs:
	@DBName: self-explanatory
	@WithFullScan: Compute statistics by scanning all rows in the table or indexed view.

	History:
	02/17/2009	DBA	Created
	02/24/2010	DBA	Renamed sp
	04/23/2014	DBA	Add @WithFullScan param.
	02/13/2015	DBA	Verify table exists before attempting stats update.
*/
DECLARE @Schema SYSNAME
DECLARE @Table SYSNAME
DECLARE @Tsql NVARCHAR(MAX)

IF NOT EXISTS ( SELECT 1 FROM master.sys.databases WHERE name = @DBName )
BEGIN
	PRINT 'Database "' + COALESCE(@DBName, '') + '" does not exist.'
	RETURN
END

CREATE TABLE #UserTables (
	SchemaName SYSNAME,
	TableName SYSNAME
)

SET @Tsql = 
	'SELECT s.name SchemaName, so.name TableName ' + 
	'FROM [' + @DBName + '].sys.stats st ' + 
	'JOIN [' + @DBName + '].sys.sysobjects so ' + 
	'	ON so.id = st.object_id ' + 
	'JOIN [' + @DBName + '].sys.tables t ' + 
	'	ON t.object_id = so.id ' + 
	'JOIN [' + @DBName + '].sys.schemas s ' + 
	'	ON s.schema_id = t.schema_id ' + 
	'GROUP BY s.name, so.name'

INSERT INTO #UserTables (SchemaName, TableName)
EXEC (@Tsql)

BEGIN
	DECLARE curTables CURSOR FAST_FORWARD READ_ONLY FOR
		SELECT SchemaName, TableName
		FROM #UserTables
		ORDER BY SchemaName, TableName
	OPEN curTables
	FETCH NEXT FROM curTables INTO @Schema, @Table

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Tsql = 
'IF EXISTS (
	SELECT * FROM [' + @DBName + '].INFORMATION_SCHEMA.TABLES t 
	WHERE t.TABLE_SCHEMA = ''' + @Schema + ''' AND t.TABLE_NAME = ''' + @Table + ''')
	UPDATE STATISTICS [' + @DBName + '].[' + @Schema + '].[' + @Table + '] ' 
		IF @WithFullScan = 1
			SET @Tsql = @Tsql + ' WITH FULLSCAN '
		PRINT @Tsql
		EXEC (@Tsql)
		FETCH NEXT FROM curTables INTO @Schema, @Table
	END

	CLOSE curTables
	DEALLOCATE curTables
END
DROP TABLE #UserTables
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'UpdateStatistics'
)
	DROP PROCEDURE dba.UpdateStatistics 
GO

CREATE PROCEDURE dba.UpdateStatistics
	@WithFullScan BIT = 0
AS 
/*
	Purpose:	
	Updates Statistics on multiple databases:
		•Non-system db's 
		•Online db's
		•Multi-user db's
		•Db's not in standby

	History:
	02/24/2010	DBA	Created
*/
DECLARE @DbName SYSNAME

DECLARE curDBs cursor FOR
	SELECT name
	FROM sys.databases
	WHERE User_Access_Desc = 'MULTI_USER'
	AND State_Desc = 'ONLINE'
	AND Is_Read_Only = 0
	AND Is_In_Standby = 0
	AND name NOT IN 
	(
		'master', 'tempdb', 'model'
	)
	ORDER BY name

OPEN curDBs
FETCH NEXT FROM curDBs INTO @DbName

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC dba.UpdateStatisticsByDatabase @DbName, @WithFullScan
	FETCH NEXT FROM curDBs INTO @DbName
END

CLOSE curDBs
DEALLOCATE curDBs
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'VerifyLatestBackup'
)
	DROP PROCEDURE dba.VerifyLatestBackup 
GO

CREATE PROCEDURE [dba].[VerifyLatestBackup]
	@DBName SYSNAME,
	@BackupType CHAR(1)
AS
/*
	Purpose:
	Verifies the last full or differrential backup of a database.
	
	Inputs:
	@DBName - self-explanatory.  
	@BackupType - D for database (full) backup, I for differential backup,
				  L for transaction log backup.

	History:
	02/12/2010	DBA	Created
	05/05/2010	DBA	Skip RESTORE VERIFYONLY if the db no longer exists.
	12/07/2010	DBA	Happy B-day, Larry Bird!
						Change logic of [LastBackup] CTE.  On Mondays, the SP was 
						trying to verify the Friday backups, which no longer exist.
	07/28/2014	DBA	Allow backup type 'L' for transaction log backup verification.
	08/19/2014	DBA	For obvious reasons, don't attempt to verify a transaction log
						backup for a database set for the SIMPLE recovery model.
	02/22/2015	DBA	Change case for instances/db's with case-sensitive collation.
*/
IF COALESCE(@BackupType, '') NOT IN ('D', 'I', 'L')
BEGIN
	RAISERROR('Invalid value for @BackupType (Valid values are ''D'', ''I'', or ''L'').', 16, 1);
	RETURN
END

IF NOT EXISTS ( SELECT * FROM master.sys.databases WHERE name = @DBName )
BEGIN
	--RAISERROR('@DBName must be the name of an existing database.', 16, 1);
	PRINT 'Database [' + @DBName + '] does not exist.  Latest backup will not be verified...'
	RETURN
END

IF @BackupType = 'L' AND EXISTS (
	SELECT 1
	FROM master.sys.databases d
	WHERE d.name = @DBName
	AND d.recovery_model_desc = 'SIMPLE'
)
	RETURN;

DECLARE @Tsql VARCHAR(MAX)
SET @Tsql = 'RESTORE VERIFYONLY FROM '

;WITH LastBackup AS
(
	SELECT TOP(1) media_set_id
	FROM msdb.dbo.backupset
	WHERE database_name = @DBName
	AND type = @BackupType
	ORDER BY backup_start_date DESC
)
SELECT @Tsql = @Tsql + CHAR(13) + CHAR(10) + CHAR(9) + 'DISK = ''' + bmf.physical_device_name + ''', '
FROM LastBackup lb
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = lb.media_set_id
WHERE bmf.mirror = 0
ORDER BY bmf.family_sequence_number

SET @Tsql = LEFT(@Tsql, LEN(@Tsql) - 1)
--PRINT @Tsql
PRINT 'Verify last ' + CASE @BackupType WHEN 'D' THEN 'FULL' WHEN 'I' THEN 'DIFFERENTIAL' WHEN 'L' THEN 'TRANSACTION LOG' END + ' backup for [' + @DBName + '] :'
EXEC(@Tsql)

GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'VerifyLatestBackups'
)
	DROP PROCEDURE dba.VerifyLatestBackups 
GO

CREATE PROCEDURE dba.VerifyLatestBackups
	@BackupType CHAR(1)
AS
/*
	Purpose:
	Verifies the last full or differrential backups of all backed up databases.
	
	Inputs:
	@BackupType - D for database (full) backup, I for differential backup.

	History:
	02/12/2010	DBA	Created
	12/21/2010	DBA	Only verify backups for db's currently on the server.
	01/26/2011	DBA	Only verify backups for db's that are online.
	03/22/2011	DBA	Check @@ERROR after running [dba].[VerifyLatestBackup] on 
						each db.  Keep a list of db's with invalidated backups and 
						RAISERROR with appropriate message as needed.
	07/28/2014	DBA	Allow backup type 'L' for transaction log backup verification.
	02/22/2015	DBA	Change case for instances/db's with case-sensitive collation.
*/
IF COALESCE(@BackupType, '') NOT IN ('D', 'I', 'L')
BEGIN
	RAISERROR('Invalid value for @BackupType (Valid values are ''D'', ''I'', or ''L'').', 16, 1);
	RETURN
END

DECLARE @InvalidBackups TABLE ( DbName SYSNAME )
DECLARE @Db SYSNAME
DECLARE curDb CURSOR FOR
	SELECT database_name
	FROM msdb.dbo.backupset
	WHERE type = @BackupType
	--Exclude these system db's, which are not backed up.
	AND database_name NOT IN ('tempdb', 'model')
	AND database_name IN ( SELECT name FROM master.sys.databases WHERE state_desc = 'ONLINE')
	GROUP BY database_name
	ORDER BY database_name

OPEN curDb
FETCH NEXT FROM curDb INTO @Db

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC dba.VerifyLatestBackup @Db, @BackupType
	
	IF @@ERROR != 0
	BEGIN
		INSERT INTO @InvalidBackups(DbName) VALUES(@Db)

		--If Trx Log failed verification fails, the log chain may be 
		--effectively broken.  What next?  
		--Take a differential backup?  Send an email?
	END
	
	FETCH NEXT FROM curDb INTO @Db
END

CLOSE curDb
DEALLOCATE curDb

IF EXISTS ( SELECT 1 FROM @InvalidBackups )
BEGIN
	DECLARE @ErrMsg VARCHAR(MAX)
	SET @ErrMsg = CASE WHEN @BackupType = 'D' THEN 'FULL' WHEN @BackupType = 'I' THEN 'DIFFERENTIAL' ELSE 'TRANSACTION LOG' END
	SET @ErrMsg = 'The last ' + @ErrMsg + ' backup could not be validated for the following databases:'
	
	SELECT @ErrMsg = @ErrMsg + CHAR(13) + CHAR(10) + DbName
	FROM @InvalidBackups
	
	RAISERROR(@ErrMsg, 16, 1);
	RETURN
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'CheckDatabases'
)
	DROP PROCEDURE dba.CheckDatabases 
GO

CREATE PROCEDURE dba.CheckDatabases
	@PhysicalOnly BIT
AS 
/*
	Purpose:	
	Performs DBCC CHECKDB on the following databases:
		•System db's
		•User db's that are online 
		•User db's that are not snapshots
	
	Inputs:
	@PhysicalOnly - an option for DBCC CHECKDB:
		see http://msdn.microsoft.com/en-us/library/ms176064.aspx.

	History:
	07/12/2010	DBA	Created
	12/22/2010	DBA	Comment out the TRY/CATCH error handling.  If the sp fails,
						the error message should be recorded in the sql log.
	03/22/2011	DBA	Check @@ERROR after running DBCC CHECKDB on each db.  Keep 
						a list of suspect db's and RAISERROR with appropriate message 
						as needed.
	06/13/2014	DBA	Begin checking [tempdb] and [model]
	09/18/2015	DBA	Specify ALL_ERRORMSGS, NO_INFOMSGS
	09/21/2016	DBA	Resume use of TRY...CATCH error handling.
						But enhance it with Extended Events.
						http://itsalljustelectrons.blogspot.com/2016/09/Enhanced-TSQL-Error-Handling-With-Extended-Events-Part2.html
*/
DECLARE @ErrOccurred BIT = 0;
DECLARE @ErrData AS dba.ErrorReported;
--This will be the body of an HTML-formatted email message.
DECLARE @Body NVARCHAR(MAX) = '<table border="1">' +
	'<tr>' +
	'<th align="center" colspan="3" style="background-color: wheat;">DBCC CHECKDB output</th>' +
	'</tr>' +
	'<tr>' +
	'<th style="background-color: lightgrey;">Database Name</th>' +
	'<th style="background-color: lightgrey;">Error Number</th>' +
	'<th style="background-color: lightgrey;">Error Message</th>' +
	'</tr>';
DECLARE @DbName SYSNAME;
DECLARE curDBs CURSOR FOR
	SELECT name
	FROM sys.databases
	WHERE State_Desc = 'ONLINE'
	AND Source_Database_Id IS NULL
	ORDER BY name

WAITFOR DELAY '00:00:01';
EXEC dba.CreateXESession_ErrorReported;

OPEN curDBs;
FETCH NEXT FROM curDBs INTO @DbName;

WHILE @@FETCH_STATUS = 0
BEGIN
	--DBCC CHECKDB errors will *not* transfer control to CATCH.
	BEGIN TRY
		IF @PhysicalOnly = 1
			DBCC CHECKDB (@DbName) WITH PHYSICAL_ONLY, ALL_ERRORMSGS, NO_INFOMSGS;
		ELSE
			DBCC CHECKDB (@DbName) WITH ALL_ERRORMSGS, NO_INFOMSGS;
	END TRY
	BEGIN CATCH
	END CATCH

	--Capture error data from XEvent session (if any).
	INSERT INTO @ErrData 
	EXEC dba.GetXESessionData_ErrorReported;

	IF EXISTS (
		SELECT * FROM @ErrData
	)
	BEGIN
		SET @ErrOccurred = 1;

		--Add "rows" to the HTML <table>.
		SELECT @Body = @Body + CHAR(13) + CHAR(10) + '<tr>' +
			'<td>' + @DbName + '</td>' +
			'<td>' + CAST(e.ErrorNumber AS VARCHAR) + '</td>' +
			'<td>' + e.[Message] + '</td>' +
			'</tr>'
		FROM @ErrData e

		--Drop & recreate the XEvent session to clear out the ring buffer target.
        EXEC dba.DropXESession_ErrorReported;
        EXEC dba.CreateXESession_ErrorReported;
        DELETE FROM @ErrData;
	END

	FETCH NEXT FROM curDBs INTO @DbName;
	WAITFOR DELAY '00:00:01';
END

CLOSE curDBs;
DEALLOCATE curDBs;
EXEC dba.DropXESession_ErrorReported;

IF @ErrOccurred = 1
BEGIN
	SET @Body = @Body + '</table>';
	DECLARE @Subject NVARCHAR(255) = @@SERVERNAME + ' -- Integrity checks failure';
	
	EXEC msdb.dbo.sp_send_dbmail 
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'Default',
		@subject = @Subject,
		@body = @Body,
		@body_format = 'HTML';

	DECLARE @ErrMsg VARCHAR(MAX) = 'Integrity checks failure on one or more databases.  ' +
		'Check the Sql Server Log for more details.';
	RAISERROR(@ErrMsg, 16, 1);
	RETURN;
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' 
	AND r.ROUTINE_NAME = 'BackupFileList' 
	AND r.ROUTINE_TYPE = 'FUNCTION'
)
	DROP FUNCTION dba.BackupFileList
GO

CREATE FUNCTION dba.BackupFileList
(
	@MediaSetId INT
)
RETURNS VARCHAR(MAX)
AS
/*
	Purpose:	
	Returns a csv list of backup file name(s) for a specific backup
	
	Inputs:
	@MediaSetId - media set identification number.
	
	History:
	04/24/2014	DBA	Created
*/
BEGIN
	DECLARE @Files VARCHAR(MAX) 
	SET @Files = ''

	SELECT @Files = @Files + bmf.physical_device_name + ','
	FROM msdb.dbo.backupmediafamily bmf
	WHERE bmf.media_set_id = @MediaSetId
	AND bmf.mirror = 0
	ORDER BY family_sequence_number

	IF LEN(@Files) > 1
		SET @Files = LEFT(@Files, LEN(@Files) - 1)

	RETURN @Files
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'GetDeletableBackupFiles'
)
	DROP PROCEDURE dba.GetDeletableBackupFiles 
GO

CREATE PROCEDURE dba.GetDeletableBackupFiles 
AS
/*
	Purpose:	
	Gets a list of database backup files that can safely be deleted from disk.

	History:
	05/31/2017	DBA	Rewritten. Backup files are obtained by selecting from 
		view [dba].[BackupFiles], which has logic incidicating whether a backup 
		file is deletable or not.
*/
--Update stats on pertinent tables before selecting from the view.
UPDATE STATISTICS msdb.dbo.backupmediafamily
UPDATE STATISTICS msdb.dbo.backupset

SELECT f.physical_device_name, f.backup_finish_date, f.[type], 
	f.database_name, f.name, f.IsDeletable
FROM dba.BackupFiles f
WHERE f.IsDeletable = 1
ORDER BY f.name, f.backup_finish_date, f.physical_device_name
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'CheckFixedDriveFreeSpace'
)
	DROP PROCEDURE dba.CheckFixedDriveFreeSpace 
GO

CREATE PROCEDURE dba.CheckFixedDriveFreeSpace
	@FreeSpaceThresholdMB INT = 1024,
	@DuplicateAlertThreshold_Min INT = 15
AS
/******************************************************************************
* Name     : dba.CheckFixedDriveFreeSpace
* Purpose  : Performs a rudimentary check of free space on fixed drives,
*			 triggers an alert with error severity level 16/17 as needed.
* Inputs   : @FreeSpaceThresholdMB - the threshold for free disk space.
*				If free space is greater, no action is taken.  
*				If less, an	error will be raised.
*			 @DuplicateAlertThreshold_Min - If X number of minutes has elapsed 
*				since the last time disk space was checked, the severity level 
*				for RAISERROR will be 17, where X = @DuplicateAlertThreshold_Min.
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	06/10/2014	DBA	Created
*	04/15/2015	DBA	Added @DuplicateAlertThreshold_Min parameter.
*						Revised logic - always raise an error if free space falls
*						below the threshold. The severity level will be 16.  If
*						any of these conditions is true, raise severity to 17:
*							1. The amount of free space for any drive changed 
*								since the last alert.
*							2. It has been @DuplicateAlertThreshold_Min min or 
*								more since the last alert.
*	05/05/2017	DBA	Updates to dba.FixedDrive in an explicit transaction.
******************************************************************************/
BEGIN
	CREATE TABLE #FixedDrives (
		Drive CHAR(1) PRIMARY KEY NONCLUSTERED,
		MBFree INT
	)

	INSERT INTO #FixedDrives EXEC xp_fixeddrives

	DECLARE @Severity TINYINT
	DECLARE @ErrMsg VARCHAR(MAX)
	SET @ErrMsg = ''

	--Build the err msg string if any drive is below the space threshold.
	--Forgo any other logic at this step.
	SELECT @ErrMsg = @ErrMsg + t.Drive + ':' + CHAR(9) + CAST(t.MBFree AS VARCHAR) + ' MB' + CHAR(13) + CHAR(10)
	FROM #FixedDrives t
	JOIN dba.FixedDrive fd
		ON fd.Drive = t.Drive
	WHERE t.MBFree < @FreeSpaceThresholdMB

	IF @@ROWCOUNT > 0
	BEGIN
	--One or more drives has free space below @FreeSpaceThresholdMB.
		BEGIN TRAN
		
		--Conditionally update [dba].[FixedDrive] using all logic conditions.
		UPDATE fd SET
			LastAmountFree_MB = t.MBFree,
			LastAlert = CURRENT_TIMESTAMP 
		FROM #FixedDrives t
		JOIN dba.FixedDrive fd
			ON fd.Drive = t.Drive
		WHERE t.MBFree < @FreeSpaceThresholdMB
		AND 
		(
			t.MBFree <> fd.LastAmountFree_MB
			OR fd.LastAlert < DATEADD(MINUTE, -@DuplicateAlertThreshold_Min, CURRENT_TIMESTAMP)
		)
		
		IF @@ROWCOUNT > 0
		BEGIN
			--The above UPDATE affected one or more rows. 
			--Set the Severity level to 17 to trigger an Alert.
			SET @Severity = 17

			--TODO:  specify custom message id with default severity of 16.
			--This will trigger a custom alert, which in turn, sends an email.
			--RAISERROR(@custom_mesage_id, -1, -1, @ErrMsg) WITH LOG;
		END
		ELSE
		BEGIN
			--Set the Severity level to 16, which does not trigger an Alert in our environment. 
			--We will still see the related error message in the SQL log. 
			SET @Severity = 16

			--TODO:	RAISERROR(@ErrMsg, @Severity, 1) WITH LOG;
		END

		COMMIT
		SET @ErrMsg = 'Warning: one or more disk drives is running out of free space:' + CHAR(13) + CHAR(10) + @ErrMsg
		RAISERROR(@ErrMsg, @Severity, 1) WITH LOG;
		PRINT @ErrMsg
	END

	DROP TABLE #FixedDrives
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'CheckServiceLogins'
)
	DROP PROCEDURE dba.CheckServiceLogins 
GO

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = '
CREATE PROCEDURE dba.CheckServiceLogins
AS
/******************************************************************************
* Name     : CheckServiceLogins
* Purpose  : Checks the Windows Login used to run SQL Server and the SQL Agent.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	01/30/2015	DBA	Created.
*	03/24/2015	DBA	Get allowed logins from config table.
*	04/16/2015	DBA	Enhance for case-sensitive collations.
******************************************************************************/
DECLARE @DBEngineLogin VARCHAR(100)
DECLARE @AgentLogin VARCHAR(100)
 
EXECUTE master.dbo.xp_instance_regread
	@rootkey = N''HKEY_LOCAL_MACHINE'',
	@key = N''SYSTEM\CurrentControlSet\Services\MSSQLServer'',
	@value_name = N''ObjectName'',
	@value = @DBEngineLogin OUTPUT
 
EXECUTE master.dbo.xp_instance_regread
	@rootkey = N''HKEY_LOCAL_MACHINE'',
	@key = N''SYSTEM\CurrentControlSet\Services\SQLServerAgent'',
	@value_name = N''ObjectName'',
	@value = @AgentLogin OUTPUT

DECLARE @AllowedDBEngineLogin VARCHAR(128)
DECLARE @AllowedAgentLogin VARCHAR(128)

SELECT @AllowedDBEngineLogin = ''' + DbaData.dba.GetInstanceConfiguration('SQL DB Engine Login') + '''
SELECT @AllowedAgentLogin = ''' + DbaData.dba.GetInstanceConfiguration('SQL Agent Login') + '''

IF CHARINDEX(@DBEngineLogin, @AllowedDBEngineLogin COLLATE Latin1_General_CI_AS, 1) = 0 
	OR CHARINDEX(@AgentLogin, @AllowedAgentLogin COLLATE Latin1_General_CI_AS, 1) = 0
BEGIN
	DECLARE @ErrMsg NVARCHAR(MAX)
	DECLARE @SingleNameDBLogin VARCHAR(128)
	DECLARE @SingleNameAgentLogin VARCHAR(128)

	SET @ErrMsg = CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 

	/*
		The "allowed" login might be a csv list of multiple logins, or a 
		csv list including variations of the same login.  For instance: 
			"domain\mssqladmin, MSSQLAdmin@domain.com"
		Pare down the data to one login name and assign to the "@SingleName" variables.
	*/
	IF CHARINDEX('','', @AllowedDBEngineLogin, 0) > 0
		SET @SingleNameDBLogin = LEFT(@AllowedDBEngineLogin, CHARINDEX('','', @AllowedDBEngineLogin, 0) - 1)
	ELSE
		SET @SingleNameDBLogin = @AllowedDBEngineLogin

	IF CHARINDEX('','', @AllowedAgentLogin, 0) > 0
		SET @SingleNameAgentLogin = LEFT(@AllowedAgentLogin, CHARINDEX('','', @AllowedAgentLogin, 0) - 1)
	ELSE
		SET @SingleNameAgentLogin = @AllowedAgentLogin

	IF CHARINDEX(@DBEngineLogin, @AllowedDBEngineLogin COLLATE Latin1_General_CI_AS, 1) = 0
		SET @ErrMsg = @ErrMsg + ''The Windows Login for the SQL Server service was changed to "'' + @DBEngineLogin + ''"'' + CHAR(13) + CHAR(10) + 
			''Please change the Windows Login back to "'' + @SingleNameDBLogin + ''" and restart the service.'' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

	IF CHARINDEX(@AgentLogin, @AllowedAgentLogin COLLATE Latin1_General_CI_AS, 1) = 0
		SET @ErrMsg = @ErrMsg + ''The Windows Login for the SQL Server Agent service was changed to "'' + @AgentLogin + ''"'' + CHAR(13) + CHAR(10) + 
			''Please change the Windows Login back to "'' + @SingleNameAgentLogin + ''" and restart the service.'' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

	SET @ErrMsg = @ErrMsg + ''If you need assistance, please contact DBA@Domain.com'' + CHAR(13) + CHAR(10) 

	--TODO: if SP is run at startup, will the SqlAgent catch this as an alert?
	--Will the SP run before or after SqlAgent starts?
	RAISERROR(@ErrMsg, 18, 1) WITH LOG;
END
'
--PRINT @Tsql
EXEC sp_executesql @Tsql;
GO

--If services are not to be checked when SQL Server (re)starts, recreate the SP as a simple PRINT statement.
IF DbaData.dba.GetInstanceConfiguration('Alert Service Login Changed') <> '1'
BEGIN
	DECLARE @Tsql NVARCHAR(MAX)
	SET @Tsql = '
ALTER PROCEDURE dba.CheckServiceLogins
AS 
BEGIN
	PRINT ''"Alert Service Login Changed" is not enabled.  This is an informational message only. No user action is required.''
END
	'
	EXEC sp_executesql @Tsql;
END
GO 

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'ResetFixedDrives'
)
	DROP PROCEDURE dba.ResetFixedDrives 
GO

CREATE PROCEDURE dba.ResetFixedDrives
/*****************************************************************************
* Name     : dba.ResetFixedDrives
* Purpose  : Truncate/populate table [dba].[FixedDrive]
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	04/15/2015	DBA	Created
******************************************************************************/
AS
BEGIN
	TRUNCATE TABLE dba.FixedDrive

	INSERT INTO dba.FixedDrive (Drive, LastAmountFree_MB)
	EXEC xp_fixeddrives

	IF SERVERPROPERTY('MachineName') = 'ATLONSELGRAP201' AND DEFAULT_DOMAIN() = 'ASPCUST'
	BEGIN
		--The H: drive on this server is only 100MB.
		DELETE FROM dba.FixedDrive
		WHERE Drive = 'H'
	END

	UPDATE dba.FixedDrive
	SET LastAmountFree_MB = -1 
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'StartupNotification'
)
	DROP PROCEDURE dba.StartupNotification 
GO

CREATE PROCEDURE dba.StartupNotification
/*****************************************************************************
* Name     : dba.StartupNotification
* Purpose  : Sends an email when SQL Server (re)starts.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	08/27/2014	DBA	Created
*	01/30/2015	DBA	Revised to accomodate SQL 2005 versions.
*	04/15/2015	DBA	Moved to [DbaData] database under schema [dba].
*	04/18/2016	DBA	Don't send notification during maintenance window.
******************************************************************************/
AS 
	SET LANGUAGE 'us_english';
	--Current maintenance window is Sunday from midnight to 5pm (local server time).
	IF DATENAME(dw, CURRENT_TIMESTAMP) COLLATE Latin1_General_CI_AS = 'Sunday'
	BEGIN
		DECLARE @WindowOpen TIME = '00:00:00'
		DECLARE @WindowClose TIME = '17:00:00'

		IF CAST(CURRENT_TIMESTAMP AS TIME) BETWEEN @WindowOpen AND @WindowClose
		BEGIN
			--The SQL instance was restarted during the maintenance window.  Take no action.
			RETURN;
		END
	END
	
	DECLARE @Subj NVARCHAR(255) 
	DECLARE @MailBody NVARCHAR(MAX)
	DECLARE @ProdVer VARCHAR(128)
	DECLARE @MajorVer SMALLINT
	DECLARE @Tsql NVARCHAR(MAX)
	DECLARE @Restart NVARCHAR(MAX)
	DECLARE @IP NVARCHAR(MAX)

	SELECT @ProdVer = CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR) 
	SET @MajorVer = CAST(PARSENAME(@ProdVer, 4) AS SMALLINT)

	IF @MajorVer >= 10	--SQL 2008 or higher
		SET @Tsql = 'SELECT @Last = CONVERT(NVARCHAR, sqlserver_start_time, 109) FROM sys.dm_os_sys_info'
	ELSE	--SQL 2005 or less
		SET @Tsql = 'SELECT @Last = CONVERT(NVARCHAR, DATEADD(ms, -ms_ticks, CURRENT_TIMESTAMP), 109) FROM sys.dm_os_sys_info'

	EXEC sp_executesql @Tsql, N'@Last NVARCHAR(MAX) OUTPUT', @Restart output

	WAITFOR DELAY '00:01:00'
	SELECT TOP(1) @IP = CAST(dec.local_net_address AS NVARCHAR(MAX))
	FROM sys.dm_exec_connections AS dec
	WHERE COALESCE(dec.local_net_address, '') <> '';

	SET @Subj = @@SERVERNAME + ' - SQL Server service restart'
	SELECT @MailBody = 'The SQL Server service for the following was (re)started.  If this was not a planned restart, notify others as needed.' + 
		CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
		'SQL Server: ' + @@SERVERNAME + CHAR(13) + CHAR(10) +
		'IP Address: ' + COALESCE(@IP, '[undetermined]') + CHAR(13) + CHAR(10) +
		'Last Restart: ' + @Restart

	EXEC msdb.dbo.sp_send_dbmail
		@recipients = 'DBA@Domain.com;TechSupport@YourDomain.com', 
		@blind_copy_recipients = 'DbaPager@GMail.com',
		@profile_name = 'Default',
		@subject = @Subj,
		@body = @MailBody
GO

--If an alert is not to be sent when SQL Server (re)starts, recreate the SP as a simple PRINT statement.
IF DbaData.dba.GetInstanceConfiguration('Alert Startup') <> '1'
BEGIN
	DECLARE @Tsql NVARCHAR(MAX)
	SET @Tsql = '
ALTER PROCEDURE dba.StartupNotification
AS 
BEGIN
	PRINT ''"Alert Startup" is not enabled.  This is an informational message only. No user action is required.''
END
	'
	EXEC sp_executesql @Tsql;
END
GO 

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'EnableTraceFlags'
)
	DROP PROCEDURE dba.EnableTraceFlags 
GO

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = '
CREATE PROCEDURE dba.EnableTraceFlags
/*****************************************************************************
* Name     : dba.EnableTraceFlags
* Purpose  : Enables one or more Trace Flags globally when SQL Server (re)starts.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	05/01/2014	DBA	Created
*	12/14/2015	DBA	Drop Event Notification, run DBCC cmd, 
*						create Event Notification.
******************************************************************************/
AS
BEGIN'
IF DbaData.dba.GetInstanceConfiguration('Trace Flags') <> ''
BEGIN
	SET @Tsql = @Tsql + '
	IF EXISTS (
		SELECT *
		FROM master.sys.server_event_notifications 
		WHERE name = ''enDbccCommand''
	)
	BEGIN
		DROP EVENT NOTIFICATION enDbccCommand
		ON SERVER;

		DBCC TRACEON (' + DbaData.dba.GetInstanceConfiguration('Trace Flags') + ', -1);

		CREATE EVENT NOTIFICATION enDbccCommand
		ON SERVER
		WITH FAN_IN
		FOR AUDIT_DBCC_EVENT
		TO SERVICE ''svcDbccCommandNotification'', ''current database'';
	END
	ELSE
	BEGIN
		DBCC TRACEON (' + DbaData.dba.GetInstanceConfiguration('Trace Flags') + ', -1);
	END'
END
ELSE
	SET @Tsql = @Tsql + '	PRINT ''No Trace Flags specified in customizable instance configuration.  This is an informational message only. No user action is required.'''
	
SET @Tsql = @Tsql + '
END'

EXEC sp_executesql @Tsql;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'ClonedServerCheck'
)
	DROP PROCEDURE dba.ClonedServerCheck 
GO

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = '
CREATE PROCEDURE dba.ClonedServerCheck
	@DbaContactInfo VARCHAR(MAX) = ''DBA@Domain.com'',
	@MailRecipients VARCHAR(MAX) = ''' + DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Email') + '''
AS
/*
	Purpose:
	Check the current host name & current host domain to the hard-coded values
	that existed for the original host at install time.  Ensure neither has changed.
	
	Inputs:
	@DbaEmail: Contact info for the DBA (email, phone, etc.)
	@MailRecipients: a semicolon-delimited list of e-mail addresses to send a message to.

	History:
	10/01/2015	DBA	Created
*/'

IF DbaData.dba.GetInstanceConfiguration('Cloned Server Check') = '1'
	SET @Tsql += '
DECLARE @OrigHost VARCHAR(128) = ''' + CAST(SERVERPROPERTY(N'MachineName') AS VARCHAR) + '.' + DEFAULT_DOMAIN() + '''
DECLARE @CurrHost VARCHAR(128) = CAST(SERVERPROPERTY(N''MachineName'') AS VARCHAR) + ''.'' + DEFAULT_DOMAIN()

IF @OrigHost <> @CurrHost
BEGIN
	DECLARE @ErrMsg VARCHAR(MAX) = ''Warning!  A SQL Server host appears to have been cloned.'' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
		''Original Host: '' + @OrigHost + CHAR(13) + CHAR(10) +
		''Current Host: '' + @CurrHost + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
		''Please contact '' + @DbaContactInfo + '' for assistance.''

	BEGIN TRY
		--This makes no sense unless database mail was configured on the source SQL host.
		--Note that the cloned host may not have connectivity to the SMTP relay/server.
		--We do not care if this fails.  Give it a try anyway...
		EXEC msdb.dbo.sp_send_dbmail
			@recipients = @MailRecipients, 
			@profile_name = ''Default'',
			@subject = ''Cloned SQL Server Host?'',
			@body = @ErrMsg
	END TRY
	BEGIN CATCH
	END CATCH

	--Take all user databases offline.
	DECLARE @Tsql NVARCHAR(MAX)
	DECLARE @DBName SYSNAME
	DECLARE curDBs CURSOR FORWARD_ONLY READ_ONLY STATIC FOR
		SELECT name
		FROM sys.databases
		WHERE State_Desc = ''ONLINE''
		AND Source_Database_Id IS NULL
		AND name NOT IN (''master'', ''msdb'', ''model'', ''tempdb'', ''DbaData'')

	OPEN curDBs
	FETCH NEXT FROM curDBs INTO @DBName

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Tsql = ''ALTER DATABASE ['' + @DBName + ''] SET OFFLINE WITH ROLLBACK IMMEDIATE;''

		BEGIN TRY
			EXEC sp_executesql @Tsql;
		END TRY
		BEGIN CATCH
		END CATCH

		FETCH NEXT FROM curDBs INTO @DBName
	END

	CLOSE curDBs
	DEALLOCATE curDBs

	--Disable all SQL Agent jobs.
	DECLARE @JobName SYSNAME
	DECLARE curJobs CURSOR FORWARD_ONLY READ_ONLY STATIC FOR
		SELECT j.name
		FROM msdb.dbo.sysjobs j
		WHERE j.enabled = 1

	OPEN curJobs
	FETCH NEXT FROM curJobs INTO @JobName

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			EXEC msdb.dbo.sp_update_job 
				@job_name=@JobName, 
				@enabled=0
		END TRY
		BEGIN CATCH
		END CATCH

		FETCH NEXT FROM curJobs INTO @JobName
	END

	CLOSE curJobs
	DEALLOCATE curJobs

	--Raise a (logged) error every 10 min.
	SET @ErrMsg = ''Warning!  This appears to be a cloned SQL Server host.'' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
		''Original Host: '' + @OrigHost + CHAR(13) + CHAR(10) +
		''Current Host: '' + @CurrHost + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
		''Please contact '' + @DbaContactInfo + '' for assistance.''

	WHILE 1 = 1
	BEGIN
		BEGIN TRY
			RAISERROR (@ErrMsg, 19, 1) WITH LOG
		END TRY
		BEGIN CATCH
		END CATCH

		WAITFOR DELAY ''00:10:00''
	END
END
'
ELSE
	SET @Tsql = @Tsql + 'PRINT ''"Cloned Server Check" is disabled in instance configuration.  This is an informational message only. No user action is required.'''

EXEC sp_executesql @Tsql;
GO

--Only objects in the master database owned by dbo can have the startup setting changed.
USE master
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'StartupTasks'
)
	DROP PROCEDURE dbo.StartupTasks 
GO

CREATE PROCEDURE dbo.StartupTasks
/*****************************************************************************
* Name     : dbo.StartupTasks
* Purpose  : Performs various tasks when SQL Server (re)starts.
* Notes    : Normally, the sp would be created in the [maint] schema.  
*			 However, only objects in the master database owned by dbo can 
*			 have the startup setting changed.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	08/27/2014	DBA	Created
*	01/30/2015	DBA	Revised to accomodate SQL 2005 versions.
*	01/30/2015	DBA	Renamed SP to [StartupTasks]. Each task is now in a 
*						separate SP within [DbaData] database.
******************************************************************************/
AS 
	EXEC DbaData.dba.StartupNotification
	EXEC DbaData.dba.CheckServiceLogins
	EXEC DbaData.dba.ResetFixedDrives
	EXEC DbaData.dba.CheckFixedDriveFreeSpace
	EXEC DbaData.dba.EnableTraceFlags
	EXEC DbaData.dba.ClonedServerCheck
GO

EXEC sp_procoption @ProcName = 'dbo.StartupTasks',
	@OptionName = 'startup',
	@OptionValue = 'true' 
GO

USE DbaData
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'ConfigureSecurityByDatabase'
)
	DROP PROCEDURE dba.ConfigureSecurityByDatabase 
GO

CREATE PROCEDURE dba.ConfigureSecurityByDatabase
	@DBName SYSNAME = NULL
AS 
/******************************************************************************
* Name     : dba.ConfigureSecurityByDatabase
* Purpose  : Sets db owner, removes db users from fixed db roles with 
*				elevated permissions, grants permissions to db users that 
*				were removed from [db_owner].
* Inputs   : @DBName - name of the db.  IF NULL, all databases are affected.
* Outputs  : None
* Returns  : Nothing
* Notes    : The intent is for this SP to be run using the name of any db that 
*			 is the result of RESTORE DATABASE.  In contrast, new db's that 
*			 are the result of CREATE DATABASE should inherit adequate 
*			 authorization from the [model] database.
******************************************************************************
* Change History
*	12/05/2014	DBA	Created
*	03/23/2015	DBA	Revised to accomodate NULL @DBName input value.
******************************************************************************/
DECLARE @SA SYSNAME
DECLARE @SaSid VARBINARY(85)

--Assumption: sa is the owner of [master].
SELECT @SA = l.name, @SaSid = l.sid
FROM master.sys.syslogins l
JOIN master.sys.databases d
	ON d.owner_sid = l.sid
	AND d.name = 'master'

DECLARE @Tsql NVARCHAR(MAX)
DECLARE @DB SYSNAME
DECLARE @DBOwner VARBINARY(85)
DECLARE curDB CURSOR FOR
	SELECT d.name, d.owner_sid
	FROM master.sys.databases d
	WHERE d.name = COALESCE(@DBName, name)
	AND d.name NOT IN ('master','tempdb')

OPEN curDB
FETCH NEXT FROM curDB INTO @DB, @DBOwner

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @DB
	--Can't change ownership of these db's.
	IF @DB NOT IN ('model','distribution') AND @DBOwner <> @SaSid
	BEGIN
		SET @Tsql = N'ALTER AUTHORIZATION ON DATABASE :: [' + @DB + '] TO [' + @SA + '];'
		EXEC sp_executesql @Tsql
	END

	/*
		Remove users from these db roles:
			db_accessadmin
			db_backupoperator
			db_securityadmin
	*/
	SET @Tsql = '
DECLARE @User SYSNAME, @Role SYSNAME
DECLARE curUsers CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT u.name UserName, r.name dbRole
	FROM sys.database_principals u
	JOIN sys.database_role_members rm
		ON rm.member_principal_id = u.principal_id
	JOIN sys.database_principals r
		ON r.principal_id = rm.role_principal_id
	WHERE r.name IN (''''db_accessadmin'''', ''''db_backupoperator'''', ''''db_securityadmin'''')
	AND u.name NOT IN (''''dbo'''', ''''guest'''', ''''INFORMATION_SCHEMA'''', ''''public'''', ''''sys'''')
	ORDER BY u.name, r.name

OPEN curUsers
FETCH NEXT FROM curUsers INTO @User, @Role

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC sp_droprolemember @Role, @User
	FETCH NEXT FROM curUsers INTO @User, @Role
END
 
CLOSE curUsers
DEALLOCATE curUsers
'

	SET @Tsql = '[' + @DB + ']..sp_executesql N''' + @Tsql + ''''
	EXEC sp_executesql @Tsql;

	/*
		Remove users from db_owner, add to these roles:
			db_datareader
			db_datawriter
			db_ddladmin
		Also grant the following to the users:
			GRANT EXECUTE
			GRANT CREATE SCHEMA
			GRANT VIEW DEFINITION
			GRANT CONTROL ON CERTIFICATE (any that exist)
	*/
	SET @Tsql = '
DECLARE @User SYSNAME, @Role SYSNAME
DECLARE curUsers CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT u.name UserName--, r.name dbRole
	FROM sys.database_principals u
	JOIN sys.database_role_members rm
		ON rm.member_principal_id = u.principal_id
	JOIN sys.database_principals r
		ON r.principal_id = rm.role_principal_id
	WHERE r.name IN (''''db_owner'''')
	AND u.name NOT IN (''''dbo'''', ''''guest'''', ''''INFORMATION_SCHEMA'''', ''''public'''', ''''sys'''')
	ORDER BY u.name, r.name

OPEN curUsers
FETCH NEXT FROM curUsers INTO @User

WHILE @@FETCH_STATUS = 0
BEGIN
	DECLARE @Tsql NVARCHAR(MAX)
	SET @Tsql = N''''GRANT EXECUTE TO ['''' + @User + '''']''''
	EXEC sp_executesql @Tsql

	SET @Tsql = N''''GRANT CREATE SCHEMA TO ['''' + @User + '''']''''
	EXEC sp_executesql @Tsql

	SET @Tsql = N''''GRANT VIEW DEFINITION TO ['''' + @User + '''']''''
	EXEC sp_executesql @Tsql

	EXEC sp_addrolemember N''''db_datareader'''', @User
	EXEC sp_addrolemember N''''db_datawriter'''', @User
	EXEC sp_addrolemember N''''db_ddladmin'''', @User
	EXEC sp_droprolemember N''''db_owner'''', @User


	SET @Tsql = ''''''''
	SELECT @Tsql = @Tsql + ''''GRANT CONTROL ON CERTIFICATE::['''' + c.name + ''''] TO ['''' + @User + '''']; ''''
	FROM sys.certificates c
	EXEC sp_executesql @Tsql

	FETCH NEXT FROM curUsers INTO @User
END
 
CLOSE curUsers
DEALLOCATE curUsers
'
	SET @Tsql = '[' + @DB + ']..sp_executesql N''' + @Tsql + ''''
	EXEC sp_executesql @Tsql;

	FETCH NEXT FROM curDB INTO @DB, @DBOwner
END

CLOSE curDB
DEALLOCATE curDB
GO

--If "risky" database roles are allowed, drop the SP.
IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'ConfigureSecurityByDatabase'
) 
AND dba.GetInstanceConfiguration('Allow Risky Database Roles') = 1
	DROP PROCEDURE dba.ConfigureSecurityByDatabase 
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'CopyOnlyBackup'
)
	DROP PROCEDURE dba.CopyOnlyBackup 
GO

CREATE PROCEDURE dba.CopyOnlyBackup
	@DBName SYSNAME,
	@Path VARCHAR(255)
AS 
/*
	Purpose:	
	Creates a copy-only backup of a single database to a hard-coded path.
	The backup is a single file, with date/time embedded in file name.

	Background:
	SP was created for Executime.  They "needed" the ability to create backups,
	but I did not want them to have elevated privileges.
	
	Inputs:
	@DBName - self-explanatory
	@Path - self-explanatory

	History:
	03/10/2016	DBA	Created
	08/23/2016	DBA	NAME = 'Archive - External Request'
		Disk cleanup routines should exclude backups created via this SP.
		The backup NAME will allow them to be identified and ignored.
*/
BEGIN
	SET LANGUAGE 'us_english';
	DECLARE @Tsql NVARCHAR(MAX)
	DECLARE @Filename VARCHAR(255)

	IF NOT EXISTS ( SELECT 1 FROM master.sys.databases WHERE name = @DBName )
	BEGIN
		PRINT 'Database "' + COALESCE(@DBName, '') + '" does not exist.'
		RETURN
	END

	IF RIGHT(@Path, 1) != '\'
		SET @Path = @Path + '\'

	--Format for the filename is
	--DBname yyyy-mm-dd_hhmiss.DayOfWeek.FULL.bak
	SET @Filename = @DBName + ' ' + 
		REPLACE(CONVERT(VARCHAR, GETDATE(), 111), '/', '-') +
		'_' + REPLACE(CONVERT(VARCHAR, GETDATE(), 108), ':', '') +
		'.' + DATENAME(weekday, GETDATE()) + '.FULL.bak'

	SET @Path = @Path + @Filename
	--PRINT @Path 

	BACKUP DATABASE @DBName
	TO DISK = @Path 
	WITH INIT, COMPRESSION, COPY_ONLY, NAME = 'Archive - External Request'
END
GO

/*********************************************************************/
IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveAddServerRoleMemberEvent'
)
	DROP PROCEDURE dbo.ReceiveAddServerRoleMemberEvent 
GO

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = '
CREATE PROCEDURE dbo.ReceiveAddServerRoleMemberEvent
	@EventData XML
/*****************************************************************************
* Name     : dbo.ReceiveAddServerRoleMemberEvent
* Purpose  : Raises an error when someone tries to add a login to a 
*			 restricted fixed server role. Conditionally attempts to "undo" 
*			 the DDL command, based on server configuration.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	04/22/2016	DBA	Created
******************************************************************************/
AS
BEGIN
	DECLARE @Login NVARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/LoginName)[1]'',''NVARCHAR(MAX)'')
	DECLARE @AllowedLogins VARCHAR(256) = ''' + DbaData.dba.GetInstanceConfiguration('ADD SERVER ROLE - Allowed Logins') + '''
	DECLARE @AllowRiskyRoles BIT = ' + DbaData.dba.GetInstanceConfiguration('Allow Risky Server Roles') + '

	IF @AllowRiskyRoles = 0 AND CHARINDEX(@Login COLLATE SQL_Latin1_General_CP1_CI_AS, @AllowedLogins, 1) = 0
	BEGIN

		DECLARE @RoleName NVARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/RoleName)[1]'', ''NVARCHAR(MAX)'');
		DECLARE @ObjName NVARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/ObjectName)[1]'', ''NVARCHAR(MAX)'');

		IF @RoleName IN (''SYSADMIN'', ''SERVERADMIN'', ''SECURITYADMIN'', ''DISKADMIN'', ''DBCREATOR'')
		BEGIN
			DECLARE @ErrMsg NVARCHAR(MAX) = CHAR(13) + CHAR(10) + 
				''Membership in the following fixed server roles is not permitted in SPS production environments:'' +
				CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• sysadmin'' + CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• serveradmin'' + CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• securityadmin''+ CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• diskadmin'' + CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• dbcreator'' + CHAR(13) + CHAR(10) + 
				CHAR(13) + CHAR(10) +
				''If you need assistance, please contact DBA@Domain.com'' +
				CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)

			RAISERROR(@ErrMsg, 16, 1) WITH LOG;
	
			DECLARE @Tsql NVARCHAR(MAX) = ''ALTER SERVER ROLE ['' + @RoleName + ''] DROP MEMBER ['' + @ObjName + '']'';
			EXEC sp_executesql @Tsql;
		END

	END
END
'
--PRINT @Tsql
EXEC sp_executesql @Tsql
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveAddDatabaseRoleMemberEvent'
)
	DROP PROCEDURE dbo.ReceiveAddDatabaseRoleMemberEvent 
GO

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = '
CREATE PROCEDURE dbo.ReceiveAddDatabaseRoleMemberEvent
	@EventData XML
/*****************************************************************************
* Name     : dbo.ReceiveAddDatabaseRoleMemberEvent
* Purpose  : Raises an error when a non-dba attempts to add a 
*			 database user to a restricted fixed database role.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	04/22/2016	DBA	Created
******************************************************************************/
AS
DECLARE @Tsql NVARCHAR(MAX) = ''''
DECLARE @DBName NVARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/DatabaseName)[1]'', ''NVARCHAR(MAX)'');
DECLARE @ObjName NVARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/ObjectName)[1]'', ''NVARCHAR(MAX)'');
DECLARE @RoleName NVARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/RoleName)[1]'', ''NVARCHAR(MAX)'');
DECLARE @LoginName NVARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/LoginName)[1]'', ''NVARCHAR(MAX)'');

DECLARE @AllowedLogins VARCHAR(256) = ''' + dba.GetInstanceConfiguration('sp_addrolemember - Allowed Logins') + '''
DECLARE @AllowRiskyRoles BIT = ' + dba.GetInstanceConfiguration('Allow Risky Database Roles') + '

IF @AllowRiskyRoles = 0 AND CHARINDEX(@LoginName, @AllowedLogins, 1) = 0
BEGIN
	IF @RoleName IN (''db_owner'', ''db_accessadmin'', ''db_backupoperator'', ''db_securityadmin'')
	BEGIN
		
		--Make some concessions for SSRS-related permissions (and others).
		DECLARE @SsrsPerms BIT = 0;
		DECLARE @OtherPerms BIT = 0;

		--TODO: validate @ObjName is a role.
		IF @ObjName = ''RSExecRole'' 
			SET @SsrsPerms = 1;

		--TODO: validate @ObjName is a windows user.
		ELSE IF @ObjName IN (''NT SERVICE\ReportServer'', ''NT SERVICE\ReportServer$' + COALESCE(CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(MAX)), '') + ''')
			SET @SsrsPerms = 1;

		ELSE IF @ObjName IN (''SchemaCreator'')
			SET @OtherPerms = 1;

		IF @SsrsPerms = 0 AND @OtherPerms = 0
		BEGIN
			DECLARE @ErrMsg NVARCHAR(MAX) = CHAR(13) + CHAR(10) + 
				''Membership in the following fixed database roles is not permitted in SPS production environments:'' +
				CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• db_accessadmin'' + CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• db_backupoperator'' + CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• db_owner''+ CHAR(13) + CHAR(10) + 
				CHAR(9) + ''• db_securityadmin'' + CHAR(13) + CHAR(10) + 
				CHAR(13) + CHAR(10) +
				''If you need assistance, please contact DBA@Domain.com'' +
						CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
		
			RAISERROR(@ErrMsg, 16, 1) WITH LOG;

			--This won''t execute when RAISERROR is called w/ severity level 21
			--"Undo" the action (must be executed in the correct db context).
			--NOTE: this invokes [trgSecurityDDL] via the DROP_SERVER_ROLE_MEMBER event.
			SET @Tsql = ''['' + @DBName + '']..sp_executesql N''''ALTER ROLE ['' + @RoleName + ''] DROP MEMBER ['' + @ObjName + '']'''''';
			EXEC sp_executesql @Tsql;
		END
	END
END
'
--PRINT @Tsql
EXEC sp_executesql @Tsql
GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES r
    WHERE r.ROUTINE_SCHEMA = 'dba'
    AND r.ROUTINE_NAME = 'GetCurrentBackupFiles'
)
BEGIN
    EXEC('CREATE PROCEDURE dba.GetCurrentBackupFiles AS PRINT CURRENT_TIMESTAMP;');
END;
GO

ALTER PROCEDURE dba.GetCurrentBackupFiles 
AS
/*
	Purpose:	
	Gets a list of current database backup files that should still reside on disk.
	
	History:
	05/31/2017	DBA	Rewritten. Backup files are obtained by selecting from 
		view [dba].[BackupFiles], which has logic incidicating whether a backup 
		file is deletable or not.
*/
--Update stats on pertinent tables before selecting from the view.
UPDATE STATISTICS msdb.dbo.backupmediafamily
UPDATE STATISTICS msdb.dbo.backupset

SELECT f.physical_device_name, f.backup_finish_date, f.[type], 
	f.database_name, f.name, f.IsDeletable
FROM dba.BackupFiles f
WHERE f.IsDeletable = 0
ORDER BY f.name, f.backup_finish_date, f.physical_device_name
GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES r
    WHERE r.ROUTINE_SCHEMA = 'dba'
    AND r.ROUTINE_NAME = 'GetOldestFullBackupDates'
)
BEGIN
    EXEC('CREATE PROCEDURE dba.GetOldestFullBackupDates AS PRINT CURRENT_TIMESTAMP;');
END;
GO

ALTER PROCEDURE dba.GetOldestFullBackupDates 
	@BackupKeepDate DATE
AS
/*
	Purpose:	
	Gets a ist of db's and date of the most recent full backup that occurred 
	prior to @BackupKeepDate.
	
	Inputs:
	@BackupKeepDate - reference date, based on retention policy.

	History:
	09/29/2016	DBA	Written
*/
UPDATE STATISTICS msdb.dbo.backupmediafamily
UPDATE STATISTICS msdb.dbo.backupset

--The last full backup (on disk) for databases that are currently on the instance.
;WITH LastFullBackups AS
(
	SELECT rs.database_guid, MAX(bs.backup_finish_date) Backup_Finish_Date	
	FROM msdb.dbo.backupset bs
	JOIN master.sys.database_recovery_status rs
		ON rs.database_guid = bs.database_guid
	JOIN msdb.dbo.backupmediafamily bmf
		ON bmf.media_set_id = bs.media_set_id
		AND bmf.device_type IN (2, 102)	--Disk
	WHERE bs.type = 'D' --Database (Full)
	AND bs.is_copy_only = 0
	AND bs.server_name = @@SERVERNAME
	GROUP BY rs.database_guid
)
--List of db's and date of the most recent full 
--backup that occurred prior to @BackupKeepDate.
SELECT bs.database_guid, MAX(bs.backup_finish_date) Backup_Finish_Date	
FROM msdb.dbo.backupset bs
JOIN LastFullBackups lfb
	ON lfb.database_guid = bs.database_guid
WHERE bs.type = 'D' --Database (Full)
AND bs.is_copy_only = 0
AND bs.server_name = @@SERVERNAME
AND bs.backup_finish_date < @BackupKeepDate
		
--Files for the most recent backup should remain on disk, no matter how long ago it was.
AND bs.backup_finish_date < lfb.Backup_Finish_Date
		
GROUP BY bs.database_guid
GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES r
    WHERE r.ROUTINE_SCHEMA = 'dba'
    AND r.ROUTINE_NAME = 'MissingBackupFilesReport'
)
BEGIN
    EXEC('CREATE PROCEDURE dba.MissingBackupFilesReport AS PRINT CURRENT_TIMESTAMP;');
END;
GO

ALTER PROCEDURE dba.MissingBackupFilesReport 
AS
/*
	Purpose:	
	Checks the file system for missing backup files that should still
	reside on disk. If any are discovered missing, an email/alert is sent.
	
	Inputs:	None

	History:
	05/31/2017	DBA	Rewritten. Use SP [dba].[GetCurrentBackupFiles] 
		for consistency with query logic.
*/
CREATE TABLE #CurrentBackupFiles (
	physical_device_name NVARCHAR(260), 
	backup_finish_date DATETIME, 
	[type] CHAR(1), 
	database_name SYSNAME, 
	name NVARCHAR(128),
	IsDeletable BIT
)

INSERT INTO #CurrentBackupFiles
EXEC dba.GetCurrentBackupFiles;

DECLARE curBF CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT b.database_name, b.physical_device_name
	FROM #CurrentBackupFiles b
	ORDER BY b.database_name, b.backup_finish_date, b.physical_device_name

DECLARE @DBName SYSNAME;
DECLARE @PhysicalDeviceName VARCHAR(256);
DECLARE @Subject NVARCHAR(255) = @@SERVERNAME + ' -- Missing Backup Files';
DECLARE @Body NVARCHAR(MAX) = '<table border="1">' +
    '<tr>' +
    '<th>Database Name</th>' +
    '<th>Backup File</th>' +
    '</tr>';

OPEN curBF;
FETCH NEXT FROM curBF INTO @DBName, @PhysicalDeviceName;

DECLARE @Exists INT;
DECLARE @FilesMissing BIT = 0;
SET NOCOUNT ON;

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @Exists = NULL;
	EXEC master.dbo.xp_fileexist @PhysicalDeviceName, @Exists OUTPUT;

	IF @Exists = 0
	BEGIN
		IF @FilesMissing = 0 
			SET @FilesMissing = 1;

		SET @Body = @Body + 
			'<tr>' +
				'<td>' + @DBName + '</td>' +
				'<td>' + @PhysicalDeviceName + '</td>' +
            '</tr>';
	END 

	FETCH NEXT FROM curBF INTO @DBName, @PhysicalDeviceName;
END

CLOSE curBF;
DEALLOCATE curBF;
DROP TABLE #CurrentBackupFiles;

IF @FilesMissing = 1
BEGIN
	SET @Body = @Body + '</table>';
	EXEC msdb.dbo.sp_send_dbmail 
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'Default',
		@subject = @Subject,
		@body = @Body,
		@body_format = 'HTML',
		@exclude_query_output = 1
END
GO

IF NOT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES r
    WHERE r.ROUTINE_SCHEMA = 'dba'
    AND r.ROUTINE_NAME = 'InsertPageReadHistory'
)
BEGIN
    EXEC('CREATE PROCEDURE dba.InsertPageReadHistory AS PRINT CURRENT_TIMESTAMP;');
END;
GO

ALTER PROCEDURE dba.InsertPageReadHistory 
AS
/*
	Purpose:	
	Inserts Buffer Manager (page reads) data to a table.
	
	Inputs:	None

	History:
	06/16/2017	DBA	Created.
*/
INSERT INTO dba.PageReadHistory(SecondsSinceStartup, PagesReadSinceStartup)
SELECT TOP(1) 
	DATEDIFF(ss, sqlserver_start_time, CURRENT_TIMESTAMP) AS SecondsSinceStartup,
	PagesReadSinceStartup
FROM sys.dm_os_sys_info
CROSS APPLY
(
	SELECT object_name, cntr_value PagesReadSinceStartup
	FROM sys.dm_os_performance_counters 
	WHERE [object_name] LIKE '%Buffer Manager%'
	AND [counter_name] = 'Page reads/sec'
) prs
GO

/*
	Execute any stored procs, as necessary.
*/
--Startup taks that should be run, in lieu of restarting SQL.
EXEC DbaData.dba.EnableTraceFlags;
EXEC DbaData.dba.ResetFixedDrives;
EXEC DbaData.dba.CheckFixedDriveFreeSpace;
GO