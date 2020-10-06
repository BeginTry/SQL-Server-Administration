USE master
GO
------------------------------------------------------------------------
IF EXISTS (
	SELECT * 
	FROM sys.server_triggers
    WHERE name = 'trgCreateDatabase'
)
	DROP TRIGGER trgCreateDatabase
	ON ALL SERVER;
GO

CREATE TRIGGER trgCreateDatabase 
ON ALL SERVER 
FOR CREATE_DATABASE 
/*****************************************************************************
* Name     : trgCreateDatabase
* Purpose  : Sends an email to the dba when a database is created.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	07/03/2014	DBA	Created
******************************************************************************/
AS 
	DECLARE @Subj NVARCHAR(255) ;
	DECLARE @MailBody NVARCHAR(MAX);

	SET @Subj = @@SERVERNAME + ' - Database Created'
	SET @MailBody = 
		'<table border="1">' +
		CAST (EVENTDATA().query('
			for $X in /EVENT_INSTANCE/*
			return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
			') AS VARCHAR(MAX)) + 
		'</table><br/>';
	SET @MailBody = REPLACE(@MailBody, CHAR(13) + CHAR(10), '<br/>');
	EXEC msdb..sp_send_dbmail
		@recipients = 'DBA@Domain.com;DbaStandby@Domain.com', 
		@blind_copy_recipients = 'DbaPager@GMail.com',
		@profile_name = 'Default',
		@subject = @Subj,
		@body = @MailBody,
		@body_format = 'HTML',
		@exclude_query_output = 1
GO
------------------------------------------------------------------------
USE DbaData
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'RecreateAlterDBTrigger'
)
	DROP PROCEDURE dba.RecreateAlterDBTrigger 
GO

CREATE PROCEDURE dba.RecreateAlterDBTrigger 
AS 
/*
	Purpose:	
	Dynamically drops and creates sever DDL trigger trgAlterDatabase.
	
	Inputs:	None

	History:
	03/23/2016	DBA	Created
*/
BEGIN
	DECLARE @Tsql NVARCHAR(MAX)

	SET @Tsql = '
	IF EXISTS (
		SELECT * 
		FROM sys.server_triggers
		WHERE name = ''trgAlterDatabase''
	)
		DROP TRIGGER trgAlterDatabase
		ON ALL SERVER;
	'
	EXEC sp_executesql @Tsql

	SET @Tsql = '
CREATE TRIGGER trgAlterDatabase 
ON ALL SERVER 
FOR ALTER_DATABASE 
/*****************************************************************************
* Name     : trgAlterDatabase
* Purpose  : Conditionally sends email to the dba when a database is altered.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	07/03/2014	DBA	Created
*	08/25/2014	DBA	Rollback specific ALTER DATABASE statements that should
*						not be performed by non-DBAs.
*	03/12/2015	DBA	Clarification for ALTER DATABASE w/ ROLLBACK.
*	08/10/2015	DBA	Conditionally send email to dba (not every time).
*	08/17/2015	DBA	Enhance for instances/databases with case sensitive 
*						collations.
******************************************************************************/ 
AS 
	DECLARE @TsqlCmd NVARCHAR(MAX)
	DECLARE @Login NVARCHAR(MAX)
	DECLARE @ErrMsg NVARCHAR(MAX)

	SET @TsqlCmd = EVENTDATA().value(''(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]'',''NVARCHAR(MAX)'')
	SET @Login = EVENTDATA().value(''(/EVENT_INSTANCE/LoginName)[1]'',''NVARCHAR(MAX)'')
	
	DECLARE @AllowedLogins VARCHAR(512)
	DECLARE @TempAllowedLogins VARCHAR(512)
	DECLARE @AllowAlterDB BIT

	SET @AllowedLogins = ''' + DbaData.dba.GetInstanceConfiguration('ALTER DATABASE - Allowed Logins') + '''
	SET @TempAllowedLogins = ''' + DbaData.dba.GetInstanceConfiguration('ALTER DATABASE - Temporary Allowed Logins') + '''
	SET @AllowAlterDB = ' + DbaData.dba.GetInstanceConfiguration('Allow ALTER DATABASE') + '

	IF @AllowAlterDB = 0 AND CHARINDEX(@Login COLLATE SQL_Latin1_General_CP1_CI_AS, @AllowedLogins, 1) = 0
	BEGIN
		IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%SET RECOVERY%''
			SET @ErrMsg = ''Altering the RECOVERY model is not permitted in SPS production environments.''
		ELSE IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%MODIFY FILE%''
			SET @ErrMsg = ''Altering database files is not permitted in SPS production environments.''
		ELSE IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%SET PAGE_VERIFY%''
			SET @ErrMsg = ''Altering PAGE_VERIFY is not permitted in SPS production environments.''
		ELSE IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%SET AUTO_SHRINK%''
			SET @ErrMsg = ''Altering AUTO_SHRINK is not permitted in SPS production environments.''
		ELSE IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%SET AUTO_CLOSE%''
			SET @ErrMsg = ''Altering AUTO_CLOSE is not permitted in SPS production environments.''
		ELSE IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%SET AUTO_UPDATE_STATISTICS%''
			SET @ErrMsg = ''Altering AUTO_UPDATE_STATISTICS (or AUTO_UPDATE_STATISTICS_ASYNC) is not permitted in SPS production environments.''
		ELSE IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%SET AUTO_CREATE_STATISTICS%''
			SET @ErrMsg = ''Altering AUTO_CREATE_STATISTICS (or AUTO_CREATE_STATISTICS_ASYNC) is not permitted in SPS production environments.''

		IF @ErrMsg IS NOT NULL AND CHARINDEX(@Login COLLATE SQL_Latin1_General_CP1_CI_AS, @TempAllowedLogins, 1) = 0
		BEGIN
			--If a specific ALTER DATABASE command is encountered that is particularly egregious,
			--raise an error with severity 18.  WITH LOG triggers the alerts notification(s).
			--NOTE: most operations for ALTER DATABASE are not transacted operations and 
			--cannot be rolled back.  :-(
			--https://connect.microsoft.com/SQLServer/feedback/details/181652/ddl-trigger-does-not-properly-handle-alter-database-events
			SET @ErrMsg = @ErrMsg + '' If you need assistance, please contact DBA@Domain.com'' +
				CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + @TsqlCmd
			RAISERROR(@ErrMsg, 18, 1) WITH LOG;
			ROLLBACK;
			--RETURN;
		END
		
		--For all ALTER DATABASE commands, email the dba.
		DECLARE @Subj NVARCHAR(255) = @@SERVERNAME + '' - Database Altered'';
		DECLARE @MailBody NVARCHAR(MAX)
		SET @MailBody = 
			''<table border="1">'' +
			CAST (EVENTDATA().query(''
				for $X in /EVENT_INSTANCE/*
				return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
				'') AS VARCHAR(MAX)) + 
			''</table><br/>'';
		SET @MailBody = REPLACE(@MailBody, CHAR(13) + CHAR(10), ''<br/>'');
		
		--PRINT @MailBody
		EXEC msdb..sp_send_dbmail
			@recipients = ''DBA@Domain.com;DbaStandby@Domain.com'', 
			@blind_copy_recipients = ''DbaPager@GMail.com'',
			@profile_name = ''Default'',
			@subject = @Subj,
			@body = @MailBody,
			@body_format = ''HTML'',
			@exclude_query_output = 1
	END'
	--PRINT @Tsql
	EXEC sp_executesql @Tsql
END
GO

EXEC DbaData.dba.RecreateAlterDBTrigger
GO

USE master
GO
------------------------------------------------------------------------
IF EXISTS (
	SELECT * 
	FROM sys.server_triggers
    WHERE name = 'trgDropDatabase'
)
	DROP TRIGGER trgDropDatabase
	ON ALL SERVER;
GO

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = '
CREATE TRIGGER trgDropDatabase 
ON ALL SERVER 
FOR DROP_DATABASE 
/*****************************************************************************
* Name     : trgDropDatabase
* Purpose  : Sends an email to the dba when someone tries to drop a database.
*			 Conditionally attempts to ROLLBACK the DDL command, based on 
*			 server configuration.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	08/25/2014	DBA	Created
*	08/17/2015	DBA	Enhance for instances/databases with case sensitive 
*						collations.
******************************************************************************/
AS 
	DECLARE @TsqlCmd NVARCHAR(MAX);
	DECLARE @Subj NVARCHAR(255);
	DECLARE @MailBody NVARCHAR(MAX);
	DECLARE @Login NVARCHAR(MAX);
	--DECLARE @PostTime NVARCHAR(MAX);

	SET @Login = EVENTDATA().value(''(/EVENT_INSTANCE/LoginName)[1]'',''NVARCHAR(MAX)'');
	--SET @PostTime = EVENTDATA().value(''(/EVENT_INSTANCE/PostTime)[1]'',''NVARCHAR(MAX)'')
	SET @Subj = @@SERVERNAME + '' - Database Dropped'';
	SET @TsqlCmd = EVENTDATA().value(''(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]'',''NVARCHAR(MAX)'');
	SET @MailBody = 
		''<table border="1">'' +
		CAST (EVENTDATA().query(''
			for $X in /EVENT_INSTANCE/*
			return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
			'') AS VARCHAR(MAX)) + 
		''</table><br/>'';
	SET @MailBody = REPLACE(@MailBody, CHAR(13) + CHAR(10), ''<br/>'');

	--PRINT @MailBody
	EXEC msdb..sp_send_dbmail
		@recipients = ''DBA@Domain.com;DbaStandby@Domain.com'', 
		@blind_copy_recipients = ''DbaPager@GMail.com'',
		@profile_name = ''Default'',
		@subject = @Subj,
		@body = @MailBody,
		@body_format = ''HTML'',
		@exclude_query_output = 1

	DECLARE @AllowedLogins VARCHAR(256)
	SET @AllowedLogins = ''' + DbaData.dba.GetInstanceConfiguration('DROP DATABASE - Allowed Logins') + '''
	DECLARE @AllowDropDB BIT
	SET @AllowDropDB = ' + DbaData.dba.GetInstanceConfiguration('Allow DROP DATABASE') + '

	--IF @Login NOT LIKE ''%DBA%'' AND @Login NOT LIKE ''%MSSqlAdmin%''
	IF @AllowDropDB = 0 AND CHARINDEX(@Login COLLATE SQL_Latin1_General_CP1_CI_AS, @AllowedLogins, 1) = 0
	BEGIN
		DECLARE @ErrMsg NVARCHAR(MAX)

		SET @ErrMsg = ''Dropping databases is not permitted in SPS production environments.'' + 
			'' If you need assistance, please contact DBA@Domain.com'' +
			CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + 
			''TSql Command: '' + @TsqlCmd + CHAR(13) + CHAR(10) +
			''Login Name: '' + @Login

		--Raise an error with severity 21.
		--WITH LOG triggers the alerts notification(s).
		RAISERROR(@ErrMsg, 21, 1) WITH LOG;
		ROLLBACK;
		RETURN;
	END'
--PRINT @Tsql
EXEC sp_executesql @Tsql
GO
------------------------------------------------------------------------
IF EXISTS (
	SELECT * 
	FROM sys.server_triggers t
    WHERE t.name = 'trgSecurityDDL'
)
	DROP TRIGGER trgSecurityDDL
	ON ALL SERVER;
GO

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = '
CREATE TRIGGER trgSecurityDDL 
ON ALL SERVER FOR  
	DDL_GDR_DATABASE_EVENTS,		--GRANT_DATABASE, DENY_DATABASE, REVOKE_DATABASE,
	DDL_APPLICATION_ROLE_EVENTS,	--CREATE_APPLICATION_ROLE, ALTER_APPLICATION_ROLE, DROP_APPLICATION_ROLE,
	DDL_USER_EVENTS,				--CREATE_USER, ALTER_USER, DROP_USER 
	DDL_LOGIN_EVENTS,				--CREATE_LOGIN, ALTER_LOGIN, DROP_LOGIN

	--CREATE_ROLE, ALTER_ROLE, DROP_ROLE, 
	--ADD_ROLE_MEMBER, DROP_ROLE_MEMBER
	DDL_ROLE_EVENTS,

' + 

	--These event types are valid beginning with SQL 2012.
	CASE
		WHEN CAST(PARSENAME( CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR), 4) AS INT) >= 11
		THEN '	CREATE_SERVER_ROLE, ALTER_SERVER_ROLE, DROP_SERVER_ROLE,
'
		ELSE ''
	END + 

'	ADD_SERVER_ROLE_MEMBER,	DROP_SERVER_ROLE_MEMBER
	
	--ALTER_AUTHORIZATION
/*****************************************************************************
* Name     : trgSecurityDDL
* Purpose  : When security-related events occur, this trigger will:
*				Conditionally send an email to the dba .
*			 	Conditionally logs event data to table.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	04/21/2016	DBA	Created
*	07/28/2016	DBA	Enhanced logic.  All responses are conditional.
******************************************************************************/
AS 
	DECLARE @EventData XML = EVENTDATA();
	DECLARE @EventName VARCHAR(MAX) = @EventData.value(''(/EVENT_INSTANCE/EventType)[1]'',''NVARCHAR(MAX)'');
	DECLARE @EventDateTime DATETIME = @EventData.value(''(/EVENT_INSTANCE/PostTime)[1]'',''DATETIME'');
	DECLARE @MailBody NVARCHAR(MAX);
	';

IF DbaData.dba.GetInstanceConfiguration('Audit Security Events') = 1
BEGIN
	SELECT @Tsql = @Tsql + '
	BEGIN TRAN;
		INSERT INTO DbaData.dba.EventNotification(EventName, [EventData], EventDate)
		VALUES (@EventName, @EventData, @EventDateTime);
	COMMIT;
	'
END

IF DbaData.dba.GetInstanceConfiguration('Alert Security Events') = 1
BEGIN
	SELECT @Tsql = @Tsql + '
	--Send email to DBA.
	DECLARE @Subj NVARCHAR(255) = @@SERVERNAME + '' - '' + @EventName;
	SET @MailBody = 
		''<table border="1">'' +
		CAST (@EventData.query(''
			for $X in /EVENT_INSTANCE/*
			return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
			'') AS VARCHAR(MAX)) + 
		''</table><br/>'';
	SET @MailBody = REPLACE(@MailBody, CHAR(13) + CHAR(10), ''<br/>'');

	EXEC msdb..sp_send_dbmail
		@recipients = ''DBA@Domain.com;DbaStandby@Domain.com'', 
		--@blind_copy_recipients = ''DbaPager@GMail.com'',
		@profile_name = ''Security'',
		@subject = @Subj,
		@body = @MailBody,
		@body_format = ''HTML'',
		@exclude_query_output = 1
	'
END

SELECT @Tsql = @Tsql + '
	--These events have specific handlers.
	IF @EventName = ''ADD_SERVER_ROLE_MEMBER''
		EXEC DbaData.dbo.ReceiveAddServerRoleMemberEvent @EventData;
	ELSE IF @EventName = ''ADD_ROLE_MEMBER''
		EXEC DbaData.dbo.ReceiveAddDatabaseRoleMemberEvent @EventData;	
';
--PRINT @Tsql
EXEC sp_executesql @Tsql
GO
------------------------------------------------------------------------
IF EXISTS (
	SELECT * 
	FROM sys.server_triggers
    WHERE name = 'trgAlterInstance'
)
	DROP TRIGGER trgAlterInstance
	ON ALL SERVER;
GO

IF DbaData.dba.GetInstanceConfiguration('Alert Instance Altered') = '1'
BEGIN
	DECLARE @Tsql NVARCHAR(MAX)
	SET @Tsql = '
CREATE TRIGGER trgAlterInstance 
ON ALL SERVER 
FOR ALTER_INSTANCE 
/*****************************************************************************
* Name     : trgAlterInstance
* Purpose  : Sends an email to the dba when there are changes to global 
*				configuration settings on the SQL instance.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	08/08/2016	DBA	Created
******************************************************************************/
AS 
	DECLARE @Login NVARCHAR(MAX);
	SET @Login = EVENTDATA().value(''(/EVENT_INSTANCE/LoginName)[1]'',''NVARCHAR(MAX)'');

	IF @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%DBA%'' 
	AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%Dave.Mason%'' 
	AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%MSSqlAdmin%''
	BEGIN
		DECLARE @Subj NVARCHAR(255);
		DECLARE @MailBody NVARCHAR(MAX);

		SET @Subj = @@SERVERNAME + '' - Instance Altered'';
		SET @MailBody = 
			''<table border="1">'' +
			CAST (EVENTDATA().query(''
				for $X in /EVENT_INSTANCE/*
				return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
				'') AS VARCHAR(MAX)) + 
			''</table><br/>'';
		SET @MailBody = REPLACE(@MailBody, CHAR(13) + CHAR(10), ''<br/>'');

		EXEC msdb..sp_send_dbmail
			@recipients = ''DBA@Domain.com'', 
			@profile_name = ''Default'',
			@subject = @Subj,
			@body = @MailBody,
			@body_format = ''HTML'',
			@exclude_query_output = 1
	END
';

	EXEC sp_executesql @Tsql;
END
GO
