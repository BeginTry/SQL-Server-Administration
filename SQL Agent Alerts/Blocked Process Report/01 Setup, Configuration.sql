--TODO: 
--1) Set the blocking threshold seconds.
--2) Set the @database_name parameter for sp_add_jobstep.
--3) Ensure "Replace tokens for all job responses to alerts" is checked/enabled in SQL Agent 
--   properties, Alert System page. (Note: after enabling the setting, restart the SQL Agent service.)

--TODO: specify the DB where logging table is to be created.
USE DbaMetrics	
GO

--Set the blocked process threshold (in seconds).
DECLARE @Threshold_Seconds INT = 5;	--TODO
IF NOT EXISTS (
	SELECT *
	FROM sys.configurations c
	WHERE c.name = 'blocked process threshold (s)'
	AND c.value = @Threshold_Seconds	--number of seconds
)
BEGIN
	EXEC sp_configure 'blocked process threshold', @Threshold_Seconds;
	RECONFIGURE WITH OVERRIDE; 
END
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = 'dbo' AND t.TABLE_NAME = 'BlockedProcesses')
BEGIN
	CREATE TABLE dbo.BlockedProcesses (
		BlockedProcessId INT IDENTITY(1,1) NOT NULL
			CONSTRAINT PK_BlockedProcess PRIMARY KEY CLUSTERED,
		PostTime DATETIME2
			CONSTRAINT DF_BlockedProcesses_PostTime DEFAULT (CURRENT_TIMESTAMP),

		DatabaseID INT,
		Duration BIGINT,
		EndTime DATETIME2,
		EventSequence INT,
		IndexID INT,
		IsSystem BIT,
		LoginSid VARCHAR(128),
		Mode INT,
		ObjectID INT,
		ServerName VARCHAR(128),
		SessionLoginName VARCHAR(128),
		TextData XML,	--NVARCHAR(MAX),
		TransactionID BIGINT
	)
END
GO


USE [msdb]
GO

--Create category for jobs (as needed).
IF NOT EXISTS (
	SELECT name 
	FROM msdb.dbo.syscategories 
	WHERE name=N'[Uncategorized (Local)]' 
	AND category_class=1
)
	EXEC msdb.dbo.sp_add_category 
		@class=N'JOB', 
		@type=N'LOCAL', 
		@name=N'[Uncategorized (Local)]'
GO

--Create job.
IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'Blocked Process WMI Event')
BEGIN
	EXEC msdb.dbo.sp_add_job 
		@job_name=N'Blocked Process WMI Event', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'This job is invoked by the [BlockedProcess] WMI alert .', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa';

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'Blocked Process WMI Event', 
		@server_name = N'(local)';
END
GO

--Add job step(s).
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'Blocked Process WMI Event')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'Blocked Process WMI Event', 
		@step_id=0

	DECLARE @Cmd NVARCHAR(MAX)
	SET @Cmd = N'DECLARE @DatabaseID INT = ''$(ESCAPE_SQUOTE(WMI(DatabaseID)))'';
DECLARE @Duration BIGINT = ''$(ESCAPE_SQUOTE(WMI(Duration)))'';
DECLARE @EndTime DATETIME2;
IF ISDATE(''$(ESCAPE_SQUOTE(WMI(EndTime)))'') = 1
	SET @EndTime = ''$(ESCAPE_SQUOTE(WMI(EndTime)))'';
DECLARE @EventSequence INT = ''$(ESCAPE_SQUOTE(WMI(EventSequence)))'';
DECLARE @IndexID INT = ''$(ESCAPE_SQUOTE(WMI(IndexID)))'';
DECLARE @IsSystem BIT = ''$(ESCAPE_SQUOTE(WMI(IsSystem)))'';
DECLARE @LoginSid VARCHAR(128) = ''$(ESCAPE_SQUOTE(WMI(LoginSid)))'';
DECLARE @Mode INT = ''$(ESCAPE_SQUOTE(WMI(Mode)))'';
DECLARE @ObjectID INT = ''$(ESCAPE_SQUOTE(WMI(ObjectID)))'';
DECLARE @ServerName VARCHAR(128) = ''$(ESCAPE_SQUOTE(WMI(ServerName)))'';
DECLARE @SessionLoginName VARCHAR(128) = ''$(ESCAPE_SQUOTE(WMI(SessionLoginName)))'';
DECLARE @TextData XML = ''$(ESCAPE_SQUOTE(WMI(TextData)))'';
DECLARE @TransactionID BIGINT = ''$(ESCAPE_SQUOTE(WMI(TransactionID)))'';

INSERT INTO dbo.BlockedProcesses
	(DatabaseID, Duration, EndTime, EventSequence, IndexID, IsSystem, LoginSid, Mode, ObjectID, ServerName, SessionLoginName, TextData, TransactionID)
VALUES
	(@DatabaseID, @Duration, @EndTime, @EventSequence, @IndexID, @IsSystem, @LoginSid, @Mode, @ObjectID, @ServerName, @SessionLoginName, @TextData, @TransactionID)	

';

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'Blocked Process WMI Event', 
		@step_name=N'Insert', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=@Cmd, 
		@database_name=N'DbaMetrics', --TODO
		@flags=0;
END
ELSE
	RAISERROR('SQL Server job "Blocked Process WMI Event" does not exist.', 16, 1);
GO

--Add alert
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysalerts a WHERE a.name = 'BlockedProcess')
BEGIN
	DECLARE @namespace SYSNAME;
	SELECT @namespace = N'\\.\root\Microsoft\SqlServer\ServerEvents\' + COALESCE(CAST(SERVERPROPERTY('InstanceName') AS SYSNAME), N'MSSQLSERVER');
	SELECT @namespace

	EXEC msdb.dbo.sp_add_alert 
		@name=N'BlockedProcess', 
		@message_id=0, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=0, 
		@category_name=N'[Uncategorized]', 
		@wmi_namespace=@namespace,
		@wmi_query=N'SELECT * FROM BLOCKED_PROCESS_REPORT', 
		@job_name=N'Blocked Process WMI Event'
END
GO
