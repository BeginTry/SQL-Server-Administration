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
IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'Ntirety - Disk Space Monitor')
BEGIN
	EXEC msdb.dbo.sp_add_job 
		@job_name=N'Ntirety - Disk Space Monitor', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'This job is invoked by the [Autogrowth - data file] and [Autogrowth - log file] WMI alerts. (It can also be scheduled as needed.)', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa';

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'Ntirety - Disk Space Monitor', 
		@server_name = N'(local)';
END
GO

--Add job step(s).
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'Ntirety - Disk Space Monitor')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'Ntirety - Disk Space Monitor', 
		@step_id=0

	DECLARE @Cmd NVARCHAR(MAX)
	SET @Cmd = N'
DECLARE @PctFreeThreshold TINYINT = 10;
DECLARE @GigsFreeThreshold SMALLINT = 10
DECLARE @Msg NVARCHAR(MAX) = ''The following drives have less than '' + 
	CAST(@PctFreeThreshold AS NVARCHAR(MAX)) + ''% free space or less than '' + 
	CAST(@GigsFreeThreshold AS NVARCHAR(MAX)) + '' GB free space (or both):'' + CHAR(13) + CHAR(10);

SELECT DISTINCT vs.volume_mount_point AS Drive,
	CONVERT(INT,vs.total_bytes/1048576.0) AS TotalSpaceInMB,
	CONVERT(INT,vs.available_bytes/1048576.0) AS FreeSpaceInMB,
	CONVERT(INT,vs.available_bytes * 100.0 /vs.total_bytes) AS FreeSpacePct
INTO #Drives
FROM sys.master_files mf
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs

SELECT @Msg = @Msg + d.Drive + CHAR(13) + CHAR(10)
FROM #Drives d
WHERE d.FreeSpacePct <= @PctFreeThreshold OR d.FreeSpaceInMB <= (@GigsFreeThreshold * 1024)
ORDER BY d.Drive

IF @@ROWCOUNT > 0
BEGIN
	RAISERROR (@Msg, 17, 1) WITH LOG;
END
';

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'Ntirety - Disk Space Monitor', 
		@step_name=N'RAISERROR on low disk space', 
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
		@database_name=N'master',
		@flags=0;
END
ELSE
	RAISERROR('SQL Server job "Ntirety - Disk Space Monitor" does not exist.', 16, 1);
GO

--Add alert for DATA_FILE_AUTO_GROW event.
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysalerts a WHERE a.name = 'Autogrowth - data file')
BEGIN
	DECLARE @namespace SYSNAME;
	SELECT @namespace = N'\\.\root\Microsoft\SqlServer\ServerEvents\' + COALESCE(CAST(SERVERPROPERTY('InstanceName') AS SYSNAME), N'MSSQLSERVER');
	SELECT @namespace

	EXEC msdb.dbo.sp_add_alert 
		@name=N'Autogrowth - data file', 
		@message_id=0, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=0, 
		@category_name=N'[Uncategorized]', 
		@wmi_namespace=@namespace,
		@wmi_query=N'SELECT * FROM DATA_FILE_AUTO_GROW', 
		@job_name=N'Ntirety - Disk Space Monitor'
END
GO

--Add alert for LOG_FILE_AUTO_GROW event.
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysalerts a WHERE a.name = 'Autogrowth - log file')
BEGIN
	DECLARE @namespace SYSNAME;
	SELECT @namespace = N'\\.\root\Microsoft\SqlServer\ServerEvents\' + COALESCE(CAST(SERVERPROPERTY('InstanceName') AS SYSNAME), N'MSSQLSERVER');
	SELECT @namespace

	EXEC msdb.dbo.sp_add_alert 
		@name=N'Autogrowth - log file', 
		@message_id=0, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=0, 
		@category_name=N'[Uncategorized]', 
		@wmi_namespace=@namespace,
		@wmi_query=N'SELECT * FROM LOG_FILE_AUTO_GROW', 
		@job_name=N'Ntirety - Disk Space Monitor'
END
GO
