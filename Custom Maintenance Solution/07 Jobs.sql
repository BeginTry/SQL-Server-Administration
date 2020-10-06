--Create "empty" placeholder jobs with no job steps.
--Assume the schedules already exist.  (Schedules are created in previous script.)

--Rename job if it already exists.
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Weekend Maintenance')
	EXEC msdb.dbo.sp_update_job 
		@job_name=N'DBA-Weekend Maintenance',
		@new_name=N'DBA-Saturday Maintenance';
GO

IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Saturday Maintenance')
BEGIN
	DECLARE @domainOperator SYSNAME
	SELECT @domainOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')

	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Saturday Maintenance', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Various maintenance tasks to be performed each Saturday.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=@domainOperator,
		@notify_page_operator_name=N'Dave Mason',
		@start_step_id = 1

	EXEC msdb.dbo.sp_attach_schedule
		@job_name=N'DBA-Saturday Maintenance',
		@schedule_name=N'DBA-Saturday Maintenance Schedule'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Saturday Maintenance', 
		@server_name = N'(local)'
END
GO

IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Sunday Maintenance')
BEGIN
	DECLARE @domainOperator SYSNAME
	SELECT @domainOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')

	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Sunday Maintenance', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Various maintenance tasks to be performed each Sunday.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=@domainOperator,
		@notify_page_operator_name=N'Dave Mason',
		@start_step_id = 1

	EXEC msdb.dbo.sp_attach_schedule
		@job_name=N'DBA-Sunday Maintenance',
		@schedule_name=N'DBA-Sunday Maintenance Schedule'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Sunday Maintenance', 
		@server_name = N'(local)'
END
GO

IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Daily Maintenance - Midnight')
BEGIN
	DECLARE @domainOperator SYSNAME
	SELECT @domainOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')

	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Daily Maintenance - Midnight', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Various tasks to be performed daily at midnight.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=@domainOperator,
		@notify_page_operator_name=N'Dave Mason',
		@start_step_id = 1

	EXEC msdb.dbo.sp_attach_schedule
		@job_name=N'DBA-Daily Maintenance - Midnight',
		@schedule_name=N'DBA-Midnight Schedule'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Daily Maintenance - Midnight', 
		@server_name = N'(local)'
END
GO

DECLARE @BackupToDisk BIT 
SET @BackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk') AS BIT)

IF @BackupToDisk = 1
BEGIN
IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Backup Transaction Logs')
BEGIN
	DECLARE @domainOperator SYSNAME
	SELECT @domainOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')

	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Backup Transaction Logs', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Trx log backup tasks to be performed every day at regular intervals throughout the day.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=@domainOperator,
		@notify_page_operator_name=N'Dave Mason',
		@start_step_id = 1

	EXEC msdb.dbo.sp_attach_schedule
		@job_name=N'DBA-Backup Transaction Logs',
		@schedule_name=N'DBA-Transaction Log Backup Schedule'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Backup Transaction Logs', 
		@server_name = N'(local)'
END
END
GO

IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Weekday Maintenance')
BEGIN
	DECLARE @domainOperator SYSNAME
	SELECT @domainOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')

	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Weekday Maintenance', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Various tasks to be performed "after hours" once each weekday.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=@domainOperator,
		@notify_page_operator_name=N'Dave Mason',
		@start_step_id = 1

	EXEC msdb.dbo.sp_attach_schedule
		@job_name=N'DBA-Weekday Maintenance',
		@schedule_name=N'DBA-Weekday Maintenance Schedule'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Weekday Maintenance', 
		@server_name = N'(local)'
END
GO

IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Check Fixed Drive Free Space')
BEGIN
	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Check Fixed Drive Free Space', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Self-explanatory.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'Dave Mason',
		@notify_page_operator_name=N'Dave Mason',
		@start_step_id = 1

	EXEC msdb.dbo.sp_attach_schedule
		@job_name=N'DBA-Check Fixed Drive Free Space',
		@schedule_name=N'DBA-Hourly Schedule'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Check Fixed Drive Free Space', 
		@server_name = N'(local)'
END
GO

DECLARE @BackupToDisk BIT 
SET @BackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk') AS BIT)

IF @BackupToDisk = 1
BEGIN
IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Disk Maintenance')
BEGIN
	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Disk Maintenance', 
		@enabled=1, 

		--Since this job will be run manually on demand, there's no need for notifications.
		--@notify_level_eventlog=0, 
		--@notify_level_email=0, 
		--@notify_level_netsend=0, 
		--@notify_level_page=0,
		--@notify_email_operator_name=N'Dave Mason',
		--@notify_page_operator_name=N'Dave Mason',	
			 
		@delete_level=0, 
		@description=N'Frees up disk space by deleting old backups.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@start_step_id = 1

	--Don't attach a schedule.  Job is to be run manually on demand.
	--EXEC msdb.dbo.sp_attach_schedule
	--	@job_name=N'DBA-Disk Maintenance',
	--	@schedule_name=N'DBA-Midnight Schedule'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Disk Maintenance', 
		@server_name = N'(local)'
END
END
GO

DECLARE @ArchiveBackupToDisk BIT 
SET @ArchiveBackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk - Archive') AS BIT);

IF @ArchiveBackupToDisk = 1
BEGIN
	IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Archive Backup')
	BEGIN
		DECLARE @domainOperator SYSNAME
		SELECT @domainOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')

		EXEC msdb.dbo.sp_add_job 
			@job_name=N'DBA-Archive Backup', 
			@enabled=1, 
			@notify_level_eventlog=0, 
			@notify_level_email=2, 
			@notify_level_netsend=0, 
			@notify_level_page=2, 
			@delete_level=0, 
			@description=N'Tasks for creating FULL database backups that are archived and saved per SunGard''s backup policy and domain customer SLA.', 
			@category_name=N'[Uncategorized (Local)]', 
			@owner_login_name=N'sa', 
			@notify_email_operator_name=@domainOperator,
			@notify_page_operator_name=N'Dave Mason',
			@start_step_id = 1

		EXEC msdb.dbo.sp_attach_schedule
			@job_name=N'DBA-Archive Backup',
			@schedule_name=N'DBA-Archive Backup Schedule'

		EXEC msdb.dbo.sp_add_jobserver 
			@job_name=N'DBA-Archive Backup', 
			@server_name = N'(local)'
	END
END
GO
