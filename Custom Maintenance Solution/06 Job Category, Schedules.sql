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

--Create job schedules (as needed).
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Transaction Log Backup Schedule'
)
AND DbaData.dba.GetInstanceConfiguration('Backup To Disk') = 1
	EXEC msdb.dbo.sp_add_schedule 
		@schedule_name=N'DBA-Transaction Log Backup Schedule', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=1,  
		@active_start_date=20140101, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Midnight Schedule'
)
	EXEC msdb.dbo.sp_add_schedule 
		@schedule_name=N'DBA-Midnight Schedule', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0,  
		@active_start_date=20140101, 
		@active_end_date=99991231, 
		@active_start_time=500, 
		@active_end_time=235959
GO

--Rename schedule if it already exists.
IF EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-After Hours Schedule'
)
	EXEC msdb.dbo.sp_update_schedule 
		@name=N'DBA-After Hours Schedule',
		@new_name=N'DBA-Weekday Maintenance Schedule';
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Weekday Maintenance Schedule'
)
	EXEC msdb.dbo.sp_add_schedule 
		@schedule_name=N'DBA-Weekday Maintenance Schedule', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20140101, 
		@active_end_date=99991231, 
		--@active_start_time=10000, 
		@active_start_time=63000, 
		@active_end_time=235959
GO

--Rename schedule if it already exists.
IF EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Weekend Schedule'
)
	EXEC msdb.dbo.sp_update_schedule 
		@name=N'DBA-Weekend Schedule',
		@new_name=N'DBA-Saturday Maintenance Schedule';
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Saturday Maintenance Schedule'
)
	EXEC msdb.dbo.sp_add_schedule 
		@schedule_name=N'DBA-Saturday Maintenance Schedule', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=64, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20140101, 
		@active_end_date=99991231, 
		--@active_start_time=10000, 
		@active_start_time=80000, 
		@active_end_time=235959
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Sunday Maintenance Schedule'
)
	EXEC msdb.dbo.sp_add_schedule 
		@schedule_name=N'DBA-Sunday Maintenance Schedule', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20140101, 
		@active_end_date=99991231, 
		@active_start_time=123000, 
		@active_end_time=235959
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Hourly Schedule'
)
	EXEC msdb.dbo.sp_add_schedule 
		@schedule_name=N'DBA-Hourly Schedule', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=1,  
		@active_start_date=20140101, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name = N'DBA-Archive Backup Schedule'
)
	EXEC msdb.dbo.sp_add_schedule 
		@schedule_name=N'DBA-Archive Backup Schedule', 
		@enabled=1, 
		@freq_type=16, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0,  
		@freq_recurrence_factor=1,
		@active_start_date=20160101, 
		@active_end_date=99991231, 
		@active_start_time=10000, 
		@active_end_time=235959
GO
/*
	SELECT *
	FROM msdb.dbo.sysschedules s
	WHERE s.name IN (N'Transaction Log Backup Schedule', N'Midnight Schedule', 
		N'After Hours Schedule', N'Weekend Schedule')
*/
