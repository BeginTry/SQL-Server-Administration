USE msdb;
GO

--TODO: ensure Token Replacement is enabled in SQL Server Agent (Alert System).

--TODO: specify an Operator for alert notifications.
DECLARE @Operator SYSNAME = 'Support';
--Optional: specify a "prefix" for alert/job name.
DECLARE @Prefix NVARCHAR(64) = 'DBA - ';

DECLARE @JobName SYSNAME = @Prefix + N'Failed sa Login Attempt - Alert Handler';
DECLARE @Alert SYSNAME = @Prefix + N'Failed sa Login Attempt';

--Placeholder for custom error message.
EXEC sp_addmessage
	@msgnum = 90125,   
	@severity = 16, @msgtext = 'Temp placeholder', @replace = 'replace'; 

--Add the alert.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = @Alert
)
	EXEC msdb.dbo.sp_add_alert 
		@name=@Alert, 
		@message_id=90125, 
		@enabled=1, 
		@delay_between_responses=0, --Recommend leaving this at zero.
		@include_event_description_in=7


--Add the alert notification.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysnotifications n
	JOIN msdb.dbo.sysalerts a
		ON a.id = n.alert_id
	JOIN msdb.dbo.sysoperators o
		ON o.id = n.operator_id
	WHERE a.name = @Alert
	AND o.name = @Operator
)
BEGIN
	EXEC msdb.dbo.sp_add_notification 
		@alert_name=@Alert, 
		@operator_name=@Operator, 
		@notification_method = 1;
END

--EXEC msdb.dbo.sp_update_notification 
--	@alert_name=@Alert, 
--		@operator_name=@Operator, 
--		@notification_method = 1;

--Add the "handler" SQL Agent job.
IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs j WHERE j.name = @JobName)
BEGIN
	EXEC msdb.dbo.sp_add_job 
		@job_name=@JobName, 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa'

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=@JobName, 
		@server_name = N'(local)'
END

--Add the job step.
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = @JobName)
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=@JobName, 
		@step_id=0;

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=@JobName, 
		@step_name=N'Check For sa', 
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
		@command=N'DECLARE @AlertMsg NVARCHAR(MAX) = ''$(ESCAPE_SQUOTE(A-MSG))'';

IF @AlertMsg LIKE ''%''''sa''''%''
BEGIN
	EXEC sp_addmessage
		@msgnum = 90125,   
		@severity = 16, 
		@msgtext = @AlertMsg, 
		@replace = ''replace''; 

	RAISERROR(90125, -1, 1, @AlertMsg) WITH LOG;
END', 
		@database_name=N'master', 
		@flags=0
END

/**************************************************/
--Add generic alert for failed logins.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'18456 - Failed Login Attempt'
)
	EXEC msdb.dbo.sp_add_alert 
		@name=N'18456 - Failed Login Attempt', 
		@message_id=18456, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, --Important to leave this at zero.
		@include_event_description_in=0, 
		@job_name=@JobName
GO
