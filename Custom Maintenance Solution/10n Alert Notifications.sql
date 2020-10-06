/*
	Brian Humphrey performs scans and penetration testing measures from an Alien Vault server.
	These activities cause SQL to raise severity level 20 errors, which fire the SQL Alerts
	that we configured.  Normally, our alerts automatically respond with an email.  But the 
	vast number of severity level 20 errors results in a flood of email.

	The script steps below change the alerting environment.  Severity level 20 errors will
	continue to fire an alert.  But the alert will no longer respond with an email if the
	error originated from the Alien Vault server (172.30.42.20).
*/

/*
	Create "Catch Alert" job, if it doesn't already exist.
*/
IF NOT EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Catch Severity 20 Alert')
BEGIN
	DECLARE @domainOperator SYSNAME
	SELECT @domainOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')

	EXEC msdb.dbo.sp_add_job 
		@job_name=N'DBA-Catch Severity 20 Alert', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Responds to Severity Level 20 alerts, conditionally sends email.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=@domainOperator,
		@notify_page_operator_name=N'Dave Mason',
		@start_step_id = 1

	EXEC msdb.dbo.sp_add_jobserver 
		@job_name=N'DBA-Catch Severity 20 Alert', 
		@server_name = N'(local)'
END
GO

/*
	Add job step(s) to "Catch Alert".
*/
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Catch Severity 20 Alert')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Catch Severity 20 Alert', 
		@step_id=0

	--Add job step(s).
	DECLARE @Cmd NVARCHAR(MAX) = '
DECLARE @Descr VARCHAR(MAX) = ''$(ESCAPE_SQUOTE(A-MSG))'';

IF @Descr LIKE ''%CLIENT: 172.30.42.20%''
BEGIN
	--Do nothing.
	PRINT ''Alert originating from Alien Vault host server.''
END
ELSE
BEGIN
	--DECLARE @To NVARCHAR(MAX) = ''DBA@Domain.com'';
	DECLARE @To NVARCHAR(MAX) = ''' + DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Email') + ''';
	DECLARE @Subj NVARCHAR(255);
	DECLARE @EmailBody NVARCHAR(MAX);
	DECLARE @Date VARCHAR(MAX) = ''$(ESCAPE_SQUOTE(DATE))'';
	DECLARE @Time VARCHAR(MAX) = ''$(ESCAPE_SQUOTE(TIME))'';

	SET @Subj = ''SQL Server Alert System: ''''20-Fatal Error in Current Process'''' occurred on '' + @@SERVERNAME
	SET @Date = SUBSTRING(@Date, 5, 2) + ''/'' + RIGHT(@Date, 2) + ''/'' + LEFT(@Date, 4);
	SET @Time = LEFT(@Time, 2) + '':'' + SUBSTRING(@Time, 3, 2) + '':'' + RIGHT(@Time, 2);
	SET @EmailBody = ''DATE/TIME:	'' + @Date + '' '' + @Time + ''

DESCRIPTION:	'' + @Descr + ''


COMMENT:	(None)

JOB RUN:	(None)''
	
	
	EXEC msdb..sp_send_dbmail
		@recipients = @To, 
		@blind_copy_recipients = ''DbaPager@Gmail.com'',
		@profile_name = ''Default'',
		@subject = @Subj,
		@body = @EmailBody
END';

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Catch Severity 20 Alert', 
		@step_name=N'Send Alert Email', 
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
		@flags=0
END
ELSE
	RAISERROR('SQL Server job "DBA-Catch Severity 20 Alert" does not exist.', 16, 1);
GO

/*
	Notifications are now handled by the SQL Agent job.
	Remove alert notifications.
*/
DECLARE @Tsql NVARCHAR(MAX) = ''
SELECT @Tsql = @Tsql + 'EXEC msdb.dbo.sp_delete_notification @alert_name=''' + a.name + ''', @operator_name = ''' + o.name + ''';'
FROM msdb.dbo.sysalerts a
JOIN msdb.dbo.sysnotifications n
	ON a.id = n.alert_id
JOIN msdb.dbo.sysoperators o
	ON o.id = n.operator_id 
WHERE a.name = '20-Fatal Error in Current Process'

--PRINT @Tsql
EXEC sp_executesql @Tsql
GO

/*
	Add alert response.
*/
EXEC msdb.dbo.sp_update_alert 
	@name=N'20-Fatal Error in Current Process', 
	@job_name = N'DBA-Catch Severity 20 Alert';
GO
