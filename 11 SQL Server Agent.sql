/*
	Set various properties for the SQL Server Agent.
*/
EXEC msdb.dbo.sp_set_sqlagent_properties 
	@alert_replace_runtime_tokens=1,
	@sqlserver_restart=1, 
	@monitor_autostart=1,
	@databasemail_profile=N'Default'
GO

--Enable fail-safe operator.
EXEC master.dbo.sp_MSsetalertinfo 
	@failsafeoperator=N'Dave Mason', 
	@notificationmethod=1, 
	@pagersendsubjectonly=1
GO

--Enable mail profile.
EXEC master.dbo.xp_instance_regwrite 
	N'HKEY_LOCAL_MACHINE', 
	N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent', 
	N'DatabaseMailProfile', 
	N'REG_SZ', 
	N'Default'
GO

/*
	Personal notes/observations:
	For new installations, when a SQL Agent job fails, email notification will fail to be sent,
	even if database mail is properly configured.  If you look in the Log File Viewer for Job
	History, you may see something like the following in a job message:

		NOTE: Failed to notify 'domain DBA Team' via email.  
		NOTE: Failed to notify 'Dave Mason' via pager.

	Surprisingly, there is no corresponding record in table msdb.dbo.sysmail_faileditems
	One way to fix this is to right-click the SQL Server Agent, and open the properties
	dialog.  Select the Alert System page, uncheck "Enable fail-safe operator", and click
	OK to save changes.  Open the SQL Server Agent properties dialog, go back to the Alert
	System page, and revert the fail-safe operator settings to their previous value(s).
	Restart the service for the SQL instance (this also restarts the SQL Server Agent service).
*/