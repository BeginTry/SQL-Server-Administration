USE [msdb]
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Daily Maintenance - Midnight')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Daily Maintenance - Midnight', 
		@step_id=0

	--Add job step(s).
	IF DbaData.dba.GetInstanceConfiguration('Cloned Server Check') = '1'
	BEGIN
		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Daily Maintenance - Midnight', 
			@step_name=N'Cloned Server Check', 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=3, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, 
			@subsystem=N'TSQL', 
			@command=N'EXEC DbaData.dba.ClonedServerCheck
GO', 
			@database_name=N'master', 
			@flags=0
	END

	IF DbaData.dba.GetInstanceConfiguration('Alert Service Login Changed') = '1'
	BEGIN
		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Daily Maintenance - Midnight', 
			@step_name=N'Alert Service Login Changed', 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=3, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, 
			@subsystem=N'TSQL', 
			@command=N'EXEC DbaData.dba.CheckServiceLogins
GO', 
			@database_name=N'master', 
			@flags=0
	END

	IF DbaData.dba.GetInstanceConfiguration('Alert Missing Backup Files') = '1'
	BEGIN
	DECLARE @Cmd NVARCHAR(MAX) 
	SET @Cmd = N'DECLARE @KeepBackupsAsOf DATE = ' + DbaData.dba.GetInstanceConfiguration('Backup Retention Period Expression') + '; 
EXEC DbaData.dba.MissingBackupFilesReport @KeepBackupsAsOf;
GO';

		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Daily Maintenance - Midnight', 
			@step_name=N'Alert Missing Backup Files', 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=3, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, 
			@subsystem=N'TSQL', 
			@command=@Cmd, 
			@database_name=N'master', 
			@flags=0
	END

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Daily Maintenance - Midnight', 
		@step_name=N'Cycle Error Log', 
		@subsystem=N'TSQL', 
		@command=N'EXEC sp_cycle_errorlog;
	GO', 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@database_name=N'master', 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@flags=0
END
ELSE
	RAISERROR('SQL Server job "DBA-Daily Maintenance - Midnight" does not exist.', 16, 1);
GO

DECLARE @BackupToDisk BIT 
SET @BackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk') AS BIT)

IF @BackupToDisk = 1
BEGIN
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Backup Transaction Logs')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Backup Transaction Logs', 
		@step_id=0;

	DECLARE @Cmd NVARCHAR(MAX) 
	SET @Cmd = N'EXECUTE DbaData.dba.BackupTransactionLogs 
	@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - LOG') + ''',
	@WithEncryption = ' + DbaData.dba.GetInstanceConfiguration('Backup WITH ENCRYPTION') + ', 
	@ServerCertificate = ''' + DbaData.dba.GetInstanceConfiguration('Backup Encryption - Server Certificate') + '''
GO';

	--Add job step(s).
	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Backup Transaction Logs', 
		--@step_id=1, 
		@step_name=N'Backup Transaction Logs', 
		@subsystem=N'TSQL', 
		@command=@Cmd, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@database_name=N'master', 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@flags=0

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Backup Transaction Logs', 
		@step_name=N'Verify Backups', 
		--@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.VerifyLatestBackups
	@BackupType = ''L''
GO', 
		@database_name=N'master', 
		@flags=0
	
	SET @Cmd = '--Take a backup of MSDB so we have the backup history of the Trx Logs that were backed up in the previous step.
EXECUTE DbaData.dba.BackupDatabase_DIFF
	@DBName = ''msdb'',
	@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - DIFFERENTIAL') + ''',
	@WithEncryption = ' + DbaData.dba.GetInstanceConfiguration('Backup WITH ENCRYPTION') + ', 
	@ServerCertificate = ''' + DbaData.dba.GetInstanceConfiguration('Backup Encryption - Server Certificate') + '''
GO'
	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Backup Transaction Logs', 
		--@step_id=3, 
		@step_name=N'Backup MSDB', 
		@subsystem=N'TSQL', 
		@command=@Cmd, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@database_name=N'master', 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@flags=0

	EXEC msdb.dbo.sp_update_job 
		@job_name=N'DBA-Backup Transaction Logs', 
		@start_step_id = 1
END
ELSE
	RAISERROR('SQL Server job "DBA-Backup Transaction Logs" does not exist.', 16, 1);
END
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Saturday Maintenance')
BEGIN
	DECLARE @BackupToDisk BIT 
	SET @BackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk') AS BIT)

	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Saturday Maintenance', 
		@step_id=0

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Saturday Maintenance', 
		@step_name=N'Check Databases', 
		--@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.CheckDatabases
	@PhysicalOnly = 0
GO', 
		@database_name=N'master', 
		@flags=0
	
	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Saturday Maintenance', 
		@step_name=N'Defrag Indexes', 
		--@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.DefragmentIndexes
	@MinFragmentation = 5.0,
	@ReorgVsRebuildPercentThreshold = 30.0
GO', 
		@database_name=N'master', 
		@flags=0

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Saturday Maintenance', 
		@step_name=N'Update Statistics', 
		--@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.UpdateStatistics
	@WithFullScan = 0
GO', 
		@database_name=N'master', 
		@flags=0

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Saturday Maintenance', 
		@step_name=N'Reorganize Full Text Catalogs', 
		--@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.ReorganizeFullTextCatalogs
GO', 
		@database_name=N'master', 
		@flags=0

	IF @BackupToDisk = 1
	BEGIN
		DECLARE @Cmd NVARCHAR(MAX) 
		SET @Cmd = N'EXECUTE DbaData.dba.BackupDatabases 
	@DifferentialOnly = 0,
	@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - FULL') + ''',
	@WithEncryption = ' + DbaData.dba.GetInstanceConfiguration('Backup WITH ENCRYPTION') + ', 
	@ServerCertificate = ''' + DbaData.dba.GetInstanceConfiguration('Backup Encryption - Server Certificate') + '''
GO'
		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Saturday Maintenance', 
			@step_name=N'Backup Databases - FULL', 
			--@step_id=5, 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, @subsystem=N'TSQL', 
			@command=@Cmd, 
			@database_name=N'master', 
			@flags=0
	
		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Saturday Maintenance', 
			@step_name=N'Verify Backups', 
			--@step_id=6, 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, @subsystem=N'TSQL', 
			@command=N'EXECUTE DbaData.dba.VerifyLatestBackups
	@BackupType = ''D''
GO', 
			@database_name=N'master', 
			@flags=0

		SET @Cmd = N'
#----------------------------------------------------------------------
#    ©2017 SunGard Public Sector
#----------------------------------------------------------------------
#Script Name: Delete Old Backup Files.ps1
#Description: Deletes old sql server backup files.
#Expects: 
#Notes:  Be careful with interactive/GUI POSH commands. These may lead
#        to SQL Agent job step errors such as the following:
#    A command that prompts the user failed because the host program 
#    or the command type does not support user interaction.
#----------------------------------------------------------------------
#Modification History
#    04/30/2014 -- Written By: DBA
#    04/07/2017 -- DBA
#        Code converted from VBScript.
#	06/05/2017 -- DBA
#		Code changes to reflect new version of
#		[dba].[GetDeletableBackupFiles].
#----------------------------------------------------------------------

# Open SQL connection
$conn = New-Object System.Data.SqlClient.SqlConnection;
$conn.ConnectionString = "Server=' + @@SERVERNAME + ';Database=DbaData;Integrated Security=SSPI";
$conn.Open();

# Get data via SqlDataReader
$cmd = New-Object System.Data.SqlClient.SqlCommand;
$cmd.CommandType = [System.Data.CommandType]::StoredProcedure;
$cmd.CommandText = "dba.GetDeletableBackupFiles";
$cmd.Connection = $conn;
$dr = $cmd.ExecuteReader();

#Loop through the files, deleting the ones that still exist on disk.
while ($dr.Read())
{
    $backupFile = $dr["physical_device_name"].ToString();
		
    if ([System.IO.File]::Exists($backupFile)){
        try{
            [System.IO.File]::Delete($backupFile)
        }
        catch{
        }
    }
}

#Clean up resources.
$dr.Dispose();
$cmd.Dispose();
$conn.Dispose();
';
		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Saturday Maintenance', 
			@step_name=N'Delete Old Backups From Disk', 
			--@step_id=7, 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, 
			@subsystem=N'PowerShell', 
			@command=@Cmd, 
			--@database_name=N'master', 
			@flags=0
	END

	SET @Cmd = N'DECLARE @OldestDate DATETIME
SET @OldestDate = CURRENT_TIMESTAMP - ' + DbaData.dba.GetInstanceConfiguration('Backup History Keep Days') + '
EXEC msdb.dbo.sp_delete_backuphistory 
	@oldest_date = @OldestDate

SET @OldestDate = CURRENT_TIMESTAMP - ' + DbaData.dba.GetInstanceConfiguration('Job History Keep Days') + '
EXEC msdb.dbo.sp_purge_jobhistory 
	@oldest_date = @OldestDate'

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Saturday Maintenance', 
		@step_name=N'Delete Backup History', 
		--@step_id=8, 
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

	EXEC msdb.dbo.sp_update_job 
		@job_name=N'DBA-Saturday Maintenance', 
		@start_step_id = 1
END
ELSE
	RAISERROR('SQL Server job "DBA-Saturday Maintenance" does not exist.', 16, 1);
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Sunday Maintenance')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Sunday Maintenance', 
		@step_id=0
	
	DECLARE @BackupToDisk BIT;
	SET @BackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk') AS BIT);

	IF @BackupToDisk = 1
	BEGIN
		DECLARE @Cmd NVARCHAR(MAX) 
		SET @Cmd = N'EXECUTE DbaData.dba.BackupDatabases 
	@DifferentialOnly = 1,
	@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - DIFFERENTIAL') + ''',
	@WithEncryption = ' + DbaData.dba.GetInstanceConfiguration('Backup WITH ENCRYPTION') + ', 
	@ServerCertificate = ''' + DbaData.dba.GetInstanceConfiguration('Backup Encryption - Server Certificate') + '''
GO'
		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Sunday Maintenance', 
			@step_name=N'Backup Databases - DIFFERENTIAL', 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
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

		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Sunday Maintenance', 
			@step_name=N'Verify Backups', 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, 
			@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.VerifyLatestBackups
	@BackupType = ''I''
GO', 
			@database_name=N'master', 
			@flags=0
	END

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Sunday Maintenance', 
		@step_name=N'Defrag Indexes', 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.DefragmentIndexes
	@MinFragmentation = 5.0,
	@ReorgVsRebuildPercentThreshold = 30.0
GO', 
		@database_name=N'master', 
		@flags=0

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Sunday Maintenance', 
		@step_name=N'Update Statistics', 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.UpdateStatistics
	@WithFullScan = 0
GO', 
		@database_name=N'master', 
		@flags=0
END
ELSE
	RAISERROR('SQL Server job "DBA-Sunday Maintenance" does not exist.', 16, 1);
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Weekday Maintenance')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Weekday Maintenance', 
		@step_id=0
	
	DECLARE @BackupToDisk BIT 
	SET @BackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk') AS BIT)

	IF @BackupToDisk = 1
	BEGIN
		DECLARE @Cmd NVARCHAR(MAX) 
		SET @Cmd = N'EXECUTE DbaData.dba.BackupDatabases 
	@DifferentialOnly = 1,
	@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - DIFFERENTIAL') + ''',
	@WithEncryption = ' + DbaData.dba.GetInstanceConfiguration('Backup WITH ENCRYPTION') + ', 
	@ServerCertificate = ''' + DbaData.dba.GetInstanceConfiguration('Backup Encryption - Server Certificate') + '''
GO'
		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Weekday Maintenance', 
			@step_name=N'Backup Databases - DIFFERENTIAL', 
			--@step_id=1, 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
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

		EXEC msdb.dbo.sp_add_jobstep 
			@job_name=N'DBA-Weekday Maintenance', 
			@step_name=N'Verify Backups', 
			--@step_id=2, 
			@cmdexec_success_code=0, 
			@on_success_action=3, 
			@on_success_step_id=0, 
			@on_fail_action=2, 
			@on_fail_step_id=0, 
			@retry_attempts=0, 
			@retry_interval=0, 
			@os_run_priority=0, 
			@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.VerifyLatestBackups
	@BackupType = ''I''
GO', 
			@database_name=N'master', 
			@flags=0
	END

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Weekday Maintenance', 
		@step_name=N'Defrag Indexes', 
		--@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.DefragmentIndexes
	@MinFragmentation = 5.0,
	@ReorgVsRebuildPercentThreshold = 30.0
GO', 
		@database_name=N'master', 
		@flags=0

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Weekday Maintenance', 
		@step_name=N'Update Statistics', 
		--@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.UpdateStatistics
	@WithFullScan = 0
GO', 
		@database_name=N'master', 
		@flags=0
END
ELSE
	RAISERROR('SQL Server job "DBA-Weekday Maintenance" does not exist.', 16, 1);
GO

IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Check Fixed Drive Free Space')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Check Fixed Drive Free Space', 
		@step_id=0

	DECLARE @Cmd NVARCHAR(MAX)
	SET @Cmd = N'EXEC DbaData.dba.InsertPageReadHistory
GO'
	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Check Fixed Drive Free Space', 
		@step_name=N'Insert page read history', 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
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

	SET @Cmd = N'EXEC DbaData.dba.CheckFixedDriveFreeSpace
	@FreeSpaceThresholdMB = ' + DbaData.dba.GetInstanceConfiguration('Available Disk Space Threshold') + '
GO'
	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Check Fixed Drive Free Space', 
		@step_name=N'Check Fixed Drive Free Space', 
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
	RAISERROR('SQL Server job "DBA-Check Fixed Drive Free Space" does not exist.', 16, 1);
GO

DECLARE @BackupToDisk BIT 
SET @BackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk') AS BIT)

IF @BackupToDisk = 1
BEGIN
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Disk Maintenance')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Disk Maintenance', 
		@step_id=0


	DECLARE @Cmd NVARCHAR(MAX)
	SET @Cmd = N'
#----------------------------------------------------------------------
#    ©2017 SunGard Public Sector
#----------------------------------------------------------------------
#Script Name: Delete Old Backup Files.ps1
#Description: Deletes old sql server backup files.
#Expects: 
#Notes:  Be careful with interactive/GUI POSH commands. These may lead
#        to SQL Agent job step errors such as the following:
#    A command that prompts the user failed because the host program 
#    or the command type does not support user interaction.
#----------------------------------------------------------------------
#Modification History
#    04/30/2014 -- Written By: DBA
#    04/07/2017 -- DBA
#        Code converted from VBScript.
#----------------------------------------------------------------------

#region functions
function Get-CutoffDate([string]$BackupRetentionPeriodExpression, [System.Data.SqlClient.SqlConnection]$cn)
{
    # Get data via SqlDataReader
    $cmd = New-Object System.Data.SqlClient.SqlCommand;
    $cmd.CommandType = [System.Data.CommandType]::Text;
    $cmd.CommandText = "SELECT $BackupRetentionPeriodExpression AS DateCutoff";
    $cmd.Connection = $cn;
    $dr = $cmd.ExecuteReader();

    if ($dr.Read())
    {		
        [DateTime]$retVal = $dr["DateCutoff"];
    }

    #Clean up resources.
    $dr.Dispose();
    $cmd.Dispose();

    return $retVal
}
#endregion

# Open SQL connection
$conn = New-Object System.Data.SqlClient.SqlConnection;
$conn.ConnectionString = "Server=' + @@SERVERNAME + ';Database=DbaData;Integrated Security=SSPI";
$conn.Open();

# Get data via SqlDataReader
$cmd = New-Object System.Data.SqlClient.SqlCommand;
$cmd.CommandType = [System.Data.CommandType]::StoredProcedure;
$cmd.CommandText = "dba.GetDeletableBackupFiles";

#Configurable date in the past (plus one day, for some cushion).
[DateTime]$KeepBackupsAsOf = Get-CutoffDate "' + DbaData.dba.GetInstanceConfiguration('Backup Retention Period Expression') + '" $conn;
$KeepBackupsAsOf = $KeepBackupsAsOf.AddDays(1);

$param = $cmd.Parameters.Add("@BackupKeepDate" , [System.Data.SqlDbType]::Date);
$param.Value = $KeepBackupsAsOf;
$cmd.Connection = $conn;
$dr = $cmd.ExecuteReader();

#Loop through the files, deleting the ones that still exist on disk.
while ($dr.Read())
{
    $backupFile = $dr["physical_device_name"].ToString();
		
    if ([System.IO.File]::Exists($backupFile)){
        try{
            [System.IO.File]::Delete($backupFile)
        }
        catch{
        }
    }
}

#Clean up resources.
$dr.Dispose();
$cmd.Dispose();
$conn.Dispose();
';
	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Disk Maintenance', 
		@step_name=N'Delete Old Backups From Disk', 
		--@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=1, 
		@on_fail_step_id=0,  
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@subsystem=N'PowerShell', 
		@command=@Cmd, 
		--@database_name=N'master', 
		@flags=0

	EXEC msdb.dbo.sp_update_job 
		@job_name=N'DBA-Disk Maintenance', 
		@start_step_id = 1
END
ELSE
	RAISERROR('SQL Server job "DBA-Disk Maintenance" does not exist.', 16, 1);
END
GO

DECLARE @ArchiveBackupToDisk BIT 
SET @ArchiveBackupToDisk = CAST(DbaData.dba.GetInstanceConfiguration('Backup To Disk - Archive') AS BIT);

IF @ArchiveBackupToDisk = 1
BEGIN
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'DBA-Archive Backup')
BEGIN
	--Calling sp_delete_jobstep with a step_id value of zero deletes all job steps for the job.
	EXEC msdb.dbo.sp_delete_jobstep
		@job_name=N'DBA-Archive Backup', 
		@step_id=0

	DECLARE @Cmd NVARCHAR(MAX) 
	SET @Cmd = N'EXECUTE DbaData.dba.BackupDatabasesForArchive 
	@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - Archive') + ''',
	@WithEncryption = ' + DbaData.dba.GetInstanceConfiguration('Backup WITH ENCRYPTION') + ', 
	@ServerCertificate = ''' + DbaData.dba.GetInstanceConfiguration('Backup Encryption - Server Certificate') + '''
GO'
	--Add job step(s).
	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Archive Backup', 
		--@step_id=1, 
		@step_name=N'Backup Databases - Archive', 
		@subsystem=N'TSQL', 
		@command=@Cmd, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@database_name=N'master', 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@flags=0

	EXEC msdb.dbo.sp_add_jobstep 
		@job_name=N'DBA-Archive Backup', 
		--@step_id=2, 
		@step_name=N'Verify Backups', 
		@subsystem=N'TSQL', 
		@command=N'EXECUTE DbaData.dba.VerifyLatestBackups
	@BackupType = ''D''
GO', 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@database_name=N'master', 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, 
		@flags=0

	EXEC msdb.dbo.sp_update_job 
		@job_name=N'DBA-Archive Backup', 
		@start_step_id = 1
END
ELSE
	RAISERROR('SQL Server job "DBA-Archive Backup" does not exist.', 16, 1);
END
GO
