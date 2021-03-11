USE tempdb;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'GenerateRestoreCommands_Log'
)
	DROP PROCEDURE dbo.GenerateRestoreCommands_Log 
GO

CREATE PROCEDURE dbo.GenerateRestoreCommands_Log
	@DBName SYSNAME = NULL
/*
	Purpose:	
	Generates RESTORE DB commands for transaction logs from existing backup history in msdb.
	Copy and paste the output into an SSMS window.
	
	Inputs:
	@DBName : name of the database (NULL for all db's).
	History:
	11/06/2014	DBA	Created
	08/05/2016	DBA	Exclude COPY_ONLY backups when searching for last full backup.
*/
AS
SET NOCOUNT ON
DECLARE @Tsql VARCHAR(MAX) 
SET @Tsql = ''

;WITH LastFullOrDiffBackups AS
(
	SELECT bs.database_name,
		MAX(backup_finish_date) LastBackup
	FROM msdb.dbo.backupset bs
	JOIN master.sys.databases d
		ON d.name = bs.database_name 
		AND d.name = COALESCE(@DBName, d.name)
	WHERE type IN ('D', 'I')
	AND bs.is_copy_only = 0
	GROUP BY bs.database_name
)
--One row per transaction log backup per db.
--(assumes trx logs are backed up to a single file on disk).
SELECT @Tsql = @Tsql + 
	'RESTORE DATABASE ' + bs.database_name + CHAR(13) + CHAR(10) +
	CHAR(9) + 'FROM DISK = ''' + bmf.physical_device_name + '''' + CHAR(13) + CHAR(10) +
	'WITH REPLACE, NORECOVERY ' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
FROM msdb.dbo.backupset bs
JOIN LastFullOrDiffBackups lfodb
	ON lfodb.database_name = bs.database_name
	AND bs.backup_finish_date > lfodb.LastBackup
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = bs.media_set_id
--AND Mirror = 1
ORDER BY bs.database_name, bs.backup_finish_date

PRINT @Tsql
GO
