USE DbaData
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dr' AND r.ROUTINE_NAME = 'GenerateRestoreCommands'
)
	DROP PROCEDURE dr.GenerateRestoreCommands 
GO

CREATE PROCEDURE dr.GenerateRestoreCommands
	@BackupType CHAR(1),
	@DBName SYSNAME = NULL
/*
	Purpose:	
	Generates RESTORE DB commands from existing backup history in msdb.
	Copy and paste the output into an SSMS window.
	
	Inputs:
	@BackupType :  valid values are 'D' (Full Backup) or 'I' (Differential Backup)
	@DBName : name of the database (NULL for all db's).

	History:
	11/06/2014	DBA	Created
	08/05/2016	DBA	Exclude COPY_ONLY backups when searching for last full backup.
*/
AS
SET NOCOUNT ON

IF @BackupType NOT IN ('D', 'I')
BEGIN
	RAISERROR('Invalid value for input parameter @BackupType.  Valid values are ''D'' (Full Backup) or ''I'' (Differential Backup)', 16, 1);
	RETURN
END

SELECT database_name,
	MAX(backup_finish_date) Backup_Finish_Date
INTO #LastFullBackups
FROM msdb.dbo.backupset bs
JOIN master.sys.databases d
	ON d.name = bs.database_name
WHERE type = @BackupType
AND database_name = COALESCE(@DBName, database_name)
AND bs.is_copy_only = 0
GROUP BY database_name

--RESTORE DB:  one row per db.
SELECT database_name, CAST('RESTORE DATABASE ' + database_name AS VARCHAR(4000)) AS Command, CAST(1 AS NUMERIC(3,1)) AS CmdOrder
INTO #Commands
FROM #LastFullBackups

--FROM:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'FROM' AS Command, 2 AS CmdOrder
FROM #LastFullBackups

--DISK =:  one row per backup file per db.
;WITH BackupFileCount AS
(
	SELECT bs.database_name, MAX(bmf.family_sequence_number) FileCount
	FROM msdb.dbo.backupset bs
	JOIN #LastFullBackups lfb
		ON lfb.database_name = bs.database_name
		AND lfb.Backup_Finish_Date = bs.backup_finish_date
	JOIN msdb.dbo.backupmediafamily bmf
		ON bmf.media_set_id = bs.media_set_id
	--AND Mirror = 1
	GROUP BY bs.database_name
)
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT bs.database_name, CHAR(9) + 'DISK = ''' + bmf.physical_device_name + '''' + CASE WHEN bmf.family_sequence_number = bfc.FileCount THEN '' ELSE ',' END, 3
FROM msdb.dbo.backupset bs
JOIN #LastFullBackups lfb
	ON lfb.database_name = bs.database_name
	AND lfb.Backup_Finish_Date = bs.backup_finish_date
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = bs.media_set_id
JOIN BackupFileCount bfc
	ON bfc.database_name = bs.database_name
--AND Mirror = 1

--WITH:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'WITH ' AS Command, 4 AS CmdOrder
FROM #LastFullBackups

IF @BackupType = 'D'
BEGIN
	--Comment:  one row per db.
	INSERT INTO #Commands (database_name, Command, CmdOrder)
	SELECT database_name, CHAR(9) + '--TODO: replace the source database file paths below with the database file paths for the target.' AS Command, 4.5 AS CmdOrder
	FROM #LastFullBackups

	--MOVE:  one row per logical filename per db.
	INSERT INTO #Commands (database_name, Command, CmdOrder)
	SELECT bs.database_name, CHAR(9) + 'MOVE ''' + logical_name + ''' TO ''' + bf.physical_name + ''',', 5
	FROM msdb.dbo.backupset bs
	JOIN #LastFullBackups lfb
		ON lfb.database_name = bs.database_name
		AND lfb.Backup_Finish_Date = bs.backup_finish_date
	JOIN msdb.dbo.backupfile bf
		ON bf.backup_set_id = bs.backup_set_id
END

--REPLACE, NORECOVERY:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, CHAR(9) + 'REPLACE, NORECOVERY ' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) AS Command, 6 AS CmdOrder
FROM #LastFullBackups

DECLARE @Tsql VARCHAR(MAX)
SET @Tsql = ''

--Copy and paste this output into an SSMS window.
SELECT @Tsql = @Tsql + Command + CHAR(13) + CHAR(10)
FROM #Commands 
ORDER BY database_name, CmdOrder

PRINT @Tsql

--Cleanup
DROP TABLE #LastFullBackups
DROP TABLE #Commands
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dr' AND r.ROUTINE_NAME = 'GenerateRestoreCommands_Log'
)
	DROP PROCEDURE dr.GenerateRestoreCommands_Log 
GO

CREATE PROCEDURE dr.GenerateRestoreCommands_Log
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

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dr' AND r.ROUTINE_NAME = 'GenerateRestoreCommands_All'
)
	DROP PROCEDURE dr.GenerateRestoreCommands_All 
GO

CREATE PROCEDURE dr.GenerateRestoreCommands_All
	@DBName SYSNAME
/*
	Purpose:	
	Generates RESTORE DB commands for a single database.
	The resulting output will restore the most recent FULL backup,
	followed by the most recent DIFFERENTIAL backup (if availabel),
	followed by the most recent LOG backups (if available).
	Copy and paste the output into an SSMS window.
	
	Inputs:
	@DBName : name of the database (NULL for all db's).

	History:
	11/06/2014	DBA	Created
*/
AS
SET NOCOUNT ON
EXEC DbaData.dr.GenerateRestoreCommands @BackupType = 'D', @DBName = @DBName
EXEC DbaData.dr.GenerateRestoreCommands @BackupType = 'I', @DBName = @DBName
EXEC DbaData.dr.GenerateRestoreCommands_Log @DBName = @DBName
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dr' AND r.ROUTINE_NAME = 'GenerateRestoreCommandsByDate'
)
	DROP PROCEDURE dr.GenerateRestoreCommandsByDate 
GO

CREATE PROCEDURE dr.GenerateRestoreCommandsByDate
	@DBName SYSNAME,
	@AsOfDate DATETIME
/*
	Purpose:
	Generates RESTORE DB commands (from existing backup history in msdb) 
	for point-in-time recovery, based on the input "as of" date.
	Copy and paste the output into an SSMS window.
	
	Inputs:
	@DBName : name of the database.
	@AsOfDate: self-explanatory.

	History:
	07/31/2017	DBA	Created
*/
AS
SET NOCOUNT ON

IF DB_ID(@DBName) IS NULL
BEGIN
	RAISERROR ('Database does not exist.', 16, 1);
	RETURN;
END

IF @AsOfDate >= CURRENT_TIMESTAMP
BEGIN
	RAISERROR ('Time machine is broken. Can''t restore to a future point in time.', 16, 1);
	RETURN;
END

;WITH LastFullBackup AS
(
	SELECT TOP(1)
		bs.database_name,
		bs.database_guid,
		bs.backup_finish_date,
		bs.media_set_id,
		bs.recovery_model,
		bs.last_lsn
	FROM msdb.dbo.BackupSet bs
	JOIN master.sys.databases d
		ON d.name = bs.database_name
	JOIN master.sys.database_recovery_status s
		ON s.database_guid = bs.database_guid
	WHERE bs.backup_finish_date <= @AsOfDate
	AND bs.type = 'D'
	AND bs.is_copy_only = 0
	AND d.name = @DBName
	ORDER BY bs.backup_finish_date DESC
),
LastDiffBackup AS
(
	SELECT TOP(1)
		lfb.database_name,
		bs.database_guid,
		bs.backup_finish_date,
		bs.media_set_id,
		bs.recovery_model,
		bs.last_lsn
	FROM msdb.dbo.BackupSet bs
	JOIN LastFullBackup lfb
		ON lfb.database_guid = bs.database_guid
	WHERE bs.backup_finish_date <= @AsOfDate
	AND bs.backup_finish_date > lfb.backup_finish_date
	AND bs.type = 'I'
	AND bs.is_copy_only = 0
	ORDER BY bs.backup_finish_date DESC
)
SELECT 
	lfb.database_name, 
	lfb.database_guid,
	lfb.backup_finish_date backup_finish_date_Full,
	lfb.media_set_id media_set_id_Full,
	lfb.recovery_model recovery_model_Full,
	lfb.last_lsn last_lsn_Full,
	@AsOfDate AS AsOfDate, 
	ldb.media_set_id media_set_id_Diff, 
	ldb.backup_finish_date backup_finish_date_Diff,
	ldb.recovery_model recovery_model_Diff,
	ldb.last_lsn last_lsn_Diff
INTO #LastBackups
FROM LastFullBackup lfb
LEFT JOIN LastDiffBackup ldb
	ON ldb.database_guid = lfb.database_guid

--Was database in FULL recovery model when the last FULL/DIFF was taken?
IF NOT EXISTS (
	SELECT *
	FROM #LastBackups lb
	WHERE COALESCE(lb.recovery_model_Diff, lb.recovery_model_Full) = 'FULL'
)
BEGIN
	--Raise an error, but don't return.
	--Let the PRINT statement below proceed.
	RAISERROR ('The log chain is broken. Point in time recovery is not possible.', 16, 1);
	PRINT CHAR(13) + CHAR(10);
END

--Have there been any LOG backups after @AsOfDate?
ELSE IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.BackupSet bs
	JOIN master.sys.databases d
		ON d.name = bs.database_name
	JOIN master.sys.database_recovery_status s
		ON s.database_guid = bs.database_guid 
	WHERE bs.backup_finish_date >= @AsOfDate
	AND bs.type = 'L'
	AND d.name = @DBName
)
BEGIN
	DECLARE @Msg VARCHAR(MAX) = 'There have been no transaction log backups since ' + 
		CAST(@AsOfDate AS VARCHAR) + '. Take a tail log backup before proceeding.';

	IF DATABASEPROPERTYEX(@DBName, 'Recovery') = 'FULL'
	BEGIN
		--Raise an error and return.
		--(We still have to option to take a tail LOG backup and try again.)
		RAISERROR (@Msg, 16, 1);
		RETURN;
	END
	ELSE
	BEGIN
		--Raise an error, but don't return.
		--Let the PRINT statement below proceed.
		SET @Msg = 'There have been no transaction log backups since ' + 
			CAST(@AsOfDate AS VARCHAR) + '. Recovery to that point in time is not possible. ' +
			'(The current RECOVERY MODEL is not set to FULL.)';
		RAISERROR (@Msg, 16, 1);
		PRINT CHAR(13) + CHAR(10);
	END
END

--RESTORE DB:  one row per db.
SELECT database_name, CAST('RESTORE DATABASE ' + database_name AS VARCHAR(4000)) AS Command, CAST(1 AS NUMERIC(3,1)) AS CmdOrder
INTO #Commands
FROM #LastBackups

--FROM:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'FROM' AS Command, 2 AS CmdOrder
FROM #LastBackups

--DISK =:  one row per backup file per db.
;WITH BackupFileCount AS
(
	SELECT lfb.database_guid, MAX(bmf.family_sequence_number) FileCount
	FROM #LastBackups lfb
	JOIN msdb.dbo.backupmediafamily bmf
		ON bmf.media_set_id = lfb.media_set_id_Full
	--AND bmf.mirror = 1
	GROUP BY lfb.database_guid
)
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT lfb.database_name, CHAR(9) + 'DISK = ''' + bmf.physical_device_name + '''' + CASE WHEN bmf.family_sequence_number = bfc.FileCount THEN '' ELSE ',' END, 3
FROM #LastBackups lfb
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = lfb.media_set_id_Full
JOIN BackupFileCount bfc
	ON bfc.database_guid = lfb.database_guid
--AND bmf.mirror = 1

--WITH:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'WITH ' AS Command, 4 AS CmdOrder
FROM #LastBackups

--Comment:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, CHAR(9) + '--TODO: replace the source database file paths below with the database file paths for the target.' AS Command, 4.5 AS CmdOrder
FROM #LastBackups

--MOVE:  one row per logical filename per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT bs.database_name, CHAR(9) + 'MOVE ''' + Logical_Name + ''' TO ''' + bf.Physical_Name + ''',', 5
FROM msdb.dbo.BackupSet bs
JOIN #LastBackups lfb
	ON lfb.database_guid = bs.database_guid
	AND lfb.backup_finish_date_Full = bs.backup_finish_date
JOIN msdb.dbo.backupfile bf
	ON bf.backup_set_id = bs.backup_set_id

--REPLACE, NORECOVERY:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, CHAR(9) + 'REPLACE, NORECOVERY ' AS Command, 6 AS CmdOrder
FROM #LastBackups

--GO batch separator:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) AS Command, 7 AS CmdOrder
FROM #LastBackups

/************************************
--BEGIN RESTORE DIFF
************************************/

--RESTORE DB:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'RESTORE DATABASE ' + database_name AS Command, 8 AS CmdOrder
FROM #LastBackups lb
WHERE lb.backup_finish_date_Diff IS NOT NULL

--FROM:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'FROM' AS Command, 9 AS CmdOrder
FROM #LastBackups lb
WHERE lb.backup_finish_date_Diff IS NOT NULL

--DISK =:  one row per backup file per db.
;WITH BackupFileCount AS
(
	SELECT lfb.database_guid, MAX(bmf.family_sequence_number) FileCount
	FROM #LastBackups lfb
	JOIN msdb.dbo.backupmediafamily bmf
		ON bmf.media_set_id = lfb.media_set_id_Diff
	--AND bmf.mirror = 1
	GROUP BY lfb.database_guid
)
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT lfb.database_name, CHAR(9) + 'DISK = ''' + bmf.physical_device_name + '''' + CASE WHEN bmf.family_sequence_number = bfc.FileCount THEN '' ELSE ',' END, 10
FROM #LastBackups lfb
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = lfb.media_set_id_Diff
JOIN BackupFileCount bfc
	ON bfc.database_guid = lfb.database_guid
WHERE lfb.backup_finish_date_Diff IS NOT NULL
--AND bmf.mirror = 1

--WITH:  one row per db.
INSERT INTO #Commands (database_name, Command, CmdOrder)
SELECT database_name, 'WITH REPLACE, NORECOVERY' + CHAR(13) + CHAR(10) +
'GO' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) AS Command, 11 AS CmdOrder
FROM #LastBackups  lb
WHERE lb.backup_finish_date_Diff IS NOT NULL

DECLARE @Tsql VARCHAR(MAX)
SET @Tsql = ''

--Build RESTORE commands for FULL backup (and DIFF backup, if applicable).
SELECT @Tsql = @Tsql + Command + CHAR(13) + CHAR(10)
FROM #Commands 
ORDER BY database_name, CmdOrder

/******************************************************************************/
--One row per transaction log backup per db.
--ASSUMPTION: trx logs are backed up to a single file on disk.

--TODO: need STOP AT command for last LOG restore.
SELECT @Tsql = @Tsql + 
	'RESTORE DATABASE ' + lb.database_name + CHAR(13) + CHAR(10) +
	CHAR(9) + 'FROM DISK = ''' + bmf.physical_device_name + '''' + CHAR(13) + CHAR(10) +
	'WITH REPLACE, NORECOVERY ' + 
	CASE
		WHEN bs.backup_finish_date > @AsOfDate THEN ', STOPAT = ''' + CONVERT(VARCHAR, @AsOfDate, 120) + ''''
		ELSE ''
	END +
	CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10)
FROM #LastBackups  lb
JOIN msdb.dbo.backupset bs
	ON bs.database_guid = lb.database_guid
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = bs.media_set_id
CROSS APPLY (
	SELECT TOP(1) p.*
	FROM msdb.dbo.backupset p
	WHERE p.database_guid = bs.database_guid
	AND p.type = 'L'
	AND p.backup_finish_date < bs.backup_finish_date
	ORDER BY p.backup_finish_date DESC
) prev
WHERE bs.type = 'L'
AND (
	bs.first_lsn >= COALESCE(lb.last_lsn_Diff, lb.last_lsn_Full) OR
	bs.last_lsn >= COALESCE(lb.last_lsn_Diff, lb.last_lsn_Full)
)
AND prev.backup_finish_date < @AsOfDate
--AND Mirror = 1
ORDER BY bs.backup_finish_date

/******************************************************************************/

--Copy and paste this output into an SSMS window.
SELECT @Tsql
PRINT @Tsql

--Cleanup
DROP TABLE #LastBackups
DROP TABLE #Commands
GO
