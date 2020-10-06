USE tempdb;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'GenerateRestoreCommands'
)
	DROP PROCEDURE dbo.GenerateRestoreCommands;
GO

CREATE PROCEDURE dbo.GenerateRestoreCommands
	@BackupType CHAR(1),
	@DBName SYSNAME = NULL,
	@DataMovePath NVARCHAR(MAX) = NULL,
	@LogMovePath NVARCHAR(MAX) = NULL
/*
	Purpose:	
	Generates RESTORE DB commands from existing backup history in msdb.
	Copy and paste the output into an SSMS window.
	
	Inputs:
	@BackupType :  valid values are 'D' (Full Backup) or 'I' (Differential Backup)
	@DBName : name of the database (NULL for all db's).
	@DataMovePath/@LogMovePath: for MOVE operations during a restore.

	History:
	11/06/2014	Created
	08/05/2016	Exclude COPY_ONLY backups when searching for last full backup.
	04/23/2019	Add params @DataMovePath and @LogMovePath.
*/
AS
SET NOCOUNT ON

IF @BackupType NOT IN ('D', 'I')
BEGIN
	RAISERROR('Invalid value for input parameter @BackupType.  Valid values are ''D'' (Full Backup) or ''I'' (Differential Backup)', 16, 1);
	RETURN;
END

--Ensure @DataMovePath is not null and ends with a backslash.
SET @DataMovePath = COALESCE(@DataMovePath, '');
IF @DataMovePath <> '' AND RIGHT(@DataMovePath, 1) <> '\'
	SET @DataMovePath = @DataMovePath + '\';

--Ensure @LogMovePath is not null and ends with a backslash.
SET @LogMovePath = COALESCE(@LogMovePath, '');
IF @LogMovePath <> '' AND RIGHT(@LogMovePath, 1) <> '\'
	SET @LogMovePath = @LogMovePath + '\';

SELECT database_name,
	MAX(backup_finish_date) Backup_Finish_Date
INTO #LastFullBackups
FROM msdb.dbo.backupset bs
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = bs.media_set_id
	AND bmf.device_type = 2
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
		AND bmf.device_type = 2	--device = disk
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
WHERE bmf.device_type = 2
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
	SELECT bs.database_name, CHAR(9) + 'MOVE ''' + logical_name + ''' TO ''' + 
			CASE
				WHEN @DataMovePath <> '' AND bf.file_type = 'D' THEN @DataMovePath + REVERSE(LEFT(REVERSE(bf.physical_name), CHARINDEX('\', REVERSE(bf.physical_name)) - 1))
				WHEN @LogMovePath <> '' AND bf.file_type = 'L' THEN @LogMovePath + REVERSE(LEFT(REVERSE(bf.physical_name), CHARINDEX('\', REVERSE(bf.physical_name)) - 1))
				--Full text catalog files (et al) go to @DataMovePath, if specified.
				WHEN @DataMovePath <> '' THEN @DataMovePath + REVERSE(LEFT(REVERSE(bf.physical_name), CHARINDEX('\', REVERSE(bf.physical_name)) - 1))
				--same path as when backup was performed.
				ELSE bf.physical_name 
			END + ''',', 5
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

/*
	EXEC dbo.GenerateRestoreCommands
		@BackupType = 'D',
		@DBName = NULL,
		@DataMovePath = 'E:\SQL Server',
		@LogMovePath = 'F:\SQL Server\'


SELECT d.name ,
'EXEC dbo.GenerateRestoreCommands
	@BackupType = ''D'',
	@DBName = ''' + d.name + ''',
	@DataMovePath = ''E:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\Data\'',
	@LogMovePath = ''F:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\Data\'';'
FROM master.sys.databases d
WHERE d.database_id > 4
ORDER BY d.name
*/
