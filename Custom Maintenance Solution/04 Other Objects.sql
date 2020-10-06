USE DbaData
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba' 
	AND t.TABLE_NAME = 'BackupFiles' 
	AND t.TABLE_TYPE = 'VIEW'
)
	DROP VIEW dba.BackupFiles
GO

DECLARE @TSql NVARCHAR(MAX) = '
CREATE VIEW dba.BackupFiles 
AS
/*
	Purpose:	
	Gets a list of backup files for databases that currently exist on the instance.

	History:
	05/30/2017	DBA	Written. Combines logic of deprecated stored procs
		[dba].[GetCurrentBackupFiles] and [dba].[GetDeletableBackupFiles].
*/
--The last full backup (on disk) for databases that are currently on the instance.
WITH LastFullBackups AS
(
	SELECT rs.database_guid, MAX(bs.backup_finish_date) Backup_Finish_Date	
	FROM msdb.dbo.backupset bs
	JOIN master.sys.database_recovery_status rs
		ON rs.database_guid = bs.database_guid
	JOIN msdb.dbo.backupmediafamily bmf
		ON bmf.media_set_id = bs.media_set_id
		AND bmf.device_type IN (2, 102)	--Disk
	WHERE bs.type = ''D'' --Database (Full)
	AND bs.is_copy_only = 0
	AND bs.server_name = @@SERVERNAME
	GROUP BY rs.database_guid
),
Baseline AS
(
	--List of databases and date of the most recent 
	--full backup that occurred prior to 
	--	' + DbaData.dba.GetInstanceConfiguration('Backup Retention Period Expression') + '.
	SELECT bs.database_guid, MAX(bs.backup_finish_date) Backup_Finish_Date	
	FROM msdb.dbo.backupset bs
	JOIN LastFullBackups lfb
		ON lfb.database_guid = bs.database_guid
	WHERE bs.type = ''D'' --Database (Full)
	AND bs.is_copy_only = 0
	AND bs.server_name = @@SERVERNAME

	--Indicates how far back in time backup files (excluding archives) are to be kept on disk.
	AND bs.backup_finish_date <= ' + DbaData.dba.GetInstanceConfiguration('Backup Retention Period Expression') + '

	--Never delete backup files for the most recent backup, regardless of how long ago it was.
	AND bs.backup_finish_date <= lfb.Backup_Finish_Date

	GROUP BY bs.database_guid
)
SELECT 
	bmf.physical_device_name, 
	--bs.backup_start_date, 
	MAX(bs.backup_finish_date) AS backup_finish_date, 
	bs.type, bs.database_name, bs.name,

	CASE
		WHEN MAX(bs.backup_finish_date) < b.Backup_Finish_Date THEN CAST(1 AS BIT)
		ELSE CAST(0 AS BIT)
	END AS IsDeletable

FROM Baseline b
JOIN msdb.dbo.backupset bs
	ON bs.database_guid = b.database_guid
JOIN msdb.dbo.backupmediafamily bmf
	ON bmf.media_set_id = bs.media_set_id
WHERE bs.type IN (''D'', ''I'', ''L'')
AND bs.server_name = @@SERVERNAME

--This view should never return files that comprise an "Archive" backup.
--The list of archive backup files will be handled by a different view.
AND (bs.name IS NULL OR bs.name COLLATE Latin1_General_CI_AS <> ''Archive'')	

AND bmf.device_type IN (2, 102)	--Disk

GROUP BY bmf.physical_device_name, bs.type, bs.database_name, bs.name,
	b.Backup_Finish_Date

--ORDER BY bs.database_name, MAX(bs.backup_finish_date), bmf.physical_device_name

';

--PRINT @Tsql
EXEC sp_executesql @TSql;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba' 
	AND t.TABLE_NAME = 'LastBackups' 
	AND t.TABLE_TYPE = 'VIEW'
)
	DROP VIEW dba.LastBackups
GO

CREATE VIEW dba.LastBackups
AS
/*
	Purpose:	
	Returns information on the last FULL and DIFFERENTIAL backups for each database.

	History:
	??/??/20??	DBA	Created
	03/10/2016	DBA	Ignore COPY_ONLY backups.
*/
WITH LastBackup AS
(
	SELECT d.name DbName,
		MAX(bFull.backup_finish_date) Backup_Finish_Date_Full,
		CASE
			WHEN MAX(bDiff.backup_finish_date) < MAX(bFull.backup_finish_date) THEN NULL
			ELSE MAX(bDiff.backup_finish_date) 
		END Backup_Finish_Date_Diff
	FROM master.sys.databases d
	LEFT JOIN msdb.dbo.backupset bFull
		ON bFull.database_name = d.Name
		AND bFull.type = 'D'
		AND bFull.is_copy_only = 0
	LEFT JOIN msdb.dbo.backupset bDiff
		ON bDiff.database_name = d.Name
		AND bDiff.type = 'I'
	WHERE d.name NOT IN ('tempdb')
	GROUP BY d.name
)
,LastBackupSet AS
(

	SELECT lb.DbName, bsFull.backup_set_id Backup_Set_Id_Full,
		bsDiff.backup_set_id Backup_Set_Id_Diff
	FROM LastBackup lb
	LEFT JOIN msdb.dbo.backupset  bsFull
		ON bsFull.database_name = lb.DbName
		AND bsFull.backup_finish_date = lb.Backup_Finish_Date_Full
	LEFT JOIN msdb.dbo.backupset  bsDiff
		ON bsDiff.database_name = lb.DbName
		AND bsDiff.backup_finish_date = lb.Backup_Finish_Date_Diff
)
,MediaSetCounts AS
(
	SELECT MAX(bmf.mirror) + 1 AS NumCopies, bmf.media_set_id
	FROM msdb.dbo.backupmediafamily bmf
	GROUP BY bmf.media_set_id
)
SELECT 
	lbs.DbName, 
	busFull.backup_finish_date DateCompletedFull, busFull.name backupnamefull, 
	busFull.user_name BackupUserFull, 
	CONVERT(VARCHAR, CAST (busFull.backup_size / mscFull.NumCopies / 1024 / 1024 AS MONEY), 1) SizeMBFull,
	CONVERT(VARCHAR, CAST (busFull.compressed_backup_size / mscFull.NumCopies / 1024 / 1024 AS MONEY), 1) SizeMBCompressedFull,
	dba.BackupFileList(busFull.media_set_id) FilesFull,
	
	busDiff.backup_finish_date DateCompletedDiff, busDiff.name BackupNameDiff, 
	busDiff.user_name BackupUserDiff, 
	CONVERT(VARCHAR, CAST (busDiff.backup_size / mscDiff.NumCopies / 1024 / 1024 AS MONEY), 1) SizeMBDiff,
	CONVERT(VARCHAR, CAST (busDiff.compressed_backup_size / mscDiff.NumCopies / 1024 / 1024 AS MONEY), 1) SizeMBCompressedDiff,
	dba.BackupFileList(busDiff.media_set_id) FilesDiff
FROM LastBackupSet lbs
LEFT JOIN msdb.dbo.backupset  busFull
	ON busFull.backup_set_id = lbs.Backup_Set_Id_Full
LEFT JOIN MediaSetCounts mscFull
	ON mscFull.media_set_id = busFull.media_set_id
	
LEFT JOIN msdb.dbo.backupset  busDiff
	ON busDiff.backup_set_id = lbs.Backup_Set_Id_Diff
LEFT JOIN MediaSetCounts mscDiff
	ON mscDiff.media_set_id = busDiff.media_set_id
--ORDER BY lbs.DbName
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'PopulateEstimatedBackupSize'
)
	DROP PROCEDURE dba.PopulateEstimatedBackupSize 
GO

CREATE PROCEDURE dba.PopulateEstimatedBackupSize 
	@DbName SYSNAME = NULL,
	@UpdateUsage BIT = 0
AS
/*
	Purpose:	
	Calculates the estimated size of a FULL database backup for one db (or all db's).  
	(Inspiration from [sys].[sp_spaceused])
	
	Inputs:
	@DBName - self-explanatory
	@UpdateUsage - (optional) run DBCC UpdateUsage prior to estimating the db backup size.

	History:
	10/18/2011	DBA	Created
	02/17/2012	DBA	Calculate estimated compressed backup size.
	07/28/2013	DBA	MS recommendation is to backup databases ReportServer (with FULL recovery model) 
						and ReportServerTempDB (with SIMPLE recovery model).
						Therefore, do not exclude them.
						http://msdn.microsoft.com/en-us/library/ms155814(v=sql.105).aspx
	02/17/2014	DBA	Don't exclude [distribution].
	09/02/2014	DBA	Don't exclude any db's, except for [tempdb].
*/
DECLARE @Tsql NVARCHAR(MAX)

--Remove orphaned db's
DELETE FROM DbaData.dba.EstimatedBackupSize
WHERE DbName NOT IN ( SELECT name FROM master.sys.databases )

IF @UpdateUsage = 1
BEGIN
	--Run DBCC commands.
	SET @Tsql = ''
	SELECT @Tsql = @Tsql + 'DBCC UPDATEUSAGE([' + name + ']) WITH NO_INFOMSGS ' + CHAR(13) + CHAR(10)
	FROM master.sys.databases
	WHERE State_Desc = 'ONLINE'
	AND Source_Database_Id IS NULL
	--Skip these db's (they're never backed up).
	AND name NOT IN ('tempdb')
	AND Is_Read_Only = 0
	AND name = COALESCE(@DbName, name)
	ORDER BY name

	--PRINT @Tsql
	EXEC sp_executesql @Tsql
END

--Insert estimates.
SET @Tsql = 'INSERT INTO DbaData.dba.EstimatedBackupSize (DbName, Size_GB) ' + CHAR(13) + CHAR(10)
DELETE FROM DbaData.dba.EstimatedBackupSize
WHERE DbName = COALESCE(@DbName, DbName)

SELECT @Tsql = @Tsql + 'SELECT ''' + name + ''' Name, SUM(total_pages) * 8192 / 1024.0 / 1024.0 / 1024.0 BackupSizeGB FROM [' + name +
	'].sys.allocation_units UNION ' + CHAR(13) + CHAR(10)
FROM master.sys.databases
WHERE State_Desc = 'ONLINE'
AND Source_Database_Id IS NULL
--Skip these db's (they're never backed up).
AND name NOT IN ('tempdb')
AND name = COALESCE(@DbName, name)
ORDER BY name

SET @Tsql = LEFT(@Tsql, LEN(@Tsql) - 8)
--PRINT @Tsql
EXEC sp_executesql @Tsql

/*
	Look at the most recent full compressed backup (by db).
	Get the compression ratio, multiply it by the estimated backup size,
	and use the resulting value to update CompressedSize_GB.
*/
;WITH LastFullCompressedBackups AS
(
	SELECT bs.database_name, MAX(bs.backup_finish_date) backup_finish_date
	FROM msdb.dbo.backupset bs
	WHERE bs.type = 'D'
	AND bs.compressed_backup_size IS NOT NULL
	AND bs.database_name = COALESCE(@DbName, bs.database_name)
	GROUP BY bs.database_name
)
UPDATE ebs
SET ebs.CompressedSize_GB = bs.compressed_backup_size/bs.backup_size * ebs.Size_GB
--SELECT ebs.*, bs.backup_finish_date, bs.compressed_backup_size/bs.backup_size CompressionRatio
FROM msdb.dbo.backupset bs
JOIN LastFullCompressedBackups lfcb
	ON lfcb.database_name = bs.database_name
	AND lfcb.backup_finish_date = bs.backup_finish_date
JOIN DbaData.dba.EstimatedBackupSize ebs
	ON ebs.DbName = bs.database_name
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'BackupDatabase_FULL'
)
	DROP PROCEDURE dba.BackupDatabase_FULL 
GO

CREATE PROCEDURE dba.BackupDatabase_FULL 
	@DBName SYSNAME,
	@Path VARCHAR(255),
	@MirrorToPath VARCHAR(255) = NULL,
	@MaxBackupFileSize MONEY = 4,
	@WithEncryption BIT = 0,
	@ServerCertificate SYSNAME = NULL
AS 
/*
	Purpose:	
	Performs a FULL database backup for a specific database.
	
	Inputs:
	@DBName - self-explanatory
	@Path - the path where the backup files are to be created.
	@MirrorToPath - (optional) the path where a copy of the backup files are to be created.
	@MaxBackupFileSize - (optional) maximum size on disk (in GB) of individual db backup files.
	@WithEncryption - (optional) create backup with enctyption.
	@ServerCertificate - (optional) name of server certificate.

	History:
	10/18/2011	DBA	Created
	02/07/2014	DBA	COMPRESSION not allowed on Sql Express versions.
	08/10/2016	DBA	Stop checking for/specifying COMPRESSION. Let the system configuration
						"backup compression default" indicate if COMPRESSION is to be used.
	11/04/2016	DBA	Add encryption options.
	05/04/2017	DBA	Fix error when trying to backup to > 64 devices.
*/
IF NOT EXISTS ( SELECT 1 FROM master.sys.databases WHERE name = @DBName )
BEGIN
	PRINT 'Database "' + COALESCE(@DBName, '') + '" does not exist.'
	RETURN
END

IF @WithEncryption = 1
BEGIN
	IF COALESCE(@ServerCertificate, '') = ''
	BEGIN
		RAISERROR('Backup encryption was requested, but no server certificate was specified for parameter @ServerCertificate.', 16, 1);
        RETURN;
	END

	IF NOT EXISTS (
			SELECT *
			FROM master.sys.certificates c
			WHERE c.name = @ServerCertificate
		)
	BEGIN
		DECLARE @ErrMsg NVARCHAR(MAX) = 'Server certificate [' + @ServerCertificate + '] does not exist.';
        RAISERROR(@ErrMsg, 16, 1);
        RETURN;
	END
END

EXEC DbaData.dba.PopulateEstimatedBackupSize @DBName, @UpdateUsage = 0
SET LANGUAGE 'us_english';

CREATE TABLE #FullBackup (
	ID INT IDENTITY,
	[Path] VARCHAR(255) NOT NULL,
	MirrorToPath VARCHAR(255) NULL,
	[FileName] VARCHAR(512) NOT NULL
)

DECLARE @NumFiles INT
DECLARE @Loop INT

SET @Loop = 1
SELECT @NumFiles = CompressedSize_GB / @MaxBackupFileSize
FROM DbaData.dba.EstimatedBackupSize
WHERE DbName = @DBName

IF EXISTS ( SELECT 1 FROM DbaData.dba.EstimatedBackupSize
			WHERE DbName = @DBName
			AND CompressedSize_GB / @MaxBackupFileSize > CAST(@NumFiles AS MONEY))
	SET @NumFiles = @NumFiles + 1

--Always use a minimum of 2 files for backups.
IF @NumFiles IS NULL OR @NumFiles <= 1
	SET @NumFiles = 2
--Up to 64 backup devices may be specified in a comma-separated list.
ELSE IF @NumFiles > 64
	SET @NumFiles = 64;

IF RIGHT(@Path, 1) != '\'
	SET @Path = @Path + '\'
IF RIGHT(@MirrorToPath, 1) != '\'
	SET @MirrorToPath = @MirrorToPath + '\'

WHILE @Loop <= @NumFiles
BEGIN
	--Format for the FileName is
	--DBname yyyy-mm-dd.DayOfWeek.Full.FileNum.bak
	INSERT INTO #FullBackup ([Path], MirrorToPath, [FileName])
	VALUES (@Path, @MirrorToPath, @DBName + ' ' + 
		REPLACE(CONVERT(VARCHAR, GETDATE(), 111), '/', '-') + '.' + 
		DATENAME(WEEKDAY, GETDATE()) + '.Full.' +
		CAST(@Loop AS VARCHAR) + '.bak')
	SET @Loop = @Loop + 1
END

DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = 'BACKUP DATABASE [' + @DBName + ']' + CHAR(13) + CHAR(10) +
	'TO '

SELECT @Tsql = @Tsql + CHAR(9) + 'DISK = ''' + [Path] + [FileName] + ''',' + CHAR(13) + CHAR(10)
FROM #FullBackup
ORDER BY ID

SELECT @Tsql = LEFT(@Tsql, LEN(@Tsql) - 3) + CHAR(13) + CHAR(10)

IF COALESCE(@MirrorToPath, '') <> ''	--Backup to a second location.
BEGIN
	SET @Tsql = @Tsql + 'MIRROR TO '
	SELECT @Tsql = @Tsql + CHAR(9) + 'DISK = ''' + MirrorToPath + [FileName] + ''',' + CHAR(13) + CHAR(10)
	FROM #FullBackup
	ORDER BY ID
	SELECT @Tsql = LEFT(@Tsql, LEN(@Tsql) - 3) + CHAR(13) + CHAR(10)
END

SET @Tsql = @Tsql + 'WITH INIT, FORMAT'

IF @WithEncryption = 1
BEGIN
	SET @Tsql = @Tsql + ', ' + CHAR(13) + CHAR(10) + 'ENCRYPTION
(
	ALGORITHM = AES_256,
	SERVER CERTIFICATE = ' + @ServerCertificate + '
)';
END

--PRINT @Tsql
EXEC sp_executesql @Tsql

DROP TABLE #FullBackup
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'BackupDatabase_DIFF'
)
	DROP PROCEDURE dba.BackupDatabase_DIFF 
GO

DECLARE @TSql NVARCHAR(MAX) = '
CREATE PROCEDURE dba.BackupDatabase_DIFF
	@DBName SYSNAME,
	@Path VARCHAR(255),
	@MirrorToPath VARCHAR(255) = NULL,
	@WithEncryption BIT = 0,
	@ServerCertificate SYSNAME = NULL
AS 
/*
	Purpose:	
	Performs a DIFFERENTIAL database backup for a specific database.
	
	Inputs:
	@DBName - self-explanatory
	@Path - the path where the backup files are to be created.
	@MirrorToPath - (optional) the path where a copy of the backup files are to be created.
	@WithEncryption - (optional) create backup with enctyption.
	@ServerCertificate - (optional) name of server certificate.

	History:
	10/19/2011	DBA	Created
	02/14/2012	DBA	Enable COMPRESSION.
	04/29/2016	DBA	No COMPRESSION on EXPRESS edition.
	08/10/2016	DBA	1) Stop checking for/specifying COMPRESSION. Let the system configuration
						"backup compression default" indicate if COMPRESSION is to be used.
						2) If no FULL backup exists, take a FULL backup instead of a DIFFERENTIAL.
	11/04/2016	DBA	Add encryption options.
*/
DECLARE @FileName1 VARCHAR(255)
DECLARE @FileName2 VARCHAR(255)
DECLARE @MirrorToName1 VARCHAR(255)
DECLARE @MirrorToName2 VARCHAR(255)

IF NOT EXISTS ( SELECT 1 FROM master.sys.databases WHERE name = @DBName )
BEGIN
	PRINT ''Database "'' + COALESCE(@DBName, '''') + ''" does not exist.''
	RETURN
END

IF @WithEncryption = 1
BEGIN
	IF COALESCE(@ServerCertificate, '''') = ''''
	BEGIN
		RAISERROR(''Backup encryption was requested, but no server certificate was specified for parameter @ServerCertificate.'', 16, 1);
        RETURN;
	END

	IF NOT EXISTS (
			SELECT *
			FROM master.sys.certificates c
			WHERE c.name = @ServerCertificate
		)
	BEGIN
		DECLARE @ErrMsg NVARCHAR(MAX) = ''Server certificate ['' + @ServerCertificate + ''] does not exist.'';
        RAISERROR(@ErrMsg, 16, 1);
        RETURN;
	END
END

--Take a FULL backup if one has never been taken.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.backupset bs
	JOIN master.sys.database_recovery_status s
		ON s.database_guid = bs.database_guid
	WHERE bs.database_name = @DBName
	AND bs.type = ''D''
	AND bs.is_copy_only = 0
	AND bs.server_name = @@SERVERNAME
)
BEGIN
	EXECUTE DbaData.dba.BackupDatabase_FULL 
		@DBName = @DBName, 
		@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - FULL') + ''',
		@MirrorToPath = @MirrorToPath,
		@WithEncryption = @WithEncryption,
		@ServerCertificate = @ServerCertificate;
	RETURN;
END

IF RIGHT(@Path, 1) != ''\''
	SET @Path = @Path + ''\''
IF RIGHT(@MirrorToPath, 1) != ''\''
	SET @MirrorToPath = @MirrorToPath + ''\''

SET LANGUAGE ''us_english'';

--Format for the FileName is
--DBname yyyy-mm-dd.DayOfWeek.Differential.FileNum.bak
SET @FileName1 = @DBName + '' '' + 
	REPLACE(CONVERT(VARCHAR, GETDATE(), 111), ''/'', ''-'') +
	''.'' + DATENAME(weekday, GETDATE()) 

SET @FileName2 = @FileName1 + ''.Differential.2.bak''
SET @FileName1 = @FileName1 + ''.Differential.1.bak''
SET @MirrorToName1 = @MirrorToPath + @FileName1
SET @MirrorToName2 = @MirrorToPath + @FileName2

SET @FileName1 = @Path + @FileName1
SET @FileName2 = @Path + @FileName2

DECLARE @TSql NVARCHAR(MAX) = ''BACKUP DATABASE ['' + @DBName + '']
TO DISK = '' + CHAR(39) + @FileName1 + CHAR(39) + '',
	DISK = '' + CHAR(39) + @FileName2 + CHAR(39) + '''';

IF COALESCE(@MirrorToPath, '''') <> ''''	--Backup to multiple (two) locations.
BEGIN
	SET @TSql = @TSql + CHAR(13) + CHAR(10) + ''MIRROR TO DISK = '' + CHAR(39) + @MirrorToName1 + CHAR(39) + '',
	DISK = '' + CHAR(39) + @MirrorToName2 + CHAR(39) + '''';
END

SET @TSql = @TSql + CHAR(13) + CHAR(10) + ''WITH FORMAT, INIT, DIFFERENTIAL'';

IF @WithEncryption = 1
BEGIN
	SET @TSql = @TSql + '', '' + CHAR(13) + CHAR(10) + ''ENCRYPTION
(
	ALGORITHM = AES_256,
	SERVER CERTIFICATE = '' + @ServerCertificate + ''
)'';
END


--PRINT @TSql
EXEC sp_executesql @TSql;
';

--PRINT @Tsql
EXEC sp_executesql @TSql;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'BackupTransactionLog'
)
	DROP PROCEDURE dba.BackupTransactionLog 
GO

DECLARE @TSql NVARCHAR(MAX) = '
CREATE PROCEDURE dba.BackupTransactionLog
	@DBName SYSNAME,
	@Path VARCHAR(255),
	@MirrorToPath VARCHAR(255) = NULL,
	@WithEncryption BIT = 0,
	@ServerCertificate SYSNAME = NULL
AS 
/*
	Purpose:	
	Backs up a transaction log for a specific database.
	
	Inputs:
	@DBName - self-explanatory
	@Path - the path where the transaction log backup file is to be created.
	@MirrorToPath - (optional) the path where a copy of the trx log backup file is to be created.

	History:
	02/18/2009	DBA	Created
	01/28/2010	DBA	Add @MirrorToPath parameter to accomodate multiple backup sets.
	02/24/2010	DBA	@Path parameter is no longer optional.
	02/14/2012	DBA	Enable COMPRESSION.
	07/25/2014	DBA	Ensure a FULL backup exists before attempting the trx log backup.
	04/29/2016	DBA	No COMPRESSION on EXPRESS edition.
	08/10/2016	DBA	1) Stop checking for/specifying COMPRESSION. Let the system configuration
						"backup compression default" indicate if COMPRESSION is to be used.
						2) Dynamically establish/re-establish the log chain as needed.
	11/04/2016	DBA	Add encryption options.
*/
DECLARE @Filename VARCHAR(255)

IF NOT EXISTS ( SELECT 1 FROM master.sys.databases WHERE name = @DBName )
BEGIN
	PRINT ''Database "'' + COALESCE(@DBName, '''') + ''" does not exist.''
	RETURN
END

IF @WithEncryption = 1
BEGIN
	IF COALESCE(@ServerCertificate, '''') = ''''
	BEGIN
		RAISERROR(''Backup encryption was requested, but no server certificate was specified for parameter @ServerCertificate.'', 16, 1);
        RETURN;
	END

	IF NOT EXISTS (
			SELECT *
			FROM master.sys.certificates c
			WHERE c.name = @ServerCertificate
		)
	BEGIN
		DECLARE @ErrMsg NVARCHAR(MAX) = ''Server certificate ['' + @ServerCertificate + ''] does not exist.'';
        RAISERROR(@ErrMsg, 16, 1);
        RETURN;
	END
END

--See if the LOG chain is broken (or was never established).
IF EXISTS (
	SELECT * 
	FROM sys.database_recovery_status 
	WHERE database_id = db_id(@DBName)
	AND last_log_backup_lsn IS NULL
) 
BEGIN
	/*
		Establish/Re-establish the LOG chain via a DIFFERENTIAL backup.
		(NOTE: If a FULL backup has never been created, [DbaData].[dba].[BackupDatabase_DIFF]
		will call [DbaData].[dba].[BackupDatabase_FULL] to create one.)
	*/
	EXECUTE DbaData.dba.BackupDatabase_DIFF 
		@DBName = @DBName, 
		@Path = ''' + DbaData.dba.GetInstanceConfiguration('Backup Path - DIFFERENTIAL') + ''';
END

IF RIGHT(@Path, 1) != ''\''
	SET @Path = @Path + ''\'';

SET LANGUAGE ''us_english'';

--Format for the filename is
--DBname yyyy-mm-dd_hhmiss.DayOfWeek.trn
SET @Filename = @DBName + '' '' + 
	REPLACE(CONVERT(VARCHAR, GETDATE(), 111), ''/'', ''-'') +
	''_'' + REPLACE(CONVERT(VARCHAR, GETDATE(), 108), '':'', '''') +
	''.'' + DATENAME(weekday, GETDATE()) + ''.trn''

SET @Path = @Path + @Filename
--PRINT @Path 

DECLARE @TSql NVARCHAR(MAX) = ''BACKUP LOG ['' + @DBName + '']
TO DISK = '' + CHAR(39) + @Path + CHAR(39) + '''';

IF COALESCE(@MirrorToPath, '''') <> ''''	--Backup to multiple (two) locations.
BEGIN
	IF RIGHT(@MirrorToPath, 1) != ''\''
		SET @MirrorToPath = @MirrorToPath + ''\''

	SET @MirrorToPath = @MirrorToPath + @Filename
	SET @TSql = @TSql + CHAR(13) + CHAR(10) + ''MIRROR TO DISK = '' + CHAR(39) + @MirrorToPath + CHAR(39) + '''';
END

SET @TSql = @TSql + CHAR(13) + CHAR(10) + ''WITH FORMAT, INIT'';

IF @WithEncryption = 1
BEGIN
	SET @TSql = @TSql + '', '' + CHAR(13) + CHAR(10) + ''ENCRYPTION
(
	ALGORITHM = AES_256,
	SERVER CERTIFICATE = '' + @ServerCertificate + ''
)'';
END

--PRINT @TSql
EXEC sp_executesql @TSql;

';
--PRINT @Tsql
EXEC sp_executesql @TSql;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'BackupDatabase_Archive'
)
	DROP PROCEDURE dba.BackupDatabase_Archive 
GO

CREATE PROCEDURE dba.BackupDatabase_Archive
	@DBName SYSNAME,
	@Path VARCHAR(255),
	@MirrorToPath VARCHAR(255) = NULL,
	@MaxBackupFileSize MONEY = 4,
	@WithEncryption BIT = 0,
	@ServerCertificate SYSNAME = NULL
AS 
/*
	Purpose:	
	Performs a FULL database backup of a specific database for archival purposes.
	
	Inputs:
	@DBName - self-explanatory
	@Path - the path where the backup files are to be created.
	@MirrorToPath - (optional) the path where a copy of the backup files are to be created.
	@MaxBackupFileSize - (optional) maximum size on disk (in GB) of individual db backup files.
	@WithEncryption - (optional) create backup with enctyption.
	@ServerCertificate - (optional) name of server certificate.

	History:
	07/25/2016	DBA	Created
	11/04/2016	DBA	Add encryption options.
	05/04/2017	DBA	Fix error when trying to backup to > 64 devices.

	Notes:
	Archived backups are to be created on the first day of every month.
	The January archive is to be kept for 3 years. Others for 13 months.
*/
IF NOT EXISTS ( SELECT 1 FROM master.sys.databases WHERE name = @DBName )
BEGIN
	PRINT 'Database "' + COALESCE(@DBName, '') + '" does not exist.';
	RETURN;
END

IF @WithEncryption = 1
BEGIN
	IF COALESCE(@ServerCertificate, '') = ''
	BEGIN
		RAISERROR('Backup encryption was requested, but no server certificate was specified for parameter @ServerCertificate.', 16, 1);
        RETURN;
	END

	IF NOT EXISTS (
			SELECT *
			FROM master.sys.certificates c
			WHERE c.name = @ServerCertificate
		)
	BEGIN
		DECLARE @ErrMsg NVARCHAR(MAX) = 'Server certificate [' + @ServerCertificate + '] does not exist.';
        RAISERROR(@ErrMsg, 16, 1);
        RETURN;
	END
END

SET LANGUAGE 'us_english';
EXEC DbaData.dba.PopulateEstimatedBackupSize @DBName, @UpdateUsage = 0;

CREATE TABLE #FullBackup (
	ID INT IDENTITY,
	[Path] VARCHAR(255) NOT NULL,
	MirrorToPath VARCHAR(255) NULL,
	[FileName] VARCHAR(512) NOT NULL
)

DECLARE @NumFiles INT;
DECLARE @Loop INT = 1;

SELECT @NumFiles = Size_GB / @MaxBackupFileSize
FROM DbaData.dba.EstimatedBackupSize
WHERE DbName = @DBName

IF EXISTS ( SELECT 1 FROM DbaData.dba.EstimatedBackupSize
			WHERE DbName = @DBName
			AND Size_GB / @MaxBackupFileSize > CAST(@NumFiles AS MONEY))
	SET @NumFiles = @NumFiles + 1;

--Always use a minimum of 2 files for backups.
IF @NumFiles IS NULL OR @NumFiles <= 1
	SET @NumFiles = 2;
--Up to 64 backup devices may be specified in a comma-separated list.
ELSE IF @NumFiles > 64
	SET @NumFiles = 64;

IF RIGHT(@Path, 1) != '\'
	SET @Path = @Path + '\';
IF RIGHT(@MirrorToPath, 1) != '\'
	SET @MirrorToPath = @MirrorToPath + '\';

WHILE @Loop <= @NumFiles
BEGIN
	--Format for the FileName is
	--DBname yyyy-mm-dd.DayOfWeek.ArchiveFull.FileNum.bak
	INSERT INTO #FullBackup ([Path], MirrorToPath, [FileName])
	VALUES (@Path, @MirrorToPath, @DBName + ' ' + 
		REPLACE(CONVERT(VARCHAR, GETDATE(), 111), '/', '-') + '.' + 
		DATENAME(WEEKDAY, GETDATE()) + '.ArchiveFull.' +
		CAST(@Loop AS VARCHAR) + '.bak')
	SET @Loop = @Loop + 1;
END

DECLARE @Tsql NVARCHAR(MAX) = 'BACKUP DATABASE [' + @DBName + ']' + CHAR(13) + CHAR(10) +
	'TO ';

SELECT @Tsql = @Tsql + CHAR(9) + 'DISK = ''' + [Path] + [FileName] + ''',' + CHAR(13) + CHAR(10)
FROM #FullBackup
ORDER BY ID

SELECT @Tsql = LEFT(@Tsql, LEN(@Tsql) - 3) + CHAR(13) + CHAR(10);

IF COALESCE(@MirrorToPath, '') <> ''	--Backup mirrored to 2nd location.
BEGIN
	SET @Tsql = @Tsql + 'MIRROR TO ';
	SELECT @Tsql = @Tsql + CHAR(9) + 'DISK = ''' + MirrorToPath + [FileName] + ''',' + CHAR(13) + CHAR(10)
	FROM #FullBackup
	ORDER BY ID
	SELECT @Tsql = LEFT(@Tsql, LEN(@Tsql) - 3) + CHAR(13) + CHAR(10);
END

--	VERY IMPORTANT!
--1) We want to explicitly label the backups that are for archival purposes. 
--2) COPY_ONLY so we don't interfere with the DIFFERENTIAL backup chain or LOG chain.
--3) Create uncompressed backups.  This is slower, but gives Data Domain a better chance
--		to deduplicate data (archived backups remain on disk for longer periods of time).
SET @Tsql = @Tsql + 'WITH 
	NAME = ''Archive'', COPY_ONLY, NO_COMPRESSION,
	INIT, FORMAT'

IF @WithEncryption = 1
BEGIN
	SET @Tsql = @Tsql + ', ' + CHAR(13) + CHAR(10) + '	ENCRYPTION
	(
		ALGORITHM = AES_256,
		SERVER CERTIFICATE = ' + @ServerCertificate + '
	)';
END

--PRINT @Tsql
EXEC sp_executesql @Tsql;

DROP TABLE #FullBackup;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba'
	AND r.ROUTINE_NAME = 'GetLastBackupsHtmlTable'
	AND r.ROUTINE_TYPE = 'FUNCTION'
)
	DROP FUNCTION dba.GetLastBackupsHtmlTable
GO

CREATE FUNCTION dba.GetLastBackupsHtmlTable(
	@RowsOnly BIT = 0
)
RETURNS NVARCHAR(MAX)
AS
/******************************************************************************
* Name     : GetLastBackupsHtmlTable
* Purpose  : Returns an HTML <table> string (with formatting) showing the most 
*				recent full and differential backup for every database.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
******************************************************************************
* Date:			Author:	Description:
* 05/02/2014	DBA	Created.
* 11/03/2014	DBA	Add <style> for html table decoration.
******************************************************************************/
BEGIN
	DECLARE @BackupCount INT
	DECLARE @HtmlTable NVARCHAR(MAX) 

	IF @RowsOnly = 0
		SET @HtmlTable = '<style>
				td {
					font-size: smaller;
				}
				p {
					padding: 0px;
					margin: 0px;
					margin-auto: 0px;
				}
				.ExternalClass * 
				{
					mso-line-height-rule: exactly;
					line-height: 100%;
				} 
				.nw
				{
					white-space: nowrap;
				}
				.ar
				{
					text-align: right;
				}
			</style>
			<table border="4" cellpadding="2" stle="font-size: smaller;">' + CHAR(13) + CHAR(10)
	ELSE
		SET @HtmlTable = ''

	SET @HtmlTable = @HtmlTable + '
			<tr><th colspan="9" style="background-color: darkblue; color: white;">' + @@SERVERNAME + '</th></tr>
			<tr><td></td><th colspan="4" style="background-color: wheat;">FULL Backups</th>
				<th colspan="4" style="background-color: wheat;">DIFFERENTIAL Backups</th></tr>
			<tr><th style="background-color: lightgrey;">Database Name</th>
				<th style="background-color: lightgrey;">Completion</th><th style="background-color: lightgrey;">Size-MB</th><th style="background-color: lightgrey;">Size-MB (compressed)</th><th style="background-color: lightgrey;">Files</th>
				<th style="background-color: lightgrey;">Completion</th><th style="background-color: lightgrey;">Size-MB</th><th style="background-color: lightgrey;">Size-MB (compressed)</th><th style="background-color: lightgrey;">Files</th>
			</tr>' 

	SELECT @HtmlTable = @HtmlTable + 
		'<tr><td>' + lb.DbName + '</td>' +
		--FULL backup columns
		'<td class="nw">' + COALESCE(DATENAME(dw, lb.DateCompletedFull) + ' ' + 
			CONVERT(VARCHAR, lb.DateCompletedFull, 101) + '  ' + 
			CONVERT(VARCHAR, lb.DateCompletedFull, 108), '&nbsp;') + ' </td>' +
		'<td class="ar">' + COALESCE(lb.SizeMBFull, '&nbsp;') + '</td>' +
		'<td class="ar">' + COALESCE(lb.SizeMBCompressedFull, '&nbsp;') + '</td>' +
		--'<td class="nw"><li style="display: table;">' + REPLACE(COALESCE(lb.FilesFull, '&nbsp;'), ',', '</li><li style="display: table;">') + '</li></td>' +
		'<td class="nw"><p>' + REPLACE(COALESCE(lb.FilesFull, '&nbsp;'), ',', '</p><p>') + '</p></td>' +
		
		--DIFFERENTIAL backup columns
		'<td class="nw">' + COALESCE(DATENAME(dw, lb.DateCompletedDiff) + '&nbsp;' + 
			CONVERT(VARCHAR, lb.DateCompletedDiff, 101) + '  ' + 
			CONVERT(VARCHAR, lb.DateCompletedDiff, 108), '&nbsp;') + ' </td>' +
		'<td class="ar">' + COALESCE(lb.SizeMBDiff, '&nbsp;') + '</td>' +
		'<td class="ar">' + COALESCE(lb.SizeMBCompressedDiff, '&nbsp;') + '</td>' +
		--'<td class="nw"><li style="display: table;">' + REPLACE(COALESCE(lb.FilesDiff, '&nbsp;'), ',', '</li><li style="display: table;">') + '</li></td>' +
		'<td class="nw"><p>' + REPLACE(COALESCE(lb.FilesDiff, '&nbsp;'), ',', '</p><p>') + '</p></td>' +
	
		'</tr>' + CHAR(13) + CHAR(10)
	FROM DbaData.dba.LastBackups lb
	ORDER BY lb.DbName

	SET @BackupCount = @@ROWCOUNT
	
	IF @BackupCount > 0 
	BEGIN
		IF @RowsOnly = 0
			SET @HtmlTable = @HtmlTable + '</table>'
	END
	ELSE
		SET @HtmlTable = NULL

	RETURN @HtmlTable
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' AND r.ROUTINE_NAME = 'LastBackupsHtmlTable'
)
	DROP PROCEDURE dba.LastBackupsHtmlTable
GO

CREATE PROCEDURE dba.LastBackupsHtmlTable
	@SendEmail BIT = 0
AS
/******************************************************************************
* Name     : LastBackupJobEmailNotification
* Purpose  : Sends an email showing the most recent full and differential 
*				backup for every database.
* Inputs   : 
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
******************************************************************************
* Date:			Author:	Description:
* 05/01/2014	DBA	Created.
* 11/03/2014	DBA	Reuse existing code from function 
*						dba.GetLastBackupsHtmlTable()
******************************************************************************/
DECLARE @BackupCount INT
DECLARE @HtmlTable NVARCHAR(MAX) 

SELECT @HtmlTable = dba.GetLastBackupsHtmlTable(0)
SET @BackupCount = @@ROWCOUNT
SELECT @HtmlTable

IF @BackupCount > 0 AND @SendEmail = 1
BEGIN
	DECLARE @Subject NVARCHAR(255) 
	SET @Subject = @@SERVERNAME + ' -- Last Database Backup Report'

	EXEC msdb.dbo.sp_send_dbmail
		@recipients = 'DBA@Domain.com',
		@profile_name = 'DBA',
		@subject = @Subject,
		@body = @HtmlTable,
		@body_format = 'HTML'
END
GO

