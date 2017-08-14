USE master
GO

IF NOT EXISTS ( SELECT * FROM master.sys.databases d WHERE d.name = 'DbaData' )
BEGIN
	DECLARE @Tsql NVARCHAR(MAX)
	DECLARE @DataPath NVARCHAR(260)
	DECLARE @LogPath NVARCHAR(260)

	SELECT TOP(1) @DataPath = LEFT(f.physical_name, LEN(f.physical_name) - CHARINDEX('\', REVERSE(f.physical_name), 1) + 1)
	FROM master.sys.databases d
	JOIN master.sys.master_files f
		ON f.database_id = d.database_id
	WHERE d.name NOT IN ('master', 'model', 'msdb', 'tempdb')
	AND f.type_desc = 'ROWS'

	SELECT TOP(1) @LogPath = LEFT(f.physical_name, LEN(f.physical_name) - CHARINDEX('\', REVERSE(f.physical_name), 1) + 1)
	FROM master.sys.databases d
	JOIN master.sys.master_files f
		ON f.database_id = d.database_id
	WHERE d.name NOT IN ('master', 'model', 'msdb', 'tempdb')
	AND f.type_desc = 'LOG'

	SET @Tsql = '
	CREATE DATABASE DbaData
	ON 
	( NAME = DbaData,
		FILENAME = ''' + @DataPath + 'DbaData.mdf'',
		SIZE = 16MB,
		FILEGROWTH = 16MB )
	LOG ON
	( NAME = DbaData_log,
		FILENAME = ''' + @LogPath + 'DbaData.ldf'',
		SIZE = 32MB,
		FILEGROWTH = 32MB )'

	--If there are no user db's yet, @DataPath and @LogPath will be NULL. 
	--Have faith and assume files will be created in an appropriate path.
	SET @Tsql = COALESCE(@Tsql, 'CREATE DATABASE DbaData')
	PRINT @Tsql
	EXEC sp_executesql @Tsql

	ALTER DATABASE DbaData
	SET ENABLE_BROKER;

	ALTER DATABASE DbaData
	SET TRUSTWORTHY ON;

	ALTER DATABASE DbaData
	SET RECOVERY SIMPLE
	WITH NO_WAIT
END
GO

USE DbaData
GO

--TODO:  edit scripts to create objects in database [DbaData].
--TODO:  move existing objects from [master] to [DbaData], update references accordingly.

IF NOT EXISTS (
	SELECT *
	FROM sys.schemas
	WHERE name = 'dba'
)
	EXEC ('CREATE SCHEMA dba AUTHORIZATION dbo')
GO

IF NOT EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba' AND t.TABLE_NAME = 'EstimatedBackupSize' 
)
	CREATE TABLE dba.EstimatedBackupSize(
		DbName SYSNAME NOT NULL,
		Size_GB MONEY NULL,
		CompressedSize_GB MONEY NULL
	) 
GO

IF NOT EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba' AND t.TABLE_NAME = 'IndexRebuildHistory' 
)
	CREATE TABLE dba.IndexRebuildHistory(
		IndexRebuildHistoryId INT IDENTITY NOT NULL
			CONSTRAINT PK_IndexRebuildHistory PRIMARY KEY CLUSTERED,
		DatabaseName SYSNAME NOT NULL,
		SchemaName SYSNAME NOT NULL,
		TableName SYSNAME NOT NULL,
		IndexName SYSNAME NOT NULL,
		FragmentationPct FLOAT NULL,
		PageCount BIGINT NULL,
		RebuildDate DATETIME NULL 
			CONSTRAINT DF_IndexRebuildHistory_RebuildDate DEFAULT (GETDATE())
	)
GO

IF NOT EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba' AND t.TABLE_NAME = 'FixedDrive' 
)
	CREATE TABLE dba.FixedDrive(
		FixedDriveId INT IDENTITY NOT NULL
			CONSTRAINT PK_FixedDrive PRIMARY KEY CLUSTERED,
		Drive SYSNAME NOT NULL
			CONSTRAINT UQ_FixedDrive_Drive UNIQUE,
		LastAmountFree_MB INT NOT NULL,
		LastAlert DATETIME NOT NULL
			CONSTRAINT DF_FixedDrive_LastAlert DEFAULT(0)
	)
GO

IF NOT EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba' AND t.TABLE_NAME = 'EventNotification' 
)
	CREATE TABLE dba.EventNotification(
		EventNotificationId INT IDENTITY(1,1) NOT NULL
			CONSTRAINT PK_EventNotification PRIMARY KEY CLUSTERED,
		EventName VARCHAR(64) NOT NULL,
		EventData XML NOT NULL,
		EventDate DATETIME NOT NULL 
			CONSTRAINT DF_EventNotification_EventDate DEFAULT (CURRENT_TIMESTAMP)
	) 
GO

IF NOT EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba' AND t.TABLE_NAME = 'PageReadHistory' 
)
	CREATE TABLE dba.PageReadHistory (
		SampleDate SMALLDATETIME NOT NULL
			CONSTRAINT PK_PageReadHistory PRIMARY KEY CLUSTERED
			CONSTRAINT DF_PageReadHistory_SampleDate DEFAULT(CURRENT_TIMESTAMP),
		SecondsSinceStartup INT NOT NULL,
		PagesReadSinceStartup BIGINT NOT NULL
	)
GO