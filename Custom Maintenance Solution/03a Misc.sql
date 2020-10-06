USE [master]
GO

--Create backup folders
DECLARE @BakPath VARCHAR(512);
SET @BakPath = DbaData.dba.GetInstanceConfiguration('Backup Path - FULL');
EXEC master.dbo.xp_create_subdir @BakPath;
SET @BakPath = DbaData.dba.GetInstanceConfiguration('Backup Path - DIFFERENTIAL');
EXEC master.dbo.xp_create_subdir @BakPath;
SET @BakPath = DbaData.dba.GetInstanceConfiguration('Backup Path - LOG');
EXEC master.dbo.xp_create_subdir @BakPath;
SET @BakPath = DbaData.dba.GetInstanceConfiguration('Backup Path - Archive');
EXEC master.dbo.xp_create_subdir @BakPath;
GO

--10 error logs total:  current log, plus 9 archived logs.
EXEC xp_instance_regwrite 
	N'HKEY_LOCAL_MACHINE', 
	N'Software\Microsoft\MSSQLServer\MSSQLServer', 
	N'NumErrorLogs', 
	REG_DWORD, 
	9
GO

--Enable advanced options.
IF NOT EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'show advanced options'
	AND value = 1
)
BEGIN
	EXEC sys.sp_configure 'show advanced options', 1;
	RECONFIGURE WITH OVERRIDE
END
GO


--Default Index Fill-Factor set to 0
IF NOT EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'fill factor (%)'
	AND value = 0
)
BEGIN
	EXEC sys.sp_configure N'fill factor (%)', N'0'
	RECONFIGURE WITH OVERRIDE
END
GO

--Set server MAXDOP (anything but 1)
IF EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'max degree of parallelism'
	AND value = 1
)
BEGIN
	EXEC sys.sp_configure N'max degree of parallelism', N'0'
	RECONFIGURE WITH OVERRIDE
END
GO

--In lieu of setting MAXDOP to a low number.
IF EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'cost threshold for parallelism'
	AND CAST(value AS INTEGER) < 25
)
BEGIN
	EXEC sys.sp_configure N'cost threshold for parallelism', N'25'
	RECONFIGURE WITH OVERRIDE
END
GO

--Set Optimize for Ad-Hoc Workloads to True for InstanceName environments.
IF (@@SERVERNAME LIKE '%InstanceName' OR @@SERVERNAME LIKE '%\OS')
AND EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'optimize for ad hoc workloads'
	AND value = 0
)
BEGIN
	EXEC sys.sp_configure N'optimize for ad hoc workloads', N'1'
	RECONFIGURE WITH OVERRIDE
END
GO

--Dedicated Admin Connection
IF EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'remote admin connections'
	AND value = 0
)
BEGIN
	EXEC sp_configure 'remote admin connections', 1
	RECONFIGURE WITH OVERRIDE
END
GO

--Backup compression "on" by default (where supported)
--Backup compression (for SQL 2008 R2 and later).
IF EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'backup compression default'
	AND (value = 0 OR value IS NULL)
)
BEGIN
	EXEC sys.sp_configure N'backup compression default', N'1'
	RECONFIGURE WITH OVERRIDE
END
GO

--Alter system db files
ALTER DATABASE [master] MODIFY FILE ( NAME = N'master', FILEGROWTH = 16MB )
GO
ALTER DATABASE [master] MODIFY FILE ( NAME = N'mastlog', FILEGROWTH = 16MB )
GO
ALTER DATABASE [msdb] MODIFY FILE ( NAME = N'MSDBData', FILEGROWTH = 32MB )
GO
ALTER DATABASE [msdb] MODIFY FILE ( NAME = N'MSDBLog', FILEGROWTH = 32MB )
GO

/*
	Commands for [tempdb] are in a separate script.
*/
--ALTER DATABASE [tempdb] MODIFY FILE ( NAME = N'tempdev', FILEGROWTH = 50MB )
--GO
--ALTER DATABASE [tempdb] MODIFY FILE ( NAME = N'templog', FILEGROWTH = 100MB )
--GO

ALTER DATABASE [model] MODIFY FILE ( NAME = N'modeldev', FILEGROWTH = 32MB )
GO
ALTER DATABASE [model] MODIFY FILE ( NAME = N'modellog', FILEGROWTH = 64MB )
GO

--Set page verify to checksum.
DECLARE @TSql NVARCHAR(MAX)
SET @TSql = ''
SELECT @TSql = @TSql + 'ALTER DATABASE [' + d.name + '] SET PAGE_VERIFY CHECKSUM  WITH NO_WAIT;'
FROM master.sys.databases d
WHERE d.page_verify_option_desc <> 'CHECKSUM' --AND d.name NOT IN ('tempdb')
ORDER BY d.name

IF @@ROWCOUNT > 0
	PRINT @TSql
	EXEC (@TSql)
GO

--Set databases to full recovery model.
DECLARE @TSql NVARCHAR(MAX)
SET @TSql = ''
SELECT @TSql = @TSql + 'ALTER DATABASE [' + d.name + '] SET RECOVERY FULL WITH NO_WAIT;'
FROM master.sys.databases d
WHERE d.recovery_model_desc = 'SIMPLE'
AND d.name NOT IN ('master', 'tempdb','msdb','DbaData')
AND d.name NOT LIKE 'ReportServer%TempDB'
ORDER BY d.name

IF @@ROWCOUNT > 0
	PRINT ''
	PRINT 'Copy/Paste/Execute as needed:'
	PRINT @TSql
	--EXEC (@TSql)
GO
