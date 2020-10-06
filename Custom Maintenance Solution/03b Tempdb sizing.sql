/*
	Generatate and PRINT dynamic tsql for [tempdb]:
		Add more data files (as needed)
		Adjust AutoGrowth factor for data files (as needed)
*/
--Max of 4 data files (for now)
DECLARE @MaxDataFiles SMALLINT
SET @MaxDataFiles = 4

--The minimum size of all data files combined is to be 256MB.
DECLARE @MinDataFilesSize_MB SMALLINT
SET @MinDataFilesSize_MB = 256

DECLARE @CurDataFileCount SMALLINT

SELECT @CurDataFileCount = COUNT(*)
FROM tempdb.sys.database_files f
WHERE f.type_desc = 'ROWS'

DECLARE @ProposedDataFileCount SMALLINT
DECLARE @AutoGrowth_MB SMALLINT
DECLARE @ProposedDataFileSize_MB SMALLINT

--Number of [tempdb] data files: 1 per processor, up to a specified max.
--AutoGrowth size: min size of [tempdb] data files / number of data files.
SELECT 
	@ProposedDataFileCount = MIN(dt.cpu_count), 
	@AutoGrowth_MB = @MinDataFilesSize_MB / MIN(dt.cpu_count) 
FROM (
	SELECT i.cpu_count
	FROM sys.dm_os_sys_info i
	UNION ALL
	SELECT @MaxDataFiles
) dt

SELECT 
	@ProposedDataFileSize_MB = MAX(ProposedTempDbSize_MB) / @ProposedDataFileCount
FROM (
	--Estimate the size of [tempdb] data files at startup.
	SELECT MAX(size)  * 8 / 1024 * @CurDataFileCount ProposedTempDbSize_MB
	FROM master.sys.master_files f
	WHERE database_id IN ( SELECT database_id FROM master.sys.databases d WHERE d.name IN ('model', 'tempdb') )
	AND type_desc = 'ROWS'
	UNION ALL
	SELECT @MinDataFilesSize_MB
) dt

--Tweak existing [tempdb] data file(s).
DECLARE @Tsql NVARCHAR(MAX)
SET @Tsql = ''
SELECT @Tsql = @Tsql + 
'
ALTER DATABASE [tempdb] 
MODIFY FILE ( 
	NAME = N''' + name + '''' + 

	CASE 
		WHEN (f.size * 8 / 1024) < @ProposedDataFileSize_MB THEN ',
	SIZE = ' + CAST(@ProposedDataFileSize_MB AS VARCHAR) + 'MB'
		ELSE ''
	END + 

	CASE
		WHEN (f.growth * 8 / 1024) < @AutoGrowth_MB OR f.is_percent_growth = 1 THEN ', 
	FILEGROWTH = ' + CAST(@AutoGrowth_MB AS VARCHAR) + 'MB '
		ELSE ''
	END + '
)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
FROM tempdb.sys.database_files f
WHERE f.type_desc = 'ROWS'
AND (
	(f.size * 8 / 1024) < @ProposedDataFileSize_MB OR
	(f.growth * 8 / 1024) < @AutoGrowth_MB OR 
	f.is_percent_growth = 1
)

PRINT @Tsql

--Add new [tempdb] data file(s)--as needed.
WHILE @ProposedDataFileCount > @CurDataFileCount
BEGIN
	DECLARE @Path NVARCHAR(MAX)
	SET @Tsql = ''
	
	SELECT TOP(1) @Path = LEFT(physical_name, LEN(physical_name) - CHARINDEX('\', REVERSE(physical_name), 0) + 1)
	FROM tempdb.sys.database_files f
	WHERE f.type_desc = 'ROWS'

	SET @Tsql = '
ALTER DATABASE [tempdb] 
ADD FILE ( 
	NAME = N''tempdev' + CAST((@CurDataFileCount + 1) AS VARCHAR) + ''', 
	FILENAME = N''' + @Path + 'tempdb' + CAST((@CurDataFileCount + 1) AS VARCHAR) + '.ndf'', 
	SIZE = ' + CAST(@ProposedDataFileSize_MB AS VARCHAR) + 'MB, 
	FILEGROWTH = ' + CAST(@AutoGrowth_MB AS VARCHAR) + 'MB 
)
'
	PRINT @Tsql
	PRINT ''
	SET @CurDataFileCount = @CurDataFileCount + 1
END

SET @Tsql = ''

--Tweak existing [tempdb] log file.
SELECT @Tsql = @Tsql + 
'
ALTER DATABASE [tempdb] 
MODIFY FILE ( 
	NAME = N''' + name + '''' + 

	CASE 
		WHEN (f.size * 8 / 1024) < @MinDataFilesSize_MB THEN ',
	SIZE = ' + CAST(@MinDataFilesSize_MB AS VARCHAR) + 'MB'
		ELSE ''
	END + 

	CASE
		WHEN (f.growth * 8 / 1024) < @MinDataFilesSize_MB OR f.is_percent_growth = 1 THEN ', 
	FILEGROWTH = ' + CAST(@MinDataFilesSize_MB AS VARCHAR) + 'MB '
		ELSE ''
	END + '
)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
FROM tempdb.sys.database_files f
WHERE f.type_desc = 'LOG'
AND (
	(f.size * 8 / 1024) < @MinDataFilesSize_MB OR
	(f.growth * 8 / 1024) < @MinDataFilesSize_MB OR 
	f.is_percent_growth = 1
)

PRINT @Tsql
