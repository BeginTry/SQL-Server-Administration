/*
	Iterate over each database and gather index usage statistics for all user tables.
	Output is primarily from sys.dm_db_index_Usage_stats(),
	data is persisted to a "permanent" table in [tempdb].
*/
IF EXISTS (
	SELECT *
	FROM tempdb.INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'guest'
	AND t.TABLE_NAME = 'IndexUsageStats'
)
BEGIN
	EXEC('DROP TABLE tempdb.guest.IndexUsageStats;');
END

--Create table in tempdb.
CREATE TABLE tempdb.guest.IndexUsageStats (
	IndexUsageStatsID INT IDENTITY 
		CONSTRAINT PK_IndexUsageStats PRIMARY KEY,
	DBName NVARCHAR(128),
	SchemaName NVARCHAR(128),
	TableName NVARCHAR(128),
	IndexName NVARCHAR(128),
	index_id INT,
	IndexType NVARCHAR(128),
	PartitionNumber INT,
	UserSeeks BIGINT,
    UserScans BIGINT,
    UserLookups BIGINT,
    UserUpdates BIGINT,
	CompressionLevel NVARCHAR(128),
	TotalRows BIGINT,
	IndexSize_MB BIGINT,
	InRowPages BIGINT,
	RowOverflowPages BIGINT,
	LobPages BIGINT,
	StatsDate DATETIME
);

/*
	Iterate through databases and write index meta data to a single table.
	Output is primarily from sys.dm_db_index_Usage_stats()
*/
TRUNCATE TABLE tempdb.guest.IndexUsageStats;

DECLARE @TSql VARCHAR(2000) = 'USE [?]; 

IF DB_NAME() IN (''master'', ''model'', ''msdb'', ''tempdb'')
	RETURN;

INSERT INTO tempdb.guest.IndexUsageStats
SELECT 
	DB_NAME(),
	SCHEMA_NAME(o.schema_id),
	o.name,
	COALESCE(i.name, ''''),
	i.index_id,
	i.type_desc,
	p.partition_number,
	ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates,
	p.data_compression_desc,
	p.rows AS TotalRows,
	ps.used_page_count * 8 / 1024,
	ps.in_row_used_page_count,
	ps.row_overflow_used_page_count,
	ps.lob_used_page_count,
	CURRENT_TIMESTAMP
FROM sys.dm_db_partition_stats ps
JOIN sys.partitions p 
	ON ps.partition_id = p.partition_id
JOIN sys.indexes i 
	ON p.index_id = i.index_id 
	AND p.object_id = i.object_id
JOIN sys.objects o 
	ON o.object_id= i.object_id
LEFT JOIN sys.dm_db_index_usage_stats AS ius ON 
    ius.database_id = DB_ID() 
    and i.object_id = ius.object_id 
    and i.index_id = ius.index_id
WHERE o.type = ''U''
AND p.rows > 0
ORDER BY o.name, i.index_id';

EXEC sp_MSforeachdb @TSql;

/***********************************************************************/
DECLARE @Last DATETIME;
SELECT @Last = sqlserver_start_time FROM sys.dm_os_sys_info;

SELECT *,
	i.UserSeeks/ca.SecondsSinceStartup AS SeeksPerSec,
	i.UserSeeks/(ca.SecondsSinceStartup/60) AS SeeksPerMin,
	i.UserSeeks/(ca.SecondsSinceStartup/60/60) AS SeeksPerHour,
	i.UserScans/ca.SecondsSinceStartup AS ScansPerSec,
	i.UserScans/(ca.SecondsSinceStartup/60) AS ScansPerMin,
	i.UserScans/(ca.SecondsSinceStartup/60/60) AS ScansPerHour,
	'USE ' + QUOTENAME(i.DBName) + '; EXEC sp_estimate_data_compression_savings ''' + i.SchemaName + ''', ''' + i.TableName + ''', ' + 
		CAST(i.index_id AS VARCHAR)+ ', NULL, ''ROW'';' AS EstimateCompressionCmd,
	'USE ' + QUOTENAME(i.DBName) + '; ALTER ' +
		CASE 
			WHEN i.index_id = 0 THEN 'TABLE '
			ELSE 'INDEX ' + QUOTENAME(i.IndexName) + ' ON ' 
		END  + QUOTENAME(i.SchemaName) + '.' + QUOTENAME(i.TableName) + 
		' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ROW);' AS EnableCompressionCmd
FROM tempdb.guest.IndexUsageStats i
CROSS APPLY ( SELECT CAST(DATEDIFF(SECOND, @Last, i.StatsDate)AS NUMERIC(30, 10)) AS SecondsSinceStartup) AS ca;
