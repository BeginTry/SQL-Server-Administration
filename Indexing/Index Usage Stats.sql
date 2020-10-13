/*
	Iterate over each database and write index usage statistics meta data for each table to a single table in [tempdb].
	Output is primarily from sys.dm_db_index_Usage_stats()
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
	LobPages BIGINT
);

DECLARE @TSql VARCHAR(1000) = 'USE [?]; 

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
	ps.lob_used_page_count
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

EXEC sp_MSforeachdb @TSql

SELECT *
FROM tempdb.guest.IndexUsageStats i
