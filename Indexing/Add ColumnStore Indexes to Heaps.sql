/*
	This script adds a CLUSTERED COLUMNSTORE index to the first 10 heaps found in the database (with 200,000 rows).
*/

USE YourDatabase;

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @TSql NVARCHAR(MAX) = '';
SELECT TOP(15) @TSql = @TSql + 
	'CREATE CLUSTERED COLUMNSTORE INDEX [Idx_cci_' + t.name + '] ON ' + QUOTENAME(DB_NAME()) + '.' + QUOTENAME(sc.name) + '.[' + t.name + '];' + 
	CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) 
FROM sys.indexes AS si
JOIN sys.tables AS t ON si.object_id=t.object_id
JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id
LEFT JOIN sys.dm_db_index_usage_stats AS stat ON 
	stat.database_id = DB_ID() 
	and si.object_id=stat.object_id 
	and si.index_id=stat.index_id
LEFT JOIN sys.partition_schemes AS psc ON si.data_space_id=psc.data_space_id
LEFT JOIN sys.partition_functions AS pf ON psc.function_id=pf.function_id
LEFT JOIN sys.filegroups AS fg ON si.data_space_id=fg.data_space_id
/* Partitions */ OUTER APPLY ( 
	SELECT 
		COUNT(*) AS partition_count,
		CAST(SUM(ps.in_row_reserved_page_count)*8./1024./1024. AS NUMERIC(32,2)) AS reserved_in_row_GB,
		CAST(SUM(ps.lob_reserved_page_count)*8./1024./1024. AS NUMERIC(32,2)) AS reserved_LOB_GB,
		SUM(ps.row_count) AS row_count
	FROM sys.partitions AS p
	JOIN sys.dm_db_partition_stats AS ps ON
		p.partition_id=ps.partition_id
	WHERE p.object_id = si.object_id
		and p.index_id=si.index_id
	) AS partition_sums
WHERE si.index_id = 0 /* heaps */
AND partition_sums.row_count >= 200000
ORDER BY NEWID()
OPTION (RECOMPILE);

PRINT @TSql;
EXEC (@Tsql);
GO
