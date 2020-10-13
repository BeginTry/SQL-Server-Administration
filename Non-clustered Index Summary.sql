/*
	Iterate over each table in each database gathering summary 
	information about non-clustered indexes:
		• Count of indexes
		• Count of indexed columns (key columns and include columns)
		• Count of indexed key columns
		• Count of indexed include columns

	The above information can sometimes help identify tables that are "over-indexed",
	indexes that can be consolidated, indexes that might be removed in favor of a
	non-clustered columnstore index, etc.
*/

IF EXISTS (
	SELECT *
	FROM tempdb.INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'guest'
	AND t.TABLE_NAME = 'NonClusteredIndexSummary'
)
BEGIN
	EXEC('DROP TABLE tempdb.guest.NonClusteredIndexSummary;');
END

--Create table in tempdb.
CREATE TABLE tempdb.guest.NonClusteredIndexSummary (
	NonClusteredIndexSummaryID INT IDENTITY 
		CONSTRAINT PK_NonClusteredIndexSummary PRIMARY KEY,
	DBName NVARCHAR(128),
	SchemaName NVARCHAR(128),
	TableName NVARCHAR(128),
	TableColumns INT,
	IndexCount INT,
	TotalIndexedColumns INT,
	TotalKeyColumns INT,
	TotalIncludedColumns INT
);


--Run once per database.
DECLARE @TSql NVARCHAR(MAX) = '';
DECLARE @DBName SYSNAME;
DECLARE curDB CURSOR FAST_FORWARD READ_ONLY FOR 
	SELECT name 
	FROM master.sys.databases d
	WHERE d.database_id > 4
	AND d.source_database_id IS NULL	--Exclude snapshots
	AND d.state_desc = 'ONLINE'
	AND d.name NOT IN ('')				--SSRS databases, etc.
	ORDER BY d.name;

OPEN curDB;
FETCH NEXT FROM curDB INTO @DBName;

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @TSql = '
	USE ' + QUOTENAME(@DBName) + ';

	--Run once per database.
	INSERT INTO tempdb.guest.NonClusteredIndexSummary(DBName, SchemaName, TableName, TableColumns, IndexCount, TotalIndexedColumns, TotalKeyColumns, TotalIncludedColumns)
	SELECT 
		DB_NAME() AS DBName,
		sch.name AS SchemaName,
		t.name AS TableName,
		COUNT(DISTINCT c.name) TableColumns,
		COUNT(DISTINCT i.name) AS IndexCount,
		COUNT(ic.column_id) TotalIndexedColumns,
		SUM(CAST(~ic.is_included_column AS INT)) AS TotalKeyColumns,
		SUM(CAST(ic.is_included_column AS INT)) AS TotalIncludedColumns
	FROM sys.schemas AS sch 
	JOIN sys.tables AS t 
		ON t.schema_id = sch.schema_id
	JOIN sys.columns AS c 
		ON c.object_id = t.object_id
	JOIN sys.indexes AS i
		ON i.object_id = t.object_id
	LEFT JOIN sys.index_columns AS ic 
		ON ic.object_id = i.object_id
		AND ic.index_id = i.index_id
		AND ic.column_id = c.column_id
	WHERE i.type = 2
	GROUP BY sch.name, t.name
	ORDER BY sch.name, t.name
	OPTION(MAXDOP 4)'
	EXEC (@Tsql);

	FETCH NEXT FROM curDB INTO @DBName;
END

CLOSE curDB;
DEALLOCATE curDB;


SELECT f.*
FROM tempdb.guest.NonClusteredIndexSummary f
