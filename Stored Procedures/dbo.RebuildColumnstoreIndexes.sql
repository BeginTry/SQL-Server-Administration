CREATE PROCEDURE dbo.RebuildColumnstoreIndexes
	@CompressedRowGroupDeletedRowCountThreshold BIGINT = 5000,
	@CompressedRowGroupDeletedRowPercentThreshold BIGINT = 2
/******************************************************************************
* Name     : dbo.RebuildColumnstoreIndexes
* Purpose  : Performs index rebuild operations on column store indexes.
* Inputs   : @CompressedRowGroupDeletedRowCountThreshold - if any compressed
*	rowgroup has X or more deleted rows, the index will be rebuilt.
*			 @CompressedRowGroupDeletedRowPercentThreshold - if any compressed
*	rowgroup has X% or more deleted rows, the index will be rebuilt.
* Outputs  : Nothing
* Returns  : Nothing
* Notes	   : Intended for SQL Server 2016 (13.x) or later.
******************************************************************************
* Change History
*	01/30/2020	DMason	Created.
******************************************************************************/
AS
DROP TABLE IF EXISTS #CSI;

CREATE TABLE #CSI (
	DatabaseName SYSNAME NOT NULL,
	TableSchema SYSNAME NOT NULL,
	TableName SYSNAME NOT NULL,
	IndexName SYSNAME NOT NULL,
	IndexType SYSNAME,
	state_description SYSNAME,
	row_group_count INT,
	total_rows BIGINT,
	deleted_rows BIGINT
)

DECLARE @Cmd VARCHAR(2000) =
'USE [?];

INSERT INTO #CSI
SELECT 
	DB_NAME(),
	s.name, 
	o.name, 
	i.name,
	i.type_desc, 
	rg.state_description,
	COUNT(*),
	SUM(rg.total_rows),
	SUM(rg.deleted_rows)
FROM sys.objects o
JOIN sys.schemas s
	ON s.schema_id = o.schema_id
JOIN sys.indexes i
	ON i.object_id = o.object_id
JOIN sys.column_store_row_groups rg
	ON rg.object_id = o.object_id
	AND rg.index_id = i.index_id
WHERE i.type_desc LIKE ''%COLUMNSTORE%''
GROUP BY s.name, o.name, 
	i.name, i.type_desc, rg.state_description';

EXEC sp_MSforeachdb @Cmd;
/*
	SELECT * FROM #CSI;
	SELECT * FROM #CSI WHERE deleted_rows > 0;
	SELECT * FROM #CSI WHERE state_description = 'OPEN';
*/

DECLARE @AlterIdxCmd NVARCHAR(MAX);
DECLARE curCI CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT DISTINCT 'ALTER INDEX ' + QUOTENAME(IndexName) + ' ON ' + QUOTENAME(DatabaseName) + '.' + QUOTENAME(TableSchema) + '.' + QUOTENAME(TableName) + ' REBUILD' AS AlterIdxCmd
	FROM #CSI
	WHERE state_description = 'COMPRESSED'
	AND (
		--Any one COMPRESSED row group with more than X deleted rows.
		deleted_rows >= @CompressedRowGroupDeletedRowCountThreshold
		OR 
		--Any one COMPRESSED row group with X% or more deleted rows.
		deleted_rows*100/total_rows >= @CompressedRowGroupDeletedRowPercentThreshold
	)

OPEN curCI;
FETCH NEXT FROM curCI INTO @AlterIdxCmd;

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @AlterIdxCmd;

	BEGIN TRY
		EXEC (@AlterIdxCmd + ' WITH(ONLINE = ON);');
	END TRY
	BEGIN CATCH
		PRINT ERROR_MESSAGE();
		EXEC (@AlterIdxCmd);
	END CATCH

	FETCH NEXT FROM curCI INTO @AlterIdxCmd;
END

CLOSE curCI;
DEALLOCATE curCI;
