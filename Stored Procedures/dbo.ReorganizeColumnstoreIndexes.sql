CREATE OR ALTER PROCEDURE dbo.ReorganizeColumnstoreIndexes
	@CompressAllRowGroups BIT = 1
/******************************************************************************
* Name     : dbo.ReorganizeColumnstoreIndexes
* Purpose  : Performs index reorganize operations on column store indexes.
* Inputs   : None
* Outputs  : Nothing
* Returns  : Nothing
* Notes	   : Intended for SQL Server 2016 (13.x) or later.
******************************************************************************
* Change History
*	07/29/2020	DMason	Created.
*	11/03/2020	DMason	If REORGANIZE fails with error/messageId 35379, try
*		again with REBUILD. I have observed the following error, but have
*		not yet found a resolution:
*	Internal error occurred while flushing delete buffer database id <x>, 
*		table id <y>, index id <z>, partition number 1. 
*	Additional messages in the SQL Server error log may provide more details.
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

DECLARE @ReorgIdxCmd NVARCHAR(MAX);
DECLARE @RebuildIdxCmd NVARCHAR(MAX);
DECLARE curCI CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT DISTINCT 
		'ALTER INDEX ' + QUOTENAME(IndexName) + ' ON ' + QUOTENAME(DatabaseName) + '.' + QUOTENAME(TableSchema) + '.' + QUOTENAME(TableName) + 
		' REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ' + CASE WHEN @CompressAllRowGroups = 1 THEN 'ON' ELSE 'OFF' END + ');' AS ReorgIdxCmd,
		'ALTER INDEX ' + QUOTENAME(IndexName) + ' ON ' + QUOTENAME(DatabaseName) + '.' + QUOTENAME(TableSchema) + '.' + QUOTENAME(TableName) + 
		' REBUILD;' AS RebuildIdxCmd
	FROM #CSI

OPEN curCI;
FETCH NEXT FROM curCI INTO @ReorgIdxCmd, @RebuildIdxCmd;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		EXEC (@ReorgIdxCmd);
	END TRY
	BEGIN CATCH
		PRINT ERROR_MESSAGE();
		PRINT @ReorgIdxCmd;

		IF ERROR_NUMBER() = 35379
		BEGIN
			--Attempt a REBUILD instead
			BEGIN TRY
				EXEC (@RebuildIdxCmd);
			END TRY
			BEGIN CATCH
				PRINT ERROR_MESSAGE();
				PRINT @RebuildIdxCmd;
			END CATCH
		END
	END CATCH

	FETCH NEXT FROM curCI INTO @ReorgIdxCmd, @RebuildIdxCmd;
END

CLOSE curCI;
DEALLOCATE curCI;
GO
