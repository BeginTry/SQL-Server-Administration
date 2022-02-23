CREATE OR ALTER PROCEDURE dbo.SetDataCompressionByTable
	@DBName SYSNAME,
	@SchemaName SYSNAME,
	@TableName SYSNAME,
	@CompressionTypeFrom VARCHAR(16),
	@CompressionTypeTo VARCHAR(16),
	@OutputOnly BIT = 0
/******************************************************************************
* Name     : dbo.SetDataCompressionByTable
* Purpose  : Changes data compression on a table and all its nonclustered 
*				indexes from one type to another.
* Inputs   : @DBName - self-explanatory
*			@SchemaName and @TableName - self-explanatory
*			@CompressionTypeFrom and @CompressionTypeTo - 'ROW' or 'PAGE' or 'NONE'
*			@OutputOnly - returns DDL commands without running them.
* Outputs  : 
* Returns  : 
* Notes    : It is assumed partitioning is not in use. COLUMNSTORE indexes
*				are ignored.
******************************************************************************
* Change History
*	02/23/2022	DMason	Created.
******************************************************************************/
AS

IF @CompressionTypeFrom NOT IN ('ROW', 'PAGE', 'NONE')
BEGIN
	RAISERROR('PROCEDURE dbo.SetDataCompressionByTable: @CompressionTypeFrom must be IN (''ROW'', ''PAGE'', ''NONE'')', 16, 1);
	RETURN;
END

IF @CompressionTypeTo NOT IN ('ROW', 'PAGE', 'NONE')
BEGIN
	RAISERROR('PROCEDURE dbo.SetDataCompressionByTable: @CompressionTypeTo must be IN (''ROW'', ''PAGE'', ''NONE'')', 16, 1);
	RETURN;
END

IF @CompressionTypeFrom = @CompressionTypeTo
BEGIN
	RAISERROR('PROCEDURE dbo.SetDataCompressionByTable: @CompressionTypeFrom and @CompressionTypeTo are equal. No action taken.', 10, 1);
	RETURN;
END

DECLARE @Cmd NVARCHAR(MAX) = '';
DROP TABLE IF EXISTS #Objects;
CREATE TABLE #Objects (
	object_id INT,
	index_name NVARCHAR(128),
	index_id INT
)

SET @Cmd = 'USE ' + QUOTENAME(@DBName) + ';
INSERT INTO #Objects
SELECT 
	i.object_id, 
	i.name index_name, 
	i.index_id
FROM sys.objects o
JOIN sys.indexes i
	ON i.object_id = o.object_id
JOIN sys.partitions AS p
    ON p.object_id = i.object_id
	and p.index_id = i.index_id
WHERE SCHEMA_NAME(o.schema_id) = ''' + @SchemaName + '''
AND o.name = ''' + @TableName + '''
AND p.data_compression_desc = ''' + @CompressionTypeFrom + ''';';
EXEC (@Cmd);

DECLARE @EngineEdition INT = CAST(SERVERPROPERTY('EngineEdition') AS INT);
DECLARE @OnlineOption VARCHAR(4) = 'OFF';

IF @EngineEdition = 3	--3 = Enterprise (This is returned for Evaluation, Developer, and Enterprise editions.)
	SET @OnlineOption = 'ON';

IF @OutputOnly = 0
BEGIN
	DECLARE curObj CURSOR READ_ONLY FAST_FORWARD FOR
	SELECT 'USE [' + @DBName + ']; ' +
			CASE WHEN o.index_id = 0 THEN 'ALTER TABLE ' ELSE 'ALTER INDEX [' + o.index_name + '] ON ' END + 
			'[' + OBJECT_SCHEMA_NAME(o.object_id, DB_ID(@DBName)) + '].[' + OBJECT_NAME(o.object_id, DB_ID(@DBName)) + '] ' +
			'REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ' + @CompressionTypeTo + ', ' +
			'ONLINE = ' + @OnlineOption + ');' AS compression_cmd
	FROM #Objects o
	ORDER BY NEWID();

	OPEN curObj;
	FETCH NEXT FROM curObj INTO @Cmd;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			EXEC(@Cmd);
		END TRY
		BEGIN CATCH
		END CATCH

		FETCH NEXT FROM curObj INTO @Cmd;
	END

	CLOSE curObj;
	DEALLOCATE curObj;
END
ELSE
BEGIN
	SELECT 
		OBJECT_SCHEMA_NAME(o.object_id, DB_ID(@DBName)) schema_name, 
		OBJECT_NAME(o.object_id, DB_ID(@DBName)) obj_name, o.index_name, 
		o.index_id,
		'USE [' + @DBName + ']; ' +
			CASE WHEN o.index_id = 0 THEN 'ALTER TABLE ' ELSE 'ALTER INDEX [' + o.index_name + '] ON ' END + 
			'[' + OBJECT_SCHEMA_NAME(o.object_id, DB_ID(@DBName)) + '].[' + OBJECT_NAME(o.object_id, DB_ID(@DBName)) + '] ' +
			'REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ' + @CompressionTypeTo + ', ' +
			'ONLINE = ' + @OnlineOption + ');' AS compression_cmd
	FROM #Objects o
	ORDER BY OBJECT_SCHEMA_NAME(o.object_id, DB_ID(@DBName)), OBJECT_NAME(o.object_id, DB_ID(@DBName))
END
GO
