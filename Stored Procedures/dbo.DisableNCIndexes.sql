CREATE OR ALTER PROCEDURE dbo.DisableNCIndexes
	@DatabaseName SYSNAME,
	@TableSchema SYSNAME,
	@TableName SYSNAME
/******************************************************************************
* Name     : dbo.DisableNCIndexes
* Purpose  : Disables non-clustered indexes on a specified table.
* Inputs   : @DatabaseName, @TableSchema, @TableName - 3-part name of the table.
* Outputs  : none
* Returns  : nothing
* Notes    : SQL Server 2016 or later required. Not tested yet on indexed views.
******************************************************************************
* Change History
*	07/06/2020	DMason	Created.
******************************************************************************/
AS
BEGIN

	DECLARE @Msg NVARCHAR(2047);
	DECLARE @TSql NVARCHAR(MAX);

	--Verify DB exists.
	IF DB_ID(@DatabaseName) IS NULL
	BEGIN
		SET @Msg = 'Database does not exist: ' + QUOTENAME(@DatabaseName);
		RAISERROR(@Msg, 16, 1);
		RETURN;
	END

	--Verify schema/object exists.
	ELSE IF OBJECT_ID(QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@TableSchema) + '.' + QUOTENAME(@TableName)) IS NULL
	BEGIN
		SET @Msg = 'Table (or schema) does not exist: ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@TableSchema) + '.' + QUOTENAME(@TableName);
		RAISERROR(@Msg, 16, 1);
		RETURN;
	END																								

	/********************************************************************/

	--Get the list of non-clustered indexes as JSON data.
	SET @TSql = 'SET @IdxNames = (
	SELECT i.name
	FROM ' + QUOTENAME(@DatabaseName) + '.sys.objects o
	JOIN ' + QUOTENAME(@DatabaseName) + '.sys.schemas s
		ON s.schema_id = o.schema_id
	JOIN ' + QUOTENAME(@DatabaseName) + '.sys.indexes i
		ON i.object_id = o.object_id
	WHERE s.name = @TableSchema
	AND o.name = @TableName
	AND i.type_desc = ''NONCLUSTERED''
	AND i.is_disabled = 0
	FOR JSON AUTO);'

	DECLARE @JsonIndexNames NVARCHAR(MAX);
	EXEC sp_executesql @TSql, N'@TableSchema SYSNAME, @TableName SYSNAME, @IdxNames NVARCHAR(MAX) OUTPUT', @TableSchema, @TableName, @JsonIndexNames OUTPUT;
	--SELECT @JsonIndexNames;

	--Build a string of TSQL statements from the list of index names.
	SET @TSql = '';
	SELECT @TSql = @TSql + 
		'ALTER INDEX ' + QUOTENAME(JSON_VALUE(j.value, '$.name')) + ' ON ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@TableSchema) + '.' + QUOTENAME(@TableName) + ' DISABLE; ' +
		CHAR(13) + CHAR(10)
	FROM OPENJSON(@JsonIndexNames) j;

	PRINT @TSql;
	EXEC (@TSql);
END
GO
