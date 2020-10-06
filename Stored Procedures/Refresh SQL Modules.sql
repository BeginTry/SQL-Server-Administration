/*
	This script iterates through all databases and attempts to identify
	objects with invalid definitions via [sys].[sp_refreshsqlmodule]
	(Schemabound objects are omitted.)
	Presumably, objects that cannot be refreshed need to be fixed or could be dropped.

	object types:
	***************************************
	P = SQL Stored Procedure
	FN = SQL scalar function
	IF = SQL inline table-valued function
	TF = SQL table-valued-function
	V = View
	TR = SQL DML trigger
*/
IF EXISTS (
	SELECT *
	FROM tempdb.INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'guest'
	AND t.TABLE_NAME = 'InvalidObjectDefinitions'
)
BEGIN
	EXEC('DROP TABLE tempdb.guest.InvalidObjectDefinitions;');
END
GO

CREATE TABLE tempdb.guest.InvalidObjectDefinitions (
	ID INT IDENTITY
		CONSTRAINT PK_InvalidObjectDefinitions PRIMARY KEY,
	DBName SYSNAME,
	SchemaName SYSNAME,
	ObjectName SYSNAME,
	ObjectType NVARCHAR(60),
	ErrMsg NVARCHAR(4000)
);
GO

DECLARE @Cmd NVARCHAR(MAX) =
'USE [?]; 

DECLARE @TSql NVARCHAR(MAX);
DECLARE @Schema SYSNAME;
DECLARE @Obj SYSNAME;
DECLARE @Type NVARCHAR(60);
DECLARE @ErrMsg NVARCHAR(4000);
DECLARE curViews CURSOR FOR
	SELECT SCHEMA_NAME(o.schema_id) AS SchemaName, o.name AS ObjectName, o.type_desc AS ObjectType
	FROM sys.objects o
	WHERE o.type IN (''P'', ''FN'', ''IF'', ''TF'', ''V'', ''TR'')
	AND o.is_ms_shipped = 0
	AND COALESCE(OBJECTPROPERTY(o.object_id, ''IsSchemaBound''), 0) = 0

OPEN curViews;
FETCH NEXT FROM curViews INTO @Schema, @Obj, @Type;
SET NOCOUNT ON;

WHILE @@FETCH_STATUS = 0
BEGIN
	DECLARE @SchemaName_ObjName NVARCHAR(776);
	SET @SchemaName_ObjName = @Schema + ''.'' + @Obj;

	BEGIN TRY
		EXEC sys.sp_refreshsqlmodule @SchemaName_ObjName;
	END TRY
	BEGIN CATCH
		SELECT @ErrMsg = ERROR_MESSAGE();
		INSERT INTO tempdb.guest.InvalidObjectDefinitions(DBName, SchemaName, ObjectName, ObjectType, ErrMsg)
			VALUES(DB_NAME(), @Schema, @Obj, @Type, @ErrMsg);
	END CATCH

	FETCH NEXT FROM curViews INTO @Schema, @Obj, @Type;
END

CLOSE curViews;
DEALLOCATE curViews;
';
EXEC sp_MSforeachdb @Cmd;

SELECT *
FROM tempdb.guest.InvalidObjectDefinitions
