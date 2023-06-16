/*
	This script shows sql_modules (procs, functions, triggers, views, etc.) 
	and the entities (objects) and minor entities (columns) they are referencing.
	It's rather slow, most likely because a cursor is used. (Dynamic management
	function sys.dm_sql_referenced_entities raises an error when column dependencies 
	cannot be resolved. Instead of a set-based operation, which might not return all 
	results, the DMF is invoked one object at a time within a TRY/CATCH structure.)
*/
DROP TABLE IF EXISTS tempdb.guest.SqlReferencedEntities;
SELECT DB_NAME() AS database_name,
	DB_NAME() AS schema_name,
	DB_NAME() AS object_name,
	e.*
INTO tempdb.guest.SqlReferencedEntities
FROM sys.dm_sql_referenced_entities(NULL, NULL) e
ALTER TABLE tempdb.guest.SqlReferencedEntities REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);

DECLARE @Cmd NVARCHAR(2000) 
SELECT @Cmd = 'USE [?]; 

IF DB_NAME() IN (''master'', ''model'', ''msdb'', ''tempdb'', ''SSISDB'', ''distribution'')
	OR DB_NAME() LIKE ''ReportServer%''
	OR CAST(DATABASEPROPERTYEX(DB_NAME(), ''Updateability'') AS VARCHAR(MAX)) = ''READ_ONLY''
	RETURN;

DECLARE @Schema SYSNAME, @Object SYSNAME;
DECLARE curModules CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT OBJECT_SCHEMA_NAME(m.object_id), OBJECT_NAME(m.object_id)
	FROM sys.sql_modules m

OPEN curModules;
FETCH NEXT FROM curModules INTO @Schema, @Object;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		INSERT INTO tempdb.guest.SqlReferencedEntities 
			(database_name, schema_name, object_name, 
			referencing_minor_id, referenced_server_name, referenced_database_name, 
			referenced_schema_name, referenced_entity_name, referenced_minor_name, 
			referenced_id, referenced_minor_id, referenced_class, referenced_class_desc, 
			is_caller_dependent, is_ambiguous, is_selected, is_updated, is_select_all, 
			is_all_columns_found, is_insert_all, is_incomplete)
		SELECT DB_NAME() AS database_name, @Schema AS schema_name, @Object AS object_name,
			e.referencing_minor_id, e.referenced_server_name, e.referenced_database_name, 
			e.referenced_schema_name, e.referenced_entity_name, e.referenced_minor_name, 
			e.referenced_id, e.referenced_minor_id, e.referenced_class, e.referenced_class_desc, 
			e.is_caller_dependent, e.is_ambiguous, e.is_selected, e.is_updated, e.is_select_all, 
			e.is_all_columns_found, e.is_insert_all, e.is_incomplete
		FROM sys.dm_sql_referenced_entities(QUOTENAME(@Schema) + ''.'' + QUOTENAME(@Object), ''OBJECT'') e
	END TRY
	BEGIN CATCH
	END CATCH

	FETCH NEXT FROM curModules INTO @Schema, @Object;
END

CLOSE curModules;
DEALLOCATE curModules;
';

EXEC sp_MSforeachdb @Cmd; 

SELECT *
FROM tempdb.guest.SqlReferencedEntities;
