/*
	This script iterates through all databases and finds dependency information for all objects.
	It is particularly helpful with identifying cross-database and cross-server entities (among other things).
*/
/*
	Find object dependencies across all databases.
*/
IF EXISTS (
	SELECT *
	FROM tempdb.INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'guest'
	AND t.TABLE_NAME = 'SqlExpressionDependencies'
)
BEGIN
	EXEC('DROP TABLE tempdb.guest.SqlExpressionDependencies;');
END
GO

SELECT 
	DB_NAME() AS database_name, 
	SCHEMA_NAME(0) AS schema_name,
	OBJECT_NAME(d.referencing_id, DB_ID()) AS object_name, 
	d.referenced_server_name,
	d.referenced_database_name,	
	d.referenced_schema_name,	
	d.referenced_entity_name	
INTO tempdb.guest.SqlExpressionDependencies
FROM sys.sql_expression_dependencies d	
WHERE 1 = 2;	
		
DECLARE @Cmd VARCHAR(2000) 
SELECT @Cmd = 'USE [?];

INSERT INTO tempdb.guest.SqlExpressionDependencies
SELECT 
	DB_NAME() AS database_name, 	
	SCHEMA_NAME(o.schema_id) AS schema_name,
	OBJECT_NAME(d.referencing_id) AS object_name, 
	d.referenced_server_name,
	d.referenced_database_name,	
	d.referenced_schema_name,	
	d.referenced_entity_name	
FROM sys.sql_expression_dependencies d
JOIN sys.objects o
	ON o.object_id = d.referencing_id
WHERE o.is_ms_shipped = 0';

EXEC sp_MSforeachdb @Cmd

SELECT *
FROM tempdb.guest.SqlExpressionDependencies;
