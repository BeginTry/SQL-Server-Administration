/*
	This script analyzes a table and autogenerates TSQL scripts to 'convert' it
	to a star schema with one Fact table and one or more Dimension tables.
	Good candidates are "flat", "wide" tables with lots of (n)char/(n)varchar columns.

	Getting Started
	1. Set the database context for the table to be analyzed.
	2. Enter the schema name and table name.
	3. Run the script.
	4. The first result set includes column [DataExploration]. It contains queries that
		you will want to run to see which columns have cardinality low enough to warrant a Dimension Table.
		Each column will potentially have a corresponding Dimension table created for it.
	5. After you have explored the data, assign any columns that should not have a corresponding Dimension
		table created as a CSV list to the @SourceTableStringColumnsToExclude variable.
	6. Run the script again.
	7. Click the XML output of the second result set (this assumes you are using SSMS). Copy/paste
		the text into a new SSMS query window/tab.
	8. Run the query in each section:
		• DimensionTables: creates one or more Dimension tables.
		• FactTable: creates a Fact table.
		• ViewDefinition: creates a view on the star schema. It's schema matches that of the source table.
		• TriggerDefinition: creates an INSTEAD OF INSERT TRIGGER on the view. Inserts to the source table
			could be redirected to the view, and the trigger will separate out the data to populate
			all of the related Dimension tables and the Fact table.
		• InsertTest: inserts a batch of rows from the source table to the view for testing purposes.
*/
USE YourDatabase;

--This is the "wide" table that might benefit from a star schema: enter the schema name and table name.
DECLARE @SourceSchemaName VARCHAR(MAX) = 'dbo';
DECLARE @SourceTableName VARCHAR(MAX) = 'YourTableName';

--This is a csv list of columns in the base table that not be converted to a Dimension table Key column.
DECLARE @SourceTableStringColumnsToExclude VARCHAR(MAX) = 'Column1,Column2,Column3,...';

/******************************************************************************
	Everything from here onward is automated code.
******************************************************************************/
DROP TABLE IF EXISTS #UserDefinedVariables;
CREATE TABLE #UserDefinedVariables (
	SourceSchemaName SYSNAME,
	SourceTableName SYSNAME,
	[SourceTableExcludeColumns] VARCHAR(MAX),
);
INSERT INTO #UserDefinedVariables(SourceSchemaName, SourceTableName, SourceTableExcludeColumns) 
	VALUES(@SourceSchemaName, @SourceTableName, @SourceTableStringColumnsToExclude);

/***********************************************************************************
***********************************************************************************/
/*
	This script generates a T-SQL CREATE TABLE script using the following:
		• PowerShell
		• SMO (SQL Management Object)
		• The Windows command line (cmd.exe)
		• xp_cmdshell

	sysadmin server role membership required.
	Hot shower recommended afterwards (this is dirty AF).
	Send hate mail and general disdain to: https://mastodon.social/@DaveMasonDotMe
*/
DECLARE @DatabaseName SYSNAME = DB_NAME();
DECLARE @TableSchema SYSNAME = @SourceSchemaName;
DECLARE @TableName SYSNAME = @SourceTableName;

DECLARE @PoShCmd VARCHAR(8000) = '$SqlInstance = "' + @@SERVERNAME + '" 
$DBName = "' + @DatabaseName + '"
$TableSchema = "' + @TableSchema + '"
$TableName = "' + @TableName + '"
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") | Out-Null
$serverInstance = New-Object ("Microsoft.SqlServer.Management.Smo.Server") $SqlInstance
$so = new-object ("Microsoft.SqlServer.Management.Smo.ScriptingOptions")
$TableSO = New-Object ("Microsoft.SqlServer.Management.Smo.ScriptingOptions")
$TableSO.DriAllConstraints = $true
$TableSO.DriAllKeys = $true
$TableSO.Indexes = $false
$TableSO.FullTextCatalogs = $false
$TableSO.FullTextIndexes = $false
$TableSO.Triggers = $false
$TableSO.XmlIndexes = $false
$DBName = $DBName.replace("[","").replace("]","")
$db = $serverInstance.Databases[$DBName] 
$tables = $db.Tables[$TableName, $TableSchema]
foreach ($tbl in $tables){ If ($tbl.Schema -ieq $TableSchema) { $tbl.Script($TableSO)+"GO" } }';
--Combine PoSh code lines into a single line, delimited by a ';' character.
SET @PoShCmd = REPLACE(@PoshCmd, CHAR(13) + CHAR(10), '; ');
--Escape double-quote char for Windows command line.
SET @PoShCmd = '"' + REPLACE(@PoshCmd, '"', '\"') + '"';
SET @PoshCmd = 'PowerShell.exe ' + @PoshCmd;

--Save to #temp table (variable will go out of scope).
DROP TABLE IF EXISTS #Variables;
CREATE TABLE #Variables (Variable VARCHAR(64), Value NVARCHAR(MAX));
INSERT INTO #Variables(Variable, Value) VALUES('@PoShCmd', @PoShCmd);

DROP TABLE IF EXISTS #Configure;
CREATE TABLE #Configure (
	name NVARCHAR(35),
	minimum INT, 
	maximum INT, 
	config_value INT,
	run_value INT
);
INSERT INTO #Configure EXECUTE sp_configure 'show advanced options';

--Enable [show advanced options] (if needed).
IF EXISTS (
	SELECT *
	FROM #Configure
	WHERE name = 'show advanced options'
	AND config_value = 0
)
BEGIN
	EXECUTE sp_configure 'show advanced options', 1;
	RECONFIGURE;
END
GO

INSERT INTO #Configure EXECUTE sp_configure 'xp_cmdshell';
--Enable [xp_cmdshell] (if needed).
IF EXISTS (
	SELECT *
	FROM #Configure
	WHERE name = 'xp_cmdshell'
	AND config_value = 0
)
BEGIN
	EXECUTE sp_configure 'xp_cmdshell', 1;
	RECONFIGURE;
END
GO

DECLARE @PoShCmd VARCHAR(8000);
SELECT @PoShCmd = Value FROM #Variables WHERE Variable = '@PoShCmd';

DROP TABLE IF EXISTS #XpCmdShell;
CREATE TABLE #XpCmdShell (ID INT IDENTITY, [Output] NVARCHAR(255));
INSERT INTO #XpCmdShell
EXEC xp_cmdshell @PoshCmd;

--Revert [show advanced options] and [xp_cmdshell] (if needed).
IF EXISTS (
	SELECT *
	FROM #Configure
	WHERE name = 'xp_cmdshell'
	AND config_value = 0
)
BEGIN
	EXECUTE sp_configure 'xp_cmdshell', 0;
	RECONFIGURE;
END
GO

IF EXISTS (
	SELECT *
	FROM #Configure
	WHERE name = 'show advanced options'
	AND config_value = 0
)
BEGIN
	EXECUTE sp_configure 'show advanced options', 0;
	RECONFIGURE;
END
GO

DECLARE @SourceTableDef VARCHAR(MAX);
SELECT @SourceTableDef = STRING_AGG(x.Output, CHAR(13) + CHAR(10))
	WITHIN GROUP(ORDER BY x.ID)
FROM #XpCmdShell x
WHERE x.Output IS NOT NULL
/***********************************************************************************
***********************************************************************************/
DECLARE @SourceSchemaName VARCHAR(MAX);
DECLARE @SourceTableName VARCHAR(MAX);
DECLARE @SourceTableStringColumnsToExclude VARCHAR(MAX);

SELECT @SourceSchemaName = SourceSchemaName,
	@SourceTableName = SourceTableName,
	@SourceTableStringColumnsToExclude = SourceTableExcludeColumns
FROM #UserDefinedVariables;

--Create schemas (if they don't already exist)
IF SCHEMA_ID('Fact') IS NULL EXEC('CREATE SCHEMA Fact AUTHORIZATION dbo;');
IF SCHEMA_ID('Dim') IS NULL EXEC('CREATE SCHEMA Fact AUTHORIZATION dbo;');
IF SCHEMA_ID('Etl') IS NULL EXEC('CREATE SCHEMA Fact AUTHORIZATION dbo;');

--Get list of constraints on the source table.
DROP TABLE IF EXISTS #HelpConstraint;
CREATE TABLE #HelpConstraint (
	constraint_type SYSNAME,
	constraint_name SYSNAME,
	delete_action SYSNAME,
	update_action SYSNAME,
	status_enabled SYSNAME,
	status_for_replication SYSNAME,
	constraint_keys NVARCHAR(MAX)
)
DECLARE @ObjectName NVARCHAR(MAX) = QUOTENAME(@SourceSchemaName) + '.' +QUOTENAME(@SourceTableName);
INSERT INTO #HelpConstraint
EXEC sp_helpconstraint @objname = @ObjectName, @nomsg = NULL;

DROP TABLE IF EXISTS #Dimensions;
SELECT c.COLUMN_NAME AS Dim
INTO #Dimensions
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = @SourceSchemaName
AND c.TABLE_NAME = @SourceTableName
AND c.DATA_TYPE LIKE '%char%'
AND c.COLUMN_NAME NOT IN (SELECT value FROM STRING_SPLIT(@SourceTableStringColumnsToExclude, ','))

ALTER TABLE #Dimensions ADD KeyNameCols AS REPLACE(Dim, '_', '') + 'Key';
EXEC('
ALTER TABLE #Dimensions ADD [DataExploration-Which columns have cardinality low enough to warrant a Dimension Table?] AS ''SELECT '' + REPLACE(Dim, ''_'', '''') + '', COUNT(*) FROM ' + @SourceSchemaName + '.' + @SourceTableName + ' GROUP BY '' + REPLACE(Dim, ''_'', '''') + '' ORDER BY COUNT(*) DESC; ''
');
EXEC('
ALTER TABLE #Dimensions ADD CreateDimTables AS ''SELECT IDENTITY(INT,1,1) AS '' + REPLACE(Dim, ''_'', '''') + ''Key, '' + Dim + '' AS Name INTO Dim.'' + REPLACE(Dim, ''_'', '''') + '' FROM ' + @SourceSchemaName + '.' + @SourceTableName + ' WHERE 1 = 2; '' +
		''ALTER TABLE Dim.'' + REPLACE(Dim, ''_'', '''') + '' ALTER COLUMN '' + REPLACE(Dim, ''_'', '''')+ ''Key INT NOT NULL; '' +
		''ALTER TABLE Dim.'' + REPLACE(Dim, ''_'', '''') + '' ADD CONSTRAINT PK_Dim'' + REPLACE(Dim, ''_'', '''') + '' PRIMARY KEY('' + REPLACE(Dim, ''_'', '''') + ''Key); '' +
		''ALTER TABLE Dim.'' + REPLACE(Dim, ''_'', '''') + '' ADD CONSTRAINT UQ_Dim'' + REPLACE(Dim, ''_'', '''') + '' UNIQUE(Name); '' +
		''ALTER TABLE Dim.'' + REPLACE(Dim, ''_'', '''') + '' REBUILD WITH(DATA_COMPRESSION = ROW);''
');
ALTER TABLE #Dimensions ADD ViewKeyNameCols AS 'Dim.' + REPLACE(Dim, '_', '') + '.Name AS ' + Dim;
ALTER TABLE #Dimensions ADD ViewJoins AS 'LEFT JOIN Dim.' + REPLACE(Dim, '_', '') + ' ON Dim.' + REPLACE(Dim, '_', '') + '.' + REPLACE(Dim, '_', '') + 'Key = f.' + REPLACE(Dim, '_', '') + 'Key'
ALTER TABLE #Dimensions ADD TriggerDimInserts AS 'INSERT INTO Dim.' + REPLACE(Dim, '_', '') + '(Name) SELECT DISTINCT ins.' + REPLACE(Dim, '_', '') + ' FROM inserted ins
		WHERE NOT EXISTS (SELECT * FROM Dim.' + REPLACE(Dim, '_', '') + ' d WHERE d.Name = ins.' + REPLACE(Dim, '_', '') + ')
		AND ins.' + REPLACE(Dim, '_', '') + ' IS NOT NULL;'
ALTER TABLE #Dimensions ADD InsertKeyNameCols AS 'Dim.' + REPLACE(Dim, '_', '') + '.' + REPLACE(Dim, '_', '') + 'Key';
ALTER TABLE #Dimensions ADD TriggerJoins AS 'LEFT JOIN Dim.' + REPLACE(Dim, '_', '') + ' ON Dim.' + REPLACE(Dim, '_', '') + '.Name = i.' + Dim
ALTER TABLE #Dimensions ADD SelectTables AS 'SELECT * FROM Dim.' + REPLACE(Dim, '_', '') + ';'
ALTER TABLE #Dimensions ADD TruncateTables AS 'TRUNCATE TABLE Dim.' + REPLACE(Dim, '_', '') + ';'

--Create the Dimension tables.
DECLARE @DimTableDefs VARCHAR(MAX) = '';
SELECT @DimTableDefs = @DimTableDefs + d.CreateDimTables + CHAR(13) + CHAR(10) FROM #Dimensions d
SELECT @DimTableDefs = CHAR(13) + CHAR(10) + @DimTableDefs + 'GO' + CHAR(13) + CHAR(10)

--Create Fact table definition.
DECLARE @FactTableDef VARCHAR(MAX) = REPLACE(@SourceTableDef, QUOTENAME(@SourceSchemaName) + '.', 'Fact.');
--Change "string" columns defintions to "key" column definitions
SELECT @FactTableDef = REPLACE(@FactTableDef, QUOTENAME(d.Dim), REPLACE(d.Dim, '_', '') + 'Key INT NULL, --')
FROM #Dimensions d
--Rename constraints as needed.
SELECT @FactTableDef = REPLACE(@FactTableDef, QUOTENAME(c.constraint_name), 
	QUOTENAME(
		CASE
			WHEN c.constraint_type LIKE 'CHECK%' THEN 'CK'
			WHEN c.constraint_type LIKE 'DEFAULT%' THEN 'DF'
			WHEN c.constraint_type LIKE 'FOREIGN_KEY%' THEN 'FK'
			WHEN c.constraint_type LIKE 'PRIMARY_KEY%' THEN 'PK'
			WHEN c.constraint_type LIKE 'UNIQUE%' THEN 'UQ'
		END + '_Fact_' + @SourceTableName + '_' + CAST(NEWID() AS SYSNAME)
	)
)
FROM #HelpConstraint c
SELECT @FactTableDef = CHAR(13) + CHAR(10) + @FactTableDef + CHAR(13) + CHAR(10)

--Create VIEW definition.
DECLARE @ViewDef VARCHAR(MAX) = 'CREATE OR ALTER VIEW Etl.vw' + @SourceTableName + CHAR(13) + CHAR(10) + 'AS ' + CHAR(13) + CHAR(10) + 'SELECT ' + CHAR(13) + CHAR(10);
SELECT @ViewDef = @ViewDef + CHAR(9) + 'f.' + c.COLUMN_NAME + ',' + CHAR(13) + CHAR(10)
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = @SourceSchemaName
AND c.TABLE_NAME = @SourceTableName

SELECT @ViewDef = TRIM(',' + CHAR(13) + CHAR(10) FROM @ViewDef) + CHAR(13) + CHAR(10) + 'FROM Fact.' + @SourceTableName + ' f';
SELECT @ViewDef = REPLACE(@ViewDef, 'f.' + d.Dim, d.ViewKeyNameCols) FROM #Dimensions d
SELECT @ViewDef = @ViewDef + CHAR(13) + CHAR(10) + d.ViewJoins FROM #Dimensions d
SELECT @ViewDef = CHAR(13) + CHAR(10) + @ViewDef + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)

--Create Instead Of Trigger definition.
DECLARE @TriggerDef VARCHAR(MAX) = 'CREATE OR ALTER TRIGGER Etl.' + @SourceTableName + '_Ins' + CHAR(13) + CHAR(10) + 
	'ON Etl.vw' + @SourceTableName + + CHAR(13) + CHAR(10) + 'INSTEAD OF INSERT' + CHAR(13) + CHAR(10) + 
	'AS ' + CHAR(13) + CHAR(10) + 'BEGIN' + CHAR(13) + CHAR(10);
	--SELECT @TriggerDef, 1
SELECT @TriggerDef = @TriggerDef + d.TriggerDimInserts + CHAR(13) + CHAR(10) FROM #Dimensions d
	--SELECT @TriggerDef, 2
SELECT @TriggerDef = @TriggerDef + 'INSERT INTO Fact.' + @SourceTableName + ' (' + CHAR(13) + CHAR(10)
	--SELECT @TriggerDef, 3
SELECT @TriggerDef = @TriggerDef + CHAR(9) + STRING_AGG(COALESCE(d.KeyNameCols, c.COLUMN_NAME), ', ') WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN #Dimensions d
	ON d.Dim = c.COLUMN_NAME
WHERE c.TABLE_SCHEMA = @SourceSchemaName
AND c.TABLE_NAME = @SourceTableName
AND COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 0
	--SELECT @TriggerDef, 4
SELECT @TriggerDef = @TriggerDef + CHAR(13) + CHAR(10) + ')' + CHAR(13) + CHAR(10) + 'SELECT ' + CHAR(13) + CHAR(10)
	--SELECT @TriggerDef, 5
SELECT @TriggerDef = @TriggerDef + CHAR(9) + 
	STRING_AGG('i.' + c.COLUMN_NAME, ', ' + CHAR(13) + CHAR(10) + CHAR(9)) 
	WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = @SourceSchemaName
AND c.TABLE_NAME = @SourceTableName
AND COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 0
	--SELECT @TriggerDef, 6

SELECT @TriggerDef = REPLACE(@TriggerDef, 'i.' + d.Dim, d.InsertKeyNameCols) FROM #Dimensions d
	--SELECT @TriggerDef, 7
SELECT @TriggerDef = @TriggerDef + CHAR(13) + CHAR(10) + 'FROM inserted i' + CHAR(13) + CHAR(10)
	--SELECT @TriggerDef, 8
SELECT @TriggerDef = @TriggerDef + d.TriggerJoins + CHAR(13) + CHAR(10) FROM #Dimensions d
	--SELECT @TriggerDef, 9
SELECT @TriggerDef = CHAR(13) + CHAR(10) + @TriggerDef + CHAR(13) + CHAR(10) + 
	'END' + CHAR(13) + CHAR(10) +
	'GO' + CHAR(13) + CHAR(10)
	--SELECT @TriggerDef, 10

--Create Insert statement for testing.
DECLARE @InsertTest VARCHAR(MAX) = 'INSERT INTO Etl.vw' + @SourceTableName + '(' + CHAR(13) + CHAR(10);
SELECT @InsertTest = @InsertTest + CHAR(9) + 
	STRING_AGG(c.COLUMN_NAME, ', ') 
	WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = @SourceSchemaName
AND c.TABLE_NAME = @SourceTableName 
SELECT @InsertTest = @InsertTest + CHAR(13) + CHAR(10) + ')' + CHAR(13) + CHAR(10) +
	'SELECT TOP (25000)' + CHAR(13) + CHAR(10) 
SELECT @InsertTest = @InsertTest + CHAR(9) + 
	STRING_AGG(c.COLUMN_NAME, ', ') 
	WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = @SourceSchemaName
AND c.TABLE_NAME = @SourceTableName 
SELECT @InsertTest = @InsertTest + CHAR(13) + CHAR(10) + 'FROM ' + @SourceSchemaName + '.' + @SourceTableName + ';'
SELECT @InsertTest = CHAR(13) + CHAR(10) + @InsertTest + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)

SELECT d.Dim, d.[DataExploration-Which columns have cardinality low enough to warrant a Dimension Table?],
	d.SelectTables SelectDimTables, 
	d.TruncateTables TruncateDimTables
FROM #Dimensions d
SELECT 
	[DimensionTables] = @DimTableDefs,
	[FactTable] = @FactTableDef,
	[ViewDefinition] = @ViewDef,
	[TriggerDefinition] = @TriggerDef,
	[InsertTest] = @InsertTest
FOR XML PATH(''),TYPE
