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
DECLARE @DatabaseName SYSNAME = 'AdventureWorks';
DECLARE @TableSchema SYSNAME = 'HumanResources';
DECLARE @TableName SYSNAME = 'Employee';

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

DECLARE @TableDDL VARCHAR(MAX);
SELECT @TableDDL = STRING_AGG(x.Output, CHAR(13) + CHAR(10))
FROM #XpCmdShell x
WHERE x.Output IS NOT NULL

SELECT [Create_Table_DDL] = CHAR(13) +CHAR(10) + @TableDDL + CHAR(13) +CHAR(10) FOR XML PATH(''),TYPE



