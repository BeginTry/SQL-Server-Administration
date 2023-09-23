/*
	This script will check the following configuration items:
	• show advanced options
	• xp_cmdshell

	If either (or both) are disabled, they will be enabled.
	xp_cmdshell can then be executed successfully.

	Afterwards, the two configuration item settings are reverted (if they were changed).
*/
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
GO

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

--Run code that includes xp_cmdshell.

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
