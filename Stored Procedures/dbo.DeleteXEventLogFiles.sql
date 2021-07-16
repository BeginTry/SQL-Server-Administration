CREATE OR ALTER PROCEDURE dbo.DeleteXEventLogFiles
	@XEventSessionName SYSNAME
/******************************************************************************
* Name     : dbo.DeleteXEventLogFiles
* Purpose  : Deletes *.xel event file targets of an Extended Event session.
* Inputs   : @XEventSessionName - self-explanatory.
* Outputs  : 
* Returns  : 
******************************************************************************
* Change History
*	2021-07-16	DMason	Created.
*	TODO: optional parameter to stop XEvent session before delete?
		ALTER EVENT SESSION @XEventSessionName ON SERVER STATE = STOP;
		ALTER EVENT SESSION @XEventSessionName ON SERVER STATE = START;
******************************************************************************/
AS
DECLARE @Cmd NVARCHAR(4000);

SELECT 
	--field.*, ca.ErrorLogFileName, xe.xel_Wildcard
	@Cmd = 'xp_cmdshell ''del "' + xe.xel_Wildcard + '"'''
FROM sys.server_event_sessions AS s
JOIN sys.server_event_session_targets AS t 
	ON t.event_session_id = s.event_session_id
INNER JOIN sys.dm_xe_object_columns AS col 
	ON t.name = col.object_name 
	AND col.column_type = 'customizable' 
LEFT OUTER JOIN sys.server_event_session_fields AS sf 
	ON t.event_session_id = sf.event_session_id 
	AND t.target_id = sf.object_id 
	AND col.name = sf.name 
CROSS APPLY (
	SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS VARCHAR(MAX)) AS ErrorLogFileName
) AS ca
CROSS APPLY (
	SELECT LEFT(ca.ErrorLogFileName, LEN(ca.ErrorLogFileName) - CHARINDEX('\', REVERSE(ca.ErrorLogFileName))) + '\' + CAST(sf.value AS VARCHAR(MAX)) + '*.xel' xel_Wildcard
) AS xe
WHERE sf.name = 'filename'
AND s.name = @XEventSessionName

--SELECT @Cmd;

IF EXISTS (
	SELECT *
	FROM master.sys.configurations c
	WHERE c.name = 'xp_cmdshell'
	AND value = 1)
BEGIN
	EXEC (@cmd);
END
ELSE
BEGIN
	--Enable [xp_cmdshell] config setting, EXECUTE, revert config setting.
	EXECUTE sp_configure 'show advanced options', 1;
	RECONFIGURE;
	EXECUTE sp_configure 'xp_cmdshell', 1;
	RECONFIGURE;

	EXEC (@cmd);

	EXECUTE sp_configure 'xp_cmdshell', 0;
	RECONFIGURE;
END
