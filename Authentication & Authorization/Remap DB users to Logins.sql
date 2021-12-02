/*
	This script iterates through all system and user databases.
	If any "orphaned" database users are found, it will attempt
	to remap the user to a SQL Authentication login with the
	same name. TRY/CATCH is used to discard any errors (for 
	instance, if the login name does not exist or is already
	mapped to another user in the database).
*/
DECLARE @Cmd NVARCHAR(2000) 
SELECT @Cmd = 'USE [?]; 

DECLARE @Cmd SYSNAME;
DECLARE curUser CURSOR FAST_FORWARD READ_ONLY FOR
SELECT ''ALTER USER ['' + p.name + ''] WITH LOGIN = ['' + p.name + ''];''
FROM sys.database_principals p
LEFT JOIN master.sys.server_principals l
	ON l.sid = p.sid
WHERE p.type_desc = ''SQL_USER''
AND p.authentication_type_desc = ''INSTANCE''
AND l.name IS NULL

OPEN curUser;
FETCH NEXT FROM curUser INTO @Cmd;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		EXEC (@Cmd);
	END TRY
	BEGIN CATCH
	END CATCH

	FETCH NEXT FROM curUser INTO @Cmd;
END

CLOSE curUser;
DEALLOCATE curUser;
';

EXEC sp_MSforeachdb @Cmd; 
