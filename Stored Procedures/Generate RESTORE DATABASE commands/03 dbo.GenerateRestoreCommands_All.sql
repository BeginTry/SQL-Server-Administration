USE tempdb;
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'GenerateRestoreCommands_All'
)
	DROP PROCEDURE dbo.GenerateRestoreCommands_All 
GO

CREATE PROCEDURE dbo.GenerateRestoreCommands_All
	@DBName SYSNAME
/*
	Purpose:	
	Generates RESTORE DB commands for a single database.
	The resulting output will restore the most recent FULL backup,
	followed by the most recent DIFFERENTIAL backup (if availabel),
	followed by the most recent LOG backups (if available).
	Copy and paste the output into an SSMS window.
	
	Inputs:
	@DBName : name of the database (NULL for all db's).
	History:
	11/06/2014	DBA	Created
*/
AS
SET NOCOUNT ON
EXEC dbo.GenerateRestoreCommands @BackupType = 'D', @DBName = @DBName
EXEC dbo.GenerateRestoreCommands @BackupType = 'I', @DBName = @DBName
EXEC dbo.GenerateRestoreCommands_Log @DBName = @DBName
GO
