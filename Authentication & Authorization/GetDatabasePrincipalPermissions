/*
	Returns a list of database principals (users, et al) and any
	permissions that have explicitly been granted or denied for all system and user databases.
*/
IF OBJECT_ID('tempdb.dbo.#DatabasePrincipalPermissions') IS NOT NULL DROP TABLE dbo.#DatabasePrincipalPermissions;

CREATE TABLE #DatabasePrincipalPermissions (
	[Database Name] SYSNAME,
	[Database Principal] SYSNAME,
	[Database Principal Type] CHAR(1),
	[Database Permission] NVARCHAR(MAX),
	Class NVARCHAR(256),
	SecurableSchema SYSNAME,
	SecurableObject SYSNAME,
	Securible_Id INT,
	[Server Principal] NVARCHAR(128) NULL
);

DECLARE @command VARCHAR(1000) ;
SELECT @command = 'USE [?];

INSERT INTO #DatabasePrincipalPermissions
SELECT 
	DB_NAME() AS database_name,
	prin.[name] [Database Principal], 
	prin.type,
	COALESCE(perm.state_desc + '' '' + perm.permission_name, ''no permission statements'') [Database Permission],
	COALESCE(perm.class_desc, '''') Class, 

	COALESCE(OBJECT_SCHEMA_NAME(perm.major_id), '''') [SecurableSchema],
	COALESCE(OBJECT_NAME(perm.major_id), '''') [SecurableObject], 
	perm.major_id [Securible_Id],
	p.name AS [Server Principal]
FROM sys.database_principals prin
LEFT JOIN sys.database_permissions perm 
	ON perm.grantee_principal_id = prin.principal_id 
LEFT JOIN master.sys.server_principals p
	ON p.sid = prin.sid
WHERE prin.is_fixed_role = 0;';

EXEC sp_MSforeachdb @command;

SELECT 
	dpp.[Database Name], 
	dpp.[Database Principal], 
	CASE dpp.[Database Principal Type]
		WHEN 'A' THEN 'Application role'
		WHEN 'C' THEN 'User mapped to a certificate'
		WHEN 'E' THEN 'External user from Azure Active Directory'
		WHEN 'G' THEN 'Windows group'
		WHEN 'K' THEN 'User mapped to an asymmetric key'
		WHEN 'R' THEN 'Database role'
		WHEN 'S' THEN 'User (SQL Auth)'
		WHEN 'U' THEN 'User (Windows Auth)'
		WHEN 'X' THEN 'External group from Azure Active Directory group or applications'
	END	AS [Database Principal Type],
	dpp.[Database Permission], 
	dpp.Class, 
	dpp.SecurableSchema, 
	dpp.SecurableObject,
	COALESCE(dpp.[Server Principal], '') AS [Server Principal]
FROM #DatabasePrincipalPermissions dpp
ORDER BY dpp.[Database Name], dpp.[Database Principal], dpp.[Database Permission], dpp.SecurableObject;
GO
