/***************************************************************************************
	This script renames the [sa] login and creates a new [sa] login to take its place.
	The new [sa] login will have authorizations scaled down in multiple steps, 
	which can be implemented all at once, or implemented over any period of time.
***************************************************************************************/

/*
	SELECT p.* 
	FROM sys.server_principals p 
	WHERE p.sid = 0x01

	SELECT *
	FROM sys.syslogins l
	WHERE l.sid = 0x01
*/


--STEP 1: rename original [sa], create fake [sa] with sysadmin membership.
IF EXISTS (
	SELECT p.* 
	FROM sys.server_principals p 
	WHERE p.sid = 0x01
	AND p.name = 'sa'
)
BEGIN
	--Rename the 0x01 login.
	ALTER LOGIN [sa]
	WITH NAME = [Orig_sa];

	--Create a fake "sa" login.
	DECLARE @TSql NVARCHAR(MAX) = '';
	SELECT @TSql = 'CREATE LOGIN [sa] ' +
		'WITH PASSWORD = ' + CONVERT(VARCHAR(1000), password_hash, 1) + ' HASHED, ' +
		'DEFAULT_DATABASE = [tempdb], DEFAULT_LANGUAGE = [us_english], CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;'
	FROM sys.sql_logins l
	WHERE l.sid = 0x01

	EXEC(@TSql);

	ALTER SERVER ROLE sysadmin ADD MEMBER [sa];
END

--TODO: change the [Orig_sa] password.

--STEP 2
USE master;
IF NOT EXISTS (
	SELECT 
		prin.name AS PrincipalName, 
		perm.state_desc, 
		perm.permission_name
	FROM sys.server_principals AS prin
	JOIN sys.server_permissions AS perm
		ON prin.principal_id = perm.grantee_principal_id
	WHERE prin.name = 'sa'
	AND perm.state_desc = 'GRANT'
	AND perm.permission_name = 'CONTROL SERVER'
)
BEGIN
	GRANT CONTROL SERVER to [sa];
END
GO

--Allow some time for SQL to revalidate current [sa] connections. Otherwise, you may encounter errors similar to the following:
--Login failed for user 'sa'. Reason: Failed to open the database '%s' configured in the login object while revalidating the login on the connection. [CLIENT: %s]
--Login failed for user 'sa'. Reason: Failed to open the explicitly specified database '%s'. [CLIENT: %s]
WAITFOR DELAY '00:01:30';
GO

IF IS_SRVROLEMEMBER('sysadmin', 'sa') = 1
BEGIN
	ALTER SERVER ROLE sysadmin DROP MEMBER [sa];
END
GO


--STEP 3: DENY specific database-level and sever-level authorizations.

--Database-level authorizations. Allow most of these.
	--DENY 	ADMINISTER DATABASE BULK OPERATIONS	 TO [sa];
	--DENY 	ALTER	 TO [sa];
	--DENY 	ALTER ANY APPLICATION ROLE	 TO [sa];
	--DENY 	ALTER ANY ASSEMBLY	 TO [sa];
	--DENY 	ALTER ANY ASYMMETRIC KEY	 TO [sa];
	--DENY 	ALTER ANY CERTIFICATE	 TO [sa];
	--DENY 	ALTER ANY COLUMN ENCRYPTION KEY	 TO [sa];
	--DENY 	ALTER ANY COLUMN MASTER KEY	 TO [sa];
	--DENY 	ALTER ANY CONTRACT	 TO [sa];
	--DENY 	ALTER ANY DATABASE AUDIT	 TO [sa];
	--DENY 	ALTER ANY DATABASE DDL TRIGGER	 TO [sa];
	--DENY 	ALTER ANY DATABASE EVENT NOTIFICATION	 TO [sa];
	--DENY 	ALTER ANY DATABASE EVENT SESSION	 TO [sa];
	--DENY 	ALTER ANY DATABASE SCOPED CONFIGURATION	 TO [sa];
	--DENY 	ALTER ANY DATASPACE	 TO [sa];
	--DENY 	ALTER ANY EXTERNAL DATA SOURCE	 TO [sa];
	--DENY 	ALTER ANY EXTERNAL FILE FORMAT	 TO [sa];
	--DENY 	ALTER ANY EXTERNAL LIBRARY	 TO [sa];
	--DENY 	ALTER ANY FULLTEXT CATALOG	 TO [sa];
	--DENY 	ALTER ANY MASK	 TO [sa];
	--DENY 	ALTER ANY MESSAGE TYPE	 TO [sa];
	--DENY 	ALTER ANY REMOTE SERVICE BINDING	 TO [sa];
	DENY 	ALTER ANY ROLE	 TO [sa];
	--DENY 	ALTER ANY ROUTE	 TO [sa];
	--DENY 	ALTER ANY SCHEMA	 TO [sa];
	--DENY 	ALTER ANY SECURITY POLICY	 TO [sa];
	--DENY 	ALTER ANY SERVICE	 TO [sa];
	--DENY 	ALTER ANY SYMMETRIC KEY	 TO [sa];
	DENY 	ALTER ANY USER	 TO [sa];
	--DENY 	AUTHENTICATE	 TO [sa];
	DENY 	BACKUP DATABASE	 TO [sa];
	DENY 	BACKUP LOG	 TO [sa];
	--DENY 	CHECKPOINT	 TO [sa];
	--DENY 	CONNECT	 TO [sa];
	--DENY 	CONNECT REPLICATION	 TO [sa];
	--DENY 	CONTROL	 TO [sa];
	--DENY 	CREATE AGGREGATE	 TO [sa];
	--DENY 	CREATE ASSEMBLY	 TO [sa];
	--DENY 	CREATE ASYMMETRIC KEY	 TO [sa];
	--DENY 	CREATE CERTIFICATE	 TO [sa];
	--DENY 	CREATE CONTRACT	 TO [sa];
	DENY 	CREATE DATABASE	 TO [sa];
	--DENY 	CREATE DATABASE DDL EVENT NOTIFICATION	 TO [sa];
	--DENY 	CREATE DEFAULT	 TO [sa];
	--DENY 	CREATE EXTERNAL LIBRARY	 TO [sa];
	--DENY 	CREATE FULLTEXT CATALOG	 TO [sa];
	--DENY 	CREATE FUNCTION	 TO [sa];
	--DENY 	CREATE MESSAGE TYPE	 TO [sa];
	--DENY 	CREATE PROCEDURE	 TO [sa];
	--DENY 	CREATE QUEUE	 TO [sa];
	--DENY 	CREATE REMOTE SERVICE BINDING	 TO [sa];
	--DENY 	CREATE ROLE	 TO [sa];
	--DENY 	CREATE ROUTE	 TO [sa];
	--DENY 	CREATE RULE	 TO [sa];
	--DENY 	CREATE SCHEMA	 TO [sa];
	--DENY 	CREATE SERVICE	 TO [sa];
	--DENY 	CREATE SYMMETRIC KEY	 TO [sa];
	--DENY 	CREATE SYNONYM	 TO [sa];
	--DENY 	CREATE TABLE	 TO [sa];
	--DENY 	CREATE TYPE	 TO [sa];
	--DENY 	CREATE VIEW	 TO [sa];
	--DENY 	CREATE XML SCHEMA COLLECTION	 TO [sa];
	--DENY 	DELETE	 TO [sa];
	--DENY 	EXECUTE	 TO [sa];
	--DENY 	EXECUTE ANY EXTERNAL SCRIPT	 TO [sa];
	--DENY 	INSERT	 TO [sa];
	DENY 	KILL DATABASE CONNECTION	 TO [sa];
	--DENY 	REFERENCES	 TO [sa];
	--DENY 	SELECT	 TO [sa];
	--DENY 	SHOWPLAN	 TO [sa];
	--DENY 	SUBSCRIBE QUERY NOTIFICATIONS	 TO [sa];
	--DENY 	TAKE OWNERSHIP	 TO [sa];
	--DENY 	UNMASK	 TO [sa];
	--DENY 	UPDATE	 TO [sa];
	--DENY 	VIEW ANY COLUMN ENCRYPTION KEY DEFINITION	 TO [sa];
	--DENY 	VIEW ANY COLUMN MASTER KEY DEFINITION	 TO [sa];
	--DENY 	VIEW DATABASE STATE	 TO [sa];
	--DENY 	VIEW DEFINITION	 TO [sa];

--Server-level authorizations. DENY most of these.
	--DENY 	ADMINISTER BULK OPERATIONS	 TO [sa];
	DENY 	ALTER ANY AVAILABILITY GROUP	 TO [sa];
	DENY 	ALTER ANY CONNECTION	 TO [sa];
	DENY 	ALTER ANY CREDENTIAL	 TO [sa];
	DENY 	ALTER ANY DATABASE	 TO [sa];
	DENY 	ALTER ANY ENDPOINT	 TO [sa];
	DENY 	ALTER ANY EVENT NOTIFICATION	 TO [sa];
	DENY 	ALTER ANY EVENT SESSION	 TO [sa];
	DENY 	ALTER ANY LINKED SERVER	 TO [sa];
	DENY 	ALTER ANY LOGIN	 TO [sa];
	DENY 	ALTER ANY SERVER AUDIT	 TO [sa];
	DENY 	ALTER ANY SERVER ROLE	 TO [sa];
	DENY 	ALTER RESOURCES	 TO [sa];
	DENY 	ALTER SERVER STATE	 TO [sa];
	DENY 	ALTER SETTINGS	 TO [sa];
	DENY 	ALTER TRACE	 TO [sa];
	--DENY 	AUTHENTICATE SERVER	 TO [sa];
	--DENY 	CONNECT ANY DATABASE	 TO [sa];
	--DENY 	CONNECT SQL	 TO [sa];
	--DENY 	CONTROL SERVER	 TO [sa];
	DENY 	CREATE ANY DATABASE	 TO [sa];
	DENY 	CREATE AVAILABILITY GROUP	 TO [sa];
	DENY 	CREATE DDL EVENT NOTIFICATION	 TO [sa];
	DENY 	CREATE ENDPOINT	 TO [sa];
	DENY 	CREATE SERVER ROLE	 TO [sa];
	DENY 	CREATE TRACE EVENT NOTIFICATION	 TO [sa];
	DENY 	EXTERNAL ACCESS ASSEMBLY	 TO [sa];
	DENY 	IMPERSONATE ANY LOGIN	 TO [sa];
	--DENY 	SELECT ALL USER SECURABLES	 TO [sa];
	DENY 	SHUTDOWN	 TO [sa];
	DENY 	UNSAFE ASSEMBLY	 TO [sa];
	--DENY 	VIEW ANY DATABASE	 TO [sa];
	--DENY 	VIEW ANY DEFINITION	 TO [sa];
	--DENY 	VIEW SERVER STATE	 TO [sa];

--STEP 4: create a user for the [sa] login in every system and user database, add to [db_owner] fixed database role.
DECLARE @TSql NVARCHAR(MAX) = '';
SELECT @TSql = @TSql +
	'USE ' + QUOTENAME(d.name) + '; IF USER_ID(''sa'') IS NULL CREATE USER [sa] FOR LOGIN [sa]; ALTER ROLE db_owner ADD MEMBER [sa];' + CHAR(13) + CHAR(10)
FROM master.sys.databases d
WHERE d.source_database_id IS NULL
AND d.is_read_only = 0
AND d.state_desc = 'ONLINE'
ORDER BY d.name;

EXEC(@TSql);
REVOKE CONTROL SERVER to [sa];
GO

--STEP 5: "disable" [sa] database users in specific databases (as needed).
--TODO: Which databases does the fake [sa] account *not* need to connect to?
--USE <dbname1>; DENY CONNECT TO [sa];
--USE <dbname2>; DENY CONNECT TO [sa];
--USE <dbname3>; DENY CONNECT TO [sa];
--...
--USE <dbnameN>; DENY CONNECT TO [sa];


--STEP 6: scale back [sa] database user authorizations by juggling fixed database role membership.
DECLARE @TSql NVARCHAR(MAX) = '';
SELECT @TSql = @TSql +
	'USE ' + QUOTENAME(d.name) + '; IF USER_ID(''sa'') IS NOT NULL 
	BEGIN
		ALTER ROLE db_owner DROP MEMBER [sa];
		ALTER ROLE db_datareader ADD MEMBER [sa];
		ALTER ROLE db_datawriter ADD MEMBER [sa];
		ALTER ROLE db_ddladmin ADD MEMBER [sa]
		GRANT EXECUTE TO [sa];
	END' + CHAR(13) + CHAR(10)
FROM master.sys.databases d
WHERE d.source_database_id IS NULL
AND d.is_read_only = 0
AND d.state_desc = 'ONLINE'
ORDER BY d.name;

EXEC(@TSql);

--STEP 7: further scale back [sa] database user authorizations in databases where read-only access (and nothing more) is needed.
--TODO: Which databases does the fake [sa] account only need read-access to?
--USE <dbname1>; ALTER ROLE db_datawriter DROP MEMBER [sa]; ALTER ROLE db_ddladmin DROP MEMBER [sa];
--USE <dbname2>; ALTER ROLE db_datawriter DROP MEMBER [sa]; ALTER ROLE db_ddladmin DROP MEMBER [sa];
--USE <dbname3>; ALTER ROLE db_datawriter DROP MEMBER [sa]; ALTER ROLE db_ddladmin DROP MEMBER [sa];
--...
--USE <dbnameN>; ALTER ROLE db_datawriter DROP MEMBER [sa]; ALTER ROLE db_ddladmin DROP MEMBER [sa];
