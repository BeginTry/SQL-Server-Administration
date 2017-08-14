USE DbaData
GO

IF NOT EXISTS (
	SELECT 1
	FROM INFORMATION_SCHEMA.TABLES t
	WHERE t.TABLE_SCHEMA = 'dba'
	AND t.TABLE_NAME = 'InstanceConfiguration'
)
	CREATE TABLE dba.InstanceConfiguration (
		InstanceConfigurationId INT IDENTITY
			CONSTRAINT PK_InstanceConfiguration PRIMARY KEY,
		Name VARCHAR(128) NOT NULL
			CONSTRAINT UQ_InstanceConfiguration_Name UNIQUE,
		Value VARCHAR(512) NOT NULL,
		Description VARCHAR(512)
		
	)
GO	

;WITH ConfigurationItems AS
(
	SELECT 'Backup To Disk' Nm, '' Val, '(BIT) 1 = yes, 0 = no. Indicates if databases are to be backed up to disk (as opposed by backup via network backup software (CommVault, Symantec Backup Exec, etc.)' Descr UNION 
	SELECT 'Backup File Keep Days', '', '(INT) The number of days worth of backup files to keep on disk.' UNION 
	SELECT 'Available Disk Space Threshold', '', '(INT) The amount of space (in MB) remaining on a fixed disk that will invoke an alert/notification.' UNION 
	SELECT 'domain DBA Team Operator Name', '', 'The name of the SQL Agent Operator for the domain/DBA team.  (ie aspinfrastructure, chico backups, customer-specific, etc.)' UNION 
	SELECT 'domain DBA Team Operator Email', '', 'Email address(es) of the SQL Agent Operator (usually a distribution group, but could be list of semi-colon seperated addresses).' UNION 
	SELECT 'Backup Path - FULL', '', 'Local or UNC path where FULL database backups will be created.' UNION 
	SELECT 'Backup Path - DIFFERENTIAL', '', 'Local or UNC path where DIFFERENTIAL database backups will be created.' UNION 
	SELECT 'Backup Path - LOG', '', 'Local or UNC path where database LOG backups will be created.' UNION 
	--TODO: Update/delete this row?
	SELECT 'Backup History Keep Days', '', '(INT) Number of days of backup history to keep (in [msdb]).' UNION 
	SELECT 'Job History Keep Days', '', '(INT) Number of days of SQL Agent Job history to keep (in [msdb]).' UNION 
	SELECT 'Allow ALTER DATABASE', '', '(BIT) 1 = yes, 0 = no. Indicates if ALTER DATABASE is allowed by non-dba''s' UNION 
	SELECT 'ALTER DATABASE - Allowed Logins', '', 'csv list of logins that are permitted to run ALTER DATABASE commands.' UNION 

	SELECT 'ALTER DATABASE - Temporary Allowed Logins', '', 'csv list of logins that are temporarily permitted to run ALTER DATABASE commands.  ie. installers, professional services people, developers, etc.' UNION 

	SELECT 'Allow DROP DATABASE', '', '(BIT) 1 = yes, 0 = no. Indicates if DROP DATABASE is allowed by non-dba''s' UNION 
	SELECT 'DROP DATABASE - Allowed Logins', '', 'csv list of logins that are permitted to run DROP DATABASE commands.' UNION 
	SELECT 'Allow Risky Server Roles', '', '(BIT) 1 = yes, 0 = no.  Indicates that server logins can be added to the following fixed server roles: SYSADMIN, SERVERADMIN, SECURITYADMIN, DISKADMIN, DBCREATOR' UNION 
	SELECT 'ADD SERVER ROLE - Allowed Logins', '', 'csv list of logins that are permitted to add logins to "risky" server roles.' UNION 
	SELECT 'Allow Risky Database Roles', '', '(BIT) 1 = yes, 0 = no.  Indicates that database users can be added to the following fixed database roles: db_owner, db_accessadmin, db_backupoperator, db_securityadmin' UNION 
	SELECT 'sp_addrolemember - Allowed Logins', '', 'csv list of logins that are permitted to add users to "risky" database roles.' UNION 

	--TODO: delete these three configuration records.
	SELECT 'Database Mail Address', '', 'The associated email address of the default/public profile.' UNION 
	SELECT 'Database Mail Display Name', '', 'The email address display name of the default/public profile.' UNION 
	SELECT 'Database Mail Reply To Address', '', 'The "Reply To" email address of the default/public profile.' UNION 

	--NEW: mail profile-specific configurations.
	SELECT 'Database Mail Address - Default', '', 'The email address of the "Default" mail account.' UNION 
	SELECT 'Database Mail Display Name - Default', '', 'The display name of the "Default" mail account.' UNION 
	SELECT 'Database Mail Reply To Address - Default', '', 'The "Reply To" address of the "Default" mail account.' UNION 
	SELECT 'Database Mail Address - DBA', '', 'The email address of the "DBA" mail account.' UNION 
	SELECT 'Database Mail Display Name - DBA', '', 'The display name of the "DBA" mail account.' UNION 
	SELECT 'Database Mail Reply To Address - DBA', '', 'The "Reply To" address of the "DBA" mail account.' UNION 
	SELECT 'Database Mail Address - Security', '', 'The email address of the "Security" mail account.' UNION 
	SELECT 'Database Mail Display Name - Security', '', 'The display name of the "Security" mail account.' UNION 
	SELECT 'Database Mail Reply To Address - Security', '', 'The "Reply To" address of the "Security" mail account.' UNION 

	SELECT 'Database Mail Server', '', 'The name (or IP address) of the SMTP server for the default/public profile.' UNION 
	SELECT 'Database Mail Anonymous Authentication', '', '(BIT) 1 = yes, 0 = no.  Indicates if the SMTP Mail Server requires authentication.' UNION 
	SELECT 'Alert Backup/Restore', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent for Backup/Restore events.' UNION 
	SELECT 'Alert Startup', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent when SQL Server (re)starts.' UNION 
	SELECT 'Alert Service Login Changed', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent when service account logins are changed.' UNION 
	SELECT 'SQL DB Engine Login', '', 'Name of the Windows Login (including variations) for the main SQL Server service.' UNION 
	SELECT 'SQL Agent Login', '', 'Name of the Windows Login (including variations) for the SQL Agent service.' UNION
	SELECT 'Alert [sa] Login', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent for login attempts via [sa] (failed or successful).' UNION
	SELECT 'Trace Flags', '', 'csv list of global Trace Flags (integer values only) that are enabled each time SQL Server starts.' UNION
	SELECT 'Cloned Server Check', '', '(BIT) 1 = yes, 0 = no.  Indicates that at startup, an attempt will be made to determine if the instance was cloned from another VM.'  UNION
	SELECT 'Alert DB Owner Changed', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent when the owner of a database is changed.' UNION
	SELECT 'Alert DBCC Command Issued', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent when a DBCC command is issued.' UNION
	SELECT 'Audit Security Events', '', '(BIT) 1 = yes, 0 = no.  Indicates that security-related events are to be logged to a table. (CREATE/ALTER/DROP USERs/LOGINs/ROLEs, ADD/DROP ROLE MEMBER, GRANT, DENY, REVOKE, etc.)' UNION
	SELECT 'Alert Security Events', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent for security-related events. (CREATE/ALTER/DROP USERs/LOGINs/ROLEs, ADD/DROP ROLE MEMBER, GRANT, DENY, REVOKE, etc.)' UNION
	SELECT 'Backup To Disk - Archive', '', '(BIT) 1 = yes, 0 = no. Indicates if databases are to be backed up to disk for archive purposes.' UNION 
	SELECT 'Backup Path - Archive', '', 'Local or UNC path where FULL database backups will be created for archive purposes.' UNION
	SELECT 'Alert Instance Altered', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent for changes to global configuration settings on the SQL instance.' UNION
	SELECT 'Backup Retention Period Expression', '', 'A DATEADD() expression indicating how far back in time backup files (excluding archives) are to be kept on disk.  ie. DATEADD(wk, -14, CURRENT_TIMESTAMP)' UNION
	SELECT 'Backup WITH ENCRYPTION', '', '(BIT) 1 = yes, 0 = no.  Indicates that backups are to use native SQL Server encryption.' UNION
	SELECT 'Backup Encryption - Server Certificate', '', 'Name of the server certificate to be used for backup encryption.' UNION
	SELECT 'Alert Missing Backup Files', '', '(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent when missing backup files are discovered.' 
)
INSERT INTO dba.InstanceConfiguration (Name, Value, [Description])
SELECT Nm, Val, Descr
FROM ConfigurationItems ci
WHERE NOT EXISTS (
	SELECT *
	FROM dba.InstanceConfiguration c
	WHERE c.Name = ci.Nm
)
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dba' 
	AND r.ROUTINE_NAME = 'GetInstanceConfiguration' 
	AND r.ROUTINE_TYPE = 'FUNCTION'
)
	DROP FUNCTION dba.GetInstanceConfiguration
GO

CREATE FUNCTION dba.GetInstanceConfiguration
(
	@Name VARCHAR(128)
)
RETURNS VARCHAR(512)
AS
/*
	Purpose:	
	Returns a config value based on the input config name.
	
	Inputs:
	@Name - name of the config item.
	
	History:
	09/19/2014	DBA	Created
*/
BEGIN
	DECLARE @Value VARCHAR(512) 
	SET @Value = ''

	SELECT @Value = Value
	FROM dba.InstanceConfiguration ic
	WHERE ic.Name = @Name

	--IF @Value = ''
	--BEGIN
	--	DECLARE @Msg VARCHAR(MAX)
	--	SET @Msg = 'Instance Configuration item "' + @Name + '" does not exist.'
	--	RAISERROR(@Msg, 16, 1)
	--END

	RETURN @Value
END
GO
