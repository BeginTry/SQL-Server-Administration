USE DbaData
GO

/*
	Supply a value for every instance configuration item.
	These values will be used dynamically in subsequent scripts.
	
	/*	Query to generate WHEN clauses	*/
	SELECT 
		CHAR(9) + CHAR(9) + '--' + Description + CHAR(13) + CHAR(10) +
		CHAR(9) + CHAR(9) + 'WHEN Name = ''' + Name + ''' THEN ''''' + CHAR(13) + CHAR(10)
	FROM dba.InstanceConfiguration
	--WHERE value = ''
	ORDER BY InstanceConfigurationId
*/
DECLARE @IP VARCHAR(256)	--IP address of Data Domain Virtual Interface.

SELECT TOP(1) @IP = CAST(CONNECTIONPROPERTY('local_net_address') AS VARCHAR);

SET @IP = PARSENAME(@IP, 4) + '.' + PARSENAME(@IP, 3) + '.' + PARSENAME(@IP, 2) 
SELECT @IP = COALESCE(@IP,'')

SET @IP = @IP +
	CASE
		--Network segments for Internal/Infrastructure SQL Server hosts.
		WHEN @IP LIKE '172.30.%' THEN '.202'

		--Network segments for domain customer "bubbles".
		WHEN @IP LIKE '10.30.%' THEN '.62'

		ELSE '.62'
	END

--Custom logic for DBA's management instance.
IF @IP LIKE '169.254.13.%' 
	SET @IP = '172.30.20.202'

--Logic for SQL instances in LKM. These will all backup to the same IP address 
--(the LKM Data Domain device), with traffic passing through a firewall.
ELSE IF @IP LIKE '172.20.%'
	SET @IP = '172.20.50.200'

PRINT '@IP = ' + @IP


DECLARE @BackupEncryptionSupported BIT = 1;

--Backup encryption is supported beginning with SQL 2014.
IF CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR), 4) AS SMALLINT) < 12
	SET @BackupEncryptionSupported = 0;
--SQL Server Express Editions do not support encryption during backup.
ELSE IF CAST(SERVERPROPERTY('EngineEdition') AS SMALLINT) = 4 
	SET @BackupEncryptionSupported = 0;
--SQL Server Web Edition does not support encryption during backup.
ELSE IF CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR) LIKE 'Web%' 
	SET @BackupEncryptionSupported = 0;


--Optional WHERE clause below (helpful when adding new config items to existing instances).
BEGIN TRAN

UPDATE dba.InstanceConfiguration
SET Value = 
	CASE
		--(BIT) 1 = yes, 0 = no. Indicates if databases are to be backed up to disk (as opposed by backup via network backup software (CommVault, Symantec Backup Exec, etc.)
		WHEN Name = 'Backup To Disk' THEN '1'

		--(INT) The number of days worth of backup files to keep on disk.
		WHEN Name = 'Backup File Keep Days' THEN '27'

		--(INT) The amount of space (in MB) remaining on a fixed disk that will invoke an alert/notification.
		WHEN Name = 'Available Disk Space Threshold' THEN '2048'

		--The name of the SQL Agent Operator for the domain/DBA team.  (ie aspinfrastructure, chico backups, customer-specific, etc.)
		WHEN Name = 'domain DBA Team Operator Name' THEN 'domain DBA Team'

		--Email address(es) of the SQL Agent Operator (usually a distribution group, but could be list of semi-colon seperated addresses).
		WHEN Name = 'domain DBA Team Operator Email' THEN 'DbaTeam@YourDomain.com;TechSupport@YourDomain.com'

		--Local or UNC path where FULL database backups will be created.
		WHEN Name = 'Backup Path - FULL' THEN '\\' + @IP + '\Backup Path\' + DEFAULT_DOMAIN() + '\' + @@SERVERNAME + '\Full'

		--Local or UNC path where DIFFERENTIAL database backups will be created.
		WHEN Name = 'Backup Path - DIFFERENTIAL' THEN '\\' + @IP + '\Backup Path\' + DEFAULT_DOMAIN() + '\' + @@SERVERNAME + '\Differential'

		--Local or UNC path where database LOG backups will be created.
		--WHEN Name = 'Backup Path - LOG' THEN '\\Inf-BakDD001.domain.lcl\Backup Path\' + DEFAULT_DOMAIN() + '\' + @@SERVERNAME + '\Trx Log'
		WHEN Name = 'Backup Path - LOG' THEN '\\' + @IP + '\Backup Path\' + DEFAULT_DOMAIN() + '\' + @@SERVERNAME + '\Trx Log'

		--(INT) Number of days of backup history to keep (in [msdb]).
		WHEN Name = 'Backup History Keep Days' THEN '1120'	--3 years, plus some wiggle room.

		--(INT) Number of days of SQL Agent Job history to keep (in [msdb]).
		WHEN Name = 'Job History Keep Days' THEN '370'	--1 year, plus some wiggle room.

		--(BIT) 1 = yes, 0 = no. Indicates if ALTER DATABASE is allowed by non-dba's
		WHEN Name = 'Allow ALTER DATABASE' THEN '0'

		--csv list of logins that are permitted to run ALTER DATABASE commands.
		WHEN Name = 'ALTER DATABASE - Allowed Logins' THEN 'domain\DBA,domain\mssqladmin'

		--Don't change the value for this config item here.  It may have been 
		--set explicitly in another script and the value must stay intact.
		WHEN Name = 'ALTER DATABASE - Temporary Allowed Logins' THEN Value

		--(BIT) 1 = yes, 0 = no. Indicates if DROP DATABASE is allowed by non-dba's
		WHEN Name = 'Allow DROP DATABASE' THEN '0'

		--csv list of logins that are permitted to run DROP DATABASE commands.
		WHEN Name = 'DROP DATABASE - Allowed Logins' THEN 'domain\DBA,domain\mssqladmin'

		--(BIT) 1 = yes, 0 = no.  Indicates that server logins can be added to the following fixed server roles: SYSADMIN, SERVERADMIN, SECURITYADMIN, DISKADMIN, DBCREATOR
		WHEN Name = 'Allow Risky Server Roles' THEN '0'

		--csv list of logins that are permitted to add logins to "risky" server roles.
		WHEN Name = 'ADD SERVER ROLE - Allowed Logins' THEN 'domain\DBA,domain\mssqladmin'

		--(BIT) 1 = yes, 0 = no.  Indicates that database users can be added to the following fixed database roles: db_owner, db_accessadmin, db_backupoperator, db_securityadmin
		WHEN Name = 'Allow Risky Database Roles' THEN '0'

		--csv list of logins that are permitted to add users to "risky" database roles.
		WHEN Name = 'sp_addrolemember - Allowed Logins' THEN 'domain\DBA,domain\mssqladmin'


		--TODO: delete these three confuration items.
		--The associated email address of the default/public profile.
		WHEN Name = 'Database Mail Address' THEN 'MS SQL Administrator <MSSqlAlerts@Domain.com>'

		--The email address display name of the default/public profile.
		WHEN Name = 'Database Mail Display Name' THEN 'MS SQL Administrator'

		--The "Reply To" email address of the default/public profile.
		WHEN Name = 'Database Mail Reply To Address' THEN 'DoNotReply@Domain.com'





		--NEW: mail profile-specific configurations.
		--The email address of the "Default" mail account.
		WHEN Name = 'Database Mail Address - Default' THEN 'SQL Server Alerts <MSSqlAlerts@Domain.com>' 

		--The display name of the "Default" mail account.
		WHEN Name = 'Database Mail Display Name - Default' THEN 'SQL Server Alerts' 

		--The "Reply To" address of the "Default" mail account.
		WHEN Name = 'Database Mail Reply To Address - Default' THEN 'DoNotReply@Domain.com' 

		--The email address of the "DBA" mail account.
		WHEN Name = 'Database Mail Address - DBA' THEN 'Dave Mason (Lake Mary) <DBA@Domain.com>' 

		--The display name of the "DBA" mail account.
		WHEN Name = 'Database Mail Display Name - DBA' THEN 'Dave Mason (Lake Mary)' 

		--The "Reply To" address of the "DBA" mail account.
		WHEN Name = 'Database Mail Reply To Address - DBA' THEN 'DBA@Domain.com' 

		--The email address of the "Security" mail account.
		WHEN Name = 'Database Mail Address - Security' THEN 'SQL Server Security <MSSqlSecurity@Domain.com>' 

		--The display name of the "Security" mail account.
		WHEN Name = 'Database Mail Display Name - Security' THEN 'SQL Server Security' 

		--The "Reply To" address of the "Security" mail account.
		WHEN Name = 'Database Mail Reply To Address - Security' THEN 'DoNotReply@Domain.com' 








		--The name (or IP address) of the SMTP server for the default/public profile.
		WHEN Name = 'Database Mail Server' THEN 'SMTP.domain.com'

		--(BIT) 1 = yes, 0 = no.  Indicates if the SMTP Mail Server requires authentication.
		WHEN Name = 'Database Mail Anonymous Authentication' THEN '1'

		--(BIT) 1 = yes, 0 = no.  Indicates if notifications for Backup/Restore alerts are to be sent.
		WHEN Name = 'Alert Backup/Restore' THEN '1'

		--(BIT) 1 = yes, 0 = no.  Indicates that an alert is to be sent when SQL Server (re)starts.
		WHEN Name = 'Alert Startup' THEN '1'

		--(BIT) 1 = yes, 0 = no.  Indicates that an alert is to be sent when service account logins are changed.
		WHEN Name = 'Alert Service Login Changed' THEN '1'

		--Name of the Windows Login for the main SQL Server service.
		WHEN Name = 'SQL DB Engine Login' THEN 'domain\mssqladmin, domain.pri\MSSqlAdmin, MSSQLAdmin@domain.pri'

		--Name of the Windows Login for the SQL Agent service.
		WHEN Name = 'SQL Agent Login' THEN 'domain\mssqladmin, domain.pri\MSSqlAdmin, MSSQLAdmin@domain.pri'

		--(BIT) 1 = yes, 0 = no.  Indicates that email alerts are to be sent for logins via [sa] (failed or successful).
		WHEN Name = 'Alert [sa] Login' THEN '1'

		--csv list of global Trace Flags (integer values only) that are enabled each time SQL Server starts.
		WHEN Name = 'Trace Flags' THEN '1118, 3226, 2371'

		--(BIT) 1 = yes, 0 = no.  Indicates that at startup, an attempt will be made to determine if the instance was cloned from another VM.
		WHEN Name = 'Cloned Server Check' THEN '1'

		--(BIT) 1 = yes, 0 = no.  Indicates that an alert is to be sent when the owner of a database changes.
		WHEN Name = 'Alert DB Owner Changed' THEN '1'

		--(BIT) 1 = yes, 0 = no.  Indicates that an alert is to be sent when a DBCC command is issued.
		WHEN Name = 'Alert DBCC Command Issued' THEN '1'

		--(BIT) 1 = yes, 0 = no.  Indicates that an alert is to be sent when a security-related event occurs. (CREATE/ALTER/DROP USERs/LOGINs/ROLEs, ADD/DROP ROLE MEMBER, GRANT, DENY, REVOKE, etc.)
		WHEN Name = 'Audit Security Events' THEN '1'

		--(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent for security-related events. (CREATE/ALTER/DROP USERs/LOGINs/ROLEs, ADD/DROP ROLE MEMBER, GRANT, DENY, REVOKE, etc.)
		WHEN Name = 'Alert Security Events' THEN '1'
		
		--(BIT) 1 = yes, 0 = no. Indicates if databases are to be backed up to disk for archive purposes.
		WHEN Name = 'Backup To Disk - Archive' THEN '1'

		--Local or UNC path where FULL database backups will be created for archive purposes.
		WHEN Name = 'Backup Path - Archive' THEN '\\' + @IP + '\Backup Path\' + DEFAULT_DOMAIN() + '\' + @@SERVERNAME + '\Archive'

		--(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent for changes to global configuration settings on the SQL instance.
		WHEN Name = 'Alert Instance Altered' THEN '1'

		--A DATEADD() expression indicating how far back in time backup files (excluding archives) are to be kept on disk.  ie. DATEADD(wk, -14, CURRENT_TIMESTAMP)
		--TODO: different values for customer PROD/TEST/DEV environments?
		WHEN Name = 'Backup Retention Period Expression' THEN 'DATEADD(wk, -14, CURRENT_TIMESTAMP)'

		--(BIT) 1 = yes, 0 = no.  Indicates that backups are to use native SQL Server encryption.
		--Use bitwise "AND" with flag indicating the version/edition supports the feature.
		WHEN Name = 'Backup WITH ENCRYPTION' THEN CAST(1 & @BackupEncryptionSupported AS VARCHAR)

		--Name of the server certificate to be used for backup encryption.
		WHEN Name = 'Backup Encryption - Server Certificate' THEN 'InfrastructureBackupEncryption' 

		--(BIT) 1 = yes, 0 = no.  Indicates that an email/alert is to be sent when missing backup files are discovered.
		WHEN Name = 'Alert Missing Backup Files' THEN '1'

		ELSE ''
	END
WHERE Value IS NULL OR Value = ''
GO

--Ensure all instance configuration values have been supplied.
IF EXISTS (
	SELECT *
	FROM DbaData.dba.InstanceConfiguration ic
	WHERE ic.Value = ''
	AND Name NOT IN ('ALTER DATABASE - Temporary Allowed Logins')
)
BEGIN
	SELECT *
	FROM DbaData.dba.InstanceConfiguration ic
	WHERE ic.Value = ''

	RAISERROR('One or more instance configuration values is missing.', 16, 1);
END
GO

/*
	COMMIT
	ROLLBACK
*/
