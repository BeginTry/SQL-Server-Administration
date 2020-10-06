USE master;
GO

IF DbaData.dba.GetInstanceConfiguration('Backup WITH ENCRYPTION') = '1'
BEGIN

	--<SeeNPM Note="Get this block of TSQL from NPM, which includes the passwords.">
	RAISERROR('Certificate path\file and encryption passwords must be retrieved from NPM.', 20, 1) WITH LOG;

	IF NOT EXISTS (
		SELECT * 
		FROM master.sys.symmetric_keys k
		WHERE k.name = '##MS_DatabaseMasterKey##'
	)
	BEGIN
		CREATE MASTER KEY
		ENCRYPTION BY PASSWORD = 'Replace Me';
	END

	IF NOT EXISTS (
		SELECT *
		FROM sys.certificates c
		WHERE c.name = 'InfrastructureBackupEncryption'
	)
	BEGIN
		CREATE CERTIFICATE InfrastructureBackupEncryption
		FROM FILE = 'Replace Me'
		WITH PRIVATE KEY
		(
			FILE = 'Replace Me',
			DECRYPTION BY PASSWORD = 'Replace Me'
		)
	END
	--</SeeNPM>

END

