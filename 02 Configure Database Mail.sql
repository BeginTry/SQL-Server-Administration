/*
	http://www.databasejournal.com/features/mssql/article.php/3626056/Database-Mail-in-SQL-Server-2005.htm

	Other Notes:
	In SSMS, right-click "SQL Server Agent" and select properties.
	Select the "Alert System" page and check "Enable Mail Profile".
	This may require a restart of the SQL Server Agent service.

	History
	DBA	02-07-2014	Revised for compatibility with 2008 R2 Express Edition.
						Presumably, the script will work with Std and Ent Editions.
						(The script may also still be compatible with prev SQL versions.)
*/
--use master
--GO

IF EXISTS (
	SELECT * 
	FROM master.sys.databases d 
	WHERE d.name = 'msdb' 
	AND COALESCE(d.is_broker_enabled, 0) = 0
)
	ALTER DATABASE msdb SET ENABLE_BROKER ;
GO

IF NOT EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'show advanced options'
	AND value = 1
)
BEGIN
	EXEC sys.sp_configure 'show advanced options', 1;
	RECONFIGURE WITH OVERRIDE;
END
GO

IF NOT EXISTS (
	SELECT *
	FROM sys.configurations
	WHERE name = 'Database Mail XPs'
	AND value = 1
)
BEGIN
	EXEC sys.sp_configure 'Database Mail XPs', 1;
	RECONFIGURE WITH OVERRIDE;
END
GO

--Create mail profiles.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profile p
	WHERE p.name = 'Default'
)
	EXECUTE msdb.dbo.sysmail_add_profile_sp
		@profile_name = 'Default',
		@description = 'Default profile for database mail'
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profile p
	WHERE p.name = 'DBA'
)
	EXECUTE msdb.dbo.sysmail_add_profile_sp
		@profile_name = 'DBA',
		@description = 'Profile for database mail from DBA'
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profile p
	WHERE p.name = 'Security'
)
	EXECUTE msdb.dbo.sysmail_add_profile_sp
		@profile_name = 'Security',
		@description = 'Profile for security-related database mail'
GO

DECLARE @Anon BIT
DECLARE @SmtpServer SYSNAME
DECLARE @EmailUser NVARCHAR(128)
DECLARE @EmailPwd NVARCHAR(128)

SELECT
	@Anon = CAST(DbaData.dba.GetInstanceConfiguration('Database Mail Anonymous Authentication') AS BIT),
	@SmtpServer = DbaData.dba.GetInstanceConfiguration('Database Mail Server');

IF @Anon = 0
BEGIN
	--TODO:  If authentication to the SMTP server 
	--is required, enter credentials below.
	SET @EmailUser = '';
	SET @EmailPwd = '';

	IF @EmailUser = '' OR @EmailPwd = ''
	BEGIN
		RAISERROR('You must specify SMTP server authentication values for @EmailUser and @EmailPwd.', 20, 1) WITH LOG;
		RETURN;
	END
END

DECLARE @Address NVARCHAR(128);
DECLARE @DisplayName NVARCHAR(128);
DECLARE @ReplyTo NVARCHAR(128);

--Create mail accounts.
SELECT
	@Address = DbaData.dba.GetInstanceConfiguration('Database Mail Address - Default'),
	@DisplayName = DbaData.dba.GetInstanceConfiguration('Database Mail Display Name - Default'),
	@ReplyTo = DbaData.dba.GetInstanceConfiguration('Database Mail Reply To Address - Default');

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_account a
	WHERE a.name = 'Default'
)
BEGIN
	EXECUTE msdb.dbo.sysmail_add_account_sp
		@account_name = 'Default',
		@email_address = @Address,
		@display_name = @DisplayName,
		@replyto_address = @ReplyTo,
		@description = 'Default account for Database Mail',
		@mailserver_name = @SmtpServer,
		--If anonymous authentication is used, these two param values will be NULL.
		@username = @EmailUser,
		@password = @EmailPwd
END

SELECT
	@Address = DbaData.dba.GetInstanceConfiguration('Database Mail Address - DBA'),
	@DisplayName = DbaData.dba.GetInstanceConfiguration('Database Mail Display Name - DBA'),
	@ReplyTo = DbaData.dba.GetInstanceConfiguration('Database Mail Reply To Address - DBA');

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_account a
	WHERE a.name = 'DBA'
)
BEGIN
	EXECUTE msdb.dbo.sysmail_add_account_sp
		@account_name = 'DBA',
		@email_address = @Address,
		@display_name = @DisplayName,
		@replyto_address = @ReplyTo,
		@description = 'Account for database mail from DBA',
		@mailserver_name = @SmtpServer,
		--If anonymous authentication is used, these two param values will be NULL.
		@username = @EmailUser,
		@password = @EmailPwd
END

SELECT
	@Address = DbaData.dba.GetInstanceConfiguration('Database Mail Address - Security'),
	@DisplayName = DbaData.dba.GetInstanceConfiguration('Database Mail Display Name - Security'),
	@ReplyTo = DbaData.dba.GetInstanceConfiguration('Database Mail Reply To Address - Security');

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_account a
	WHERE a.name = 'Security'
)
BEGIN
	EXECUTE msdb.dbo.sysmail_add_account_sp
		@account_name = 'Security',
		@email_address = @Address,
		@display_name = @DisplayName,
		@replyto_address = @ReplyTo,
		@description = 'Account for security-related database mail',
		@mailserver_name = @SmtpServer,
		--If anonymous authentication is used, these two param values will be NULL.
		@username = @EmailUser,
		@password = @EmailPwd
END

/*
	--Change email addresses as needed.

	EXEC msdb.dbo.sysmail_update_account_sp 
		@account_name = 'Default', 
		@email_address = 'SQL Server Alerts <MSSqlAlerts@Domain.com>';

	EXEC msdb.dbo.sysmail_update_account_sp 
		@account_name = 'DBA', 
		@email_address = 'Dave Mason (Lake Mary) <DBA@Domain.com>';

	EXEC msdb.dbo.sysmail_update_account_sp 
		@account_name = 'Security', 
		@email_address = 'SQL Server Security <MSSqlSecurity@Domain.com>';
*/

--Add accounts to profiles.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profileaccount pa
	JOIN msdb.dbo.sysmail_profile p
		ON p.profile_id = pa.profile_id
	JOIN msdb.dbo.sysmail_account a
		ON a.account_id = pa.account_id
	WHERE p.name = 'Default'
	AND a.name = 'Default'
)
	EXECUTE msdb.dbo.sysmail_add_profileaccount_sp
		@profile_name = 'Default',
		@account_name = 'Default',
		@sequence_number = 1;

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profileaccount pa
	JOIN msdb.dbo.sysmail_profile p
		ON p.profile_id = pa.profile_id
	JOIN msdb.dbo.sysmail_account a
		ON a.account_id = pa.account_id
	WHERE p.name = 'DBA'
	AND a.name = 'DBA'
)
	EXECUTE msdb.dbo.sysmail_add_profileaccount_sp
		@profile_name = 'DBA',
		@account_name = 'DBA',
		@sequence_number = 1;

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profileaccount pa
	JOIN msdb.dbo.sysmail_profile p
		ON p.profile_id = pa.profile_id
	JOIN msdb.dbo.sysmail_account a
		ON a.account_id = pa.account_id
	WHERE p.name = 'Security'
	AND a.name = 'Security'
)
	EXECUTE msdb.dbo.sysmail_add_profileaccount_sp
		@profile_name = 'Security',
		@account_name = 'Security',
		@sequence_number = 1;
GO

--Grant permission to use profiles.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profile p
	JOIN msdb.dbo.sysmail_principalprofile pp
		ON pp.profile_id = p.profile_id
	WHERE p.name = 'Default'
)
	EXECUTE msdb.dbo.sysmail_add_principalprofile_sp
		@profile_name = 'Default',
		@principal_name = 'public',
		@is_default = 1 ;
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profile p
	JOIN msdb.dbo.sysmail_principalprofile pp
		ON pp.profile_id = p.profile_id
	WHERE p.name = 'DBA'
)
	EXECUTE msdb.dbo.sysmail_add_principalprofile_sp
		@profile_name = 'DBA',
		@principal_name = 'public',
		@is_default = 0 ;
GO

IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysmail_profile p
	JOIN msdb.dbo.sysmail_principalprofile pp
		ON pp.profile_id = p.profile_id
	WHERE p.name = 'Security'
)
	EXECUTE msdb.dbo.sysmail_add_principalprofile_sp
		@profile_name = 'Security',
		@principal_name = 'public',
		@is_default = 0 ;
GO

--/*
--	Create operators.  These can be used when tsql code needs to send an email for various reasons.
--*/

DECLARE @domainOperatorName SYSNAME
DECLARE @domainOperatorEmail NVARCHAR(100)

SELECT
	@domainOperatorName = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name'),
	@domainOperatorEmail = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Email')

--General errors/notifications
IF NOT EXISTS (
	SELECT * 
	FROM msdb.dbo.sysoperators o
	WHERE o.name = @domainOperatorName
)
	EXEC msdb.dbo.sp_add_operator 
		@name=@domainOperatorName, 
		--@name=N'domain DBA Team', 
		@enabled=1, 
		@weekday_pager_start_time=0, 
		@weekday_pager_end_time=235959, 
		@saturday_pager_start_time=0, 
		@saturday_pager_end_time=235959, 
		@sunday_pager_start_time=0, 
		@sunday_pager_end_time=235959, 
		@pager_days=127, 
		@email_address=@domainOperatorEmail,
		@pager_address =@domainOperatorEmail,
		@netsend_address =@domainOperatorEmail
GO

IF NOT EXISTS (
	SELECT * 
	FROM msdb.dbo.sysoperators o
	WHERE o.name = 'Dave Mason'
)
	EXEC msdb.dbo.sp_add_operator 
		@name=N'Dave Mason', 
		@enabled=1, 
		@weekday_pager_start_time=0, 
		@weekday_pager_end_time=235959, 
		@saturday_pager_start_time=0, 
		@saturday_pager_end_time=235959, 
		@sunday_pager_start_time=0, 
		@sunday_pager_end_time=235959, 
		@pager_days=127, 
		@email_address=N'DBA@Domain.com', 
		@pager_address =N'DbaPager@Gmail.com',
		@netsend_address =N'DbaPager@Gmail.com'
GO

EXEC msdb.dbo.sysmail_stop_sp;
GO
EXEC msdb.dbo.sysmail_start_sp;
GO

/*

	EXEC msdb.dbo.sysmail_delete_principalprofile_sp @profile_name = 'SQL Mail Profile'
	EXEC msdb.dbo.sysmail_delete_profileaccount_sp  @profile_name = 'SQL Mail Profile'
	EXEC msdb.dbo.sysmail_delete_account_sp @account_name = 'Sql Mail Account'
	
	DECLARE @Now DATETIME = CURRENT_TIMESTAMP
	EXEC msdb..sysmail_delete_mailitems_sp @sent_before = @Now
	
	--Test Email
	DECLARE @Subj NVARCHAR(255) 
	SET @Subj = @@SERVERNAME + ' - test email'
	EXEC msdb..sp_send_dbmail
		--@from_address = 'MS SQL Administrator <MSSqlAlerts@Domain.com>',
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'Default',
		@subject = @Subj,
		@body = 'Test email (Default)...',
		@exclude_query_output = 1
	EXEC msdb..sp_send_dbmail
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'DBA',
		@subject = @Subj,
		@body = 'Test email (DBA)...',
		@exclude_query_output = 1
	EXEC msdb..sp_send_dbmail
		@recipients = 'DBA@Domain.com', 
		@profile_name = 'Security',
		@subject = @Subj,
		@body = 'Test email (Security)...',
		@exclude_query_output = 1

	EXEC msdb..sysmail_help_queue_sp -- @queue_type = 'Mail' 
	SELECT * FROM msdb.dbo.sysmail_faileditems
	SELECT * FROM msdb.dbo.sysmail_allitems

	EXEC msdb.dbo.sysmail_stop_sp;
	GO
	EXEC msdb.dbo.sysmail_start_sp;
	GO
*/