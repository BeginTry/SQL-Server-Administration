USE DbaData
GO

--Drop objects first before trying to create them (in specific sequence).
IF EXISTS (
	SELECT * 
	FROM sys.server_event_notifications 
	WHERE name = 'enLoginEvents'
)
	DROP EVENT NOTIFICATION enLoginEvents
	ON SERVER
GO

IF EXISTS (
	SELECT *
	FROM sys.services
	WHERE name = 'svcLoginNotification'
)
	DROP SERVICE svcLoginNotification;
GO

IF EXISTS (
	SELECT *
	FROM sys.service_queues
	WHERE name = 'queLoginNotification'
)
BEGIN
	ALTER QUEUE dbo.queLoginNotification 
	WITH STATUS = OFF;

	DROP QUEUE queLoginNotification;
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveLoginEvent'
)
	DROP PROCEDURE dbo.ReceiveLoginEvent 
GO

IF dba.GetInstanceConfiguration('Alert [sa] Login') = '1'
BEGIN
	IF EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_SCHEMA = 'dba'
		AND t.TABLE_NAME = 'LoginEvents'
	)
	BEGIN
		DROP TABLE dba.LoginEvents 
	END

	--Create a queue just for file autogrowth events.
	CREATE QUEUE queLoginNotification

	--Create a service just for file autogrowth events.
	CREATE SERVICE svcLoginNotification
	ON QUEUE queLoginNotification ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])

	-- Create the event notification for file autogrowth events on the service.
	CREATE EVENT NOTIFICATION enLoginEvents
	ON SERVER
	WITH FAN_IN
	FOR AUDIT_LOGIN, AUDIT_LOGIN_FAILED
	TO SERVICE 'svcLoginNotification', 'current database';
END
GO

CREATE PROCEDURE dbo.ReceiveLoginEvent
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @MsgBody XML

	WHILE (1 = 1)
	BEGIN
		BEGIN TRANSACTION

		-- Receive the next available message FROM the queue
		WAITFOR (
			RECEIVE TOP(1) -- just handle one message at a time
				@MsgBody = CAST(message_body AS XML)
				FROM queLoginNotification
		), TIMEOUT 1000  -- if the queue is empty for one second, give UPDATE and go away
		-- If we didn''t get anything, bail out
		IF (@@ROWCOUNT = 0)
		BEGIN
			ROLLBACK TRANSACTION
			BREAK
		END 
		ELSE
		BEGIN
			--Do stuff here.
			DECLARE @Login NVARCHAR(255)
			SET @Login = @MsgBody.value('(/EVENT_INSTANCE/LoginName)[1]', 'VARCHAR(128)')

			IF @Login COLLATE SQL_Latin1_General_CP1_CI_AS = 'sa'
			BEGIN
				DECLARE @MailBody NVARCHAR(MAX)
				DECLARE @Subject NVARCHAR(255)
				DECLARE @LoginMsg NVARCHAR(MAX)

				IF @MsgBody.value('(/EVENT_INSTANCE/Success)[1]', 'VARCHAR(128)' ) = '0'
					SET @LoginMsg = 'Login Failed'
				ELSE
					SET @LoginMsg = 'Login Succeeded'
				
				SET @Subject = @@SERVERNAME + ' - [sa] Login'
				SET @MailBody = 
					'<table border="1">' +
					CAST (@MsgBody.query('
						for $X in /EVENT_INSTANCE/*
						return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
						') AS VARCHAR(MAX)) + 
					'</table><br/>';
				SET @MailBody = '<p>' + @LoginMsg + '</p><br/>' + 
					REPLACE(@MailBody, CHAR(13) + CHAR(10), '<br/>');
				--PRINT @Subject
				--PRINT @MailBody

				EXEC msdb.dbo.sp_send_dbmail 
					@recipients = 'DBA@Domain.com', 
					@blind_copy_recipients = 'DbaPager@GMail.com',
					@profile_name = 'Security',
					@subject = @Subject,
					@body = @MailBody,
					@body_format = 'HTML',
					@exclude_query_output = 1
			END

			/*
				Commit the transaction.  At any point before this, we 
				could roll back -- the received message would be back 
				on the queue AND the response wouldn''t be sent.
			*/
			COMMIT TRANSACTION
		END
	END
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveLoginEvent'
)
AND dba.GetInstanceConfiguration('Alert [sa] Login') = '0'
	DROP PROCEDURE dbo.ReceiveLoginEvent 
GO

IF dba.GetInstanceConfiguration('Alert [sa] Login') = '1'
BEGIN
	ALTER QUEUE dbo.queLoginNotification 
	WITH 
		STATUS = ON, 
		ACTIVATION ( 
			PROCEDURE_NAME = dbo.ReceiveLoginEvent, 
			STATUS = ON, 
			MAX_QUEUE_READERS = 1, 
			EXECUTE AS OWNER) 
END
GO

/*
-- Look at data held in the Queue
SELECT *, CAST(message_body AS XML) AS message_body_xml
FROM dbo.queLoginNotification
WHERE [service_name] = 'svcLoginNotification'

SELECT *
FROM dba.LoginEvents
WHERE LastSuccessfulLogin = CAST('20150101' AS SMALLDATETIME)

SELECT *
FROM dba.LoginEvents
WHERE LastSuccessfulLogin > CAST('20150101' AS SMALLDATETIME)
*/