USE DbaData
GO

--Drop objects first before trying to create them (in specific sequence).
IF EXISTS (
	SELECT * 
	FROM sys.server_event_notifications 
	WHERE name = 'enChangeDBOwner'
)
	DROP EVENT NOTIFICATION enChangeDBOwner
	ON SERVER
GO

IF EXISTS (
	SELECT *
	FROM sys.services
	WHERE name = 'svcChangeDBOwnerNotification'
)
	DROP SERVICE svcChangeDBOwnerNotification;
GO

IF EXISTS (
	SELECT *
	FROM sys.service_queues
	WHERE name = 'queChangeDBOwnerNotification'
)
BEGIN
	ALTER QUEUE dbo.queChangeDBOwnerNotification 
	WITH STATUS = OFF;

	DROP QUEUE queChangeDBOwnerNotification;
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveChangeDBOwner'
)
	DROP PROCEDURE dbo.ReceiveChangeDBOwner 
GO

IF dba.GetInstanceConfiguration('Alert DB Owner Changed') = '1'
BEGIN
	--Create a queue just for file autogrowth events.
	CREATE QUEUE queChangeDBOwnerNotification

	--Create a service just for file autogrowth events.
	CREATE SERVICE svcChangeDBOwnerNotification
	ON QUEUE queChangeDBOwnerNotification ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])

	-- Create the event notification for file autogrowth events on the service.
	CREATE EVENT NOTIFICATION enChangeDBOwner
	ON SERVER
	WITH FAN_IN
	FOR AUDIT_CHANGE_DATABASE_OWNER
	TO SERVICE 'svcChangeDBOwnerNotification', 'current database';
END
GO

CREATE PROCEDURE dbo.ReceiveChangeDBOwner
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
				FROM queChangeDBOwnerNotification
		), TIMEOUT 1000  -- if the queue is empty for one second, give UPDATE and go away
		-- If we didn't get anything, bail out
		IF (@@ROWCOUNT = 0)
		BEGIN
			ROLLBACK TRANSACTION
			BREAK
		END 
		ELSE
		BEGIN
			--Do stuff here.
			DECLARE @MailBody NVARCHAR(MAX);
			DECLARE @Subject NVARCHAR(255);

			SET @Subject = @@SERVERNAME + ' -- ' + @MsgBody.value('(/EVENT_INSTANCE/EventType)[1]', 'VARCHAR(128)' );
			SET @MailBody = 
				'<table border="1">' +
				CAST (@MsgBody.query('
					for $X in /EVENT_INSTANCE/*
					return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
					') AS VARCHAR(MAX)) + 
				'</table><br/>';
			SET @MailBody = REPLACE(@MailBody, CHAR(13) + CHAR(10), '<br/>');
			--PRINT @Subject
			--PRINT @MailBody

			EXEC msdb.dbo.sp_send_dbmail 
				@recipients = 'DBA@Domain.com', 
				@profile_name = 'Security',
				@subject = @Subject,
				@body = @MailBody,
				@body_format = 'HTML',
				@exclude_query_output = 1

			--TODO: if auditing security events, 
			--INSERT INTO DbaData.dba.EventNotification(EventName, [EventData], EventDate)...

			/*
				Commit the transaction.  At any point before this, we 
				could roll back -- the received message would be back 
				on the queue AND the response wouldn't be sent.
			*/
			COMMIT TRANSACTION
		END
	END
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveChangeDBOwner'
)
AND dba.GetInstanceConfiguration('Alert DB Owner Changed') <> '1'
	DROP PROCEDURE dbo.ReceiveChangeDBOwner 
GO

IF dba.GetInstanceConfiguration('Alert DB Owner Changed') = '1'
BEGIN
	ALTER QUEUE dbo.queChangeDBOwnerNotification 
	WITH 
		STATUS = ON, 
		ACTIVATION ( 
			PROCEDURE_NAME = dbo.ReceiveChangeDBOwner, 
			STATUS = ON, 
			MAX_QUEUE_READERS = 1, 
			EXECUTE AS OWNER) 
END
GO

/*
-- Look at data held in the Queue
SELECT *, CAST(message_body AS XML) AS message_body_xml
FROM dbo.queChangeDBOwnerNotification
WHERE [service_name] = 'svcChangeDBOwnerNotification'

SELECT * FROM dba.TriggerEventData
*/