USE DbaData
GO

--Drop objects first before trying to create them (in specific sequence).
IF EXISTS (
	SELECT * 
	FROM sys.server_event_notifications 
	WHERE name = 'enDeprecation'
)
	DROP EVENT NOTIFICATION enDeprecation
	ON SERVER
GO

IF EXISTS (
	SELECT *
	FROM sys.services
	WHERE name = 'svcDeprecation'
)
	DROP SERVICE svcDeprecation;
GO

IF EXISTS (
	SELECT *
	FROM sys.service_queues
	WHERE name = 'queDeprecationEvents'
)
BEGIN
	ALTER QUEUE dbo.queDeprecationEvents 
	WITH STATUS = OFF;

	DROP QUEUE queDeprecationEvents;
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveDeprecationEvent'
)
	DROP PROCEDURE dbo.ReceiveDeprecationEvent 
GO

--IF dba.GetInstanceConfiguration('Alert Deprecation Event') = '1'
--BEGIN
	--Create a queue just for file deprecation events.
	CREATE QUEUE queDeprecationEvents;

	--Create a service just for file deprecation events.
	CREATE SERVICE svcDeprecation
	ON QUEUE queDeprecationEvents ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification]);

	-- Create the event notification for file deprecation events on the service.
	CREATE EVENT NOTIFICATION enDeprecation
	ON SERVER
	WITH FAN_IN
	FOR TRC_DEPRECATION	--includes DEPRECATION_ANNOUNCEMENT, DEPRECATION_FINAL_SUPPORT
	TO SERVICE 'svcDeprecation', 'current database';
--END
GO

CREATE PROCEDURE dbo.ReceiveDeprecationEvent
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
				FROM queDeprecationEvents
		), TIMEOUT 1000  -- if the queue is empty for one second, give UPDATE and go away
		-- If we didn't get anything, bail out
		IF (@@ROWCOUNT = 0)
		BEGIN
			ROLLBACK TRANSACTION
			BREAK
		END 
		ELSE
		BEGIN
			DECLARE @DBName SYSNAME = @MsgBody.value('(/EVENT_INSTANCE/DatabaseName)[1]', 'NVARCHAR(128)' );

			IF @DBName NOT IN ('msdb')
			BEGIN
				--Do stuff here.
				DECLARE @MailBody NVARCHAR(MAX);
				DECLARE @Subject NVARCHAR(255);
				DECLARE @EventType VARCHAR(128) = @MsgBody.value('(/EVENT_INSTANCE/EventType)[1]', 'VARCHAR(128)' );

				SET @Subject = @@SERVERNAME + ' -- ' + @EventType;	
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
					@profile_name = 'DBA',
					@subject = @Subject,
					@body = @MailBody,
					@body_format = 'HTML',
					@exclude_query_output = 1

				--TODO: if auditing security events
				BEGIN TRAN
					INSERT INTO DbaData.dba.EventNotification(EventName, [EventData])
					VALUES (@EventType, @MsgBody);
				COMMIT
			END
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

--IF EXISTS (
--	SELECT *
--	FROM INFORMATION_SCHEMA.ROUTINES r
--	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveDeprecationEvent'
--)
--AND dba.GetInstanceConfiguration('Alert Deprecation Event') <> '1'
--	DROP PROCEDURE dbo.ReceiveDeprecationEvent 
--GO

--IF dba.GetInstanceConfiguration('Alert Deprecation Event') = '1'
--BEGIN
	ALTER QUEUE dbo.queDeprecationEvents 
	WITH 
		STATUS = ON, 
		ACTIVATION ( 
			PROCEDURE_NAME = dbo.ReceiveDeprecationEvent, 
			STATUS = ON, 
			MAX_QUEUE_READERS = 1, 
			EXECUTE AS OWNER) 
--END
GO

/*
-- Look at data held in the Queue
SELECT *, CAST(message_body AS XML) AS message_body_xml
FROM dbo.queDeprecationEvents
WHERE [service_name] = 'svcDeprecation'

*/