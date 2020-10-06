USE DbaData
GO

--Drop objects first before trying to create them (in specific sequence).
IF EXISTS (
	SELECT * 
	FROM sys.server_event_notifications 
	WHERE name = 'enDeadlock'
)
	DROP EVENT NOTIFICATION enDeadlock
	ON SERVER
GO

IF EXISTS (
	SELECT *
	FROM sys.services
	WHERE name = 'svcDeadlockNotification'
)
	DROP SERVICE svcDeadlockNotification;
GO

IF EXISTS (
	SELECT *
	FROM sys.service_queues
	WHERE name = 'queDeadlockNotification'
)
BEGIN
	ALTER QUEUE dbo.queDeadlockNotification 
	WITH STATUS = OFF;

	DROP QUEUE queDeadlockNotification;
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveDeadlock'
)
	DROP PROCEDURE dbo.ReceiveDeadlock 
GO

IF dba.GetInstanceConfiguration('Alert Deadlock') = '1'
BEGIN
	--Create a queue just for DBCC Command events.
	CREATE QUEUE queDeadlockNotification

	--Create a service just for DBCC Command events.
	CREATE SERVICE svcDeadlockNotification
	ON QUEUE queDeadlockNotification ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])

	-- Create the event notification for DBCC Command events on the service.
	CREATE EVENT NOTIFICATION enDeadlock
	ON SERVER
	WITH FAN_IN
	FOR DEADLOCK_GRAPH
	TO SERVICE 'svcDeadlockNotification', 'current database';
END
GO

CREATE PROCEDURE dbo.ReceiveDeadlock
/******************************************************************************
* Name     : dbo.ReceiveDeadlock
* Purpose  : Handles deadlock events (activated by QUEUE queDeadlockNotification)
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	06/26/2017	DBA	Created
******************************************************************************/
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
				FROM queDeadlockNotification
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
			DECLARE @Qry NVARCHAR(MAX);

			SET @Qry = CAST(@MsgBody.query('/EVENT_INSTANCE/TextData/deadlock-list') AS NVARCHAR(MAX));
			SET @Qry = REPLACE(@Qry, CHAR(39), CHAR(39) + CHAR(39));
			SET @Qry = 'SET NOCOUNT ON; ' + CHAR(13) + CHAR(10) +
				'SELECT ' + CHAR(39) + @Qry + CHAR(39);
			
			SET @Subject = @@SERVERNAME + ' -- ' + @MsgBody.value('(/EVENT_INSTANCE/EventType)[1]', 'VARCHAR(128)' )	
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
				@profile_name = 'Default',
				@subject = @Subject,
				@body = @MailBody,
				@body_format = 'HTML',
				@query = @Qry,
				@attach_query_result_as_file = 1,
				@query_attachment_filename = 'Deadlock Graph.xdl',
				@query_no_truncate = 1,
				@query_result_width = 32767,
				@exclude_query_output = 1

			INSERT INTO DbaData.dba.EventNotification(EventName, [EventData])
			VALUES ('DEADLOCK_GRAPH', @MsgBody)

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

IF dba.GetInstanceConfiguration('Alert Deadlock') = '1'
BEGIN
	ALTER QUEUE dbo.queDeadlockNotification 
	WITH 
		STATUS = ON, 
		ACTIVATION ( 
			PROCEDURE_NAME = dbo.ReceiveDeadlock, 
			STATUS = ON, 
			MAX_QUEUE_READERS = 1, 
			EXECUTE AS OWNER) 
END
GO

/*
-- Look at data held in the Queue
SELECT *, CAST(message_body AS XML) AS message_body_xml
FROM dbo.queDeadlockNotification
WHERE [service_name] = 'svcDeadlockNotification'

SELECT * FROM DbaData.dba.EventNotification
*/
