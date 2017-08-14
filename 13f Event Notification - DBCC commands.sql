USE DbaData
GO

--Drop objects first before trying to create them (in specific sequence).
IF EXISTS (
	SELECT * 
	FROM sys.server_event_notifications 
	WHERE name = 'enDbccCommand'
)
	DROP EVENT NOTIFICATION enDbccCommand
	ON SERVER
GO

IF EXISTS (
	SELECT *
	FROM sys.services
	WHERE name = 'svcDbccCommandNotification'
)
	DROP SERVICE svcDbccCommandNotification;
GO

IF EXISTS (
	SELECT *
	FROM sys.service_queues
	WHERE name = 'queDbccCommandNotification'
)
BEGIN
	ALTER QUEUE dbo.queDbccCommandNotification 
	WITH STATUS = OFF;

	DROP QUEUE queDbccCommandNotification;
END
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveDbccCommand'
)
	DROP PROCEDURE dbo.ReceiveDbccCommand 
GO

IF dba.GetInstanceConfiguration('Alert DBCC Command Issued') = '1'
BEGIN
	--Create a queue just for DBCC Command events.
	CREATE QUEUE queDbccCommandNotification

	--Create a service just for DBCC Command events.
	CREATE SERVICE svcDbccCommandNotification
	ON QUEUE queDbccCommandNotification ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])

	-- Create the event notification for DBCC Command events on the service.
	CREATE EVENT NOTIFICATION enDbccCommand
	ON SERVER
	WITH FAN_IN
	FOR AUDIT_DBCC_EVENT
	TO SERVICE 'svcDbccCommandNotification', 'current database';
END
GO

CREATE PROCEDURE dbo.ReceiveDbccCommand
/******************************************************************************
* Name     : dbo.ReceiveDbccCommand
* Purpose  : Handles DBCC events (activated by QUEUE queDbccCommandNotification)
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	??/??/2015	DBA	Created
*	08/03/2016	DBA	Ignore SHOW_STATISTICS
*	08/25/2016	DBA	Ignore event run by [sa] on a system process.
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
				FROM queDbccCommandNotification
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
			DECLARE @Login SYSNAME;
			DECLARE @Success BIT;
			DECLARE @IsSystem BIT;
			DECLARE @Cmd VARCHAR(1024);
			SET @Login = @MsgBody.value('(/EVENT_INSTANCE/LoginName)[1]', 'VARCHAR(128)' );
			SET @Success = @MsgBody.value('(/EVENT_INSTANCE/Success)[1]', 'VARCHAR(8)' );
			SET @IsSystem = @MsgBody.value('(/EVENT_INSTANCE/IsSystem)[1]', 'VARCHAR(8)' );
			SET @Cmd = @MsgBody.value('(/EVENT_INSTANCE/TextData)[1]', 'VARCHAR(1024)');

			--Only alert on attempts to run DBCC commands that succeeded.
			--Exclude the dba and any other logins that normally run DBCC.
			IF @Success = 1 
				AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE '%DBA%' 
				AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE '%Dave.Mason%'
				AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE '%MSSqlAdmin%'
				AND @Cmd COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE '%SHOW_STATISTICS%'
				AND (@Login <> 'sa' OR @IsSystem <> '1')
			BEGIN
				DECLARE @MailBody NVARCHAR(MAX);
				DECLARE @Subject NVARCHAR(255);

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
					@exclude_query_output = 1

				--INSERT INTO dba.TriggerEventData(EventDataXml)
				--VALUES (@MsgBody)
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

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveDbccCommand'
)
AND dba.GetInstanceConfiguration('Alert DBCC Command Issued') <> '1'
	DROP PROCEDURE dbo.ReceiveDbccCommand 
GO

IF dba.GetInstanceConfiguration('Alert DBCC Command Issued') = '1'
BEGIN
	ALTER QUEUE dbo.queDbccCommandNotification 
	WITH 
		STATUS = ON, 
		ACTIVATION ( 
			PROCEDURE_NAME = dbo.ReceiveDbccCommand, 
			STATUS = ON, 
			MAX_QUEUE_READERS = 1, 
			EXECUTE AS OWNER) 
END
GO

/*
-- Look at data held in the Queue
SELECT *, CAST(message_body AS XML) AS message_body_xml
FROM dbo.queDbccCommandNotification
WHERE [service_name] = 'svcDbccCommandNotification'

DBCC TRACESTATUS(-1)
DBCC CHECKDB(DbaData)

SELECT * FROM dba.TriggerEventData
*/