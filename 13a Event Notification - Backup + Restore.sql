USE DbaData
GO

--Drop objects first before trying to create them (in specific sequence).
IF EXISTS (
	SELECT *
	FROM sys.services
	WHERE name = 'svcBackupRestoreNotification'
)
	DROP SERVICE svcBackupRestoreNotification;
GO

IF EXISTS (
	SELECT *
	FROM sys.service_queues
	WHERE name = 'queBackupRestoreNotification'
)
	DROP QUEUE queBackupRestoreNotification;
GO

IF EXISTS (
	SELECT * 
	FROM sys.server_event_notifications 
	WHERE name = 'enBackupRestoreEvents'
)
	DROP EVENT NOTIFICATION enBackupRestoreEvents
	ON SERVER
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveBackupRestoreEvent'
)
	DROP PROCEDURE dbo.ReceiveBackupRestoreEvent 
GO

IF dba.GetInstanceConfiguration('Alert Backup/Restore') = '1'
BEGIN
	--Create a queue just for backup/restore events.
	CREATE QUEUE queBackupRestoreNotification

	--Create a service just for backup/restore events.
	CREATE SERVICE svcBackupRestoreNotification
	ON QUEUE queBackupRestoreNotification ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])

	-- Create the event notification for backup/restore events on the service.
	CREATE EVENT NOTIFICATION enBackupRestoreEvents
	ON SERVER
	WITH FAN_IN
	FOR AUDIT_BACKUP_RESTORE_EVENT
	TO SERVICE 'svcBackupRestoreNotification', 'current database';

	DECLARE @Tsql NVARCHAR(MAX)
	SET @Tsql = '
CREATE PROCEDURE dbo.ReceiveBackupRestoreEvent
/*****************************************************************************
* Name     : dbo.ReceiveBackupRestoreEvent
* Purpose  : Runs when there is an AUDIT_BACKUP_RESTORE_EVENT
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	12/15/2014	DBA	Created
*	11/03/2015	DBA	Additional logic to account for the asynchronous nature
*		of AUDIT_BACKUP_RESTORE_EVENT, restore commands WITH NORECOVERY, DB''s
*		that don''t fully restore to a status of ONLINE, etc.
*	12/16/2016	DBA	Add check for service broker error message.
*		Add TRY/CATCH error handling.
*		Rewrite message processing logic (STEP 4).
******************************************************************************/
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @MsgType SYSNAME;
	DECLARE @MsgBody XML;
	DECLARE @RowCount INT;

	WHILE (1 = 1)
	BEGIN
		BEGIN TRANSACTION

		-- STEP 1: receive next available message from the queue.
		WAITFOR (
			RECEIVE TOP(1) 
				@MsgType = [message_type_name],
				@MsgBody = CAST(message_body AS XML)
			FROM queBackupRestoreNotification
		), TIMEOUT 1000  -- if the queue is empty for one second, give UPDATE and go away

		-- STEP 2: if we didn''t get anything, bail out
		SET @RowCount = @@ROWCOUNT;

		IF (@RowCount = 0)
		BEGIN
			ROLLBACK TRANSACTION
			BREAK
		END 

		-- STEP 3: check for service broker error message
		ELSE IF @MsgType = N''http://schemas.microsoft.com/SQL/ServiceBroker/Error''
		BEGIN
			/*
				Raise an error (WITH LOG so the XML/Message is written to the error log).
				Commit the transaction--the received message shouldn''t go back on the queue.
			*/
			DECLARE @Msg VARCHAR(MAX) = CAST(@MsgBody AS VARCHAR(MAX));
			RAISERROR (N''Received error %s from service [Target]'', 10, 1, @Msg) WITH LOG;
			COMMIT TRANSACTION
		END

		-- STEP 4: process the message
		ELSE
		BEGIN
			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;

			BEGIN TRY
				DECLARE @Login NVARCHAR(255) = @MsgBody.value(''(/EVENT_INSTANCE/LoginName)[1]'', ''VARCHAR(128)'' );
				DECLARE @DB SYSNAME = @MsgBody.value(''(/EVENT_INSTANCE/DatabaseName)[1]'', ''VARCHAR(128)'' );
				DECLARE @DBState NVARCHAR(60);
				DECLARE @TsqlCmd NVARCHAR(MAX) = @MsgBody.value(''(/EVENT_INSTANCE/TextData)[1]'', ''VARCHAR(8000)'');
				DECLARE @EventSubClass INT = @MsgBody.value(''(/EVENT_INSTANCE/EventSubClass)[1]'', ''INT'' );
				DECLARE @MailBody NVARCHAR(MAX);
				DECLARE @Subject NVARCHAR(255) = @@SERVERNAME + '' -- '' + @MsgBody.value(''(/EVENT_INSTANCE/EventType)[1]'', ''VARCHAR(128)'' );
				DECLARE @SendAlert BIT = 0;

				SELECT @DBState = d.state_desc
				FROM master.sys.databases d
				WHERE d.name = @DB

				SET @MailBody = 
					''<table border="1">'' +
					CAST (@MsgBody.query(''
						for $X in /EVENT_INSTANCE/*
						return <tr><td>{string(local-name($X))}</td><td>{string($X)}</td></tr>
						'') AS VARCHAR(MAX)) + 
					''</table><br/>'';
				SET @MailBody = REPLACE(@MailBody, CHAR(13) + CHAR(10), ''<br/>'');

				--PRINT @Subject
				--PRINT @MailBody

				-- STEP 4.1: Is it a backup?
				IF @EventSubClass IN (1, 3)	--Backup, Backup LOG
				BEGIN
					--Always alert when the restore is "external" (not from the DBA or a SQL Agent job).
					--TODO:  configuration entry for these values?
					IF @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%DBA%'' 
					AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%Dave.Mason%'' 
					AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%MSSqlAdmin%''
						SET @SendAlert = 1;
				END

				-- STEP 4.2: Is it a restore?
				ELSE IF @EventSubClass IN (2)	--Restore
				BEGIN
					--Take no action for RESTORE HEADERONLY, RESTORE FILELISTONLY, etc.
					IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS LIKE ''%RESTORE%DATABASE%'' 
					BEGIN

						--Always alert when the restore is "external" (not from the DBA or a SQL Agent job).
						--TODO:  configuration entry for these values?
						IF @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%DBA%'' 
						AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%Dave.Mason%'' 
						AND @Login COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%MSSqlAdmin%''
						BEGIN
							SET @SendAlert = 1;
						END

						IF @TsqlCmd COLLATE SQL_Latin1_General_CP1_CI_AS NOT LIKE ''%NORECOVERY%''
						BEGIN
							--Endless loop while we wait for the DB to be restored and recovered.
							WHILE (1 = 1)
							BEGIN
								--If DB is restored and quickly dropped (as is the case with a DR Test,
								--we get stuck in an endless loop. :(
								IF NOT EXISTS (
									SELECT *
									FROM master.sys.databases d
									WHERE d.name = @DB
								)
								BEGIN
									SET @SendAlert = 1;
									SET @MailBody = 
										''<p><b>Restored database no longer exists:</b><br/></p><br/>'' + @MailBody;
									BREAK;
								END

								--Action taken depends on the database state.  See MSDN documentation:
								--https://msdn.microsoft.com/en-us/library/ms190442.aspx
								ELSE IF @DBState COLLATE SQL_Latin1_General_CP1_CI_AS IN (''ONLINE'')
								BEGIN' 
	--If "risky database roles" are not allowed...
	IF dba.GetInstanceConfiguration('Allow Risky Database Roles') = 0
		SET @Tsql = @Tsql + '
									--TODO:  what if user access for the DB is SINGLE_USER?
									--TODO:  what if DB is READ_ONLY?
									--Set up security when a db is restored WITH RECOVERY
									BEGIN TRY
										EXEC dba.ConfigureSecurityByDatabase @DBName = @DB
									END TRY
									BEGIN CATCH
										SELECT @ErrorMessage = ERROR_MESSAGE();
										RAISERROR (@ErrorMessage, 16, 1) WITH LOG;
									END CATCH
									'
	SET @Tsql = @Tsql + '
									BREAK;
								END

								ELSE IF @DBState COLLATE SQL_Latin1_General_CP1_CI_AS IN (''RESTORING'', ''RECOVERING'')
								BEGIN
									WAITFOR DELAY ''00:00:05''
								END

								ELSE IF @DBState COLLATE SQL_Latin1_General_CP1_CI_AS IN (''OFFLINE'', ''RECOVERY PENDING'', ''SUSPECT'', ''EMERGENCY'')
								BEGIN
									SET @SendAlert = 1;
									SET @MailBody = 
										''<p><b>Restored database is not ONLINE:</b><br/></p><br/>'' + @MailBody;
									BREAK;
								END

								ELSE IF COALESCE(@DBState, '''') = ''''
								BEGIN
									--Paranoia compels me to account for this 
									--unlikely, if not impossible scenario.
									SET @SendAlert = 1;
									SET @MailBody = 
										''<p><b>Database state is unknown:</b><br/></p><br/>'' + @MailBody;
									BREAK;
								END
							END
						END

					END

				END

				IF @SendAlert = 1
				BEGIN
					EXEC msdb.dbo.sp_send_dbmail 
						@recipients = ''DBA@Domain.com'', 
						@profile_name = ''Default'',
						@subject = @Subject,
						@body = @MailBody,
						@body_format = ''HTML'',
						@exclude_query_output = 1;
				END

			END TRY
			BEGIN CATCH
				SELECT 
					@ErrorMessage = ERROR_MESSAGE(),
					@ErrorSeverity = ERROR_SEVERITY(),
					@ErrorState = ERROR_STATE();
				RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState) WITH LOG;
				ROLLBACK;
			END CATCH
			/*
				Commit the transaction.  At any point before this, we 
				could roll back -- the received message would be back 
				on the queue AND the response wouldn''t be sent.
			*/
			COMMIT TRANSACTION
		END
	END
END
'
	EXEC sp_executesql @Tsql;


	ALTER QUEUE dbo.queBackupRestoreNotification 
	WITH 
	STATUS = ON, 
	ACTIVATION ( 
		PROCEDURE_NAME = dbo.ReceiveBackupRestoreEvent, 
		STATUS = ON, 
		--STATUS = OFF, 
		MAX_QUEUE_READERS = 1, 
		EXECUTE AS OWNER) 
END
GO

/*
	-- Look at data held in the Queue
	SELECT *, CAST(message_body AS XML) AS message_body_xml
	FROM dbo.queBackupRestoreNotification
	WHERE [service_name] = 'svcBackupRestoreNotification'
*/