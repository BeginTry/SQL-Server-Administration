USE DbaData
GO

--Drop objects first before trying to create them (in specific sequence).
IF EXISTS (
	SELECT *
	FROM sys.services
	WHERE name = 'svcFileAutoGrowthNotification'
)
	DROP SERVICE svcFileAutoGrowthNotification;
GO

IF EXISTS (
	SELECT *
	FROM sys.service_queues
	WHERE name = 'queFileAutoGrowthNotification'
)
	DROP QUEUE queFileAutoGrowthNotification;
GO

--Create a queue just for file autogrowth events.
CREATE QUEUE queFileAutoGrowthNotification
GO

--Create a service just for file autogrowth events.
CREATE SERVICE svcFileAutoGrowthNotification
ON QUEUE queFileAutoGrowthNotification ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])
GO

IF EXISTS (
	SELECT * 
	FROM sys.server_event_notifications 
	WHERE name = 'enFileAutoGrowthEvents'
)
	DROP EVENT NOTIFICATION enFileAutoGrowthEvents
	ON SERVER
GO

-- Create the event notification for file autogrowth events on the service.
CREATE EVENT NOTIFICATION enFileAutoGrowthEvents
ON SERVER
WITH FAN_IN
FOR DATA_FILE_AUTO_GROW, LOG_FILE_AUTO_GROW
TO SERVICE 'svcFileAutoGrowthNotification', 'current database';
GO

IF EXISTS (
	SELECT *
	FROM INFORMATION_SCHEMA.ROUTINES r
	WHERE r.ROUTINE_SCHEMA = 'dbo' AND r.ROUTINE_NAME = 'ReceiveFileAutoGrowthEvent'
)
	DROP PROCEDURE dbo.ReceiveFileAutoGrowthEvent 
GO

CREATE PROCEDURE dbo.ReceiveFileAutoGrowthEvent
/*****************************************************************************
* Name     : dbo.ReceiveFileAutoGrowthEvent
* Purpose  : Runs when there is a DATA_FILE_AUTO_GROW or LOG_FILE_AUTO_GROW event.
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	01/08/2015	DBA	Created
* 	Use dynamic tsql to create a stored proc, which will be slightly different 
* 	based on whether or not the engine edition of the SQL Server instance is any 
* 	of the EXPRESS variants.  This way, the SP doesn't have to check the engine 
* 	edition every time it is executed.
*
*	05/25/2016	DBA	
* 	msdb.dbo.sp_start_job runs jobs asynchronously.  Sometimes the job doesn't 
* 	finish before the next auto-growth event occurs.  This results in errors like this, 
* 	which can clutter the SQL Server Log:
* 		The activated proc '[dbo].[ReceiveFileAutoGrowthEvent]' running on queue 
* 		'DbaData.dbo.queFileAutoGrowthNotification' output the following:  'SQLServerAgent Error: Request to 
* 		run job DBA-Check Fixed Drive Free Space (from User sa) refused because the job is already running 
* 		from a request by User sa.'
* 	Job [DBA-Check Fixed Drive Free Space] is very simple.  It has one job step, 
* 	which executes a single	stored proc.  Since the code/logic for sending alerts 
* 	is within the SP, forego code re-use (running the job), and duplicate the job 
* 	step code here.  This negates the need for dynamic tsql to create the SP, 
* 	based on the SQL Server Edition type.
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
				FROM queFileAutoGrowthNotification
		), TIMEOUT 1000  -- if the queue is empty for one second, give UPDATE and go away
		-- If we didn't get anything, bail out
		IF (@@ROWCOUNT = 0)
		BEGIN
			ROLLBACK TRANSACTION
			BREAK
		END 
		ELSE
		BEGIN
			--Although we've captured the message body, we're not using any of the event data.
			EXEC DbaData.dba.CheckFixedDriveFreeSpace
				@FreeSpaceThresholdMB = 2048;
			
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

ALTER QUEUE dbo.queFileAutoGrowthNotification 
WITH 
STATUS = ON, 
ACTIVATION ( 
	PROCEDURE_NAME = dbo.ReceiveFileAutoGrowthEvent, 
	STATUS = ON, 
	--STATUS = OFF, 
	MAX_QUEUE_READERS = 1, 
	EXECUTE AS OWNER) 
GO

/***********************************************************/
--Delete the old WMI event alert.
IF EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = N'WMI-DB File Growth Events'
)
BEGIN
	EXEC msdb.dbo.sp_delete_alert 
			@name=N'WMI-DB File Growth Events'
END
GO
/***********************************************************/
-- Look at data held in the Queue
--SELECT *, CAST(message_body AS XML) AS message_body_xml
--FROM dbo.queFileAutoGrowthNotification
--WHERE [service_name] = 'svcFileAutoGrowthNotification'
--GO
