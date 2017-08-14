USE [msdb]
GO

--TODO: get operator names dynamically.
/****************************************
	Create notifications for alerts.
****************************************/
DECLARE @AlertName SYSNAME
DECLARE @domainDbaOperator SYSNAME
DECLARE curAlerts CURSOR READ_ONLY FAST_FORWARD FOR
	SELECT name
	FROM msdb.dbo.sysalerts
	WHERE name BETWEEN '17' AND '26'
	OR name BETWEEN 'Error 823' AND 'Error 825'

SET @domainDbaOperator = DbaData.dba.GetInstanceConfiguration('domain DBA Team Operator Name')
OPEN curAlerts
FETCH NEXT FROM curAlerts INTO @AlertName

WHILE @@FETCH_STATUS = 0
BEGIN
	IF NOT EXISTS (
		SELECT *
		FROM msdb.dbo.sysalerts a
		JOIN msdb.dbo.sysnotifications n
			ON a.id = n.alert_id
		JOIN msdb.dbo.sysoperators o
			ON o.id = n.operator_id 
		WHERE a.name = @AlertName
		AND o.name = @domainDbaOperator
	)
		EXEC msdb.dbo.sp_add_notification 
			@alert_name=@AlertName, 
			@operator_name=@domainDbaOperator, 
			@notification_method = 1

	IF NOT EXISTS (
		SELECT *
		FROM msdb.dbo.sysalerts a
		JOIN msdb.dbo.sysnotifications n
			ON a.id = n.alert_id
		JOIN msdb.dbo.sysoperators o
			ON o.id = n.operator_id 
		WHERE a.name = @AlertName
		AND o.name = 'Dave Mason'
	)
		EXEC msdb.dbo.sp_add_notification 
			@alert_name=@AlertName, 
			@operator_name=N'Dave Mason', 
			@notification_method = 2
	
	--If the notifications already existed, they may 
	--have been configured with different params.
	--Update them here to be sure they're configured
	--correctly.
	EXEC msdb.dbo.sp_update_notification 
		@alert_name=@AlertName, 
		@operator_name=@domainDbaOperator, 
		@notification_method = 1;

	EXEC msdb.dbo.sp_update_notification 
		@alert_name=@AlertName, 
		@operator_name=N'Dave Mason', 
		@notification_method = 2;

	FETCH NEXT FROM curAlerts INTO @AlertName
END

CLOSE curAlerts
DEALLOCATE curAlerts
GO

/*
	--TEST THE ALERTS!
	RAISERROR ('Please disregard.  Just testing.', 17, 1) WITH LOG
*/