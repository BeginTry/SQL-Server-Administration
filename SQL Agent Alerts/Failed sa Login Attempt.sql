USE msdb;
GO

--TODO: for notifications, ensure "mail profile" is enabled in SQL Server Agent (Alert System).
--TODO: specify an Operator for alert notifications.
DECLARE @Operator SYSNAME = 'Dave Mason';
DECLARE @AlertName SYSNAME = N'18456 - Failed sa Login Attempt';

--Add generic alert for failed logins.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysalerts
	WHERE name = @AlertName
)
	EXEC msdb.dbo.sp_add_alert 
		@name=@AlertName, 
		@message_id=18456, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, --Important to leave this at zero.
		@include_event_description_in=0, 
		@event_description_keyword=N'''sa''', 
		@category_name=N'[Uncategorized]', 
		@job_id=NULL

--Add the alert notification.
IF NOT EXISTS (
	SELECT *
	FROM msdb.dbo.sysnotifications n
	JOIN msdb.dbo.sysalerts a
		ON a.id = n.alert_id
	JOIN msdb.dbo.sysoperators o
		ON o.id = n.operator_id
	WHERE a.name = @AlertName
	AND o.name = @Operator
)
BEGIN
	EXEC msdb.dbo.sp_add_notification 
		@alert_name=@AlertName, 
		@operator_name=@Operator, 
		@notification_method = 1;
END
GO
