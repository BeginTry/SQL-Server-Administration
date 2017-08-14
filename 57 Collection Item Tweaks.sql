DECLARE @ColItemId INT
DECLARE @DmvShapshotsParams XML

SELECT @ColItemId = ci.collection_item_id, 
	@DmvShapshotsParams = ci.[parameters]
	--ci.collection_item_id, ci.name, ci.frequency, ci.[parameters]
FROM msdb.dbo.syscollector_collection_items ci
JOIN msdb.dbo.syscollector_collection_sets cs
	ON ci.collection_set_id = cs.collection_set_id
WHERE ci.name = 'Server Activity - DMV Snapshots'
AND cs.name = 'Server Activity'

IF @DmvShapshotsParams.value('declare namespace ns="DataCollectorType"; 
	(/ns:TSQLQueryCollector/Query[OutputTable="os_wait_stats"]/Value)[1]','nvarchar(max)') LIKE '%HAVING%'
BEGIN
	PRINT 'Already modified!  Do not proceed!';
	RETURN;
END
ELSE
BEGIN
	PRINT 'Continuing with modify()';

	--The entire chunk of XML data before any modification.
	SELECT @DmvShapshotsParams	AS [XML Params Before]

	--The specific node that we will modify.
	SELECT @DmvShapshotsParams.value('declare namespace ns="DataCollectorType"; 
		(/ns:TSQLQueryCollector/Query[OutputTable="os_wait_stats"]/Value)[1]','nvarchar(max)')

	DECLARE @NewNodeVal VARCHAR(MAX) = @DmvShapshotsParams.value('declare namespace ns="DataCollectorType"; 
		(/ns:TSQLQueryCollector/Query[OutputTable="os_wait_stats"]/Value)[1]','nvarchar(max)') + '
HAVING [wait_type] NOT IN (
        N''BROKER_EVENTHANDLER'',		N''BROKER_RECEIVE_WAITFOR'',
        N''BROKER_TASK_STOP'',		N''BROKER_TO_FLUSH'',
        N''BROKER_TRANSMITTER'',		N''CHECKPOINT_QUEUE'',
        N''CHKPT'',					N''CLR_AUTO_EVENT'',
        N''CLR_MANUAL_EVENT'',		N''CLR_SEMAPHORE'',
 
        -- Maybe uncomment these four if you have mirroring issues
        N''DBMIRROR_DBM_EVENT'',		N''DBMIRROR_EVENTS_QUEUE'',
        N''DBMIRROR_WORKER_QUEUE'',	N''DBMIRRORING_CMD'',
 
        N''DIRTY_PAGE_POLL'',			N''DISPATCHER_QUEUE_SEMAPHORE'',
        N''EXECSYNC'',				N''FSAGENT'',
        N''FT_IFTSHC_MUTEX'',			N''FT_IFTS_SCHEDULER_IDLE_WAIT'', 
 
        -- Maybe uncomment these six if you have AG issues
        N''HADR_CLUSAPI_CALL'',		N''HADR_FILESTREAM_IOMGR_IOCOMPLETION'',
        N''HADR_LOGCAPTURE_WAIT'',	N''HADR_NOTIFICATION_DEQUEUE'',
        N''HADR_TIMER_TASK'',			N''HADR_WORK_QUEUE'',
 
        N''KSOURCE_WAKEUP'',			N''LAZYWRITER_SLEEP'',
        N''LOGMGR_QUEUE'',			N''MEMORY_ALLOCATION_EXT'',
        N''ONDEMAND_TASK_QUEUE'',
        N''PREEMPTIVE_XE_GETTARGETSTATE'',
        N''PWAIT_ALL_COMPONENTS_INITIALIZED'',
        N''PWAIT_DIRECTLOGCONSUMER_GETNEXT'',
        N''QDS_ASYNC_QUEUE'',			N''QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'', 
        N''QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'',
        N''QDS_SHUTDOWN_QUEUE'',		N''REDO_THREAD_PENDING_WORK'',
        N''RESOURCE_QUEUE'',			N''REQUEST_FOR_DEADLOCK_SEARCH'', 
        N''SERVER_IDLE_CHECK'',		N''SLEEP_BPOOL_FLUSH'',
        N''SLEEP_DBSTARTUP'',			N''SLEEP_DCOMSTARTUP'',
        N''SLEEP_MASTERDBREADY'',		N''SLEEP_MASTERMDREADY'',
        N''SLEEP_MASTERUPGRADED'',	N''SLEEP_MSDBSTARTUP'',
        N''SLEEP_SYSTEMTASK'',		N''SLEEP_TASK'',
        N''SLEEP_TEMPDBSTARTUP'',		N''SNI_HTTP_ACCEPT'',
        N''SQLTRACE_BUFFER_FLUSH'',	N''SP_SERVER_DIAGNOSTICS_SLEEP'', 
        N''SQLTRACE_INCREMENTAL_FLUSH_SLEEP'',
        N''SQLTRACE_WAIT_ENTRIES'',	N''WAIT_FOR_RESULTS'',
        N''WAITFOR'',					N''WAITFOR_TASKSHUTDOWN'',
        N''WAIT_XTP_RECOVERY'',
        N''WAIT_XTP_HOST_WAIT'',		N''WAIT_XTP_OFFLINE_CKPT_NEW_LOG'',
        N''WAIT_XTP_CKPT_CLOSE'',		N''XE_DISPATCHER_JOIN'',
        N''XE_DISPATCHER_WAIT'',		N''XE_TIMER_EVENT''
		)'

	SET @DmvShapshotsParams.modify('declare namespace ns="DataCollectorType"; 
		replace value of (/ns:TSQLQueryCollector/Query[OutputTable="os_wait_stats"]/Value/text())[1]
		with sql:variable("@NewNodeVal")');

	--The entire chunk of XML data after modify().
	SELECT @DmvShapshotsParams AS [XML Params After]

	/*
		Take a moment to compare the before/after values
		for the xml parameters.  If everything looks ok,
		uncomment the SP below and run again.
	*/
	--EXEC msdb.dbo.sp_syscollector_update_collection_item
	--	@collection_item_id = @ColItemId,
	--	@parameters = @DmvShapshotsParams
END