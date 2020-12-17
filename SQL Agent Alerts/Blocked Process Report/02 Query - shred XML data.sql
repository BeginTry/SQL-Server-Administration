--TODO: set the database where the blocked process report log data exists.
USE DbaMetrics;
GO

SELECT 
	CAST(p.PostTime AS SMALLDATETIME) AS PostTime, 
	DB_NAME(p.DatabaseID) DatabaseName, p.Duration / 1000 Duration_ms, 
	p.IndexID, p.IsSystem, p.ServerName, p.SessionLoginName, p.TextData,

	--Comment/uncomment fields as desired. Note the impact on performance.

	--Blocking process.
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@status)[1]', 'VARCHAR(32)') AS BLK_status,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@spid)[1]', 'BIGINT') AS BLK_spid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@sbid)[1]', 'BIGINT') AS BLK_sbid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@ecid)[1]', 'BIGINT') AS BLK_ecid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@priority)[1]', 'BIGINT') AS BLK_priority,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@trancount)[1]', 'BIGINT') AS BLK_trancount,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lastbatchstarted)[1]', 'DATETIME') AS BLK_lastbatchstarted,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lastbatchcompleted)[1]', 'DATETIME') AS BLK_lastbatchcompleted,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lastattention)[1]', 'VARCHAR(MAX)') AS BLK_lastattention,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@clientapp)[1]', 'VARCHAR(256)') AS BLK_clientapp,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@hostname)[1]', 'VARCHAR(256)') AS BLK_hostname,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@hostpid)[1]', 'BIGINT') AS BLK_hostpid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@loginname)[1]', 'VARCHAR(128)') AS BLK_loginname,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@isolationlevel)[1]', 'VARCHAR(32)') AS BLK_isolationlevel,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@xactid)[1]', 'BIGINT') AS BLK_xactid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@currentdb)[1]', 'BIGINT') AS BLK_currentdb,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@currentdbname)[1]', 'VARCHAR(128)') AS BLK_currentdbname,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lockTimeout)[1]', 'BIGINT') AS BLK_lockTimeout,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@clientoption1)[1]', 'BIGINT') AS BLK_clientoption1,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@clientoption2)[1]', 'BIGINT') AS BLK_clientoption2,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/inputbuf)[1]', 'VARCHAR(MAX)') AS BLK_inputbuf,

	--Blocked process (victim)
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@taskpriority)[1]', 'BIGINT') AS VIC_taskpriority,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@logused)[1]', 'BIGINT') AS VIC_logused,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@waitresource)[1]', 'VARCHAR(128)') AS VIC_waitresource,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@waittime)[1]', 'BIGINT') AS VIC_waittime,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@ownerId)[1]', 'BIGINT') AS VIC_ownerId,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@transactionname)[1]', 'VARCHAR(128)') AS VIC_transactionname,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lasttranstarted)[1]', 'DATETIME') AS VIC_lasttranstarted,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@XDES)[1]', 'VARCHAR(64)') AS VIC_XDES,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lockMode)[1]', 'VARCHAR(32)') AS VIC_lockMode,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@schedulerid)[1]', 'BIGINT') AS VIC_schedulerid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@kpid)[1]', 'BIGINT') AS VIC_kpid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@status)[1]', 'VARCHAR(32)') AS VIC_status,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@spid)[1]', 'BIGINT') AS VIC_spid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@sbid)[1]', 'BIGINT') AS VIC_sbid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@ecid)[1]', 'BIGINT') AS VIC_ecid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@priority)[1]', 'BIGINT') AS VIC_priority,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@trancount)[1]', 'BIGINT') AS VIC_trancount,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lastbatchstarted)[1]', 'DATETIME') AS VIC_lastbatchstarted,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lastbatchcompleted)[1]', 'DATETIME') AS VIC_lastbatchcompleted,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lastattention)[1]', 'VARCHAR(MAX)') AS VIC_lastattention,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@clientapp)[1]', 'VARCHAR(256)') AS VIC_clientapp,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@hostname)[1]', 'VARCHAR(256)') AS VIC_hostname,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@hostpid)[1]', 'BIGINT') AS VIC_hostpid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@loginname)[1]', 'VARCHAR(128)') AS VIC_loginname,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@isolationlevel)[1]', 'VARCHAR(32)') AS VIC_isolationlevel,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@xactid)[1]', 'BIGINT') AS VIC_xactid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@currentdb)[1]', 'BIGINT') AS VIC_currentdb,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lockTimeout)[1]', 'BIGINT') AS VIC_lockTimeout,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@clientoption1)[1]', 'BIGINT') AS VIC_clientoption1,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@clientoption2)[1]', 'BIGINT') AS VIC_clientoption2,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/inputbuf)[1]', 'VARCHAR(MAX)') AS VIC_inputbuf

FROM dbo.BlockedProcesses p;
