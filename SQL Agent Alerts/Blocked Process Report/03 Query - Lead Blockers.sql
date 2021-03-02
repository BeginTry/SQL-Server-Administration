USE DbaMetrics;
GO

--Blocking events within the last X hours.
DECLARE @Hours INT = 48;
DECLARE @OldestBlockingPostTime DATETIME = DATEADD(HOUR, (ABS(@Hours) * -1), CURRENT_TIMESTAMP);

IF OBJECT_ID('tempdb.dbo.#BlockedProcesses') IS NOT NULL DROP TABLE dbo.#BlockedProcesses;

SELECT 
	BlockedProcessId,
	CAST(p.PostTime AS SMALLDATETIME) AS PostTime, 
	p.TextData.value('(/TextData/blocked-process-report/@monitorLoop)[1]', 'BIGINT') AS MonitorLoop,
	CAST(0 AS BIT) AS IsLeadBlocker,

	--Blocking chain metrics
	CAST(NULL AS INT) AS BC_SpidsBlocked,
	CAST(NULL AS BIGINT) AS BC_TotalDuration_ms,
	CAST(NULL AS BIGINT) AS BC_AvgDuration_ms,

	DB_NAME(p.DatabaseID) DatabaseName, p.Duration / 1000 Duration_ms, 
	p.IndexID, p.IsSystem, p.ServerName, p.SessionLoginName, p.TextData,

	--Comment/uncomment fields as desired. Note the impact on performance.

	--Blocking process.
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@status)[1]', 'VARCHAR(32)') AS BLK_status,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@spid)[1]', 'BIGINT') AS BLK_spid,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@sbid)[1]', 'BIGINT') AS BLK_sbid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@ecid)[1]', 'BIGINT') AS BLK_ecid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@priority)[1]', 'BIGINT') AS BLK_priority,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@trancount)[1]', 'BIGINT') AS BLK_trancount,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lastbatchstarted)[1]', 'DATETIME') AS BLK_lastbatchstarted,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lastbatchcompleted)[1]', 'DATETIME') AS BLK_lastbatchcompleted,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lastattention)[1]', 'VARCHAR(MAX)') AS BLK_lastattention,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@clientapp)[1]', 'VARCHAR(256)') AS BLK_clientapp,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@hostname)[1]', 'VARCHAR(256)') AS BLK_hostname,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@hostpid)[1]', 'BIGINT') AS BLK_hostpid,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@loginname)[1]', 'VARCHAR(128)') AS BLK_loginname,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@isolationlevel)[1]', 'VARCHAR(32)') AS BLK_isolationlevel,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@xactid)[1]', 'BIGINT') AS BLK_xactid,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@currentdb)[1]', 'BIGINT') AS BLK_currentdb,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@currentdbname)[1]', 'VARCHAR(128)') AS BLK_currentdbname,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@lockTimeout)[1]', 'BIGINT') AS BLK_lockTimeout,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@clientoption1)[1]', 'BIGINT') AS BLK_clientoption1,
	--p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@clientoption2)[1]', 'BIGINT') AS BLK_clientoption2,
	p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/inputbuf)[1]', 'VARCHAR(MAX)') AS BLK_inputbuf,

	--Blocked process (victim)
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@taskpriority)[1]', 'BIGINT') AS VIC_taskpriority,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@logused)[1]', 'BIGINT') AS VIC_logused,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@waitresource)[1]', 'VARCHAR(128)') AS VIC_waitresource,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@waittime)[1]', 'BIGINT') AS VIC_waittime,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@ownerId)[1]', 'BIGINT') AS VIC_ownerId,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@transactionname)[1]', 'VARCHAR(128)') AS VIC_transactionname,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lasttranstarted)[1]', 'DATETIME') AS VIC_lasttranstarted,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@XDES)[1]', 'VARCHAR(64)') AS VIC_XDES,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lockMode)[1]', 'VARCHAR(32)') AS VIC_lockMode,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@schedulerid)[1]', 'BIGINT') AS VIC_schedulerid,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@kpid)[1]', 'BIGINT') AS VIC_kpid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@status)[1]', 'VARCHAR(32)') AS VIC_status,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@spid)[1]', 'BIGINT') AS VIC_spid,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@sbid)[1]', 'BIGINT') AS VIC_sbid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@ecid)[1]', 'BIGINT') AS VIC_ecid,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@priority)[1]', 'BIGINT') AS VIC_priority,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@trancount)[1]', 'BIGINT') AS VIC_trancount,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lastbatchstarted)[1]', 'DATETIME') AS VIC_lastbatchstarted,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lastbatchcompleted)[1]', 'DATETIME') AS VIC_lastbatchcompleted,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lastattention)[1]', 'VARCHAR(MAX)') AS VIC_lastattention,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@clientapp)[1]', 'VARCHAR(256)') AS VIC_clientapp,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@hostname)[1]', 'VARCHAR(256)') AS VIC_hostname,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@hostpid)[1]', 'BIGINT') AS VIC_hostpid,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@loginname)[1]', 'VARCHAR(128)') AS VIC_loginname,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@isolationlevel)[1]', 'VARCHAR(32)') AS VIC_isolationlevel,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@xactid)[1]', 'BIGINT') AS VIC_xactid,
	DB_NAME(p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@currentdb)[1]', 'BIGINT')) AS VIC_currentdb,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@lockTimeout)[1]', 'BIGINT') AS VIC_lockTimeout,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@clientoption1)[1]', 'BIGINT') AS VIC_clientoption1,
	--p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@clientoption2)[1]', 'BIGINT') AS VIC_clientoption2,
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/inputbuf)[1]', 'VARCHAR(MAX)') AS VIC_inputbuf
INTO #BlockedProcesses
FROM dbo.BlockedProcesses p;

--Exclude SPIDs that are blocking themselves. Those can be dealt with separtely.
WHERE p.TextData.value('(/TextData/blocked-process-report/blocking-process/process/@spid)[1]', 'BIGINT') <>
	p.TextData.value('(/TextData/blocked-process-report/blocked-process/process/@spid)[1]', 'BIGINT')
AND p.PostTime >= @OldestBlockingPostTime;

--Remove duplicate rows for subthreads operating on behalf of a single process.
--These are identified by non-zero ecid values. (Execution context ID)
;WITH CTE AS(
   SELECT *,
       ROW_NUMBER()OVER(PARTITION BY MonitorLoop, BLK_spid, VIC_spid ORDER BY BlockedProcessId) AS RowNum
   FROM #BlockedProcesses bp
)
DELETE FROM CTE 
WHERE RowNum > 1;

--Flag the spids that are lead blockers in the blocking chain.
UPDATE p SET p.IsLeadBlocker = 1
FROM #BlockedProcesses p
WHERE p.BLK_spid NOT IN (
	SELECT vic.VIC_spid
	FROM #BlockedProcesses vic
	WHERE vic.MonitorLoop = p.MonitorLoop
)
AND (p.IsLeadBlocker <> 1 OR p.IsLeadBlocker IS NULL);

--Aggregate blocking chain metrics for each lead blocker.
;WITH LeadBlockerDurations AS
(
	SELECT 
		p.MonitorLoop, p.IsLeadBlocker, p.Duration_ms, p.BLK_spid, p.VIC_spid, p.BlockedProcessId
	FROM #BlockedProcesses p
	WHERE p.IsLeadBlocker = 1
	UNION ALL
	SELECT 
		r.MonitorLoop, r.IsLeadBlocker, r.Duration_ms, r.BLK_spid, r.VIC_spid, l.BlockedProcessId
	FROM #BlockedProcesses r
	JOIN LeadBlockerDurations l
		ON l.MonitorLoop = r.MonitorLoop
		AND r.BLK_spid = l.VIC_spid
	WHERE r.IsLeadBlocker <> 1
)
,LeadBlockerDurationsAggregrates AS
(
	SELECT SUM(Duration_ms) AS Sum_BlockingDuration_ms, AVG(Duration_ms) AS Avg_BlockingDuration_ms, 
		COUNT(*) AS Count_SpidsBlocked, BlockedProcessId
	FROM LeadBlockerDurations
	GROUP BY BlockedProcessId
)
UPDATE p SET
	p.BC_SpidsBlocked = cte.Count_SpidsBlocked,
	p.BC_TotalDuration_ms = cte.Sum_BlockingDuration_ms,
	p.BC_AvgDuration_ms = cte.Avg_BlockingDuration_ms
FROM #BlockedProcesses p
JOIN LeadBlockerDurationsAggregrates cte
	ON cte.BlockedProcessId = p.BlockedProcessId;

SELECT *
FROM #BlockedProcesses p
WHERE p.IsLeadBlocker = 1
ORDER BY p.BC_TotalDuration_ms DESC;
