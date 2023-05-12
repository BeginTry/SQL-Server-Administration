/*
	Iterate through user databases and check
	Query Store for plans with missing indexes.
	Results are "persisted" to table tempdb.guest.QSMissingIndexes
	Note that some parameters are hard codeded (below):
		Execution time of query: previous 24 hours
		Cumulative CPU time for a plan: 10,000 ms or more
*/
DROP TABLE IF EXISTS tempdb.guest.QSMissingIndexes;
SELECT DB_NAME() AS QS_database_name,
	DB_NAME() AS object_name,
	CAST(NULL AS VARCHAR(MAX)) AS query_sql_text,
	DB_NAME() AS DatabaseName,
	DB_NAME() AS SchemaName,
	DB_NAME() AS TableName,
	CAST(NULL AS FLOAT) AS impact,
	rs.plan_id, p.query_id, 
	rs.count_executions,
	CAST((rs.count_executions * rs.avg_duration)/1000 AS NUMERIC(20, 2)) AS total_duration_ms,
	CAST((rs.count_executions * rs.avg_cpu_time)/1000 AS NUMERIC(20, 2)) AS total_cpu_time_ms,
	CAST(NULL AS XML) AS query_plan
INTO tempdb.guest.QSMissingIndexes
FROM sys.query_store_runtime_stats rs
JOIN sys.query_store_plan p
	ON p.plan_id = rs.plan_id
WHERE 1 = 2;

--Needed to get a little creative to fit the query into 2k characters.
DECLARE @Cmd NVARCHAR(2000) = 'USE [?]; ' 

--Exclude system DBs, read-only  DBs, and OFFLINE DBs.
SET @Cmd = @Cmd + '
IF NOT EXISTS (
	SELECT *
	FROM master.sys.databases d
	WHERE d.name = DB_NAME()
	AND d.is_read_only = 0
	AND d.state_desc = ''ONLINE''
	AND d.database_id > 4
)
	RETURN;
';

--Dump unique plan_id''s to #temp table.
--Default period is last 24 hours.
SET @Cmd = @Cmd + 'DROP TABLE IF EXISTS #Q;
SELECT 
	rs.plan_id, p.query_id, 
	SUM(rs.count_executions) AS count_executions,
	CAST(SUM(rs.count_executions * rs.avg_duration)/1000 AS NUMERIC(20, 2)) AS td_ms,
	CAST(SUM(rs.count_executions * rs.avg_cpu_time)/1000 AS NUMERIC(20, 2)) AS cpu
INTO #Q
FROM sys.query_store_runtime_stats rs
JOIN sys.query_store_plan p
	ON p.plan_id = rs.plan_id
WHERE rs.last_execution_time > CURRENT_TIMESTAMP - 1
GROUP BY rs.plan_id, p.query_id

ALTER TABLE #Q ALTER COLUMN plan_id INT NOT NULL;
ALTER TABLE #Q ADD PRIMARY KEY (plan_id);
ALTER TABLE #Q ADD query_plan XML;
';

--Remove plans from #temp table that use little CPU.
SET @Cmd = @Cmd + 'DELETE FROM #Q
WHERE cpu < 10000

UPDATE q SET q.query_plan = (
	SELECT TOP(1) TRY_CAST(p.query_plan AS XML) 
	FROM sys.query_store_plan p
	WHERE p.plan_id = q.plan_id
)
FROM #Q q

DELETE FROM #Q WHERE query_plan IS NULL;
';
--Remove plan_id''s from #temp table, leaving only those with missing indexes.
SET @Cmd = @Cmd + 'SET QUOTED_IDENTIFIER ON;
;WITH XMLNAMESPACES(DEFAULT ''http://schemas.microsoft.com/sqlserver/2004/07/showplan'')
DELETE FROM #Q
WHERE NOT query_plan.exist(''//MissingIndexes'') = 1

;WITH XMLNAMESPACES(DEFAULT ''http://schemas.microsoft.com/sqlserver/2004/07/showplan'')
INSERT INTO tempdb.guest.QSMissingIndexes
SELECT DB_NAME(), OBJECT_NAME(q.object_id), qt.query_sql_text,
	x.value(''(QueryPlan/MissingIndexes/MissingIndexGroup/MissingIndex/@Database)[1]'', ''sysname''),
	x.value(''(QueryPlan/MissingIndexes/MissingIndexGroup/MissingIndex/@Schema)[1]'', ''sysname''),
	x.value(''(QueryPlan/MissingIndexes/MissingIndexGroup/MissingIndex/@Table)[1]'', ''sysname''),
	x.value(''(QueryPlan/MissingIndexes/MissingIndexGroup/@Impact)[1]'', ''float''),
	t.*
FROM #Q t
JOIN sys.query_store_query q
	ON q.query_id = t.query_id
JOIN sys.query_store_query_text qt
	ON qt.query_text_id = q.query_text_id
CROSS APPLY t.query_plan.nodes(''//StmtSimple'') AS stmt(x)
';

EXEC sp_MSforeachdb @Cmd; 

IF APP_NAME() NOT LIKE 'Microsoft SQL Server Management Studio%'
SELECT *
FROM tempdb.guest.QSMissingIndexes mi
ORDER BY mi.total_cpu_time_ms DESC
