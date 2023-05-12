/*
	Check Query Store for plans with missing indexes.
	Note that some parameters are hard codeded (below):
		Execution time of query: previous 24 hours
		Cumulative CPU time for a plan: 10,000 ms or more
*/
USE database_name;

--Dump unique plan_id's to #temp table.
--Default period is last 24 hours.
DROP TABLE IF EXISTS #QSQuery;
SELECT --TOP(10)
	rs.plan_id, p.query_id, 
	SUM(rs.count_executions) AS count_executions,
	CAST(SUM(rs.count_executions * rs.avg_duration)/1000 AS NUMERIC(20, 2)) AS total_duration_ms,
	CAST(SUM(rs.count_executions * rs.avg_cpu_time)/1000 AS NUMERIC(20, 2)) AS total_cpu_time_ms
INTO #QSQuery
FROM sys.query_store_runtime_stats rs
JOIN sys.query_store_plan p
	ON p.plan_id = rs.plan_id
WHERE 1 = 1
AND rs.last_execution_time > CURRENT_TIMESTAMP - 1
GROUP BY rs.plan_id, p.query_id

ALTER TABLE #QSQuery ALTER COLUMN plan_id INT NOT NULL;
ALTER TABLE #QSQuery ADD PRIMARY KEY (plan_id);
ALTER TABLE #QSQuery ADD query_plan XML;

--Remove plans from #temp table that use little CPU.
DELETE FROM #QSQuery
WHERE total_cpu_time_ms < 10000

UPDATE q SET q.query_plan = (
	SELECT TOP(1) TRY_CAST(p.query_plan AS XML) 
	FROM sys.query_store_plan p
	WHERE p.plan_id = q.plan_id
)
FROM #QSQuery q

DELETE FROM #QSQuery WHERE query_plan IS NULL;

--Remove plan_id's from #temp table, leaving only those with missing indexes.
;WITH XMLNAMESPACES(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
DELETE FROM #QSQuery
WHERE NOT query_plan.exist('//MissingIndexes') = 1

;WITH XMLNAMESPACES(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
SELECT OBJECT_NAME(q.object_id) AS Obj_Name, 
	(SELECT CAST(CHAR(10) + CHAR(10) AS NVARCHAR(MAX)) + qt.query_sql_text + CHAR(10) + CHAR(10) AS Qry FOR XML PATH(''),TYPE) AS query_sql_text,
	stmt_xml.value('(QueryPlan/MissingIndexes/MissingIndexGroup/MissingIndex/@Database)[1]', 'sysname') AS DatabaseName,
	stmt_xml.value('(QueryPlan/MissingIndexes/MissingIndexGroup/MissingIndex/@Schema)[1]', 'sysname') AS SchemaName,
	stmt_xml.value('(QueryPlan/MissingIndexes/MissingIndexGroup/MissingIndex/@Table)[1]', 'sysname') AS TableName,
	stmt_xml.value('(QueryPlan/MissingIndexes/MissingIndexGroup/@Impact)[1]', 'float') AS impact,
	t.*
FROM #QSQuery t
JOIN sys.query_store_query q
	ON q.query_id = t.query_id
JOIN sys.query_store_query_text qt
	ON qt.query_text_id = q.query_text_id
CROSS APPLY t.query_plan.nodes('//StmtSimple') AS stmt(stmt_xml)
ORDER BY t.total_cpu_time_ms DESC
