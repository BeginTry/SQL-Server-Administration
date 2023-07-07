/*************************************************************************************
	Dynamic T-SQL to create snapshots (SQL 2017 or above)
*************************************************************************************/
SELECT
'CREATE DATABASE [' + d.name + '_' + s.DatabaseNameSuffix + '] ON
	' + STRING_AGG(
		'(Name = ''' + f.name + ''', FILENAME = ''' + 
		CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS VARCHAR(MAX)) + d.name + '_' + s.DatabaseNameSuffix +
		'.' + CAST(f.file_id AS VARCHAR(MAX)) + '.snp' +
		''')', ', '
	) 
	 + '
AS SNAPSHOT OF [' + d.name + '];'
FROM master.sys.databases d
JOIN master.sys.master_files f
	ON f.database_id = d.database_id
CROSS APPLY (
	SELECT
		CONVERT(VARCHAR, CURRENT_TIMESTAMP, 112) + '_' +
		LEFT(REPLACE(CONVERT(VARCHAR, 
			--Round to nearest 10 minute increment. (Yeah, it's a bit odd.)
      --http://davemason.me/2019/08/18/rounding-to-15-minute-intervals
			CAST(ROUND(CAST(CURRENT_TIMESTAMP AS NUMERIC(38, 22)) * 24 * 6, 0) / 6 / 24 AS DATETIME), 
			108), ':', ''), 4) AS DatabaseNameSuffix
) s
WHERE f.type_desc NOT IN ('LOG')
AND d.state_desc = 'ONLINE'
AND d.source_database_id IS NULL	--exlude existing snapshots.
GROUP BY d.name, s.DatabaseNameSuffix
ORDER BY d.name


/*************************************************************************************
	Dynamic T-SQL to create snapshots (SQL 2016 or prior)
	NOTE: SERVERPROPERTY('InstanceDefaultDataPath') available starting with SQL 2012.
*************************************************************************************/
SELECT
'CREATE DATABASE [' + d.name + '_' + s.DatabaseNameSuffix + '] ON
	' + n.NamesDefinition + '
AS SNAPSHOT OF [' + d.name + '];'
FROM master.sys.databases d
CROSS APPLY (
	SELECT
		CONVERT(VARCHAR, CURRENT_TIMESTAMP, 112) + '_' +
		LEFT(REPLACE(CONVERT(VARCHAR, 
			--Round to nearest 10 minute increment.
			CAST(ROUND(CAST(CURRENT_TIMESTAMP AS NUMERIC(38, 22)) * 24 * 6, 0) / 6 / 24 AS DATETIME), 
			108), ':', ''), 4) AS DatabaseNameSuffix
) s
OUTER APPLY 
( 
	SELECT STUFF 
	((
			SELECT N', ' + '(Name = ''' + f.name + ''', FILENAME = ''' + 
				CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS VARCHAR(MAX)) + d.name + '_' + s.DatabaseNameSuffix +
				'.' + CAST(f.file_id AS VARCHAR(MAX)) + '.snp' +
				''')'
			FROM master.sys.master_files f
			WHERE f.database_id = d.database_id
			AND f.type_desc NOT IN ('LOG')
			FOR XML PATH(''), TYPE
		).value('.', 'NVARCHAR(MAX)'),1,2,''
	)
) AS n ( NamesDefinition )
WHERE d.state_desc = 'ONLINE'
AND d.source_database_id IS NULL	--exlude existing snapshots.
GROUP BY d.name, s.DatabaseNameSuffix, n.NamesDefinition
ORDER BY d.name

/*
	--Snapshot/source database cross-reference.
	SELECT d.name AS SnapshotDB, DB_NAME(d.source_database_id) SourceDB
	FROM master.sys.databases d
	WHERE d.source_database_id IS NOT NULL
	ORDER BY d.name
*/
