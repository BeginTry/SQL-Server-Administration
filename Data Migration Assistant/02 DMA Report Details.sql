--Nested JSON arrays (look for square bracket characters in the JSON data).
--Use a CROSS APPLY for each level of nesting. :(
SELECT 
	d.[name] AS [Database], d.CompatibilityLevel, r.MigratingTo,
	--r.Category, r.Severity, r.ChangeCategory, 
	r.Title, 
	--r.Impact, r.Recommendation, 
	--r.MoreInfo AS Reference, 
	i.[Name] AS ObjectName, i.ObjectType, 
	
	i.ImpactDetail AS MoreInfo
	--CASE
	--	WHEN CHARINDEX(': Line', i.ImpactDetail) > 0 THEN SUBSTRING(i.ImpactDetail, CHARINDEX(': Line', i.ImpactDetail) + 2, 4096)
	--	ELSE i.ImpactDetail
	--END AS MoreInfo
	
	--i.SuggestedFixes
FROM OPENROWSET (BULK 'C:\Folder\DMA Report.json', SINGLE_CLOB) AS json
CROSS APPLY OPENJSON(BulkColumn, '$.Databases') 
WITH( 
	[Name] VARCHAR(MAX) '$.Name',
	CompatibilityLevel VARCHAR(MAX) '$.CompatibilityLevel',
	AssessmentRecommendations NVARCHAR(MAX) '$.AssessmentRecommendations' AS JSON
) AS d
CROSS APPLY OPENJSON(d.AssessmentRecommendations) 
WITH (
	MigratingTo VARCHAR(MAX) '$.CompatibilityLevel',
	Category VARCHAR(MAX) '$.Category',
	Severity VARCHAR(MAX) '$.Severity',
	ChangeCategory VARCHAR(MAX) '$.ChangeCategory',
	Title VARCHAR(MAX) '$.Title',
	--Impact VARCHAR(MAX) '$.Impact',
	--Recommendation NVARCHAR(MAX) '$.Recommendation',
	--MoreInfo VARCHAR(MAX) '$.MoreInfo',
	ImpactedObjects NVARCHAR(MAX) '$.ImpactedObjects' AS JSON
) AS r
CROSS APPLY OPENJSON(r.ImpactedObjects) 
WITH (
	ObjectType VARCHAR(MAX) '$.ObjectType',
	[Name] VARCHAR(MAX) '$.Name',
	ImpactDetail VARCHAR(MAX) '$.ImpactDetail',
	SuggestedFixes NVARCHAR(MAX) '$.SuggestedFixes' AS JSON
) AS i
WHERE 1 = 1
--AND r.Title NOT LIKE 'Stretch Database%'
--AND r.Title <> 'Security Advisor TDE'
AND d.name NOT IN ('ReportServer', 'ReportServerTempDB')
ORDER BY r.Title
