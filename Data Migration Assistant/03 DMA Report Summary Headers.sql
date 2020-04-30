--Nested JSON arrays (look for square bracket characters in the JSON data).
--Use a CROSS APPLY for each level of nesting. :(
SELECT 
	'Title: ' + r.Title + CHAR(13) + CHAR(10) + 
	'Category: ' + r.Category + ' (' + r.ChangeCategory + ')' + CHAR(13) + CHAR(10) + 
	'Severity: ' + r.Severity + CHAR(13) + CHAR(10) + 
	'Impact: ' + r.Impact + CHAR(13) + CHAR(10) + 
	'Recommendation: ' + r.Recommendation + CHAR(13) + CHAR(10) + 
	'More Info: ' + r.MoreInfo + CHAR(13) + CHAR(10) + 
	'Applies to: ' + MIN(r.MigratingTo) + ' (or higher)' AS ReportSummaryHeader
FROM OPENROWSET (BULK 'C:\Folder\DMA Report.json', SINGLE_CLOB) AS json
CROSS APPLY OPENJSON(BulkColumn, '$.Databases') 
WITH( 
	Name VARCHAR(MAX) '$.Name',
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
	Impact VARCHAR(MAX) '$.Impact',
	Recommendation NVARCHAR(MAX) '$.Recommendation',
	MoreInfo VARCHAR(MAX) '$.MoreInfo',
	ImpactedObjects NVARCHAR(MAX) '$.ImpactedObjects' AS JSON
) AS r
--CROSS APPLY OPENJSON(r.ImpactedObjects) 
--WITH (
--	ObjectType VARCHAR(MAX) '$.ObjectType',
--	Name VARCHAR(MAX) '$.Name',
--	ImpactDetail VARCHAR(MAX) '$.ImpactDetail',
--	SuggestedFixes NVARCHAR(MAX) '$.SuggestedFixes' AS JSON
--) AS i
WHERE 1 = 1
AND d.name NOT IN ('ReportServer', 'ReportServerTempDB')
GROUP BY r.Category, r.ChangeCategory,
	r.Severity,
	r.Title,
	r.Impact,
	r.Recommendation,
	r.MoreInfo
