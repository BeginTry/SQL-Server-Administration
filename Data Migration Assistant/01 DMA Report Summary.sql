--Nested JSON arrays (look for square bracket characters in the JSON data).
--Use a CROSS APPLY for each level of nesting. :(
;WITH DistinctIssues AS
(
	SELECT 
		d.name AS [Database], d.CompatibilityLevel, 
		r.Category, r.Severity, r.ChangeCategory, r.Title, r.Impact, r.Recommendation, r.MoreInfo, 
		i.Name AS ObjectName, i.ObjectType, i.ImpactDetail
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
	CROSS APPLY OPENJSON(r.ImpactedObjects) 
	WITH (
		ObjectType VARCHAR(MAX) '$.ObjectType',
		Name VARCHAR(MAX) '$.Name',
		ImpactDetail VARCHAR(MAX) '$.ImpactDetail',
		SuggestedFixes NVARCHAR(MAX) '$.SuggestedFixes' AS JSON
	) AS i
	WHERE 1 = 1
	GROUP BY
		d.name, d.CompatibilityLevel, 
		r.Category, r.Severity, r.ChangeCategory, r.Title, r.Impact, r.Recommendation, r.MoreInfo, 
		i.Name, i.ObjectType, i.ImpactDetail
)
SELECT COUNT(*) IssueCount, 
	di.Category, di.Severity, di.ChangeCategory, 
	di.Title, di.Impact, di.Recommendation, di.MoreInfo AS Reference
FROM DistinctIssues di
GROUP BY di.Category, di.Severity, di.ChangeCategory, di.Title, di.Impact, di.Recommendation, di.MoreInfo

