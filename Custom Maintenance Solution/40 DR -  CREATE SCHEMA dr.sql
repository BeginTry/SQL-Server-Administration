USE DbaData
GO

--"Disaster Recovery" schema
IF NOT EXISTS (
	SELECT *
	FROM sys.schemas
	WHERE name = 'dr'
)
	EXEC ('CREATE SCHEMA dr AUTHORIZATION dbo')
GO
