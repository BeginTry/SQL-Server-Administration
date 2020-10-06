/*
	Data Collection setup via transact-SQL
	(SQL Server 2008 or later, Standard Edition or greater)
*/

--Disable the collector before changing data collector-wide configuration.
EXEC msdb.dbo.sp_syscollector_disable_collector
GO

EXEC msdb.dbo.sp_syscollector_set_warehouse_instance_name 
	@instance_name=N'MDW-host\DBA'

EXEC msdb.dbo.sp_syscollector_set_warehouse_database_name 
	@database_name=N'DatacenterMDW'
	
EXEC msdb.dbo.sp_syscollector_set_cache_window 
	@cache_window=1

EXEC msdb.dbo.sp_syscollector_set_cache_directory 
	@cache_directory=N''
GO

/*
	Disk Usage not needed.  We roll our own disk stats.
*/
--EXEC msdb.dbo.sp_syscollector_start_collection_set 
--	@name = 'Disk Usage'

EXEC msdb.dbo.sp_syscollector_start_collection_set 
	@name = 'Server Activity'

/*
	The QS collection set is probably gratuitous unless there is a specific performance issue
	that mandates monitoring for extended periods of time (ie longer than an XEvents Session).
*/
--EXEC msdb.dbo.sp_syscollector_start_collection_set 
--	@name = 'Query Statistics'
GO

EXEC msdb.dbo.sp_syscollector_enable_collector
GO

/*
	SELECT *
	FROM msdb.dbo.syscollector_collection_sets
*/
