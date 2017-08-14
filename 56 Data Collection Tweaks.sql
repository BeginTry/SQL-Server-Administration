/*
	Data Collection quickly consumed disk space on the
	MDW host.  This script attempts to "slow down"
	the rate of data collection and (hopefully) reduce
	the volume of data.
*/
------------------------------------------------------
--Default retention is X days.
EXEC msdb.dbo.sp_syscollector_update_collection_set 
	@name='Server Activity', 
	@days_until_expiration=10
GO

/*
	Change the frequency of [Server Activity] data 
	collection from 60 to 300 seconds.
*/
DECLARE @CollectionItemName SYSNAME
DECLARE curNames CURSOR FAST_FORWARD READ_ONLY FOR
	SELECT i.name--, i.frequency, s.name
	FROM msdb.dbo.syscollector_collection_sets s
	JOIN msdb.dbo.syscollector_collection_items i
		ON i.collection_set_id = s.collection_set_id
	WHERE s.name = 'Server Activity'
	ORDER BY s.name, i.collection_set_id

OPEN curNames
FETCH NEXT FROM curNames INTO @CollectionItemName

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC msdb.dbo.sp_syscollector_update_collection_item 
		@name = @CollectionItemName,
		@frequency = 300
	FETCH NEXT FROM curNames INTO @CollectionItemName
END

CLOSE curNames
DEALLOCATE curNames
GO

EXEC msdb.dbo.sp_syscollector_start_collection_set 
	@name='Server Activity'
GO
	