CREATE OR ALTER TRIGGER trg_DDL_CreateIndex
ON DATABASE 
FOR CREATE_INDEX
/*****************************************************************************
* Name     : trg_DDL_CreateIndex
* Purpose  : Enables ROW compression on newly created indexes that do not have
*	DATA_COMPRESSION specified (or are specified as NONE).
* Inputs   : None
* Outputs  : None
* Returns  : Nothing
******************************************************************************
* Change History
*	03/03/2021	DMason	Created (tested on SQL 2017)
******************************************************************************/
AS 
	SET NOCOUNT ON;
	SET XACT_ABORT OFF;
	DECLARE @DdlCmd NVARCHAR(MAX) = EVENTDATA().value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]','NVARCHAR(MAX)');
	DECLARE @TableSchema SYSNAME = EVENTDATA().value('(/EVENT_INSTANCE/SchemaName)[1]','NVARCHAR(MAX)');
	DECLARE @TableName SYSNAME = EVENTDATA().value('(/EVENT_INSTANCE/TargetObjectName)[1]','NVARCHAR(MAX)');
	DECLARE @IndexName SYSNAME = EVENTDATA().value('(/EVENT_INSTANCE/ObjectName)[1]','NVARCHAR(MAX)');

	IF @DdlCmd NOT LIKE '%WITH%(%DATA[_]COMPRESSION%=%ROW%)%' 
		AND @DdlCmd NOT LIKE '%WITH%(%DATA[_]COMPRESSION%=%PAGE%)%'
		AND @DdlCmd NOT LIKE '%COLUMNSTORE%'
	BEGIN
		--Kill the original CREATE INDEX statement.
		--This raises the following error. I have not found a way to 
		--suppress the error message:
			--Msg 3609, Level 16, State 2, Line 1
			--The transaction ended in the trigger. The batch has been aborted.
		BEGIN TRY
			ROLLBACK TRANSACTION;	
		END TRY 
		BEGIN CATCH
		END CATCH

		--1st pass: this should work for "vanilla" CREATE INDEX statements, such as
		--CREATE NONCLUSTERED INDEX [IndexName] ON [schema].[table]([column1], [column2], ...[columnN])
		DECLARE @NewDdl NVARCHAR(MAX) = '';
		SET @NewDdl = @DdlCmd + ' WITH(DATA_COMPRESSION = ROW)';

		BEGIN TRY
			EXEC(@NewDdl);
			--PRINT @NewDdl;
			RETURN;
		END TRY
		BEGIN CATCH
		END CATCH

		--2nd pass: this should work for CREATE INDEX statements that have a WITH clause, such as
		--CREATE NONCLUSTERED INDEX [IndexName] ON [schema].[table]([column1], [column2], ...[columnN]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
		DECLARE @LastWithPos INT = 0;
		SET @LastWithPos = CHARINDEX(REVERSE('WITH'), REVERSE(@DdlCmd), 1);

		IF @LastWithPos > 0
		BEGIN
			--Parse the "WITH" clause.
			SET @NewDdl = REVERSE(LEFT(REVERSE(@DdlCmd), @LastWithPos + LEN('WITH') - 1));
			
			--Remove whitespace
			SET @NewDdl = REPLACE(@NewDdl , ' ', '');
			SET @NewDdl = REPLACE(@NewDdl , CHAR(13), '');
			SET @NewDdl = REPLACE(@NewDdl , CHAR(10), '');
			SET @NewDdl = REPLACE(@NewDdl , CHAR(9), '');

			--Add "DATA_COMPRESSION" to the "WITH" clause.
			SET @NewDdl = REPLACE(@NewDdl, 'WITH(', 'WITH(DATA_COMPRESSION = ROW,');
			--New CREATE INDEX statement.
			SET @NewDdl = REVERSE(SUBSTRING(REVERSE(@DdlCmd), @LastWithPos + 4, LEN(@DdlCmd))) + @NewDdl;
		END

		BEGIN TRY
			EXEC(@NewDdl);
			--PRINT @NewDdl;
			RETURN;
		END TRY
		BEGIN CATCH
		END CATCH

		--3rd pass: just run the original CREATE INDEX statement and live with it for now.
		EXEC(@DdlCmd);
	END
GO
