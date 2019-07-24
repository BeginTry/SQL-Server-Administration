TITLE %~n0
ECHO OFF
COLOR 0B
CLS

REM *********************************************************************
REM *	%~dp0	The directory where this script resides.		*
REM *	%~n0	The file name of this script (without the extension).	*
REM *********************************************************************

FOR /f %%a IN ('powershell -Command "Get-Date -format yyyyMMdd_HHmmss"') DO SET DateTime=%%a
REM ECHO %DateTime%

REM 1) Output File Cleanup
powershell.exe -file "%~dp010a Purge Log Files.ps1" -LogPath "%~dp0Logs"

REM 2) CommandLog Cleanup
sqlcmd -E -S .\SQLEXPRESS -d master -Q "DELETE FROM [dbo].[CommandLog] WHERE StartTime < DATEADD(dd,-90,GETDATE())" -b -o "%~dp0Logs\%~n0__1_%DateTime%.txt"

REM 3) Delete Backup History
sqlcmd -E -S .\SQLEXPRESS -d master -Q "DECLARE @CleanupDate datetime; SET @CleanupDate = DATEADD(dd,-90,GETDATE()); EXECUTE dbo.sp_delete_backuphistory @oldest_date = @CleanupDate;" -b -o "%~dp0Logs\%~n0__1_%DateTime%.txt"



TIMEOUT /T 15