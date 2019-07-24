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

sqlcmd -E -S .\SQLEXPRESS -d master -Q "EXEC master.sys.sp_cycle_errorlog;" -b -o "%~dp0Logs\%~n0__1_%DateTime%.txt"

TIMEOUT /T 15