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

ROBOCOPY /XJ /MIR /DCOPY:T /R:1 /W:2 "C:\Storage\Backup\Server1$SQLEXPRESS" "\\Server2\SQLBackup\Server1$SQLEXPRESS" /NS /NC /NP /LOG:"%~dp0Logs\%~n0__1_%DateTime%.txt"


TIMEOUT /T 15