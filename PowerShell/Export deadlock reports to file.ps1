<#
    .SYNOPSIS
        Save SQL Server deadlock reports.

    .DESCRIPTION
        Saves the XML Deadlock Report for each event occurrence to file on disk.

    .INPUTS
        None - but note the SQL Server connection string info is hard-coded below.

    .OUTPUTS
        Folder of *.xdl deadlock graph files.

    .NOTES
        Version:        1.0
        Author:         DMason/Ntirety 
        Creation Date:  2020/09/09
        
        History:
		YYYY/MM/DD	    Author
			Notes...
#>

#Default instance: "MyHostName" 
    #-or- 
#Named instance: "MyHostName\SQLExpress"
$SqlInstance = "MyHostName"

#Leave these NULL for Windows Authentication. 
#Otherwise, for SQL Authentication, provide login name and password.
$login = $null
$password = $null




# Create, open SQL connection
$conn = New-Object System.Data.SqlClient.SqlConnection
if($login -eq $null)
{
    $conn.ConnectionString = "Server=" + $SqlInstance + ";Database=tempdb;Integrated Security=True"
}
else
{
    $conn.ConnectionString = "Server=Server=" + $SqlInstance + ";Database=tempdb;user=" + $login + ";password=" + $password
}
$conn.Open()


# Get data via SqlDataReader
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.CommandText = ";WITH XEvents AS
(
	SELECT object_name, CAST(event_data AS XML) AS event_data
	FROM sys.fn_xe_file_target_read_file ( 'system_health*.xel', NULL, NULL, NULL )  
	WHERE object_name = 'xml_deadlock_report'
)
SELECT 
	--Adjust XEvent timestamp from UTC to EST.
	DATEADD(HOUR, -4, event_data.value ('(/event/@timestamp)[1]', 'DATETIME')) AS [timestamp],
	event_data.query ('(/event/data[@name=''xml_report'']/value/deadlock)[1]') AS [xml_deadlock_report]
FROM XEvents;"

$cmd.Connection = $conn
$dr = $cmd.ExecuteReader()
$OutputPath = [System.IO.Path]::Combine($PSScriptRoot, $SqlInstance.Replace("\", "$"))
$OutputPath = [System.IO.Path]::Combine($OutputPath, "Deadlock Graphs")

if($dr.HasRows)
{
    #Create the "Deadlock Graphs" subfolder where this script resides (if necessary).
    if(-not [System.IO.Directory]::Exists($OutputPath))
    {
        [System.IO.Directory]::CreateDirectory($OutputPath) Out-Null
    }
}

while ($dr.Read())
{
    #Change file name format as desired within .ToString()
    $fileName = $dr.GetDateTime(0).ToString("yyyy-MM-dd ~ HH_mm_ss.fff") + ".xdl"
    $deadlockGraphFilepath = [System.IO.Path]::Combine($OutputPath, $fileName)
    $scriptContents = $dr.GetString(1)
    Set-Content -Path $deadlockGraphFilepath -Value $scriptContents
}
$dr.Dispose()
$cmd.Dispose()
$conn.Close()
$conn.Dispose()
