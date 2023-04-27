<#
    MIT License

    Copyright (c) 2021 Dave Mason

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
#>

<#
    .SYNOPSIS
        Export DACPAC Packages.

    .DESCRIPTION
        Creates/exports schema-only DACPAC files to disk.

    .INPUTS
        None - but note the SQL Server connection string info is hard-coded below.

    .OUTPUTS
        Date-named folders of *.dacpac files.

    .NOTES
        Version:        1.0
        Author:         DMason (https://mastodon.social/@DaveMasonDotMe)
        Creation Date:  2021/04/06
        
        History:
		YYYY/MM/DD	    Author
			Notes...
#>

$ServerInstance = "$env:COMPUTERNAME"   #Default instance of SQL Server (it's probably a good idea to hard-code this string if it's for a Windows Failover Cluster)
#$ServerInstance = "$env:COMPUTERNAME\SQL2019"  #Named instance of SQL Server.
$ExportPath = [System.IO.Path]::Combine($PSScriptRoot, $ServerInstance.Replace("\", "`$"))

#region Acquire SqlPackageExe path dynamically.
#$SqlPackageExe = "C:\Program Files (x86)\Microsoft SQL Server\140\Dac\bin\SqlPackage.exe"
#$SqlPackageExe = "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\IDE\Extensions\Microsoft\SQLDB\DAC\150\sqlpackage.exe"

$SqlPackageExe = ""
$DirList = New-Object Collections.Generic.List[string]

#1st search will be in the "SQL Server" folder.
$SearchPath = "${env:ProgramFiles(x86)}\Microsoft SQL Server"
$table = Get-Childitem –Path "$SearchPath\*\sqlpackage.exe" -Recurse -ErrorAction SilentlyContinue

foreach ($row in $table)
{
    $DirList.Add($row.Directory.FullName)
}

#If not found, the 2nd search will be in the "Visual Studio" x86 folder.
if($DirList.Count -lt 1)
{
    $SearchPath = "${env:ProgramFiles(x86)}\Microsoft Visual Studio"
    $table = Get-Childitem –Path "$SearchPath\*\sqlpackage.exe" -Recurse -ErrorAction SilentlyContinue

    foreach ($row in $table)
    {
        $DirList.Add($row.Directory.FullName)
    }
}

#If not found, the 3rd search will be in the "Visual Studio" X64 folder.
if($DirList.Count -lt 1)
{
    $SearchPath = "${env:ProgramFiles}\Microsoft Visual Studio"
    $table = Get-Childitem –Path "$SearchPath\*\sqlpackage.exe" -Recurse -ErrorAction SilentlyContinue

    foreach ($row in $table)
    {
        $DirList.Add($row.Directory.FullName)
    }
}

if($DirList.Count -gt 0)
{
    #There might be multiple copies of the file.
    #Sort the directory path strings in reverse, and assume 
    #the first path has the most recent version of the file.
    $DirList.Reverse()
    $SqlPackageExe = [System.IO.Path]::Combine($DirList[0], "sqlpackage.exe")
}

"SqlPackageExe: " + $SqlPackageExe
#endregion

#Customize query as needed to include/exclude databases.
$Query = "SELECT d.name " + [Environment]::NewLine + `
    "FROM master.sys.databases d " + [Environment]::NewLine + `
    "WHERE d.database_id > 4 " + [Environment]::NewLine + `
    "AND d.state_desc = 'ONLINE'" + [Environment]::NewLine + `
    "AND d.name NOT IN ('List of Exclustions');"
    #"AND d.name IN ('EDI', 'RouteOrganizer', 'SharedTables', 'Synergy');"

#Invoke-Sqlcmd usage varies by PowerShell version.
try {
    $databases = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $Query -OutputAs DataTables 
    $rowsCollection = $databases.Rows
}
catch {
    $databases = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $Query
    $rowsCollection = $databases 
}

$Date = Get-Date -Format "yyyy-MM-dd"

#Create directory for today (if needed).
if ([System.IO.Directory]::Exists([System.IO.Path]::Combine($ExportPath, $Date)) -ne $true)
{
    [System.IO.Directory]::CreateDirectory([System.IO.Path]::Combine($ExportPath, $Date))
}

#Iterate through the database names.
foreach($dataRow in $rowsCollection)
{
    $DatabaseName = $dataRow["name"]
    $TargetFile = [System.IO.Path]::Combine($ExportPath, $Date, $DatabaseName + ".dacpac")
    $SourceConnectionString = "Server=$ServerInstance;Integrated Security=SSPI;Database=$DatabaseName"

    & $SqlPackageExe /Action:Extract /TargetFile:$TargetFile /SourceConnectionString:$SourceConnectionString

    #Tried this to overcome some column validation errors related to OPENQUERY. It didn't work.
    #& $SqlPackageExe /Action:Extract /TargetFile:$TargetFile /SourceConnectionString:$SourceConnectionString /p:VerifyExtraction=False
}
