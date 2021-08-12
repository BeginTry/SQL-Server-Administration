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
        None - but note the list of SQL Server instances is hard-coded below.
		The path to SqlPakage.exe is also hard-coded.

    .OUTPUTS
        Date-named folders of *.dacpac files.

    .NOTES
        Version:        1.0
        Author:         DMason 
        Creation Date:  2021/08/12
        
        History:
		YYYY/MM/DD	    Author
			Notes...
#>

$ServerInstances = @("Server1", "Server2\SQLExpress", "SERVER3", "etc")
$SqlPackageExe = "C:\Program Files (x86)\Microsoft SQL Server\140\Dac\bin\SqlPackage.exe"
#$SqlPackageExe = "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\IDE\Extensions\Microsoft\SQLDB\DAC\150\sqlpackage.exe"

#Customize query as needed to include/exclude databases.
$Query = "SELECT d.name " + [Environment]::NewLine + `
    "FROM master.sys.databases d " + [Environment]::NewLine + `
    "WHERE d.database_id > 4 " + [Environment]::NewLine + `
    "AND d.name NOT IN ('SSISDB', 'ReportServer', 'ReportServerTempDB', 'tpcc') " + [Environment]::NewLine + `
    "ORDER BY d.name;"


foreach ($ServerInstance in $ServerInstances) 
{
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
    $ExportPath = [System.IO.Path]::Combine($PSScriptRoot, $ServerInstance.Replace("\", "$"), "$Date")

    #Create directory for today (if needed).
    if ([System.IO.Directory]::Exists($ExportPath) -ne $true)
    {
        [System.IO.Directory]::CreateDirectory($ExportPath)
    }

    #Iterate through the database names.
    foreach($dataRow in $rowsCollection)
    {
        $DatabaseName = $dataRow["name"]
        $TargetFile = [System.IO.Path]::Combine($ExportPath, $DatabaseName + ".dacpac")
        $SourceConnectionString = "Server=$ServerInstance;Integrated Security=SSPI;Database=$DatabaseName"

        & $SqlPackageExe /Action:Extract /TargetFile:$TargetFile /SourceConnectionString:$SourceConnectionString
    }

}
