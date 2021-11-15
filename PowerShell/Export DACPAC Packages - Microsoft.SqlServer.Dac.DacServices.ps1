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
        Class "Microsoft.SqlServer.Dac.DacServices" is used programatically.
        Because of a need to use SQL Authentication, I thought I had a need/use case for this at the time. 
        In hindsight, shelling out to SqlPackage.exe probably makes more sense. 
        But...if you have a specific need (or don't want to use SqlPackage.exe), this scipt might be of use.
        
    .HISTORY:
        Version:        1.0
        Author:         DMason 
        Creation Date:  2021/11/11
#>

[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | out-Null
[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Dac.DacServices") | out-Null
add-type -path "C:\Program Files\WindowsPowerShell\Modules\SqlServer\Microsoft.SqlServer.Dac.dll" 
Import-Module SqlServer


$ServerInstance = "server name"
$Login = "login name"
$PWord = ConvertTo-SecureString -String "password" -AsPlainText -Force
$PWord.MakeReadOnly()
$SqlAuthCredential = New-Object -TypeName System.Data.SqlClient.SqlCredential -ArgumentList $Login, $PWord
$ExportPath = [System.IO.Path]::Combine($PSScriptRoot, $ServerInstance.Replace("\", "`$"))
$Date = Get-Date -Format "yyyy-MM-dd"

#Create directory for today (if needed).
if ([System.IO.Directory]::Exists([System.IO.Path]::Combine($ExportPath, $Date)) -ne $true)
{
    [System.IO.Directory]::CreateDirectory([System.IO.Path]::Combine($ExportPath, $Date))
}

$conn = New-Object System.Data.SqlClient.SqlConnection ("Initial Catalog=master;Data Source=$ServerInstance", $SqlAuthCredential)
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$dacService = New-Object Microsoft.SqlServer.Dac.DacServices ("server=$ServerInstance;User ID=$Login;", $PWord)

#Customize query as needed to include/exclude databases.
$cmd.CommandText = "SELECT d.name " + [Environment]::NewLine + `
    "FROM master.sys.databases d " + [Environment]::NewLine + `
    "WHERE d.database_id > 4 " + [Environment]::NewLine + `
    "AND d.state_desc = 'ONLINE'" + [Environment]::NewLine + `
    "AND d.name NOT IN ('SSISDB', 'ReportServer', 'ReportServerTempDB');"
$cmd.Connection = $conn
$dr = $cmd.ExecuteReader()

#Iterate through the database names.
while ($dr.Read())
{
    $DatabaseName = $dr.GetString(0)
    $TargetFile = [System.IO.Path]::Combine($ExportPath, $Date, $DatabaseName + ".dacpac")
    $dacService.Extract($TargetFile, $DatabaseName, "AppName", "1.0.0.0", $null, $null, $null, $null)
}
$dr.Close()
$dr.Dispose()
$cmd.Dispose()
$conn.Close()
$conn.Dispose()
