<#
    MIT License

    Copyright (c) 2019 Dave Mason

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
        Gets SSIS metadata.

    .DESCRIPTION
        Gets SSIS metadata including catalogs, folders, projects, packages, and tasks.

    .INPUTS
        Name of the SQL Server instance where the SSIS catalog database(s) reside(s).

    .OUTPUTS
        List of SSIS catalogs, folders, projects, packages, package task types, task names, task order, and task descriptions.

    .NOTES
        Version:        1.0
        Author:         Dave Mason
        Creation Date:  2019/09/27
        
        History:
		YYYY/MM/DD	    Author
			Notes...
#>

<#
    Assembly versions that worked on my laptop:

    GAC    Version        Location                                                                                                                                                                                        
    ---    -------        --------                                                                                                                                                                                        
    True   v4.0.30319     C:\WINDOWS\Microsoft.Net\assembly\GAC_MSIL\Microsoft.SqlServer.ManagedDTS\v4.0_14.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.ManagedDTS.dll                                                    
    True   v2.0.50727     C:\WINDOWS\assembly\GAC_MSIL\Microsoft.SqlServer.Management.IntegrationServices\14.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.Management.IntegrationServices.dll                               
    True   v2.0.50727     C:\WINDOWS\assembly\GAC_MSIL\Microsoft.SqlServer.Management.Sdk.Sfc\14.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.Management.Sdk.Sfc.dll                                                       
    True   v2.0.50727     C:\WINDOWS\assembly\GAC_MSIL\Microsoft.SqlServer.Smo\14.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.Smo.dll                                                                                     
    True   v4.0.30319     C:\WINDOWS\Microsoft.Net\assembly\GAC_MSIL\System.IO.Compression\v4.0_4.0.0.0__b77a5c561934e089\System.IO.Compression.dll    
#>

# Possible issues: what if these assemblies aren't present? or are the wrong version?
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.ManagedDTS")
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Management.IntegrationServices")
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Management.Sdk.Sfc")
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")
[System.Reflection.Assembly]::LoadWithPartialName("System.IO.Compression")


<#
    Adds a row to the meta data table.
    (Params should be self-explanatory.)
#>
function Add-MetaDataTableRow([string]$catalog, [string]$folder, [string]$project, [string]$package, [string]$taskType, [string]$taskName, [string]$taskOrder, [string]$taskDescription)
{
    [System.Data.DataRow]$row = $SsisMetaData.NewRow()
    $row["Catalog"] = $catalog
    $row["Folder"] = $folder
    $row["Project"] = $project
    $row["Package"] = $package
    $row["Task Type"] = $taskType
    $row["Task Name"] = $taskName
    $row["Task Order"] = $taskOrder
    $row["Task Description"] = $taskDescription
            
    $SsisMetaData.Rows.Add($row)
}

function Get-SortedPrecedenceConstraints()
{
    [OutputType([System.Collections.Generic.List[Microsoft.SqlServer.Dts.Runtime.PrecedenceConstraint]])]
    Param (
        [parameter(Mandatory=$true)]
        [Microsoft.SqlServer.Dts.Runtime.PrecedenceConstraints]
        $precedenceConstraints
    )
    # The PrecedenceConstraint objects in the collection are chained together. 
    # PrecedenceExecutable is the "previous" executable. 
    # ConstrainedExecutable is the "next" executable.

    $ret = [System.Collections.Generic.List[Microsoft.SqlServer.Dts.Runtime.PrecedenceConstraint]]::new()
    $ConstrainedExecutableIds = [System.Collections.Generic.List[string]]::new()

    #Create list of constrained executable Ids.
    for ([int]$i = 0; $i -lt $precedenceConstraints.Count; $i++)
    {
        [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$container = [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$precedenceConstraints[$i].ConstrainedExecutable
        $ConstrainedExecutableIds.Add($container.ID)
    }

    #Find the PrecedenceExecutable that doesn't have its ID in the string list.
    for ([int]$i = 0; $i -lt $precedenceConstraints.Count; $i++)
    {
        [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$container = [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$precedenceConstraints[$i].PrecedenceExecutable

        if ( -not $ConstrainedExecutableIds.Contains($container.ID) )
        {
            $ret.Add($precedenceConstraints[$i])
            break
        }
    }
    
    while ($ret.Count -lt $precedenceConstraints.Count)
    {
        [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$lastContainer = [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$ret[$ret.Count - 1].ConstrainedExecutable

        # Although the PrecedenceConstraints chain may be out of order,
        # don't assume the chain is unbroken. 
        [bool]$found = $false

        for ([int]$i = 0; $i -lt $precedenceConstraints.Count; $i++)
        {
            [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$container = [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$precedenceConstraints[$i].PrecedenceExecutable

            if ($container.ID -eq $lastContainer.ID)
            {
                $ret.Add($precedenceConstraints[$i])
                $found = $true
                break
            }
        }

        # TODO: throw an exception?
        if( -not $found )
        {
            break
        }
    }

    return $ret
}

function Get-TaskType()
{
    [OutputType([string])]
    Param (
        [parameter(Mandatory=$true)]
        [Microsoft.SqlServer.Dts.Runtime.EventsProvider]
        $container
    )

    [string]$ret = $container.GetType().Name

    if ([string]::Compare($ret, "TaskHost", $true) -eq 0)
    {
        $ret = ([Microsoft.SqlServer.Dts.Runtime.TaskHost]$container).InnerObject.GetType().ToString()
        $ret = $ret.Split('.')[$ret.Split('.').Length - 1]

        if([string]::Compare($ret, "__ComObject", $true) -eq 0)
        {
            #This seems to happen for Data Flow Tasks. 
            $ret = $container.GetType().Name
        }
    }

    return $ret
}


#region Create/reset DataTable, add columns.
[System.Data.DataTable]$SsisMetaData = [System.Data.DataTable]::new()
$SsisMetaData.Columns.Add("Catalog") | Out-Null
$SsisMetaData.Columns.Add("Folder") | Out-Null
$SsisMetaData.Columns.Add("Project") | Out-Null
$SsisMetaData.Columns.Add("Package") | Out-Null
$SsisMetaData.Columns.Add("Task Type") | Out-Null
$SsisMetaData.Columns.Add("Task Name") | Out-Null
$SsisMetaData.Columns.Add("Task Order") | Out-Null
$SsisMetaData.Columns.Add("Task Description") | Out-Null
#endregion

# Prompt user.
Write-host "Enter the SQL Server instance" -ForegroundColor Yellow 
[string]$SqlInstance = Read-Host " (where the SSIS catalog database resides) " 

if([string]::IsNullOrEmpty($SqlInstance))
{
    Write-host "Nothing entered." -ForegroundColor Magenta
    return
}

[System.Data.SqlClient.SqlConnection]$conn = [System.Data.SqlClient.SqlConnection]::new()
[System.Data.SqlClient.SqlConnectionStringBuilder]$csb = [System.Data.SqlClient.SqlConnectionStringBuilder]::new()
$csb["Initial Catalog"] = "tempdb"
#$csb["Data Source"] = ".\SQL2017"
$csb["Data Source"] = $SqlInstance
$csb["Integrated Security"] = $true
$conn.ConnectionString = $csb.ToString()
[Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices]$intSvcs = [Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices]::new($conn)
           
foreach ($cat in $intSvcs.Catalogs)
{
    foreach ($folder in $cat.Folders)
    {
        foreach ($proj in $folder.Projects)
        {
            <#
                Code enhanced/adapted from Jonathan Garvey's StackOverflow answer:
                https://stackoverflow.com/questions/40439662/get-package-xml-from-ssis-catalog-with-powershell/#43368494
            #>
            [byte[]]$projectBytes = $intSvcs.Catalogs[$cat.Name].Folders[$folder.Name].Projects[$proj.Name].GetProjectBytes();
            [System.IO.Stream]$stream = [System.IO.MemoryStream]::new($projectBytes)
            [System.IO.Compression.ZipArchive]$za = [System.IO.Compression.ZipArchive]::new($stream)

            foreach ($pkgInfo in $proj.Packages)
            {
                foreach ($zipEntry in $za.Entries)
                {
                    if ($zipEntry.FullName -eq $pkgInfo.Name)
                    {
                        [Microsoft.SqlServer.Dts.Runtime.Package]$pkg = [Microsoft.SqlServer.Dts.Runtime.Package]::new()
                        [System.IO.StreamReader]$sr = [System.IO.StreamReader]::new($zipEntry.Open())
                        $pkg.LoadFromXML($sr.ReadToEnd(), $null)
        
                        [System.Collections.Generic.List[string]]$constrainedExeId = [System.Collections.Generic.List[string]]::new()
                        [System.Collections.Generic.List[Microsoft.SqlServer.Dts.Runtime.PrecedenceConstraint]]$precedenceConstraints = `
                            Get-SortedPrecedenceConstraints($pkg.PrecedenceConstraints);
                        [string]$TaskType = $null;

                        #region Iterate through the PrecedentConstraint objects.
                        # Find the executables that are part of precedent constraints.
                        # We'll want to output those first (in order).
                        if ($precedenceConstraints.Count -gt 0)
                        {
                            # Cast the Executable to an EventsProvider class so we can get 
                            # the .Name and .Description property values.
                            [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$container = `
                                [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$precedenceConstraints[0].PrecedenceExecutable
                            $TaskType = Get-TaskType($container)
                                            
                            Add-MetaDataTableRow $cat.Name $folder.Name $proj.Name $pkgInfo.Name $TaskType $container.Name "1" $container.Description
                            $constrainedExeId.Add($container.ID)

                            for ([int]$i = 0; $i -lt $precedenceConstraints.Count; $i++)
                            {
                                $container = [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$precedenceConstraints[$i].ConstrainedExecutable
                                $TaskType = Get-TaskType($container)
                                Add-MetaDataTableRow $cat.Name $folder.Name $proj.Name $pkgInfo.Name $TaskType $container.Name ($i+2).ToString() $container.Description
                                $constrainedExeId.Add($container.ID)
                            }
                        }
                        #endregion

                        #region Find remaining executables that are not part of a precedence constraint.
                        foreach ($exe in $pkg.Executables)
                        {
                            # Cast the Executable to an EventsProvider class so we can get 
                            # the .Name and .Description property values.
                            [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$container = [Microsoft.SqlServer.Dts.Runtime.EventsProvider]$exe

                            if ( -not $constrainedExeId.Contains($container.ID))
                            {
                                $TaskType = Get-TaskType($container)
                                Add-MetaDataTableRow $cat.Name $folder.Name $proj.Name $pkgInfo.Name $TaskType $container.Name "" $container.Description

                                
                            }
                        }
                        #endregion
                        break
                    }
                }
            }
        }
    }
}

$SsisMetaData | format-table | out-host
