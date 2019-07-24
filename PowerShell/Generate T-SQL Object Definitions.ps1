<#
    .SYNOPSIS
        Script object definitions.

    .DESCRIPTION
        Generate T-SQL Scripts for objects in each SQL Server Database.

    .INPUTS
        None - but note the server name and output path are hard-coded below.

    .OUTPUTS
        Folder of T-SQL script files.

    .NOTES
        Version:        1.0
        Author:         DMason
        Creation Date:  2019/07/12
        Adapted From:   https://www.mssqltips.com/sqlservertip/4606/generate-tsql-scripts-for-all-sql-server-databases-and-all-objects-using-powershell/
        
        History:
#>
$date_ = (date -f yyyy-MM-dd)
$SqlInstances = @("Server1", "Server2", "Server3\SQLExpress")

foreach ($SqlInstance in $SqlInstances) 
{
    $path = [System.IO.Path]::Combine($PSScriptRoot, $SqlInstance.Replace("\", "$"), "$date_")
    Write-Host $path -ForegroundColor Yellow
 
    [System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO')
    $serverInstance = New-Object ('Microsoft.SqlServer.Management.Smo.Server') $SqlInstance

    <#
        #https://docs.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.management.smo.database
        Collection properties of the Database class:
            ActiveConnections
            ApplicationRoles
            Assemblies
            AsymmetricKeys
            Certificates
            ColumnEncryptionKeys
            ColumnMasterKeys
            DatabaseAuditSpecifications
            DatabaseOptions
            DatabaseScopedConfigurations
            DatabaseScopedCredentials
            Defaults
            Events
            ExtendedProperties
            ExtendedStoredProcedures
            ExternalDataSources
            ExternalFileFormats
            ExternalLibraries
            FileGroups
            FullTextCatalogs
            FullTextStopLists
            LogFiles
            PartitionFunctions
            PartitionSchemes
            PlanGuides
            ReplicationOptions
            Roles
            Rules
            Schemas
            SearchPropertyLists
            SecurityPolicies
            Sequences
            StoredProcedures
            SymmetricKeys
            Synonyms
            Tables
            TransformNoiseWords
            Triggers
            UserDefinedAggregates
            UserDefinedDataTypes
            UserDefinedFunctions
            UserDefinedTableTypes
            UserDefinedTypes
            Users
            Views
            XmlSchemaCollections
    #>
    #Objects you want to backup. 
    $IncludeTypes = @("Tables","StoredProcedures","Views","UserDefinedFunctions", "Triggers", "UserDefinedDataTypes", "UserDefinedTableTypes", "Synonyms", "Schemas", "Roles", "Users") 
    $ExcludeSchemas = @("sys","Information_Schema")
    $so = new-object ('Microsoft.SqlServer.Management.Smo.ScriptingOptions')

    #Scripting options for CREATE TABLE statements.
    #https://docs.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.management.smo.scriptingoptions
    $TableSO = New-Object ('Microsoft.SqlServer.Management.Smo.ScriptingOptions')
    $TableSO.DriAllConstraints = $true
    $TableSO.DriAllKeys = $true
    $TableSO.Indexes = $true
    $TableSO.FullTextCatalogs = $true
    $TableSO.FullTextIndexes = $true
    $TableSO.Triggers = $true
    $TableSO.XmlIndexes = $true

 
    $dbs=$serverInstance.Databases 
    foreach ($db in $dbs)
    {
        if($db.IsSystemObject -eq $false)
        {
            $dbname = "$db".replace("[","").replace("]","")
            $dbpath = [System.IO.Path]::Combine("$path", "$dbname")

            if ( !(Test-Path $dbpath))
            {
                $null=new-item -type directory -name "$dbname"-path "$path"
            }
 
            foreach ($Type in $IncludeTypes)
            {
                if ($db.$Type.Count -gt 0)
                {
                    $objpath = [System.IO.Path]::Combine("$dbpath", "$Type")
                    if ( !(Test-Path $objpath))
                    {
                        $null=new-item -type directory -name "$Type"-path "$dbpath"
                    }

                    foreach ($objs in $db.$Type)
                    {
                        If ($ExcludeSchemas -notcontains $objs.Schema -and $objs.IsSystemObject -eq $false) 
                        {
                            #Some characters are invalid for Windows folder/file names.
                            #Replace as needed.
                            $ObjName = "$objs".Replace("[","").Replace("]","").Replace("\","$").Replace("*", "$")                  
                            $OutFile = [System.IO.Path]::Combine("$objpath", ("$ObjName" + ".sql"))

                            if($Type -eq "Tables") #Careful with this string comparison-case-sensitivity matters!
                            {
                                $objs.Script($TableSO)+"GO" | out-File $OutFile
                            }
                            else
                            {
                                $objs.Script($so)+"GO" | out-File $OutFile
                            }
                
                        }
                    }
                }
            }  
        } 
    }

}
