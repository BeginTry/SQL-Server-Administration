param(
    [String]$LogPath=""
)

<#
    .SYNOPSIS
        Purge log files.

    .DESCRIPTION
        Purge log files in a specific directory that are older than 30 days.

    .INPUTS
        $LogPath (required): self-explanatory

    .OUTPUTS
        None.

    .NOTES
        Log files to be purged must have this filename format:
            "*_????????_??????.txt"

    .HISTORY
        Version:        1.0
        Author:         DMason/Ntirety
        Creation Date:  2019/07/22
        Purpose/Change: Initial script development
#>


If ([String]::IsNullOrEmpty($LogPath))
{
    throw [System.IO.FileNotFoundException] "`$LogPath not specified."
}
ElseIf(![IO.Directory]::Exists($LogPath))
{
    throw [System.IO.DirectoryNotFoundException] "$LogPath not found."
}

Write-Host $LogPath
Write-Host "Hello World!"

Get-ChildItem $LogPath -File -Filter "*_????????_??????.txt" | Where CreationTime -lt  (Get-Date).AddDays(-30)  | Remove-Item -Force
