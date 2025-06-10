#Set-PSDebug -Trace 1

param (
	[string]$Files = ".\*sql"
)

$sqlFiles = Get-ChildItem -Path $Files
foreach ($file in $sqlFiles) {
    try {
        Write-Output "Processing $file"
        sqlcmd -r -b -E -S ASLDB03 -i $file

	if ($LASTEXITCODE -ne 0) {
	  $SAVE_CODE=$LASTEXITCODE
	  Write-Error "Cannot load $file"
	  Set-PSDebug -Trace 0
	  Exit 1
	}
    } catch {
        Write-Error "sqlcmd failed: $($_.Exception.Message)"
        Set-PSDebug -Trace 0
        Exit 1
    }
}

