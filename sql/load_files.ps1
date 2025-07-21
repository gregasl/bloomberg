param (
	[string]$Files = ".\*.sql",
	[string]$Database = "<database>"
)

# Set-PSDebug -Trace 1

$sqlFiles = Get-ChildItem -Path $Files

foreach ($file in $sqlFiles) {
   try {
      Write-Output "Processing $file"
	  $process_file = $file.name + ".sub"
	  Write-Output "output...Processing $process_file"
	(Get-Content -Path $file) -replace "<database>", $Database | Set-Content -Path $process_file
     sqlcmd -r -b -E -S ASLDB03 -i $process_file

	if ($LASTEXITCODE -ne 0) {
	  $SAVE_CODE=$LASTEXITCODE
	  Write-Error "Cannot load $file / $process_file"
	  Set-PSDebug -Trace 0
	  Exit 1
	}
	Remove-Item -Path $process_file
   } catch {
        Write-Error "sqlcmd failed: $($_.Exception.Message)"
        Set-PSDebug -Trace 0
        Exit 1
    }
}

Set-PSDebug -Trace 0

