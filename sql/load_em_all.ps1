# MUST BE TRUN IN PYTHIN 3.9 - 
# example - Environment - Users\greg.mahoney\python3.9env
$sqlFiles = Get-ChildItem -Path .\*.sql
foreach ($file in $sqlFiles) {
    try {
        Write-Output "Processing $file"
        mssql-cli -E -S ASLDB03 -i $file -r0

	if ($LASTEXITCODE -ne 0) {
	  Write-Error "Cannot load $file"
	  exit 1
	}
    } catch {
        Write-Error "mssql failed: $($_.Exception.Message)"
        exit 
    }
}

