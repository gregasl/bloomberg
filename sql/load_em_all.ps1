# MUST BE TRUN IN PYTHIN 3.9 - 
# example - Environment - Users\greg.mahoney\python3.9env
$sqlFiles = Get-ChildItem -Path .\*.sql
foreach ($file in $sqlFiles) {
    try {
        Write-Output "Processing $file"
        mssql-cli -E -S ASLDB03 -i $file
    } catch {
        Write-Error "mssql failed: $($_.Exception.Message)"
        exit 
    }
}

