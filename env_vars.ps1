# Bloomberg API
$env:BLOOMBERG_DL_ACCOUNT_NUMBER=791793

# Redis
$env:REDIS_HOST="cacheuat"
$env:REDIS_PORT=6379
$env:REDIS_DB=0
# $env:REDIS_PASSWORD=your_password  # optional

# SQL Server
$env:BBG_SQL_SERVER="asldb03"
$env:BBG_DATABASE="playdb"
$env:BBG_SQL_PORT="1433"
# $env:SQL_USERNAME=your_username  # optional if using Windows auth
# $env:SQL_PASSWORD=your_password  # optional if using Windows auth
$env:SQL_USE_WINDOWS_AUTH="true"  # set to true for Windows authentication

# Polling Configuration (optional)
$env:POLL_INTERVAL=15  # seconds between polls
$env:MAX_RETRIES=20    # max retries before marking as failed
$env:BATCH_SIZE=10     # max requests to process per iteration

# pip install redis pyodbc oauthlib requests-oauthlib
