REM Bloomberg API setup
@echo off

set PROGRAM=%~1
set ENVIRONMENT=%~2
:: Check if the first parameter (%1) is empty
if "%ENVIRONMENT%" == "" (
    set ENVIRONMENT="PROD"
) 

:: You can then use %param1_value% throughout your script
:: For example:
:: echo Processing with: %param1_value%

set BLOOMBERG_DL_ACCOUNT_NUMBER=791793

REM Redis
IF "%ENVIRONMENT%" == "DEV" (
   set REDIS_HOST="cacheuat"
   set REDIS_PORT=6379
   set REDIS_DB=0
   set BBG_SQL_SERVER="asldb03"
   set BBG_DATABASE="playdb"
   set BBG_SQL_PORT="1433"
   set LOG_DIR="output"
   set PYTHONPATH="//aslfile01/aslcap/IT/software/Development/python;//aslfile01/aslcap/IT/software/Production/python"
) ELSE (
    set REDIS_HOST="cacheprod"
    set REDIS_PORT=6379
    set REDIS_DB=0
    set BBG_SQL_SERVER="asldb03"
    set BBG_DATABASE="playdb"
    set BBG_SQL_PORT="1433"
    REM change this once asql and asl_logging is released
    set PYTHONPATH="//aslfile01/aslcap/IT/software/Development/python;//aslfile01/aslcap/IT/software/Production/python"
    set LOG_DIR="output"
)


REM $env:REDIS_PASSWORD=your_password  REM optional
REM $env:SQL_USERNAME=your_username  REM optional if using Windows auth
REM $env:SQL_PASSWORD=your_password REM optional if using Windows auth
REM set to true for Windows authentication
set SQL_USE_WINDOWS_AUTH "true"  

REM these are in a db table now 
REM Polling Configuration (optional)
REM set POLL_INTERVAL=15  # seconds between polls
REM set MAX_RETRIES=20    # max retries before marking as failed
REM set BATCH_SIZE=10     # max requests to process per iteration

REM pip install redis pyodbc oauthlib requests-oauthlib
echo "Environment is..."
set
echo Running %PROGRAM%
%PROGRAM%
