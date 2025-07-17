REM Bloomberg API setup
@echo on

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
) ELSE (
    set REDIS_HOST="cacheprod"
    set REDIS_PORT=6379
    set REDIS_DB=0
    set BBG_SQL_SERVER="asldb03"
    set BBG_DATABASE="Bloomberg"
    set BBG_SQL_PORT="1433"
)


REM set to true for Windows authentication
set SQL_USE_WINDOWS_AUTH "true"  

REM pip install redis pyodbc oauthlib requests-oauthlib
echo "Environment is..."
set
echo "Running "%PROGRAM%
REM %PROGRAM%

