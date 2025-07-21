REM Bloomberg API setup
@echo off

set ENVIRONMENT=%~3
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
   set REDIS_HOST=cacheuat
   set REDIS_PORT=6379
   set REDIS_DB=0
   set BBG_SQL_SERVER=asldb03
   set BBG_DATABASE=playdb
   set BBG_SQL_PORT=1433
   set LOG_DIR=output
   set PYTHONPATH=//aslfile01/aslcap/IT/software/Production/python
) ELSE (
    set REDIS_HOST=cacheprod
    set REDIS_PORT=6379
    set REDIS_DB=0
    set BBG_SQL_SERVER=asldb03
    set BBG_DATABASE=Bloomberg
    set BBG_SQL_PORT=1433
    set PYTHONPATH=//aslfile01/aslcap/IT/software/Production/python
    set LOG_DIR=output
)

set "SQL_USE_WINDOWS_AUTH=true"

REM pip install redis pyodbc oauthlib requests-oauthlib
REM echo "Environment is..."
pushd \\aslfile01\aslcap\IT\Software\Development\Bloomberg\https_requests
echo @on

REM set
REM call C:\Users\greg.mahoney\envs\bbgenv\Scripts\activate.bat
rem call C:\Applications\VirtualEnvironments\Operations\Scripts\activate.bat
echo Running request sender

C:\Applications\VirtualEnvironments\Operations\Scripts\python bbg_request_sender.py run_sequential

popd
