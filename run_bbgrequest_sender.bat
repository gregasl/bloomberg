rem pushd C:\Applications\VirtualEnvironments\Operations
pushd u:\bloomberg
call C:\Users\greg.mahoney\envs\bbgenv\Scripts\activate
rem call C:\Applications\VirtualEnvironments\Operations\Scripts\activate.bat
set PYTHONPATH=//aslfile01/aslcap/IT/software/Production/python
python 
deactivate
popd