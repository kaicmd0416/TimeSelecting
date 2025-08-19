@echo off
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"
set LOG_FILE=run.log
::cd /d "%SCRIPT_DIR%"
%ANACONDAPATH%\python -c "from signal_update_main import update_main; update_main()" >> %LOG_FILE% 2>&1

