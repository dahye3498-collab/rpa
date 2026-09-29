@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\foodiverse1\개발\vision-meat"
echo. >> routine_run.log
"venv\Scripts\python.exe" collect.py >> routine_run.log 2>&1
