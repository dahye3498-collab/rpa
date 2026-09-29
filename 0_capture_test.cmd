@echo off
chcp 65001 >nul
title 품목표 캡처 테스트 3건
cd /d "%~dp0"
echo [START] %date% %time% > _capture_test.log
for %%f in (run_*.py) do venv\Scripts\python.exe "%%f" --days 1 --capture-only --limit 3 >> _capture_test.log 2>&1
echo [END] %date% %time% >> _capture_test.log
echo.
echo 완료. 창을 닫아도 됩니다.
pause
