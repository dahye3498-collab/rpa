@echo off
chcp 65001 >nul
title 품목표 오늘 캡처
cd /d "%~dp0"
echo ============================================
echo   품목표 오늘치 캡처  (무료 - OCR 안 함)
echo ============================================
echo.
echo [1/2] 브라우저 프로필 정리 중...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_free_profile.ps1"
echo.
echo [2/2] 캡처 시작... 크롬 창이 뜹니다.
echo       (봇체크가 뜨면 그 창에서 처리 후 잠시 기다리세요)
echo.
echo [START] %date% %time% > _capture.log
venv\Scripts\python.exe run_품목표.py --days 1 --capture-only >> _capture.log 2>&1
echo [END] %date% %time% >> _capture.log
echo.
echo ============================================
echo   완료. 상세 로그: _capture.log
echo ============================================
pause
