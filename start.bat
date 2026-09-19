@echo off
title AWS Serverless Pipeline - Local Runner
echo ========================================================
echo   Starting Local AWS Serverless Pipeline Simulator...
echo   (No AWS Account or Credits required!)
echo ========================================================
echo.
echo Opening Uploader and Architecture Monitor in Browser...
timeout /t 2 /nobreak >nul
start http://localhost:3000/dashboard
start http://localhost:3000
echo.
node server.js
pause
