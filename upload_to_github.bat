@echo off
REM ============================================================
REM  UPLOAD TO GITHUB - Reg 153 Chemical Matcher
REM ============================================================
REM  Usage:
REM    upload_to_github.bat                     (uses default commit message)
REM    upload_to_github.bat "your message here" (custom commit message)
REM
REM  FIRST-TIME SETUP (run these once in terminal):
REM    git config --global user.name "Your Name"
REM    git config --global user.email "your.email@example.com"
REM    git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
REM ============================================================

cd /d "%~dp0"

REM --- Check if git is initialized ---
if not exist ".git" (
    echo [ERROR] No .git directory found. Run: git init
    pause
    exit /b 1
)

REM --- Check if remote is configured ---
git remote get-url origin >nul 2>&1
if errorlevel 1 (
    echo.
    echo ============================================================
    echo  NO REMOTE CONFIGURED
    echo ============================================================
    echo  Run this command first with your GitHub repo URL:
    echo.
    echo    git remote add origin https://github.com/USERNAME/REPO.git
    echo.
    echo  Then re-run this script.
    echo ============================================================
    pause
    exit /b 1
)

REM --- Set commit message ---
if "%~1"=="" (
    set COMMIT_MSG=Update: %date% %time:~0,5%
) else (
    set COMMIT_MSG=%~1
)

echo.
echo ============================================================
echo  UPLOADING TO GITHUB
echo ============================================================
echo  Commit: %COMMIT_MSG%
echo.

REM --- Stage all changes ---
echo [1/4] Staging changes...
git add -A

REM --- Show what's being committed ---
echo.
echo [2/4] Changes to commit:
git status --short
echo.

REM --- Commit ---
echo [3/4] Committing...
git commit -m "%COMMIT_MSG%"
if errorlevel 1 (
    echo.
    echo [INFO] Nothing to commit - working tree clean.
    pause
    exit /b 0
)

REM --- Push ---
echo.
echo [4/4] Pushing to GitHub...
git push -u origin main
if errorlevel 1 (
    echo.
    echo [WARN] Push to 'main' failed, trying 'master'...
    git push -u origin master
    if errorlevel 1 (
        echo.
        echo [ERROR] Push failed. You may need to:
        echo   - Set up authentication (GitHub token or SSH key)
        echo   - Create the repo on GitHub first
        echo   - Run: git branch -M main
        pause
        exit /b 1
    )
)

echo.
echo ============================================================
echo  UPLOAD COMPLETE
echo ============================================================
echo.
pause
