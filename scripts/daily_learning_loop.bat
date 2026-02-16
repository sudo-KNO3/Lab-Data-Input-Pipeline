@echo off
REM Daily learning loop automation for Chemical Matcher
REM Runs validation ingestion, report generation, and periodic calibration
REM
REM Schedule this script to run daily at 2 AM using Windows Task Scheduler

setlocal enabledelayedexpansion

REM ============================================================================
REM Configuration
REM ============================================================================

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
set PYTHON=python
set LOG_DIR=%PROJECT_ROOT%\logs\daily_loop

REM Create log directory if it doesn't exist
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Set timestamp for logging
set TIMESTAMP=%DATE:~-4,4%%DATE:~-10,2%%DATE:~-7,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%
set TIMESTAMP=%TIMESTAMP: =0%
set LOG_FILE=%LOG_DIR%\daily_loop_%TIMESTAMP%.log

REM ============================================================================
REM Start Daily Loop
REM ============================================================================

echo ========================================================================== >> "%LOG_FILE%"
echo Chemical Matcher Daily Learning Loop >> "%LOG_FILE%"
echo Started: %DATE% %TIME% >> "%LOG_FILE%"
echo ========================================================================== >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

echo [%TIME%] Starting Chemical Matcher Daily Learning Loop...
echo [%TIME%] Log file: %LOG_FILE%

REM Change to project root directory
cd /d "%PROJECT_ROOT%"

REM ============================================================================
REM Step 1: Auto-ingest validated review queues (Layer 1 Learning)
REM ============================================================================

echo. >> "%LOG_FILE%"
echo [STEP 1] Auto-ingesting validated review queues... >> "%LOG_FILE%"
echo -------------------------------------------------------------------------- >> "%LOG_FILE%"
echo [%TIME%] [STEP 1/5] Auto-ingesting validated review queues...

%PYTHON% scripts\12_validate_and_learn.py --auto-ingest >> "%LOG_FILE%" 2>&1

if errorlevel 1 (
    echo [%TIME%] ERROR: Validation ingestion failed! >> "%LOG_FILE%"
    echo [%TIME%] ERROR: Validation ingestion failed!
    goto :error
) else (
    echo [%TIME%] SUCCESS: Validation ingestion completed >> "%LOG_FILE%"
    echo [%TIME%] SUCCESS: Validation ingestion completed
)

REM ============================================================================
REM Step 2: Generate weekly learning health report
REM ============================================================================

echo. >> "%LOG_FILE%"
echo [STEP 2] Generating learning health report... >> "%LOG_FILE%"
echo -------------------------------------------------------------------------- >> "%LOG_FILE%"
echo [%TIME%] [STEP 2/5] Generating learning health report...

set REPORT_TIMESTAMP=%DATE:~-4,4%%DATE:~-10,2%%DATE:~-7,2%
set REPORT_TIMESTAMP=%REPORT_TIMESTAMP: =0%
set REPORT_FILE=reports\daily\learning_report_%REPORT_TIMESTAMP%.md

%PYTHON% scripts\13_generate_learning_report.py --output "%REPORT_FILE%" --days 7 >> "%LOG_FILE%" 2>&1

if errorlevel 1 (
    echo [%TIME%] WARNING: Learning report generation failed >> "%LOG_FILE%"
    echo [%TIME%] WARNING: Learning report generation failed
    REM Continue despite warning
) else (
    echo [%TIME%] SUCCESS: Learning report generated: %REPORT_FILE% >> "%LOG_FILE%"
    echo [%TIME%] SUCCESS: Learning report generated
)

REM ============================================================================
REM Step 3: Monthly threshold calibration (run on 1st of month)
REM ============================================================================

echo. >> "%LOG_FILE%"
echo [STEP 3] Checking if monthly calibration is due... >> "%LOG_FILE%"
echo -------------------------------------------------------------------------- >> "%LOG_FILE%"
echo [%TIME%] [STEP 3/5] Checking monthly calibration...

REM Get current day of month
for /f "tokens=1 delims=/" %%a in ('echo %DATE:~-7,2%') do set DAY=%%a

REM Remove leading zero
set DAY=%DAY: =%
if "%DAY:~0,1%"=="0" set DAY=%DAY:~1%

if %DAY%==1 (
    echo [%TIME%] Running monthly calibration (1st of month)... >> "%LOG_FILE%"
    echo [%TIME%] Running monthly calibration...
    
    %PYTHON% scripts\10_monthly_calibration.py >> "%LOG_FILE%" 2>&1
    
    if errorlevel 1 (
        echo [%TIME%] WARNING: Monthly calibration failed >> "%LOG_FILE%"
        echo [%TIME%] WARNING: Monthly calibration failed
    ) else (
        echo [%TIME%] SUCCESS: Monthly calibration completed >> "%LOG_FILE%"
        echo [%TIME%] SUCCESS: Monthly calibration completed
    )
) else (
    echo [%TIME%] Skipped: Monthly calibration not due (day %DAY% of month) >> "%LOG_FILE%"
    echo [%TIME%] Skipped: Monthly calibration not due
)

REM ============================================================================
REM Step 4: Quarterly retraining check (run on 1st of Jan, Apr, Jul, Oct)
REM ============================================================================

echo. >> "%LOG_FILE%"
echo [STEP 4] Checking if quarterly retraining assessment is due... >> "%LOG_FILE%"
echo -------------------------------------------------------------------------- >> "%LOG_FILE%"
echo [%TIME%] [STEP 4/5] Checking quarterly retraining assessment...

REM Get current month
for /f "tokens=1 delims=/" %%a in ('echo %DATE:~-10,2%') do set MONTH=%%a
set MONTH=%MONTH: =%
if "%MONTH:~0,1%"=="0" set MONTH=%MONTH:~1%

REM Run on 1st of Jan (1), Apr (4), Jul (7), Oct (10)
if %DAY%==1 (
    if %MONTH%==1 goto :run_retraining
    if %MONTH%==4 goto :run_retraining
    if %MONTH%==7 goto :run_retraining
    if %MONTH%==10 goto :run_retraining
)

echo [%TIME%] Skipped: Quarterly retraining check not due >> "%LOG_FILE%"
echo [%TIME%] Skipped: Quarterly retraining check not due
goto :generate_queue

:run_retraining
echo [%TIME%] Running quarterly retraining assessment... >> "%LOG_FILE%"
echo [%TIME%] Running quarterly retraining assessment...

set RETRAIN_REPORT=reports\retraining\retraining_assessment_%REPORT_TIMESTAMP%.txt

%PYTHON% scripts\14_check_retraining_need.py --output "%RETRAIN_REPORT%" >> "%LOG_FILE%" 2>&1

REM Check exit code
if errorlevel 2 (
    echo [%TIME%] ALERT: RETRAINING RECOMMENDED! >> "%LOG_FILE%"
    echo [%TIME%] ALERT: RETRAINING RECOMMENDED!
    echo [%TIME%] See report: %RETRAIN_REPORT% >> "%LOG_FILE%"
) else if errorlevel 1 (
    echo [%TIME%] INFO: Retraining should be considered >> "%LOG_FILE%"
    echo [%TIME%] INFO: Retraining should be considered
) else (
    echo [%TIME%] SUCCESS: Retraining not needed at this time >> "%LOG_FILE%"
    echo [%TIME%] SUCCESS: Retraining not needed
)

REM ============================================================================
REM Step 5: Generate weekly review queue (optional - run on Mondays)
REM ============================================================================

:generate_queue
echo. >> "%LOG_FILE%"
echo [STEP 5] Checking if review queue generation is due... >> "%LOG_FILE%"
echo -------------------------------------------------------------------------- >> "%LOG_FILE%"
echo [%TIME%] [STEP 5/5] Checking review queue generation...

REM Get day of week (1=Monday, 7=Sunday)
for /f "skip=1 tokens=1" %%d in ('wmic path win32_localtime get dayofweek') do set DOW=%%d

REM Run on Mondays (DOW=1)
if "%DOW%"=="1" (
    echo [%TIME%] Generating weekly review queue (Monday)... >> "%LOG_FILE%"
    echo [%TIME%] Generating weekly review queue...
    
    set QUEUE_FILE=reports\daily\validations\review_queue_%REPORT_TIMESTAMP%.xlsx
    
    %PYTHON% scripts\generate_review_queue.py --output "%QUEUE_FILE%" --days 7 >> "%LOG_FILE%" 2>&1
    
    if errorlevel 1 (
        echo [%TIME%] WARNING: Review queue generation failed >> "%LOG_FILE%"
        echo [%TIME%] WARNING: Review queue generation failed
    ) else (
        echo [%TIME%] SUCCESS: Review queue generated: %QUEUE_FILE% >> "%LOG_FILE%"
        echo [%TIME%] SUCCESS: Review queue generated
    )
) else (
    echo [%TIME%] Skipped: Review queue generation not due (not Monday) >> "%LOG_FILE%"
    echo [%TIME%] Skipped: Review queue generation not due
)

REM ============================================================================
REM Completion
REM ============================================================================

echo. >> "%LOG_FILE%"
echo ========================================================================== >> "%LOG_FILE%"
echo Daily Learning Loop Completed Successfully >> "%LOG_FILE%"
echo Finished: %DATE% %TIME% >> "%LOG_FILE%"
echo ========================================================================== >> "%LOG_FILE%"

echo [%TIME%] Daily learning loop completed successfully!
echo [%TIME%] See log: %LOG_FILE%

exit /b 0

REM ============================================================================
REM Error Handling
REM ============================================================================

:error
echo. >> "%LOG_FILE%"
echo ========================================================================== >> "%LOG_FILE%"
echo Daily Learning Loop Failed >> "%LOG_FILE%"
echo Finished: %DATE% %TIME% >> "%LOG_FILE%"
echo ========================================================================== >> "%LOG_FILE%"

echo [%TIME%] Daily learning loop failed!
echo [%TIME%] Check log: %LOG_FILE%

exit /b 1
