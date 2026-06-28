@echo off
echo Starting Chrome in Debug Mode with Profile 4...
echo.

REM Try different Chrome installation paths
if exist "C:\Program Files\Google\Chrome\Application\chrome.exe" (
    echo Found Chrome at: C:\Program Files\Google\Chrome\Application\
    "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --profile-directory="Profile 4" --disable-web-security --disable-features=VizDisplayCompositor
) else if exist "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe" (
    echo Found Chrome at: C:\Program Files (x86)\Google\Chrome\Application\
    "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --profile-directory="Profile 4" --disable-web-security --disable-features=VizDisplayCompositor
) else (
    echo Chrome not found in standard locations!
    echo Please check your Chrome installation path.
    echo.
    echo Common paths:
    echo - C:\Program Files\Google\Chrome\Application\chrome.exe
    echo - C:\Program Files (x86)\Google\Chrome\Application\chrome.exe
    echo.
    pause
    exit /b 1
)

echo.
echo Chrome started in debug mode on port 9222
echo You can now run the Selenium script to connect to this browser.
echo.
echo To verify debug mode is working, open: http://localhost:9222
echo.
echo Usage examples:
echo   chrome-debug.bat                    (uses temporary profile)
echo   chrome-debug.bat "Default"          (uses your main profile)
echo   chrome-debug.bat "Profile 1"        (uses Profile 1)
echo.
pause
