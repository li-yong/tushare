@echo off
echo Chrome Profiles Quick List:
echo ==========================
echo.

cd /d "%LOCALAPPDATA%\Google\Chrome\User Data" 2>nul
if errorlevel 1 (
    echo Chrome not found in standard location
    exit /b 1
)

echo Found profiles:
for /D %%i in (*) do (
    if exist "%%i\Preferences" (
        echo   - "%%i"
    )
)

echo.
echo Use any of these names with: chrome-debug.bat "ProfileName"
