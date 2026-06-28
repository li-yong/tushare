@echo off
echo Finding Chrome Profiles...
echo ========================
echo.

set "CHROME_USER_DATA=%LOCALAPPDATA%\Google\Chrome\User Data"

if not exist "%CHROME_USER_DATA%" (
    echo Chrome User Data directory not found at:
    echo %CHROME_USER_DATA%
    echo.
    echo Please check if Chrome is installed.
    pause
    exit /b 1
)

echo Chrome User Data Directory: %CHROME_USER_DATA%
echo.
echo Available Profiles:
echo -------------------

REM List Default profile
if exist "%CHROME_USER_DATA%\Default" (
    echo [✓] Default (Main Profile)
)

REM List numbered profiles
for /D %%i in ("%CHROME_USER_DATA%\Profile *") do (
    set "PROFILE_NAME=%%~nxi"
    echo [✓] !PROFILE_NAME!
)

REM List other potential profile directories
for /D %%i in ("%CHROME_USER_DATA%\Person *") do (
    set "PROFILE_NAME=%%~nxi"
    echo [✓] !PROFILE_NAME!
)

echo.
echo Usage Examples:
echo ---------------
echo chrome-debug.bat "Default"           (for Default profile)
echo chrome-debug.bat "Profile 1"         (for Profile 1)
echo chrome-debug.bat "Person 1"          (for Person 1)
echo.

REM Show profile details
echo Profile Details:
echo ----------------
for /D %%i in ("%CHROME_USER_DATA%\*") do (
    set "PROFILE_DIR=%%~nxi"
    if exist "%%i\Preferences" (
        echo Profile: !PROFILE_DIR!
        echo   Path: %%i
        if exist "%%i\Bookmarks" echo   Has Bookmarks: Yes
        if exist "%%i\History" echo   Has History: Yes
        echo.
    )
)

echo.
pause
