@echo off
echo DB Installer VTA
echo.
echo Running database installer...
echo.
cd /d "%~dp0"
.\dist\db_installer\db_installer.exe
echo.
pause
