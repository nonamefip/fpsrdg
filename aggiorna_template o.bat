@echo off
cd /d "%~dp0"
echo.
echo ====================================================
echo  Aggiorna solo il template su GitHub
echo ====================================================
echo.

REM Rimuove lock file se presenti
if exist ".git\index.lock" del /f /q ".git\index.lock"
if exist ".git\refs\heads\main.lock" del /f /q ".git\refs\heads\main.lock"

REM Sincronizza prima
git stash
git pull origin main --rebase
git stash pop

REM Committa e pusha il template
git add scripts\template.html
git diff --staged --quiet && (
    echo Nessuna modifica al template - file gia aggiornato
    pause
    exit /b 0
)
git commit -m "Aggiorna template %date% %time:~0,5%"
git push origin main

echo.
echo ====================================================
echo  FATTO! GitHub avviera il deploy automaticamente.
echo  Attendi 2-3 minuti e controlla il sito.
echo  https://nonamefip.github.io/fpsrdg/
echo ====================================================
pause
