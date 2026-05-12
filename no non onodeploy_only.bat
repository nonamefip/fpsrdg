@echo off
cd /d "%~dp0"
echo.
echo ====================================================
echo  FIP Sardegna — Solo build + deploy (no scraping)
echo ====================================================
echo.

echo [1/2] Ricostruisco docs/index.html...
python "%~dp0build.py"
if errorlevel 1 (
    echo ERRORE nel build!
    pause
    exit /b 1
)

echo.
echo [2/2] Pubblico su GitHub...

REM Rimuove eventuali lock file rimasti da processi precedenti
if exist "%~dp0.git\index.lock" del /f /q "%~dp0.git\index.lock"
if exist "%~dp0.git\refs\heads\main.lock" del /f /q "%~dp0.git\refs\heads\main.lock"

git add docs\index.html
git commit -m "Deploy template: %date% %time:~0,5%"
git push origin main
if errorlevel 1 (
    echo ERRORE nel push su main - provo git pull e ripeto...
    git pull origin main --rebase
    git push https://nonamefip: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@github.com/nonamefip/fpsrdg.git main
)
git push https://nonamefip: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@github.com/nonamefip/fpsrdg.git main:gh-pages --force

echo.
echo ====================================================
echo  FATTO! Sito aggiornato.
echo  https://nonamefip.github.io/fpsrdg/
echo  Attendi 1-2 minuti per vedere le modifiche online.
echo ====================================================
pause
