@echo off
cd /d "%~dp0"
echo.
echo ====================================================
echo  FIP Sardegna — Ripristino e Deploy
echo ====================================================
echo.

REM Abort rebase in corso
git rebase --abort 2>nul

REM Sincronizza con GitHub MA conserva il template locale
git fetch https://nonamefip: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@github.com/nonamefip/fpsrdg.git main

REM Aggiorna solo la cache dal remote (non tocca scripts/ ne docs/)
git checkout FETCH_HEAD -- cache\data_v5_new.json 2>nul
git checkout FETCH_HEAD -- cache\fip_calendari.json 2>nul
git checkout FETCH_HEAD -- cache\fip_national_cache.json 2>nul
git checkout FETCH_HEAD -- cache\fip_sardegna_cache.json 2>nul

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
git add docs\index.html scripts\template.html
git commit -m "Deploy: %date% %time:~0,5%"
git push https://nonamefip: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@github.com/nonamefip/fpsrdg.git HEAD:main --force
git push https://nonamefip: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@github.com/nonamefip/fpsrdg.git HEAD:gh-pages --force

echo.
echo ====================================================
echo  FATTO! Sito aggiornato.
echo  https://nonamefip.github.io/fpsrdg/
echo  Attendi 1-2 minuti per vedere le modifiche online.
echo ====================================================
pause
