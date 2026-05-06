@echo off
cd /d "%~dp0"
echo.
echo ====================================================
echo  FIP Sardegna — Aggiornamento dati live
echo ====================================================
echo.

echo [1/3] Scarico dati da fip.it...
python "%~dp0fip_scraper_sarda.py"
if errorlevel 1 (
    echo ERRORE nello scraping\!
    pause
    exit /b 1
)

echo.
echo [2/3] Ricostruisco docs/index.html...
python "%~dp0build.py"
if errorlevel 1 (
    echo ERRORE nel build\!
    pause
    exit /b 1
)

echo.
echo [3/3] Pubblico su GitHub...
git add fip_sarda_data.json docs\index.html
git commit -m "FIP Live: aggiornamento automatico %date% %time:~0,5%"
git push

echo.
echo ====================================================
echo  FATTO\! Sito aggiornato su GitHub Pages.
echo  Attendi 1-2 minuti per vedere le modifiche online.
echo ====================================================
pause
