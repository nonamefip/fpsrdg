@echo off
cd /d "%~dp0"
echo ============================================================
echo  FIP Sardegna - FIX COMPLETO 03/05/2026
echo ============================================================
echo.
echo Questo script applica i fix per:
echo  - Playout Divisione regionale 1 mancante
echo  - Finale Serie C mancante
echo  - Gare ritorno U15/U17 incomplete
echo  - Gara 004568 con data sbagliata
echo  - Giornate andata/ritorno sfasate nei gironi
echo.
pause

echo.
echo [1/5] Sync cache da dati live FIP (allinea cache/fip_sardegna_cache.json)
python "%~dp0sync_cache_da_live.py"
if errorlevel 1 (
    echo ERRORE nel sync!
    pause
    exit /b 1
)

echo.
echo [2/5] Verifica gare riprogrammate (Fase 3 fip_scraper.py)
python "%~dp0fip_scraper.py" --refresh-days 0 --future-days 0
if errorlevel 1 (
    echo ATTENZIONE: errore nello scraper, ma proseguo comunque
)

echo.
echo [3/5] Rigenero calendari (con nuovo algoritmo round-robin)
python "%~dp0fip_calendar_scraper.py" --rebuild
if errorlevel 1 (
    echo ERRORE nei calendari!
    pause
    exit /b 1
)

echo.
echo [4/5] Rigenero data_v5_new.json
cd /d "%~dp0"
python "%~dp0scripts\gen_data.py"
if errorlevel 1 (
    echo ERRORE in gen_data!
    pause
    exit /b 1
)

echo.
echo [5/5] Ricostruisco docs/index.html
python "%~dp0build.py"
if errorlevel 1 (
    echo ERRORE nel build!
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  FATTO!
echo  Dashboard ricostruito: docs\index.html
echo  Per pubblicare su GitHub: esegui aggiorna_fip.bat
echo ============================================================
pause
