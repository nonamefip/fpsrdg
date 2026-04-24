@echo off
title Redattore PG — Avvio automatico
color 1F

echo.
echo  ============================================
echo   REDATTORE PG — Polizia Postale Sardegna
echo  ============================================
echo.

:: Avvia Ollama in background (se non e' gia' attivo)
echo  [1/3] Avvio Ollama...
tasklist /FI "IMAGENAME eq ollama.exe" 2>NUL | find /I "ollama.exe" >NUL
if %ERRORLEVEL% NEQ 0 (
    start "" "ollama" serve
    timeout /t 3 /nobreak >NUL
    echo        OK - Ollama avviato
) else (
    echo        OK - Ollama gia' attivo
)

:: Avvia server web nella cartella CLAUDE PRO
echo  [2/3] Avvio server web sulla porta 8080...
start "Server Web — Redattore PG" cmd /k "cd /d "%~dp0" && python -m http.server 8080"
timeout /t 2 /nobreak >NUL
echo        OK - Server avviato su http://localhost:8080

:: Apre il browser automaticamente
echo  [3/3] Apertura browser...
timeout /t 1 /nobreak >NUL
start "" "http://localhost:8080/RedattorePG.html"

echo.
echo  ============================================
echo   Tutto pronto! Il browser si e' aperto.
echo.
echo   Lascia questa finestra APERTA mentre
echo   usi il Redattore PG.
echo.
echo   Per chiudere: premi un tasto qui sotto.
echo  ============================================
echo.
pause >NUL

:: Alla chiusura, ferma il server web
taskkill /FI "WINDOWTITLE eq Server Web*" /F >NUL 2>&1
echo  Server fermato. Arrivederci!
timeout /t 2 /nobreak >NUL
