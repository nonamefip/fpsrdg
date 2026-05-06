@echo off
chcp 65001 >nul
echo ====================================================
echo  Aggiorna solo il template su GitHub
echo ====================================================

cd /d "%~dp0"

:: ── PULIZIA TOTALE stato git sporco ──────────────────────
echo Pulizia stato git...
git rebase --abort 2>nul
git merge --abort 2>nul

:: Forza rimozione conflitti dall'index (anche se unmerged)
git rm -f --cached docs/index.html 2>nul
git rm -f --cached scripts/template.html 2>nul

:: Ripristina i file dall'ultimo stash se presente
git stash drop 2>nul

:: Ora reset hard può funzionare
git fetch origin
git reset --hard origin/main

:: Doppia sicurezza: forza checkout dei file critici
git checkout -- docs/index.html 2>nul
git checkout -- scripts/template.html 2>nul

:: ── BUILD ────────────────────────────────────────────────
echo Rigenerando index.html dal template...
python build.py
if errorlevel 1 (
    echo ERRORE: build.py ha fallito!
    pause
    exit /b 1
)

:: ── STAGE ────────────────────────────────────────────────
git add docs\index.html
git add scripts\template.html

:: Niente da committare?
git diff --cached --quiet
if %errorlevel%==0 (
    echo Nessuna modifica rilevata - niente da committare.
    pause
    exit /b 0
)

:: ── COMMIT ───────────────────────────────────────────────
for /f "tokens=1-3 delims=/" %%a in ("%date%") do set D=%%c%%b%%a
for /f "tokens=1-2 delims=:." %%a in ("%time: =0%") do set T=%%a%%b
git commit -m "Update template [%D%-%T%]"

:: ── PUSH ─────────────────────────────────────────────────
git push origin main
if errorlevel 1 (
    echo ERRORE nel push su main!
    pause
    exit /b 1
)

git push origin main:gh-pages --force

echo ====================================================
echo  FATTO! GitHub avviera il deploy automaticamente.
echo  Attendi 2-3 minuti e controlla il sito.
echo  https://nonamefip.github.io/fpsrdg/
echo ====================================================
pause
