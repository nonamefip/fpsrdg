@echo off
chcp 65001 >nul
echo ====================================================
echo  FIX CONFLITTI + DEPLOY
echo ====================================================
cd /d "%~dp0"

:: Step 1 - annulla tutto quello che git ha in sospeso
git rebase --abort 2>nul
git merge --abort 2>nul
git cherry-pick --abort 2>nul

:: Step 2 - salva il template PRIMA di qualsiasi reset
::          usa una cartella temp fuori dal repo git
set TMPTEMPLATE=%TEMP%\fpsrdg_template_deploy.html
copy /Y scripts\template.html "%TMPTEMPLATE%" >nul
if not exist "%TMPTEMPLATE%" (
    echo ERRORE: impossibile salvare il template in temp!
    pause
    exit /b 1
)
echo Template salvato in temp.

:: Step 3 - rimuovi i file in conflitto dall'index git
git rm -f --cached docs/index.html 2>nul
git rm -f --cached scripts/template.html 2>nul

:: Step 4 - butta via lo stash rimasto
git stash drop 2>nul

:: Step 5 - sincronizza con remote (sovrascrive scripts\template.html con quello vecchio)
git fetch origin
git reset --hard origin/main

:: Step 6 - ripristina il template nuovo da temp
copy /Y "%TMPTEMPLATE%" scripts\template.html >nul
echo Template nuovo ripristinato.

:: Step 7 - build fresca
echo Rigenerando index.html...
python build.py
if errorlevel 1 (
    echo ERRORE: build.py ha fallito!
    pause
    exit /b 1
)

:: Step 8 - stage e commit
git add docs\index.html scripts\template.html
git diff --cached --quiet
if %errorlevel%==0 (
    echo Nessuna modifica - niente da committare.
    pause
    exit /b 0
)
for /f "tokens=1-3 delims=/" %%a in ("%date%") do set D=%%c%%b%%a
for /f "tokens=1-2 delims=:." %%a in ("%time: =0%") do set T=%%a%%b
git commit -m "Update template [%D%-%T%]"

:: Step 9 - push
git push origin main
if errorlevel 1 (
    echo ERRORE push main - provo force...
    git push origin main --force
)
git push origin main:gh-pages --force

echo ====================================================
echo  FATTO! Attendi 2-3 min poi controlla:
echo  https://nonamefip.github.io/fpsrdg/
echo ====================================================
pause
