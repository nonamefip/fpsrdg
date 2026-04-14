@echo off
cd /d "%~dp0"
echo Rimozione lock file git...
if exist .git\index.lock del /f .git\index.lock
if exist .git\HEAD.lock del /f .git\HEAD.lock
if exist .git\ORIG_HEAD.lock del /f .git\ORIG_HEAD.lock
echo Git pull --rebase...
git pull --rebase
echo Git add...
git add scripts/template.html docs/index.html
echo Git commit...
git commit -m "FIP Live: fix Andata/Ritorno N-1, playoff Gara1/2/Bella"
echo Git push...
git push
echo.
echo Fatto! Premi un tasto per chiudere.
pause
