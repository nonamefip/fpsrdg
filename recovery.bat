@echo off
cd /d "%~dp0"
if exist .git\index.lock del /f .git\index.lock
if exist .git\HEAD.lock del /f .git\HEAD.lock
echo Abort rebase in corso...
git rebase --abort
echo Torno su main...
git checkout main
echo Force push nostro main su origin...
git push --force-with-lease origin main
echo.
echo Fatto! Sito aggiornato.
pause
