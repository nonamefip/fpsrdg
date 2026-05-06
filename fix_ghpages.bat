@echo off
cd /d "%~dp0"

echo Rimozione lock file git...
if exist .git\index.lock del /f .git\index.lock
if exist .git\HEAD.lock del /f .git\HEAD.lock

echo Aggiorno origin/gh-pages...
git fetch origin

echo Passo al branch gh-pages (worktree)...
if exist _ghpages_tmp rmdir /s /q _ghpages_tmp
git worktree add _ghpages_tmp gh-pages
if errorlevel 1 (
  echo ERRORE: impossibile creare worktree gh-pages
  pause
  exit /b 1
)

echo Copio docs/index.html nel branch gh-pages...
copy /y docs\index.html _ghpages_tmp\index.html

echo Creo/aggiorno .nojekyll...
type nul > _ghpages_tmp\.nojekyll

cd _ghpages_tmp
git add index.html .nojekyll
git commit -m "Ripristino manuale sito %date% %time%"
git push --force origin gh-pages
cd ..

echo Pulizia worktree...
git worktree remove _ghpages_tmp --force

echo.
echo === FATTO! Il sito dovrebbe essere online in 1-2 minuti. ===
echo Ricarica https://nonamefip.github.io/fpsrdg/
pause
