@echo off
cd /d "%~dp0"

echo Rimozione lock file git...
if exist .git\index.lock del /f .git\index.lock
if exist .git\HEAD.lock del /f .git\HEAD.lock
if exist .git\ORIG_HEAD.lock del /f .git\ORIG_HEAD.lock

echo Fetch da origin...
git fetch origin

echo Merge aggiornamenti remoti...
git merge origin/main --no-edit

echo Git add...
git add .github/workflows/main.yml
git add scripts/template.html
git add docs/index.html

echo Git commit...
git diff --cached --quiet && (echo Niente da committare.) || git commit -m "Fix JS syntax errors + filtri giorno modal + heatmap orizzontale"

echo Git push main...
git push origin main

echo.
echo Aggiorno gh-pages con index.html attuale...
if exist _ghpages_tmp rmdir /s /q _ghpages_tmp
git worktree add _ghpages_tmp gh-pages
if errorlevel 1 (
  echo ERRORE worktree gh-pages, salto sync
  goto fine
)
copy /y docs\index.html _ghpages_tmp\index.html
type nul > _ghpages_tmp\.nojekyll
cd _ghpages_tmp
git add index.html .nojekyll
git diff --cached --quiet && (echo gh-pages gia aggiornato.) || git commit -m "Deploy %date% %time%"
git push origin gh-pages
cd ..
git worktree remove _ghpages_tmp --force

:fine
echo.
echo Fatto! Il sito si aggiornerà in 1-2 minuti.
echo Ricarica: https://nonamefip.github.io/fpsrdg/
pause
