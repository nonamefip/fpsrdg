@echo off
echo === BUILD RAPIDO (senza scraper) ===

python build.py

git add -A
git commit -m "fix template"
git push origin main --force
git push origin main:gh-pages --force

echo === FATTO - attendi ~60s per GitHub Pages ===
pause
