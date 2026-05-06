@echo off
echo === AGGIORNA FIP LIVE + DEPLOY ===

python fip_scraper_sarda.py
python build.py

git add -A
git commit -m "aggiornamento fip live"
git push origin main --force
git push origin main:gh-pages --force

echo === FATTO - attendi ~60s per GitHub Pages ===
pause
