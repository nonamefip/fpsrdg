@echo off
echo === DEPLOY COMPLETO FIP DASHBOARD ===

python fip_scraper_sarda.py
python fip_calendar_scraper.py
python build.py

git add -A
git commit -m "aggiornamento automatico"
git push origin main
git push origin main:gh-pages --force

echo === FATTO ===
pause
