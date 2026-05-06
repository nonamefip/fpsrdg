@echo off
echo === DEPLOY COMPLETO FIP DASHBOARD ===
echo.

echo [1/6] Scarico gare per data (fip_scraper.py)...
python fip_scraper.py
echo.

echo [2/6] Scarico dati nazionali (fip_national_scraper.py)...
python fip_national_scraper.py
echo.

echo [3/6] Costruisco calendari (fip_calendar_scraper.py)...
python fip_calendar_scraper.py
echo.

echo [4/6] Genero JSON principale (gen_data.py)...
python gen_data.py
echo.

echo [5/6] Scarico FIP Live gironi (fip_scraper_sarda.py)...
python fip_scraper_sarda.py
echo.

echo [6/6] Build + Deploy...
python build.py
git add -A
git commit -m "aggiornamento completo"
git push origin main --force
git push origin main:gh-pages --force
echo.

echo === FATTO - attendi ~60s per GitHub Pages ===
pause
