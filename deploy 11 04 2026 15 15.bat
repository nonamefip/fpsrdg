@echo off
python "%~dp0fip_scraper_sarda.py"
python "%~dp0build_fip_sarda.py"
python "%~dp0deploy.py"
pause
