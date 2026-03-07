#!/usr/bin/env python3
"""
Build script FIP Dashboard
Legge cache/data_v5_new.json + scripts/template.html → docs/index.html
"""
import json, os, hashlib, datetime

DATA_FILE     = 'cache/data_v5_new.json'
TEMPLATE_FILE = 'scripts/template.html'
OUTPUT_FILE   = 'docs/index.html'

if not os.path.exists(DATA_FILE):
    print(f"❌ {DATA_FILE} non trovato. Esegui prima gen_data.py"); exit(1)
if not os.path.exists(TEMPLATE_FILE):
    print(f"❌ {TEMPLATE_FILE} non trovato."); exit(1)

with open(DATA_FILE, encoding='utf-8') as f:
    data = f.read()
with open(TEMPLATE_FILE, encoding='utf-8') as f:
    template = f.read()

# Calcola hash versione
hash4 = hashlib.md5(template.encode()).hexdigest()[:4].upper()
today = datetime.date.today().strftime('%Y-%m-%d')

# Sostituisci version badge
template = template.replace(
    "'v7.0  ·  '+((D.generated||'').slice(0,10)||'2026')",
    f"'v7.0  ·  {today}  ·  #{hash4}'"
)

output = template.replace('__DATA__', data)

os.makedirs('docs', exist_ok=True)
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(output)

size = os.path.getsize(OUTPUT_FILE)
print(f"✅ {OUTPUT_FILE}: {size//1024} KB ({size/1024/1024:.1f} MB)")
print(f"   Versione: v7.0 · {today} · #{hash4}")
