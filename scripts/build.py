#!/usr/bin/env python3
"""
Build script FIP Dashboard
Legge cache/data_v5_new.json + scripts/template.html → docs/index.html
"""
import json, os, hashlib, datetime, re

DATA_FILE     = 'cache/data_v5_new.json'
TEMPLATE_FILE = 'scripts/template.html'
OUTPUT_FILE   = 'docs/index.html'
FIP_DATA_FILE = 'fip_sarda_data.json'
FIP_PLACEHOLDER = '__FIP_SARDA_JSON__'

if not os.path.exists(DATA_FILE):
    print(f"❌ {DATA_FILE} non trovato. Esegui prima gen_data.py"); exit(1)
if not os.path.exists(TEMPLATE_FILE):
    print(f"❌ {TEMPLATE_FILE} non trovato."); exit(1)

with open(DATA_FILE, encoding='utf-8') as f:
    raw_data = json.load(f)
with open(TEMPLATE_FILE, encoding='utf-8') as f:
    template = f.read()

def sanitize_strings(obj):
    """Rimuove escape sequences non valide in JS strict mode dai valori stringa."""
    if isinstance(obj, str):
        # Escape sequences valide in JS: \n \r \t \\ \" \' \0 \uXXXX \xXX
        # Tutto il resto (\p \s \d \a ecc.) è invalido in strict mode
        # Soluzione: ri-serializza con ensure_ascii=False dopo aver pulito i backslash
        # Un backslash solo (non seguito da carattere speciale) va raddoppiato
        import re as _re
        # Sostituisce \X dove X non è un carattere di escape valido con \\X
        valid_escapes = set('nrtbfv\\\'"0123456789ux\n\r')
        result = []
        i = 0
        while i < len(obj):
            if obj[i] == '\\' and i + 1 < len(obj):
                next_ch = obj[i+1]
                if next_ch in valid_escapes:
                    result.append(obj[i])
                else:
                    result.append('\\\\')  # raddoppia il backslash
                i += 1
            else:
                result.append(obj[i])
            i += 1
        return ''.join(result)
    elif isinstance(obj, dict):
        return {k: sanitize_strings(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_strings(v) for v in obj]
    return obj

sanitized_data = sanitize_strings(raw_data)
data = json.dumps(sanitized_data, ensure_ascii=False, separators=(',', ':'))

# Calcola hash versione
hash4 = hashlib.md5(template.encode()).hexdigest()[:4].upper()
today = datetime.date.today().strftime('%Y-%m-%d')

# Sostituisci version badge - gestisce sia stringa dinamica che hardcoded precedente
ver_str = f"'v7.0  ·  {today}  ·  #{hash4}'"
template = re.sub(r"'v7\.0  ·  [^']*'", ver_str, template, count=1)
# Fallback se non trovato
if f"#{hash4}" not in template:
    template = template.replace(
        "'v7.0  ·  '+((D.generated||'').slice(0,10)||'2026')",
        ver_str
    )

# Inietta dati FIP Sarda
fip_json = '{"aggiornato":"","campionati":[]}'
if FIP_PLACEHOLDER in template:
    if os.path.exists(FIP_DATA_FILE):
        try:
            with open(FIP_DATA_FILE, encoding='utf-8') as f:
                fip