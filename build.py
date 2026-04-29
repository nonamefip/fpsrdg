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
    if isinstance(obj, str):
        valid_escapes = set('nrtbfv\\\'"0123456789ux\n\r')
        result = []
        i = 0
        while i < len(obj):
            if obj[i] == '\\' and i + 1 < len(obj):
                next_ch = obj[i+1]
                if next_ch in valid_escapes:
                    result.append(obj[i])
                else:
                    result.append('\\\\')
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

# ── FIP SARDA ──────────────────────────────────────────────────
fip_json = '{"aggiornato":"","campionati":[]}'  # default sicuro

if FIP_PLACEHOLDER in template:
    if os.path.exists(FIP_DATA_FILE):
        try:
            with open(FIP_DATA_FILE, encoding='utf-8') as f:
                fip_raw = json.load(f)

            # Validazione: supporta sia struttura con gironi che struttura piatta
            camps = fip_raw.get('campionati', [])
            camps_validi = []
            for c in camps:
                if not isinstance(c, dict): continue
                if not c.get('nome'): continue

                # Struttura piatta (fip_scraper_sarda.py): classifica/risultati/prossime diretti
                if 'classifica' in c or 'risultati' in c or 'prossime' in c:
                    cl = c.get('classifica', [])
                    ri = c.get('risultati', [])
                    pr = c.get('prossime', [])
                    if not isinstance(cl, list): cl = []
                    if not isinstance(ri, list): ri = []
                    if not isinstance(pr, list): pr = []
                    gironi_validi = [{
                        'nome': 'Girone Unico',
                        'classifica': cl,
                        'risultati': ri,
                        'prossime': pr,
                    }]
                else:
                    # Struttura con gironi
                    gironi = c.get('gironi', [])
                    gironi_validi = []
                    for g in gironi:
                        if not isinstance(g, dict): continue
                        cl = g.get('classifica', [])
                        ri = g.get('risultati', [])
                        pr = g.get('prossime', [])
                        if not isinstance(cl, list): cl = []
                        if not isinstance(ri, list): ri = []
                        if not isinstance(pr, list): pr = []
                        gironi_validi.append({
                            'nome': str(g.get('nome', '')),
                            'classifica': cl,
                            'risultati': ri,
                            'prossime': pr,
                        })

                camps_validi.append({
                    'nome': str(c.get('nome', '')),
                    'sesso': str(c.get('sesso', 'M')),
                    'gironi': gironi_validi,
                })

            fip_clean = {'aggiornato': str(fip_raw.get('aggiornato', '')), 'campionati': camps_validi}
            fip_json = json.dumps(fip_clean, ensure_ascii=False, separators=(',', ':'))

            # Verifica finale che sia JSON valido
            json.loads(fip_json)

            print(f"   FIP Sarda: {len(camps_validi)} campionati · {fip_clean['aggiornato']}")
        except Exception as e:
            print(f"   ⚠️  Errore dati FIP ({e}) — uso struttura vuota")
            fip_json = '{"aggiornato":"","campionati":[]}'
    else:
        print(f"   ℹ️  {FIP_DATA_FILE} non trovato — tab FIP Live vuota (lancia aggiorna_fip.bat)")

    template = template.replace(FIP_PLACEHOLDER, fip_json, 1)
# ───────────────────────────────────────────────────────────────

# Versione
hash4 = hashlib.md5(template.encode()).hexdigest()[:4].upper()
today = datetime.date.today().strftime('%Y-%m-%d')
ver_str = f"'v7.0  ·  {today}  ·  #{hash4}'"
template = re.sub(r"'v7\.0  ·  [^']*'", ver_str, template, count=1)
if f"#{hash4}" not in template:
    template = template.replace(
        "'v7.0  ·  '+((D.generated||'').slice(0,10)||'2026')",
        ver_str
    )

output = template.replace('__DATA__', data)

os.makedirs('docs', exist_ok=True)
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(output)

size = os.path.getsize(OUTPUT_FILE)
print(f"✅ {OUTPUT_FILE}: {size//1024} KB ({size/1024/1024:.1f} MB)")
print(f"   Versione: v7.0 · {today} · #{hash4}")
