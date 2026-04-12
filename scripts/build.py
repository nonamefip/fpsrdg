#\!/usr/bin/env python3
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

# Calcola hash versione
hash4 = hashlib.md5(template.encode()).hexdigest()[:4].upper()
today = datetime.date.today().strftime('%Y-%m-%d')

ver_str = f"'v7.0  ·  {today}  ·  #{hash4}'"
template = re.sub(r"'v7\.0  ·  [^']*'", ver_str, template, count=1)
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
                fip_raw = json.load(f)
            camps = fip_raw.get('campionati', [])
            ESCLUDI = {'ES','TAQB','TAQS','TSCB','TSCS','TFE','TGAB','TGAS','TLIB'}
            camps_validi = [c for c in camps if c.get('codice','') not in ESCLUDI]
            fip_clean = {'aggiornato': str(fip_raw.get('aggiornato','')), 'campionati': camps_validi}
            fip_json = json.dumps(fip_clean, ensure_ascii=False, separators=(',',':'))
            json.loads(fip_json)
            print(f"   FIP Sarda: {len(camps_validi)} campionati · {fip_clean['aggiornato']}")
        except Exception as e:
            print(f"   ⚠️  Errore dati FIP ({e}) — uso struttura vuota")
            fip_json = '{"aggiornato":"","campionati":[]}'
    else:
        print(f"   ℹ️  {FIP_DATA_FILE} non trovato — tab FIP Live vuota")
    template = template.replace(FIP_PLACEHOLDER, fip_json, 1)

# Scrivi output
output = template.replace('__DATA__', data)
os.makedirs('docs', exist_ok=True)
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(output)
size_kb = len(output.encode('utf-8')) // 1024
print(f"✅ {OUTPUT_FILE}: {size_kb} KB ({size_kb//1024:.1f} MB)")
