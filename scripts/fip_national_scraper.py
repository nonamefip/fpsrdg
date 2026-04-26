#!/usr/bin/env python3
"""
FIP National Scraper v6
Modulo 1 — Arbitri sardi:
  1. Estrae arbitri sardi dalla cache RSA (nome completo)
  2. Per ogni arbitro cerca su fip.it per cognome
  3. Scarta SOLO le gare con campionato "COMITATO REGIONALE SARDEGNA..."
  4. Verifica che il nome completo dell'arbitro sia nella gara
  5. Quello che rimane = gare fuori Sardegna

Modulo 2 — Squadre sarde nazionali (NUOVO):
  1. Lista hardcoded di squadre sarde che militano in campionati nazionali
  2. Per ogni squadra cerca su fip.it per nome_squadra
  3. Scarta le gare del Comitato Regionale Sardegna
  4. Salva tutte le gare (casa + trasferta) con tutti gli arbitri
"""
import requests, json, os, re, sys, time, random
from bs4 import BeautifulSoup
from datetime import date, timedelta

BASE_URL   = "https://fip.it/risultati/"
CACHE_FILE = "cache/fip_national_cache.json"
RSA_CACHE  = "cache/fip_sardegna_cache.json"
PROV_SARDE = {'CA','SS','NU','OR','SU','CI','OG','OT','VS'}

# Squadre sarde che militano in campionati nazionali 2025-2026
SQUADRE_SARDE_NAZIONALI = [
    {"nome": "ESPERIA",           "nome_esatto": "ESPERIA",           "desc": "Esperia Cagliari"},
    {"nome": "SENNORI",           "nome_esatto": "SENNORI",           "desc": "Klass Sennori"},
    {"nome": "BANCO DI SARDEGNA", "nome_esatto": "BANCO DI SARDEGNA", "desc": "Dinamo Sassari"},
    {"nome": "DINAMO LAB",        "nome_esatto": "DINAMO LAB",        "desc": "Dinamo Lab Sassari"},
    {"nome": "VIRTUS CAGLIARI",   "nome_esatto": "VIRTUS CAGLIARI",   "desc": "Virtus Cagliari"},
    {"nome": "SELARGIUS",         "nome_esatto": "SELARGIUS",         "desc": "San Salvatore Selargius"},
    {"nome": "SCUOLA ADD",        "nome_esatto": "SCUOLA ADD",        "desc": "Scuola Addestramento"},
]

PERIODS = [
    ("2025-09-01", "2025-11-30"),
    ("2025-12-01", "2026-02-28"),
    ("2026-03-01", "2026-06-30"),
]

PERIODS_MONTHLY = [
    ("2025-09-01", "2025-09-30"), ("2025-10-01", "2025-10-31"),
    ("2025-11-01", "2025-11-30"), ("2025-12-01", "2025-12-31"),
    ("2026-01-01", "2026-01-31"), ("2026-02-01", "2026-02-28"),
    ("2026-03-01", "2026-03-31"), ("2026-04-01", "2026-04-30"),
    ("2026-05-01", "2026-05-31"), ("2026-06-01", "2026-06-30"),
]

HEADERS_POOL = [
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"},
]

MESI_IT = {"gennaio":"01","febbraio":"02","marzo":"03","aprile":"04",
           "maggio":"05","giugno":"06","luglio":"07","agosto":"08",
           "settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"}

prov_re = re.compile(r'\bdi\s+.+?\s+\(\s*(\w{2,3})\s*\)\s*$', re.IGNORECASE)

def clean(t): return " ".join(t.split()) if t else ""

def parse_date_it(raw):
    parts = raw.lower().split()
    if len(parts) == 3:
        return f"{parts[2]}-{MESI_IT.get(parts[1],'00')}-{int(parts[0]):02d}"
    return raw

def parse_person(s):
    if not s: return None
    s = s.strip(); sl = s.lower()
    if any(x in sl for x in ('attesa','designazione','n.d.')): return None
    if re.search(r'\bn\.?d\.?\b', sl): return None
    if sl.strip() in ('','-'): return None
    m = prov_re.search(s)
    idx = s.lower().rfind(' di ')
    if m and idx > 0:
        nome = s[:idx].strip()
        rest = s[idx+4:].strip()
        cm2 = re.match(r'(.+?)\s*\(\s*(\w{2,3})\s*\)', rest)
        return {'nome': nome, 'citta': cm2.group(1).strip() if cm2 else '', 'provincia': m.group(1).upper()}
    return None

def get_info(match_div, label_text):
    for info in match_div.find_all("div", class_="info"):
        lbl = info.find("div", class_="label")
        if lbl and label_text.lower() in lbl.get_text().lower():
            vals = info.find_all(class_="value")
            if not vals: return ""
            parts = []
            for val in vals:
                v = clean(val.get_text())
                if v and v.lower() not in ('designazione in attesa di conferma.', 'n/d'):
                    parts.append(v)
            return "\n".join(parts)
    return ""

def parse_ref(ref_text):
    flat = " ".join(ref_text.split()); tokens = flat.split()
    num_gara = tokens[0] if tokens else ""
    campionato = girone = fase = ""
    m = re.search(r"-\s*(.+?)\s*(?:Girone:|$)", flat, re.IGNORECASE)
    if m: campionato = clean(m.group(1))
    gm = re.search(r"Girone:\s*(.+?)(?:,\s*Fase:|$)", flat)
    fm = re.search(r"Fase:\s*(.+)", flat)
    if gm: girone = clean(gm.group(1))
    if fm: fase = clean(fm.group(1))
    return num_gara, campionato, girone, fase

def parse_match(m):
    teams = m.find_all("div", class_="team")
    sq_casa = clean(teams[0].find("div", class_="team__name").get_text()) if teams else ""
    sq_osp  = clean(teams[1].find("div", class_="team__name").get_text()) if len(teams)>1 else ""
    pt_c = clean(teams[0].find("div", class_="team__points").get_text()) if teams and teams[0].find("div", class_="team__points") else ""
    pt_o = clean(teams[1].find("div", class_="team__points").get_text()) if len(teams)>1 and teams[1].find("div", class_="team__points") else ""
    date_div = m.find("div", class_="date"); time_div = m.find("div", class_="time")
    data_fmt = parse_date_it(clean(date_div.get_text())) if date_div else ""
    ora      = clean(time_div.get_text()) if time_div else ""
    ref_div  = m.find("div", class_="ref")
    num_gara, campionato, girone, fase = parse_ref(ref_div.get_text() if ref_div else "")
    return {
        "Data":data_fmt,"Ora":ora,"Numero Gara":num_gara,
        "Campionato":campionato,"Girone":girone,"Fase":fase,
        "Squadra Casa":sq_casa,"Squadra Ospite":sq_osp,
        "Punti Casa":pt_c,"Punti Ospite":pt_o,
        "Risultato":f"{pt_c}-{pt_o}" if pt_c and pt_o else "",
        "Campo":get_info(m,"campo di gioco"),
        "Arbitro 1":get_info(m,"1° arbitro"),"Arbitro 2":get_info(m,"2° arbitro"),
        "Arbitro 3":get_info(m,"3° arbitro"),
        "Segnapunti":get_info(m,"segnapunti"),"Cronometrista":get_info(m,"cronometrista"),
        "24 Secondi":get_info(m,"24 secondi"),"Addetto Referto":get_info(m,"addetto referto"),
        "Osservatore":get_info(m,"osservatore"),"Provvedimenti":get_info(m,"provvedimenti"),
    }

def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text().lower()
    if "numero eccessivo" in txt:
        return None
    matches = soup.find_all("div", class_="results-matches__match")
    return [parse_match(m) for m in matches]

def is_gara_sarda(campionato):
    """Scarta SOLO le gare del Comitato Regionale Sardegna."""
    return campionato.upper().startswith("COMITATO REGIONALE SARDEGNA")

def fetch(session, params, max_retries=4, use_post=False):
    for attempt in range(1, max_retries+1):
        try:
            if use_post:
                resp = session.post(BASE_URL, data=params, timeout=15)
            else:
                resp = session.get(BASE_URL, params=params, timeout=15)
            if resp.status_code == 200: return resp, False
            elif resp.status_code == 429:
                t = int(resp.headers.get("Retry-After", 30))
                print(f"\n[429] attendo {t}s"); time.sleep(t)
            else:
                print(f"\n[HTTP {resp.status_code}] tentativo {attempt}")
        except Exception as e:
            print(f"\n[ERR] {e} tentativo {attempt}")
        if attempt < max_retries:
            time.sleep(random.uniform(2, 4) * attempt)
    return None, True

def _split_by_month(session, params_base, da, a, use_post=False):
    """Divide la ricerca per mese quando ci sono troppi risultati."""
    result = []
    d_start = date.fromisoformat(da)
    d_end = date.fromisoformat(a)
    cur = d_start
    while cur <= d_end:
        if cur.month == 12:
            m_end = date(cur.year + 1, 1, 1) - timedelta(days=1)
        else:
            m_end = date(cur.year, cur.month + 1, 1) - timedelta(days=1)
        m_end = min(m_end, d_end)
        params = dict(params_base)
        params["data_da"] = cur.isoformat()
        params["data_a"] = m_end.isoformat()
        resp, _ = fetch(session, params, use_post=use_post)
        if resp:
            rows = parse_page(resp.text)
            if rows:
                result.extend(rows)
        cur = m_end + timedelta(days=1)
        time.sleep(random.uniform(1.0, 2.0))
    return result

def fetch_by_cognome(session, cognome, da, a):
    params = {
        "search":"true","data_da":da,"data_a":a,
        "cognome_arbitro":cognome,
        "data_singola":"","numero_gara":"","codice_societa":"",
        "nome_squadra":"","codice_campo":"","codice_arbitro":"","comitato":""
    }
    resp, net_err = fetch(session, params, use_post=False)
    if resp is None:
        return []
    rows = parse_page(resp.text)
    if rows is None:
        print(f"  [!] {cognome} {da}->{a} troppi risultati, divido per mese...")
        return _split_by_month(session, params, da, a, use_post=False)
    return rows or []

def fetch_by_squadra(session, nome_squadra, da, a):
    """Cerca tutte le gare di una squadra via GET mese per mese."""
    params = {
        "search":"true","data_da":da,"data_a":a,
        "nome_squadra":nome_squadra,
        "cognome_arbitro":"","data_singola":"","numero_gara":"",
        "codice_societa":"","codice_campo":"","codice_arbitro":"","comitato":""
    }
    resp, net_err = fetch(session, params, use_post=False)
    if resp is None:
        return []
    rows = parse_page(resp.text)
    if rows is None:
        print(f"  [!] {nome_squadra} {da}->{a} troppi risultati, divido per mese...")
        return _split_by_month(session, params, da, a, use_post=False)
    return rows or []

def main():
    print(f"=== FIP National Scraper v6 — {date.today()} ===")

    if not os.path.exists(RSA_CACHE):
        print(f"ERRORE: Cache RSA non trovata: {RSA_CACHE}")
        sys.exit(1)

    with open(RSA_CACHE, encoding="utf-8") as f:
        rsa_gare = json.load(f)
    print(f"Cache RSA caricata: {len(rsa_gare)} gare")

    # Estrai arbitri sardi per nome completo
    arbitri_sardi = {}  # "COGNOME NOME" -> provincia
    for g in rsa_gare:
        for field in ['Arbitro 1', 'Arbitro 2', 'Arbitro 3']:
            val = g.get(field, '')
            if not val: continue
            pp = parse_person(val)
            if not pp or pp['provincia'] not in PROV_SARDE: continue
            nome_completo = pp['nome'].upper().strip()
            if not nome_completo or len(nome_completo) < 4: continue
            if nome_completo not in arbitri_sardi:
                arbitri_sardi[nome_completo] = pp['provincia']

    print(f"Arbitri sardi unici trovati: {len(arbitri_sardi)}")

    existing = []
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            existing = json.load(f)
        print(f"Cache nazionale esistente: {len(existing)} gare")

    # Chiave univoca: numero gara + campionato
    existing_keys = {(g.get('Numero Gara',''), g.get('Campionato','')) for g in existing if g.get('Numero Gara')}

    session = requests.Session()
    session.headers.update(random.choice(HEADERS_POOL))

    new_gare = []

    # ─── MODULO 1: Arbitri sardi ───────────────────────────────────────────────
    arbitri_list = sorted(arbitri_sardi.keys())
    total = len(arbitri_list)
    print(f"\n{'='*60}")
    print(f"MODULO 1 — Arbitri sardi fuori Sardegna ({total} arbitri)")
    print('='*60)

    for i, nome_completo in enumerate(arbitri_list, 1):
        provincia = arbitri_sardi[nome_completo]
        cognome = nome_completo.split()[0]

        print(f"[{i}/{total}] {nome_completo} ({provincia})...", end=" ", flush=True)

        trovate_fuori = 0
        for da, a in PERIODS:
            gare = fetch_by_cognome(session, cognome, da, a)
            for g in gare:
                num = g.get('Numero Gara', '')
                camp = g.get('Campionato', '')
                if not num: continue
                if is_gara_sarda(camp): continue
                tutti_arbitri = " | ".join(filter(None, [
                    g.get('Arbitro 1',''), g.get('Arbitro 2',''), g.get('Arbitro 3','')
                ]))
                if nome_completo not in tutti_arbitri.upper(): continue
                key = (num, camp)
                if key in existing_keys: continue
                new_gare.append(g)
                existing_keys.add(key)
                trovate_fuori += 1
            time.sleep(random.uniform(0.8, 1.5))

        print(f"{trovate_fuori} nuove gare fuori RSA")

    # ─── MODULO 2: Squadre sarde nazionali ────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"MODULO 2 — Squadre sarde nei campionati nazionali ({len(SQUADRE_SARDE_NAZIONALI)} squadre)")
    print('='*60)

    for sq in SQUADRE_SARDE_NAZIONALI:
        nome_sq = sq['nome']
        desc    = sq['desc']
        print(f"\n▶ {desc} (cerca: '{nome_sq}')")

        trovate_sq = 0
        nome_esatto = sq.get('nome_esatto', nome_sq).upper()
        for da, a in PERIODS_MONTHLY:
            gare = fetch_by_squadra(session, nome_sq, da, a)
            for g in gare:
                num  = g.get('Numero Gara', '')
                camp = g.get('Campionato', '')
                if not num: continue
                # Scarta gare RSA
                if is_gara_sarda(camp): continue
                # Verifica nome esatto nella gara (filtra omonimi peninsulari)
                sq_casa = g.get('Squadra Casa', '').upper()
                sq_osp  = g.get('Squadra Ospite', '').upper()
                if nome_esatto not in sq_casa and nome_esatto not in sq_osp:
                    continue
                key = (num, camp)
                if key in existing_keys: continue
                new_gare.append(g)
                existing_keys.add(key)
                trovate_sq += 1
            time.sleep(random.uniform(0.8, 1.5))

        print(f"  → {trovate_sq} nuove gare trovate")

    # ─── Salvataggio ──────────────────────────────────────────────────────────
    all_gare = existing + new_gare
    print(f"\n{'='*60}")
    print(f"✅ Gare nazionali totali: {len(all_gare)} (+{len(new_gare)} nuove)")

    os.makedirs("cache", exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(all_gare, f, ensure_ascii=False, indent=2)
    print(f"💾 Cache nazionale salvata: {CACHE_FILE}")

    # Riepilogo per squadra sarda
    print("\nGare per squadra sarda nella cache:")
    keywords = ['ESPERIA','SENNORI','BANCO DI SARDEGNA','DINAMO LAB',
                'VIRTUS CAGLIARI','SELARGIUS','SCUOLA ADD']
    for kw in keywords:
        count = sum(1 for g in all_gare
                    if kw in g.get('Squadra Casa','').upper()
                    or kw in g.get('Squadra Ospite','').upper())
        if count: print(f"  {kw}: {count} gare")

    # Riepilogo arbitri sardi
    prov_count = {}
    for g in all_gare:
        for field in ['Arbitro 1', 'Arbitro 2', 'Arbitro 3']:
            val = g.get(field, '')
            pp = parse_person(val)
            if pp and pp['provincia'] in PROV_SARDE:
                prov_count[pp['provincia']] = prov_count.get(pp['provincia'], 0) + 1
    print("\nGare con arbitri sardi per provincia:")
    for pv, cnt in sorted(prov_count.items(), key=lambda x: -x[1]):
        print(f"  {pv}: {cnt}")

if __name__ == "__main__":
    main()
