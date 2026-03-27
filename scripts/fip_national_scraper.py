#!/usr/bin/env python3
"""
FIP National Scraper v2
Cerca gare nazionali di arbitri/UDC/osservatori sardi fuori dalla Sardegna.
Legge i cognomi direttamente da fip_sardegna_cache.json.
"""
import requests, json, os, re, sys, time, random
from bs4 import BeautifulSoup
from datetime import date, timedelta

BASE_URL   = "https://fip.it/risultati/"
CACHE_FILE = "cache/fip_national_cache.json"
RSA_CACHE  = "cache/fip_sardegna_cache.json"
PROV_SARDE = {'CA','SS','NU','OR','SU','CI','OG','OT','VS'}

# Stagione corrente divisa in 3 periodi per evitare il limite >100 risultati
PERIODS = [
    ("2025-09-01", "2025-11-30"),
    ("2025-12-01", "2026-02-28"),
    ("2026-03-01", "2026-06-30"),
]

HEADERS_POOL = [
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"},
]

MESI_IT = {"gennaio":"01","febbraio":"02","marzo":"03","aprile":"04",
           "maggio":"05","giugno":"06","luglio":"07","agosto":"08",
           "settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"}

prov_re = re.compile(r'\bdi\s+.+?\s+\((\w{2,3})\)\s*$', re.IGNORECASE)

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
        cm2 = re.match(r'(.+?)\s*\((\w{2,3})\)', rest)
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
        return None  # troppi risultati
    matches = soup.find_all("div", class_="results-matches__match")
    return [parse_match(m) for m in matches]

def fetch(session, params, max_retries=4):
    for attempt in range(1, max_retries+1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=15)
            if resp.status_code == 200: return resp
            elif resp.status_code == 429:
                t = int(resp.headers.get("Retry-After", 30))
                print(f"\n[429] attendo {t}s"); time.sleep(t)
            else:
                print(f"\n[HTTP {resp.status_code}] tentativo {attempt}")
        except Exception as e:
            print(f"\n[ERR] {e} tentativo {attempt}")
        if attempt < max_retries:
            time.sleep(random.uniform(2, 4) * attempt)
    return None

def fetch_by_cognome(session, cognome, da, a):
    """Cerca tutte le gare di un cognome in un periodo. Ritorna lista gare o None se troppi risultati."""
    params = {
        "search":"true","data_da":da,"data_a":a,
        "cognome_arbitro":cognome,
        "data_singola":"","numero_gara":"","codice_societa":"",
        "nome_squadra":"","codice_campo":"","codice_arbitro":"","comitato":""
    }
    resp = fetch(session, params)
    if resp is None: return []
    rows = parse_page(resp.text)
    if rows is None:
        # Troppi risultati: dividi il periodo in mesi
        print(f"  [!] {cognome} {da}→{a} troppi risultati, divido per mese...")
        result = []
        d_start = date.fromisoformat(da)
        d_end = date.fromisoformat(a)
        cur = d_start
        while cur <= d_end:
            m_end = date(cur.year, cur.month, 28) + timedelta(days=4)
            m_end = date(m_end.year, m_end.month, 1) - timedelta(days=1)
            m_end = min(m_end, d_end)
            sub = fetch_by_cognome(session, cognome, cur.isoformat(), m_end.isoformat())
            result.extend(sub)
            cur = m_end + timedelta(days=1)
            time.sleep(random.uniform(1, 2))
        return result
    return rows or []

def is_sardo(field_value):
    """Controlla se una persona è sarda dalla sigla provincia."""
    if not field_value: return False
    m = re.search(r'\((\w{2,3})\)', field_value)
    return bool(m) and m.group(1).upper() in PROV_SARDE

def campo_fuori_sardegna(campo):
    """Ritorna True se il campo è CHIARAMENTE fuori Sardegna, False se è in Sardegna o non riconoscibile."""
    if not campo: return True  # senza campo assumiamo fuori (gara nazionale)
    # Se ha sigla provincia esplicita, usiamo quella
    m = re.search(r'\((\w{2,3})\)', campo)
    if m: return m.group(1).upper() not in PROV_SARDE
    # Se non ha sigla: scarta SOLO se riconosciamo parole sarde nel campo
    parole_sarde = ['CAGLIARI','SASSARI','NUORO','ORISTANO','CARBONIA','IGLESIAS',
                    'OLBIA','TEMPIO','QUARTU','SELARGIUS','ALGHERO','MACOMER',
                    'SENNORI','PORTO TORRES','IGLESIAS','CARBONIA','ARZACHENA',
                    'OZIERI','SINISCOLA','TORTOLI','LANUSEI','MURAVERA']
    campo_up = campo.upper()
    if any(p in campo_up for p in parole_sarde): return False  # è in Sardegna
    return True  # non riconosciuto come sardo → assume fuori Sardegna

def main():
    print(f"=== FIP National Scraper v2 — {date.today()} ===")

    # Carica cache RSA per estrarre cognomi arbitri sardi
    if not os.path.exists(RSA_CACHE):
        print(f"ERRORE: Cache RSA non trovata: {RSA_CACHE}")
        sys.exit(1)

    with open(RSA_CACHE, encoding="utf-8") as f:
        rsa_gare = json.load(f)
    print(f"Cache RSA caricata: {len(rsa_gare)} gare")

    rsa_nums = {g.get('Numero Gara','') for g in rsa_gare if g.get('Numero Gara')}

    # Estrai cognomi di arbitri/UDC sardi
    cognomi_sardi = {}  # cognome -> {province, ruoli}
    for g in rsa_gare:
        for field in ['Arbitro 1','Arbitro 2','Arbitro 3','Segnapunti','Cronometrista','24 Secondi','Osservatore']:
            val = g.get(field,'')
            if not val: continue
            pp = parse_person(val)
            if not pp or pp['provincia'] not in PROV_SARDE: continue
            cogn = pp['nome'].split()[0].upper() if pp['nome'] else ''
            if not cogn or len(cogn) < 3: continue
            if cogn not in cognomi_sardi:
                cognomi_sardi[cogn] = {'province': set(), 'ruoli': set(), 'nome_completo': pp['nome']}
            cognomi_sardi[cogn]['province'].add(pp['provincia'])
            ruolo = 'Arbitro' if 'Arbitro' in field else ('UDC' if field in ['Segnapunti','Cronometrista','24 Secondi'] else 'Osservatore')
            cognomi_sardi[cogn]['ruoli'].add(ruolo)

    print(f"Cognomi sardi unici trovati: {len(cognomi_sardi)}")

    # Carica cache nazionale esistente
    existing = []
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            existing = json.load(f)
        print(f"Cache nazionale esistente: {len(existing)} gare")

    existing_nums = {g.get('Numero Gara','') for g in existing if g.get('Numero Gara')}

    session = requests.Session()
    session.headers.update(random.choice(HEADERS_POOL))

    new_gare = []
    cognomi_list = sorted(cognomi_sardi.keys())
    total = len(cognomi_list)

    print(f"\nCerco gare nazionali per {total} cognomi sardi...")

    for i, cogn in enumerate(cognomi_list, 1):
        info = cognomi_sardi[cogn]
        print(f"[{i}/{total}] {cogn} ({','.join(info['province'])})...", end=" ", flush=True)

        trovate_fuori = 0
        for da, a in PERIODS:
            gare = fetch_by_cognome(session, cogn, da, a)
            for g in gare:
                num = g.get('Numero Gara','')
                if not num: continue
                if num in rsa_nums: continue       # già in RSA
                if num in existing_nums: continue  # già in nazionale cache

                # Filtra: voglio solo gare dove QUESTO arbitro è sardo (filtra omonimi)
                persona_trovata = False
                for field in ['Arbitro 1','Arbitro 2','Arbitro 3','Segnapunti','Cronometrista','24 Secondi','Osservatore']:
                    val = g.get(field,'')
                    if not val: continue
                    pp = parse_person(val)
                    if not pp: continue
                    if pp['nome'].split()[0].upper() == cogn and pp['provincia'] in PROV_SARDE:
                        persona_trovata = True; break

                if persona_trovata:
                    new_gare.append(g)
                    existing_nums.add(num)
                    trovate_fuori += 1

            time.sleep(random.uniform(0.8, 1.5))

        print(f"{trovate_fuori} nuove gare fuori RSA")

    # Unisci con cache esistente
    all_gare = existing + new_gare
    print(f"\n✅ Gare nazionali totali: {len(all_gare)} (+{len(new_gare)} nuove)")

    # Salva
    os.makedirs("cache", exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(all_gare, f, ensure_ascii=False, indent=2)
    print(f"💾 Cache nazionale salvata: {CACHE_FILE}")

    # Riepilogo per provincia
    prov_count = {}
    for g in all_gare:
        for field in ['Arbitro 1','Arbitro 2','Osservatore']:
            val = g.get(field,'')
            pp = parse_person(val)
            if pp and pp['provincia'] in PROV_SARDE:
                prov_count[pp['provincia']] = prov_count.get(pp['provincia'],0)+1
    print("\nGare per provincia arbitro:")
    for pv, cnt in sorted(prov_count.items(), key=lambda x:-x[1]):
        print(f"  {pv}: {cnt}")

if __name__ == "__main__":
    main()
