#!/usr/bin/env python3
"""
FIP National Scraper v1
Scarica gare nazionali che coinvolgono arbitri/UDC sardi fuori RSA
e gare nazionali giocate in Sardegna.

Uso:
  python fip_national_scraper.py                  # aggiornamento normale
  python fip_national_scraper.py --full-refresh   # riscarica tutto
"""
import requests, json, os, re, sys, time, random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date

BASE_URL   = "https://fip.it/risultati/"
CACHE_RSA  = "cache/fip_sardegna_cache.json"
CACHE_NAT  = "cache/fip_national_cache.json"
DATE_START = date(2025, 9, 1)
MAX_RETRIES = 4

# Province sarde
PV_SARDE = {"CA","SS","NU","OR","SU","CI","MD","OG","OT","VS"}  # incluse vecchie sigle

HEADERS_POOL = [
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"},
]

MESI_IT = {"gennaio":"01","febbraio":"02","marzo":"03","aprile":"04",
           "maggio":"05","giugno":"06","luglio":"07","agosto":"08",
           "settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"}

# Periodi stagione (per evitare >100 risultati per richiesta)
PERIODI = [
    (DATE_START, date(2025, 11, 30)),
    (date(2025, 12, 1), date(2026, 2, 28)),
    (date(2026, 3, 1), date(2026, 6, 30)),
]

def clean(t): return " ".join(t.split()) if t else ""

def parse_date_it(raw):
    parts = raw.lower().split()
    if len(parts)==3:
        return f"{parts[2]}-{MESI_IT.get(parts[1],'00')}-{int(parts[0]):02d}"
    return raw

def pv_of_person(txt):
    """Estrae la sigla provincia da 'ROSSI MARIO di SASSARI (SS)'"""
    if not txt: return None
    m = re.search(r'\(([A-Z]{2})\)\s*$', txt.strip())
    return m.group(1) if m else None

def is_sardo(txt):
    """True se la persona è residente in una provincia sarda"""
    return pv_of_person(txt) in PV_SARDE

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

def parse_ref_national(ref_text):
    """Parsa il ref per campionati nazionali (formato diverso da RSA)"""
    flat = " ".join(ref_text.split())
    tokens = flat.split()
    num_gara = tokens[0] if tokens else ""
    campionato = girone = fase = comitato = ""
    # Formato: NUMGARA - COMITATO CAMPIONATO, Girone: X, Fase: Y
    m = re.search(r'-\s*(.+?)\s+(?:Serie|Divisione|Under|Coppa|Campionato)(.+?)(?:,\s*Girone:|$)', flat, re.IGNORECASE)
    if m:
        comitato = clean(m.group(1))
        campionato = clean("Serie"+m.group(2)) if m.group(2) else ""
    # Prova pattern più generico
    m2 = re.search(r'-\s*(.+?)\s*,\s*Girone:\s*(.+?)(?:,\s*Fase:|$)', flat, re.IGNORECASE)
    if m2 and not campionato:
        campionato = clean(m2.group(1).split('-')[-1].strip()) if '-' in m2.group(1) else clean(m2.group(1))
        girone = clean(m2.group(2))
    gm = re.search(r'Girone:\s*(.+?)(?:,\s*Fase:|$)', flat, re.IGNORECASE)
    fm = re.search(r'Fase:\s*(.+)', flat, re.IGNORECASE)
    if gm: girone = clean(gm.group(1))
    if fm: fase = clean(fm.group(1))
    # Campionato dal testo completo se ancora vuoto
    if not campionato:
        cm = re.search(r'-\s*[A-Z\s]+\s+((?:Serie|Under|Divisione|Coppa)[^,]+)', flat, re.IGNORECASE)
        if cm: campionato = clean(cm.group(1))
    # Estrai comitato
    cm2 = re.search(r'COMITATO\s+(?:REGIONALE\s+)?([A-Z\s]+?)(?:\s+(?:Serie|Under|Divisione|Coppa)|,)', flat, re.IGNORECASE)
    if cm2: comitato = clean(cm2.group(1))
    return num_gara, campionato, girone, fase, comitato

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
    ref_txt  = ref_div.get_text() if ref_div else ""
    num_gara, campionato, girone, fase, comitato = parse_ref_national(ref_txt)
    return {
        "Data":data_fmt, "Ora":ora, "Numero Gara":num_gara,
        "Campionato":campionato, "Girone":girone, "Fase":fase,
        "Comitato":comitato,
        "Squadra Casa":sq_casa, "Squadra Ospite":sq_osp,
        "Punti Casa":pt_c, "Punti Ospite":pt_o,
        "Risultato":f"{pt_c}-{pt_o}" if pt_c and pt_o else "",
        "Campo":get_info(m,"campo di gioco"),
        "Arbitro 1":get_info(m,"1° arbitro"), "Arbitro 2":get_info(m,"2° arbitro"),
        "Arbitro 3":get_info(m,"3° arbitro"),
        "Segnapunti":get_info(m,"segnapunti"), "Cronometrista":get_info(m,"cronometrista"),
        "24 Secondi":get_info(m,"24 secondi"), "Addetto Referto":get_info(m,"addetto referto"),
        "Osservatore":get_info(m,"osservatore"), "Provvedimenti":get_info(m,"provvedimenti"),
    }

def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text().lower()
    if "numero eccessivo" in txt:
        return None  # troppi risultati
    if "nessun risultato" in txt or "nessuna gara" in txt:
        return []
    return [parse_match(m) for m in soup.find_all("div", class_="results-matches__match")]

def fetch_period(session, cognome=None, nome_squadra=None, da=None, a=None):
    """Fetch per cognome arbitro o nome squadra in un periodo"""
    params = {
        "search":"true", "data_singola":"",
        "data_da": da or "", "data_a": a or "",
        "comitato":"",  # NESSUN filtro comitato = nazionale
        "numero_gara":"", "codice_societa":"",
        "nome_squadra": nome_squadra or "",
        "codice_campo":"",
        "codice_arbitro":"",
        "cognome_arbitro": cognome or "",
    }
    for attempt in range(1, MAX_RETRIES+1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=12)
            if resp.status_code == 200: return resp
            elif resp.status_code == 429:
                t = int(resp.headers.get("Retry-After", 30))
                print(f"\n  [429] attendo {t}s", flush=True); time.sleep(t)
            else:
                print(f"\n  [HTTP {resp.status_code}] t.{attempt}", flush=True)
        except Exception as e:
            print(f"\n  [ERR] {e} t.{attempt}", flush=True)
        if attempt < MAX_RETRIES: time.sleep(random.uniform(1,3)*attempt)
    return None

def fetch_all_periods(session, cognome=None, nome_squadra=None):
    """Scarica tutti i periodi per un arbitro o squadra, gestisce >100 risultati"""
    all_rows = []
    seen_keys = set()
    label = cognome or nome_squadra or "?"
    for (da, a) in PERIODI:
        da_s = da.isoformat(); a_s = min(a, date.today()+timedelta(days=14)).isoformat()
        if da > date.today()+timedelta(days=14): continue
        resp = fetch_period(session, cognome=cognome, nome_squadra=nome_squadra, da=da_s, a=a_s)
        if resp is None:
            print(f"  ⚠ {label} periodo {da_s}→{a_s} FALLITO", flush=True); continue
        rows = parse_page(resp.text)
        if rows is None:
            # Troppi risultati: dividi il periodo in mesi
            print(f"  ⚠ {label} periodo {da_s}→{a_s} >100 risultati, divido in mesi...", flush=True)
            cur = da
            while cur <= a and cur <= date.today()+timedelta(days=14):
                fine_mese = date(cur.year, cur.month, 1) + timedelta(days=32)
                fine_mese = date(fine_mese.year, fine_mese.month, 1) - timedelta(days=1)
                fine_mese = min(fine_mese, a, date.today()+timedelta(days=14))
                r2 = fetch_period(session, cognome=cognome, nome_squadra=nome_squadra,
                                   da=cur.isoformat(), a=fine_mese.isoformat())
                if r2:
                    sub = parse_page(r2.text)
                    if sub:
                        for row in sub:
                            k = row.get("Numero Gara") or (row["Data"]+row["Squadra Casa"]+row["Squadra Ospite"])
                            if k and k not in seen_keys:
                                seen_keys.add(k); all_rows.append(row)
                cur = fine_mese + timedelta(days=1)
                time.sleep(random.uniform(0.8,1.5))
        elif rows:
            for row in rows:
                k = row.get("Numero Gara") or (row["Data"]+row["Squadra Casa"]+row["Squadra Ospite"])
                if k and k not in seen_keys:
                    seen_keys.add(k); all_rows.append(row)
        time.sleep(random.uniform(0.8,1.5))
    return all_rows

def is_rsa_game(row):
    """True se la gara è già nel database RSA (ha COMITATO REGIONALE SARDEGNA nel ref)"""
    ref = row.get("Comitato","").upper()
    camp = row.get("Campionato","").upper()
    # Se il comitato contiene SARDEGNA è già in RSA
    return "SARDEGNA" in ref or "RSA" in ref

def extract_cognome(nome_completo):
    """Estrae il cognome da 'ROSSI MARIO di SASSARI (SS)' → 'ROSSI'"""
    # Rimuovi ' di CITTA (PV)'
    nome = re.sub(r'\s+di\s+\S.*$', '', nome_completo, flags=re.IGNORECASE).strip()
    return nome.split()[0] if nome else nome_completo.split()[0]

def load_rsa_persons():
    """Carica arbitri e UDC sardi dal cache RSA"""
    if not os.path.exists(CACHE_RSA):
        print("⚠ Cache RSA non trovata. Esegui prima fip_scraper.py")
        return [], []
    with open(CACHE_RSA, encoding="utf-8") as f:
        gare = json.load(f)
    
    arbitri = set(); udc = set(); osservatori = set()
    for g in gare:
        for field in ["Arbitro 1","Arbitro 2","Arbitro 3"]:
            v = g.get(field,"").strip()
            if v and is_sardo(v): arbitri.add(v)
        for field in ["Segnapunti","Cronometrista","24 Secondi","Addetto Referto"]:
            v = g.get(field,"").strip()
            if v and is_sardo(v): udc.add(v)
        v = g.get("Osservatore","").strip()
        if v and is_sardo(v): osservatori.add(v)
    
    print(f"  Arbitri sardi trovati in RSA: {len(arbitri)}")
    print(f"  UDC sardi trovati in RSA: {len(udc)}")
    print(f"  Osservatori sardi trovati in RSA: {len(osservatori)}")
    return sorted(arbitri), sorted(udc), sorted(osservatori)

def main():
    import argparse
    parser = argparse.ArgumentParser(description='FIP National Scraper v1')
    parser.add_argument('--full-refresh', action='store_true',
        help='Riscarica tutto ignorando la cache nazionale')
    parser.add_argument('--only-arbitri', action='store_true',
        help='Scarica solo gare arbitri sardi fuori RSA')
    parser.add_argument('--only-campi', action='store_true',
        help='Scarica solo gare nazionali giocate in Sardegna (per campo)')
    args = parser.parse_args()

    os.makedirs("cache", exist_ok=True)

    # Carica cache nazionale esistente
    existing = {}  # key -> gara
    if not args.full_refresh and os.path.exists(CACHE_NAT):
        with open(CACHE_NAT, encoding="utf-8") as f:
            lst = json.load(f)
        existing = {(g.get("Numero Gara") or g["Data"]+g["Squadra Casa"]+g["Squadra Ospite"]): g for g in lst}
        print(f"Cache nazionale esistente: {len(existing)} gare")

    # Carica persone sarde da RSA
    print("\n=== Carico persone sarde da cache RSA ===")
    arbitri, udc, osservatori = load_rsa_persons()
    if not arbitri and not udc:
        print("Nessuna persona trovata. Uscita."); return

    session = requests.Session()
    session.headers.update(random.choice(HEADERS_POOL))

    added = 0; skipped_rsa = 0

    def add_rows(rows, fonte):
        nonlocal added, skipped_rsa
        for row in rows:
            k = row.get("Numero Gara") or (row["Data"]+row["Squadra Casa"]+row["Squadra Ospite"])
            if not k: continue
            if is_rsa_game(row):
                skipped_rsa += 1; continue  # già in RSA
            row["_fonte"] = fonte
            if k not in existing:
                existing[k] = row; added += 1

    # ── FASE 1: Arbitri sardi fuori RSA ──
    if not args.only_campi:
        print(f"\n=== FASE 1: Arbitri sardi ({len(arbitri)}) ===")
        for i, arb in enumerate(arbitri, 1):
            cognome = extract_cognome(arb)
            pv = pv_of_person(arb) or "?"
            print(f"  [{i}/{len(arbitri)}] {arb} → cerca cognome '{cognome}'...", end="", flush=True)
            rows = fetch_all_periods(session, cognome=cognome)
            # Filtra per provincia: tieni solo righe dove l'arbitro ha sigla sarda
            rows_sardi = []
            for row in rows:
                for field in ["Arbitro 1","Arbitro 2","Arbitro 3"]:
                    v = row.get(field,"")
                    if v and extract_cognome(v).upper() == cognome.upper() and is_sardo(v):
                        rows_sardi.append(row); break
            print(f" {len(rows_sardi)} gare (di cui fuori RSA)", flush=True)
            add_rows(rows_sardi, f"arbitro:{arb}")
            time.sleep(random.uniform(1,2))

    # ── FASE 2: UDC sardi fuori RSA ──
    if not args.only_campi:
        print(f"\n=== FASE 2: UDC sardi ({len(udc)}) ===")
        # Per UDC il sito non ha un campo cognome_arbitro dedicato
        # Usiamo lo stesso parametro cognome_arbitro che FIP usa anche per UDC
        for i, persona in enumerate(udc, 1):
            cognome = extract_cognome(persona)
            print(f"  [{i}/{len(udc)}] {persona} → cerca '{cognome}'...", end="", flush=True)
            rows = fetch_all_periods(session, cognome=cognome)
            rows_sardi = []
            for row in rows:
                for field in ["Segnapunti","Cronometrista","24 Secondi","Addetto Referto"]:
                    v = row.get(field,"")
                    if v and extract_cognome(v).upper() == cognome.upper() and is_sardo(v):
                        rows_sardi.append(row); break
            print(f" {len(rows_sardi)} gare fuori RSA", flush=True)
            add_rows(rows_sardi, f"udc:{persona}")
            time.sleep(random.uniform(1,2))

    # ── FASE 3: Osservatori sardi fuori RSA ──
    if not args.only_campi and osservatori:
        print(f"\n=== FASE 3: Osservatori sardi ({len(osservatori)}) ===")
        for i, persona in enumerate(osservatori, 1):
            cognome = extract_cognome(persona)
            print(f"  [{i}/{len(osservatori)}] {persona} → cerca '{cognome}'...", end="", flush=True)
            rows = fetch_all_periods(session, cognome=cognome)
            rows_sardi = []
            for row in rows:
                v = row.get("Osservatore","")
                if v and extract_cognome(v).upper() == cognome.upper() and is_sardo(v):
                    rows_sardi.append(row)
            print(f" {len(rows_sardi)} gare fuori RSA", flush=True)
            add_rows(rows_sardi, f"osservatore:{persona}")
            time.sleep(random.uniform(1,2))

    # ── FASE 4: Gare nazionali giocate in Sardegna (cerca per campo) ──
    # Strategia: cerca gare senza comitato per ogni giorno, filtra quelle con campo in Sardegna
    # Non fattibile giorno per giorno (troppo lento). Alternativa: cerca squadre sarde note.
    # Carica lista squadre sarde dal cache RSA
    if not args.only_arbitri:
        print(f"\n=== FASE 4: Squadre sarde in campionati nazionali ===")
        with open(CACHE_RSA, encoding="utf-8") as f:
            gare_rsa = json.load(f)
        squadre_sarde = set()
        for g in gare_rsa:
            squadre_sarde.add(g.get("Squadra Casa","").strip())
            squadre_sarde.add(g.get("Squadra Ospite","").strip())
        squadre_sarde = {s for s in squadre_sarde if s}
        print(f"  Squadre sarde trovate in RSA: {len(squadre_sarde)}")

        for i, sq in enumerate(sorted(squadre_sarde), 1):
            # Usa le prime 2-3 parole significative per la ricerca
            parole = [p for p in sq.split() if len(p)>2 and p not in ('ASD','SSD','POL','BASKET','PALL','PALLACANESTRO','A.S.D.','S.S.D.')]
            if not parole: continue
            keyword = parole[0]  # prima parola significativa
            print(f"  [{i}/{len(squadre_sarde)}] '{sq}' → cerca '{keyword}'...", end="", flush=True)
            rows = fetch_all_periods(session, nome_squadra=keyword)
            # Filtra: tieni solo gare dove almeno una squadra contiene la keyword
            # E che non siano già RSA
            rows_sq = [r for r in rows if keyword.upper() in (r.get("Squadra Casa","")+" "+r.get("Squadra Ospite","")).upper()]
            print(f" {len(rows_sq)} gare trovate", flush=True)
            add_rows(rows_sq, f"squadra:{sq}")
            time.sleep(random.uniform(1.5,2.5))

    # ── Salva cache nazionale ──
    result = list(existing.values())
    result.sort(key=lambda g: g.get("Data",""))
    with open(CACHE_NAT, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"✅ Gare nazionali scaricate: {len(result)}")
    print(f"   Nuove aggiunte: {added}")
    print(f"   Scartate (già in RSA): {skipped_rsa}")
    print(f"💾 Cache salvata: {CACHE_NAT}")

if __name__ == "__main__":
    main()
