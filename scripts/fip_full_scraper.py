#!/usr/bin/env python3
"""
FIP Full Sardinia Scraper
Scarica TUTTE le gare della Sardegna (RSA + nazionali) giorno per giorno.
Salva in cache/fip_sardegna_full_cache.json — NON tocca nessun altro file.

Uso: python3 fip_full_scraper.py
     python3 fip_full_scraper.py --from 2025-09-01  (forza data inizio)
     python3 fip_full_scraper.py --rebuild           (ricostruisce da zero)
"""
import requests, json, os, re, sys, time, random, argparse
from bs4 import BeautifulSoup
from datetime import date, timedelta

BASE_URL   = "https://fip.it/risultati/"
CACHE_FILE = "cache/fip_sardegna_full_cache.json"
START_DATE = date(2025, 9, 1)   # inizio stagione 2025-2026

HEADERS_POOL = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"},
]

MESI_IT = {
    "gennaio":"01","febbraio":"02","marzo":"03","aprile":"04",
    "maggio":"05","giugno":"06","luglio":"07","agosto":"08",
    "settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"
}

def clean(t):
    return " ".join(t.split()) if t else ""

def parse_date_it(raw):
    parts = raw.lower().split()
    if len(parts) == 3:
        return f"{parts[2]}-{MESI_IT.get(parts[1],'00')}-{int(parts[0]):02d}"
    return raw

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
    flat = " ".join(ref_text.split())
    tokens = flat.split()
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
    sq_osp  = clean(teams[1].find("div", class_="team__name").get_text()) if len(teams) > 1 else ""
    pt_c = clean(teams[0].find("div", class_="team__points").get_text()) if teams and teams[0].find("div", class_="team__points") else ""
    pt_o = clean(teams[1].find("div", class_="team__points").get_text()) if len(teams) > 1 and teams[1].find("div", class_="team__points") else ""
    date_div = m.find("div", class_="date")
    time_div = m.find("div", class_="time")
    data_fmt = parse_date_it(clean(date_div.get_text())) if date_div else ""
    ora      = clean(time_div.get_text()) if time_div else ""
    ref_div  = m.find("div", class_="ref")
    num_gara, campionato, girone, fase = parse_ref(ref_div.get_text() if ref_div else "")
    return {
        "Data": data_fmt, "Ora": ora, "Numero Gara": num_gara,
        "Campionato": campionato, "Girone": girone, "Fase": fase,
        "Squadra Casa": sq_casa, "Squadra Ospite": sq_osp,
        "Punti Casa": pt_c, "Punti Ospite": pt_o,
        "Risultato": f"{pt_c}-{pt_o}" if pt_c and pt_o else "",
        "Campo": get_info(m, "campo di gioco"),
        "Arbitro 1": get_info(m, "1° arbitro"),
        "Arbitro 2": get_info(m, "2° arbitro"),
        "Arbitro 3": get_info(m, "3° arbitro"),
        "Segnapunti": get_info(m, "segnapunti"),
        "Cronometrista": get_info(m, "cronometrista"),
        "24 Secondi": get_info(m, "24 secondi"),
        "Addetto Referto": get_info(m, "addetto referto"),
        "Osservatore": get_info(m, "osservatore"),
        "Provvedimenti": get_info(m, "provvedimenti"),
    }

def fetch_day(session, giorno: date, max_retries=4):
    """Scarica tutte le gare della Sardegna per un giorno specifico."""
    params = {
        "search": "true",
        "data_singola": giorno.isoformat(),
        "codice_regione": "SAR",
        "data_da": "", "data_a": "",
        "numero_gara": "", "codice_societa": "",
        "nome_squadra": "", "codice_campo": "",
        "codice_arbitro": "", "cognome_arbitro": ""
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                matches = soup.find_all("div", class_="results-matches__match")
                return [parse_match(m) for m in matches]
            elif resp.status_code == 429:
                t = int(resp.headers.get("Retry-After", 30))
                print(f"\n  [429] attendo {t}s")
                time.sleep(t)
            else:
                print(f"\n  [HTTP {resp.status_code}] tentativo {attempt}")
        except Exception as e:
            print(f"\n  [ERR] {e} tentativo {attempt}")
        if attempt < max_retries:
            time.sleep(random.uniform(2, 4) * attempt)
    return []

def main():
    parser = argparse.ArgumentParser(description="FIP Full Sardinia Scraper")
    parser.add_argument("--from", dest="from_date", help="Data inizio (YYYY-MM-DD)")
    parser.add_argument("--rebuild", action="store_true", help="Ricostruisce da zero")
    args = parser.parse_args()

    print(f"=== FIP Full Sardinia Scraper — {date.today()} ===")
    print(f"Output: {CACHE_FILE}")
    print("ATTENZIONE: file separato, non tocca nessun'altra cache\n")

    os.makedirs("cache", exist_ok=True)

    # Carica cache esistente
    existing = []
    existing_keys = set()
    if not args.rebuild and os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            existing = json.load(f)
        existing_keys = {(g.get('Numero Gara',''), g.get('Data','')) for g in existing if g.get('Numero Gara')}
        print(f"Cache esistente: {len(existing)} gare")

        # Trova l'ultima data già scaricata
        date_presenti = [g['Data'] for g in existing if g.get('Data')]
        if date_presenti:
            ultima = max(date_presenti)
            # Riparte dal giorno dopo l'ultima data scaricata
            from_date = date.fromisoformat(ultima) + timedelta(days=1)
            print(f"Ultima data in cache: {ultima} → riprendo da {from_date}")
        else:
            from_date = START_DATE
    else:
        from_date = START_DATE
        if args.rebuild:
            print("Modalità rebuild — ricostruisce da zero")

    # Override data inizio se specificata
    if args.from_date:
        from_date = date.fromisoformat(args.from_date)
        print(f"Data inizio forzata: {from_date}")

    # Fine stagione: 30 giugno 2026
    to_date = date(2026, 6, 30)
    if from_date > to_date:
        print(f"Già aggiornato fino al {from_date - timedelta(days=1)}, nulla da fare.")
        return

    # Calcola giorni da scaricare
    giorni = []
    cur = from_date
    while cur <= min(to_date, date.today()):
        giorni.append(cur)
        cur += timedelta(days=1)

    print(f"Giorni da scaricare: {len(giorni)} ({from_date} → {min(to_date, date.today())})\n")

    session = requests.Session()
    session.headers.update(random.choice(HEADERS_POOL))

    new_gare = []
    giorni_con_gare = 0
    giorni_vuoti = 0

    for i, giorno in enumerate(giorni, 1):
        gare_giorno = fetch_day(session, giorno)

        nuove = 0
        for g in gare_giorno:
            key = (g.get('Numero Gara',''), g.get('Data',''))
            if not key[0]: continue
            if key in existing_keys: continue
            new_gare.append(g)
            existing_keys.add(key)
            nuove += 1

        if gare_giorno:
            giorni_con_gare += 1
            print(f"[{i}/{len(giorni)}] {giorno} → {len(gare_giorno)} gare ({nuove} nuove)")
        else:
            giorni_vuoti += 1
            if giorni_vuoti % 10 == 0:
                print(f"[{i}/{len(giorni)}] {giorno} → nessuna gara (ultimo {giorni_vuoti} giorni vuoti)")

        # Salvataggio incrementale ogni 50 giorni
        if i % 50 == 0:
            all_gare = existing + new_gare
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(all_gare, f, ensure_ascii=False, indent=2)
            print(f"  💾 Salvataggio intermedio: {len(all_gare)} gare totali")

        time.sleep(random.uniform(0.3, 0.8))

    # Salvataggio finale
    all_gare = existing + new_gare
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(all_gare, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ Completato!")
    print(f"   Giorni scaricati: {len(giorni)}")
    print(f"   Giorni con gare: {giorni_con_gare}")
    print(f"   Nuove gare aggiunte: {len(new_gare)}")
    print(f"   Totale gare in cache: {len(all_gare)}")
    print(f"   File: {CACHE_FILE}")

    # Riepilogo per campionato
    print(f"\nTop campionati nella cache completa:")
    camp_count = {}
    for g in all_gare:
        c = g.get('Campionato','?')
        camp_count[c] = camp_count.get(c, 0) + 1
    for c, n in sorted(camp_count.items(), key=lambda x: -x[1])[:15]:
        print(f"  {n:4d}  {c}")

    # Squadre nazionali trovate (non RSA)
    print(f"\nSquadre sarde in campionati NON RSA:")
    squadre_naz = set()
    for g in all_gare:
        if not g.get('Campionato','').upper().startswith('COMITATO REGIONALE SARDEGNA'):
            squadre_naz.add(g.get('Squadra Casa',''))
            squadre_naz.add(g.get('Squadra Ospite',''))
    # Filtra squadre sarde (campo in Sardegna o nome noto)
    keywords = ['SENNORI','SASSARI','CAGLIARI','NUORO','OLBIA','ORISTANO',
                'SARDEGNA','BANCO','ESPERIA','SELARGIUS','DINAMO']
    for sq in sorted(squadre_naz):
        if any(k in sq.upper() for k in keywords):
            count = sum(1 for g in all_gare
                       if g.get('Squadra Casa') == sq or g.get('Squadra Ospite') == sq
                       if not g.get('Campionato','').upper().startswith('COMITATO REGIONALE SARDEGNA'))
            print(f"  {sq}: {count} gare nazionali")

if __name__ == "__main__":
    main()
