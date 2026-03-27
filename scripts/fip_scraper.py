#!/usr/bin/env python3
"""
FIP Sardegna Scraper v4 – Aggiornamento automatico giornaliero
Scarica tutte le gare dall'01.10.2025 a ieri incluso.
Uso: python fip_scraper_v4_auto.py
"""
import requests, json, os, re, sys, time, random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date

BASE_URL  = "https://fip.it/risultati/"
COMITATO  = "RSA"
DATE_START = datetime(2025, 9, 1)
CACHE_FILE = "cache/fip_sardegna_cache.json"
MAX_RETRIES = 5

HEADERS_POOL = [
    {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"},
    {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"},
]

MESI_IT = {"gennaio":"01","febbraio":"02","marzo":"03","aprile":"04",
           "maggio":"05","giugno":"06","luglio":"07","agosto":"08",
           "settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"}

def clean(t): return " ".join(t.split()) if t else ""

def parse_date_it(raw):
    parts = raw.lower().split()
    if len(parts)==3:
        return f"{parts[2]}-{MESI_IT.get(parts[1],'00')}-{int(parts[0]):02d}"
    return raw

def parse_ref(ref_text):
    flat = " ".join(ref_text.split()); tokens = flat.split()
    num_gara = tokens[0] if tokens else ""
    campionato = girone = fase = ""
    m = re.search(r"-\s*COMITATO REGIONALE SARDEGNA\s+(.*)", flat, re.IGNORECASE)
    if m:
        rest = m.group(1)
        gm = re.search(r"Girone:\s*(.+?)(?:,\s*Fase:|$)", rest)
        fm = re.search(r"Fase:\s*(.+)", rest)
        cm = re.match(r"(.+?)\s*(?:[MF],|Girone:|$)", rest)
        campionato = clean(cm.group(1)) if cm else ""
        girone = clean(gm.group(1)) if gm else ""
        fase = clean(fm.group(1)) if fm else ""
    return num_gara, campionato, girone, fase

def get_info(match_div, label_text):
    for info in match_div.find_all("div", class_="info"):
        lbl = info.find("div", class_="label")
        if lbl and label_text.lower() in lbl.get_text().lower():
            # Prende TUTTI i tag con classe "value" (non solo il primo)
            vals = info.find_all(class_="value")
            if not vals:
                return ""
            parts = []
            for val in vals:
                v = clean(val.get_text())
                if v and v.lower() not in ('designazione in attesa di conferma.', 'n/d'):
                    parts.append(v)
            return "\n".join(parts)
    return ""

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
        "Stato Gara":"","Campo":get_info(m,"campo di gioco"),
        "Arbitro 1":get_info(m,"1° arbitro"),"Arbitro 2":get_info(m,"2° arbitro"),
        "Arbitro 3":get_info(m,"3° arbitro"),
        "Segnapunti":get_info(m,"segnapunti"),"Cronometrista":get_info(m,"cronometrista"),
        "24 Secondi":get_info(m,"24 secondi"),"Addetto Referto":get_info(m,"addetto referto"),
        "Osservatore":get_info(m,"osservatore"),"Provvedimenti":get_info(m,"provvedimenti"),
    }

def parse_page(html):
    soup = BeautifulSoup(html,"html.parser")
    if "numero eccessivo" in soup.get_text().lower(): return None
    return [parse_match(m) for m in soup.find_all("div", class_="results-matches__match")]

def fetch(session, da):
    params = {"search":"true","data_singola":da,"data_da":"","data_a":"",
              "comitato":COMITATO,"numero_gara":"","codice_societa":"",
              "nome_squadra":"","codice_campo":"","codice_arbitro":"","cognome_arbitro":""}
    for attempt in range(1, MAX_RETRIES+1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=10)
            if resp.status_code==200: return resp
            elif resp.status_code==429:
                t = int(resp.headers.get("Retry-After",20)); print(f"\n[429] attendo {t}s"); time.sleep(t)
            else: print(f"\n[HTTP {resp.status_code}] t.{attempt}")
        except Exception as e: print(f"\n[ERR] {e} t.{attempt}")
        if attempt<MAX_RETRIES: time.sleep(random.uniform(1,3)*attempt)
    return None

def main():
    import argparse
    parser = argparse.ArgumentParser(description='FIP Sardegna Scraper v5')
    parser.add_argument('--refresh-days', type=int, default=7,
        help='Ri-scarica anche gli ultimi N giorni già in cache (per catturare modifiche). Default: 7')
    parser.add_argument('--from-date', type=str, default=None,
        help='Scarica da questa data (YYYY-MM-DD). Override DATE_START.')
    parser.add_argument('--full-refresh', action='store_true',
        help='Ri-scarica TUTTO dal DATE_START (lento ma completo)')
    parser.add_argument('--all-provv', action='store_true',
        help='Fase 2 su TUTTE le gare senza provvedimento (usa dopo full-refresh)')
    parser.add_argument('--future-days', type=int, default=14,
        help='Scarica anche i prossimi N giorni (gare programmate). Default: 14')
    args = parser.parse_args()

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    future_end = (date.today() + timedelta(days=args.future_days)).isoformat()
    print(f"=== FIP Sardegna v5 – Aggiornamento {yesterday} + prossimi {args.future_days}gg ===")
    if args.refresh_days:
        print(f"    (ri-scarica anche gli ultimi {args.refresh_days} giorni per aggiornamenti retroattivi)")
    
    # Carica cache esistente
    existing = []
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            existing = json.load(f)
        print(f"Cache esistente: {len(existing)} gare")
    
    # Giorni già scaricati
    scraped_days = set(g["Data"] for g in existing)
    
    # Giorni da ri-scaricare per aggiornamenti retroattivi
    refresh_cutoff = (date.today() - timedelta(days=args.refresh_days)).isoformat()
    refresh_days_set = set()
    if not args.full_refresh and args.refresh_days > 0:
        for ds in scraped_days:
            if ds >= refresh_cutoff:
                refresh_days_set.add(ds)
    
    # Genera lista giorni da scaricare
    start_date = date.fromisoformat(args.from_date) if args.from_date else DATE_START.date()
    cur = start_date
    end = date.fromisoformat(future_end)
    days_to_fetch = []
    while cur <= end:
        ds = cur.isoformat()
        if ds not in scraped_days or ds in refresh_days_set or args.full_refresh:
            days_to_fetch.append(ds)
        cur += timedelta(days=1)
    
    if not days_to_fetch:
        print("✅ Cache già aggiornata — nessun nuovo giorno da scaricare.")
        return
    
    print(f"Giorni da scaricare: {len(days_to_fetch)} (dal {days_to_fetch[0]} al {days_to_fetch[-1]})")
    
    session = requests.Session()
    session.headers.update(random.choice(HEADERS_POOL))
    
    # Mappa chiave -> indice in existing (per aggiornamenti retroattivi)
    key_to_idx = {}
    for idx, g in enumerate(existing):
        key = g.get("Numero Gara") or (g["Data"]+g["Squadra Casa"]+g["Squadra Ospite"])
        key_to_idx[key] = idx
    
    added = 0; updated = 0; empty = 0

    for i, da in enumerate(days_to_fetch, 1):
        resp = fetch(session, da)
        if resp is None:
            print(f"\n[{i}/{len(days_to_fetch)}] {da} FALLITO"); empty=0; continue
        rows = parse_page(resp.text)
        if rows is None:
            print(f"\n[{i}/{len(days_to_fetch)}] {da} ⚠️ troppi risultati"); empty=0; continue
        nuovi = 0; aggiornati = 0
        for r in rows:
            key = r.get("Numero Gara") or (r["Data"]+r["Squadra Casa"]+r["Squadra Ospite"])
            if not key: continue
            if key in key_to_idx:
                # Aggiorna la gara esistente (cattura modifiche retroattive)
                if da in refresh_days_set or args.full_refresh:
                    # Preserva SEMPRE i provvedimenti già trovati dalla Fase 2
                    old_provv = existing[key_to_idx[key]].get('Provvedimenti', '').strip()
                    new_provv = r.get('Provvedimenti', '').strip()
                    r['Provvedimenti'] = new_provv if new_provv else old_provv
                    existing[key_to_idx[key]] = r
                    aggiornati += 1; updated += 1
                else:
                    # Anche senza refresh: preserva sempre i provvedimenti
                    old_provv = existing[key_to_idx[key]].get('Provvedimenti', '').strip()
                    new_provv = r.get('Provvedimenti', '').strip()
                    r['Provvedimenti'] = new_provv if new_provv else old_provv
                    existing[key_to_idx[key]] = r
            else:
                key_to_idx[key] = len(existing)
                existing.append(r); nuovi+=1; added+=1
        if nuovi>0 or aggiornati>0:
            if empty>0: print()
            tag = f"{nuovi} nuove" + (f", {aggiornati} agg." if aggiornati else "")
            print(f"[{i}/{len(days_to_fetch)}] {da} → {tag} (tot: {len(existing)})")
            empty=0
        else:
            print(".", end="", flush=True); empty+=1
        if i % 10 == 0: print(f" [{i}/{len(days_to_fetch)}] {da}", flush=True); empty=0
        time.sleep(random.uniform(0.8,1.8))
    
    if empty>0: print()
    print(f"\n✅ Aggiunte {added} nuove gare | Totale: {len(existing)}")
    
    # Salva cache
    with open(CACHE_FILE,"w",encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    print(f"💾 Cache salvata: {CACHE_FILE}")
    
    if added>0:
        print("\n▶ Per aggiornare il dashboard esegui: python build_dashboard.py")

    # ── FASE 2: Verifica provvedimenti ──
    # Modalità: --all-provv o normale (ultimi N giorni). Sempre saltata se --full-refresh senza --all-provv
    run_fase2 = args.all_provv or (not args.full_refresh)
    if run_fase2:
        if args.all_provv:
            to_check=[g for g in existing
                      if not (g.get('Provvedimenti') or '').strip()
                      and g.get('Numero Gara')]
            print(f"\n[Fase 2 - ALL] Controllo provvedimenti su TUTTE le gare: {len(to_check)} senza provvedimento")
        else:
            cutoff=(date.today()-timedelta(days=args.refresh_days)).isoformat()
            candidates=[g for g in existing
                      if g.get('Data','')>=cutoff
                      and not (g.get('Provvedimenti') or '').strip()
                      and g.get('Numero Gara')]
            # Limita a max 100 gare per run normale (le più recenti)
            # Usa --all-provv per controllare tutte
            to_check=sorted(candidates, key=lambda g: g.get('Data',''), reverse=True)[:100]
            if len(candidates)>100:
                print(f"\n[Fase 2] {len(candidates)} gare senza provvedimento — controllo le 100 più recenti (usa --all-provv per tutte)")
            else:
                print(f"\n[Fase 2] Controllo provvedimenti: {len(to_check)} gare senza provvedimento")
        if to_check:
            provv_added=0
            for i,g in enumerate(to_check,1):
                num=g['Numero Gara']
                params={"search":"true","data_singola":"","data_da":"","data_a":"",
                        "comitato":COMITATO,"numero_gara":num,"codice_societa":"",
                        "nome_squadra":"","codice_campo":"","codice_arbitro":"","cognome_arbitro":""}
                resp=None
                for attempt in range(1,4):
                    try:
                        resp=session.get(BASE_URL,params=params,timeout=20)
                        if resp.status_code==200:break
                        elif resp.status_code==429:
                            t=int(resp.headers.get("Retry-After",20));time.sleep(t)
                        resp=None
                    except Exception as e:
                        resp=None
                    if attempt<3:time.sleep(random.uniform(1,3))
                if resp is None:
                    print(f"  [{i}/{len(to_check)}] {num} FALLITO"); continue
                rows=parse_page(resp.text)
                if rows:
                    match=[r for r in rows if r.get('Numero Gara')==num]
                    if match and match[0].get('Provvedimenti','').strip():
                        idx=key_to_idx.get(num)
                        if idx is not None:
                            existing[idx]['Provvedimenti']=match[0]['Provvedimenti']
                            provv_added+=1
                            print(f"  [{i}/{len(to_check)}] {num} ✅ provvedimento trovato")
                print(".",end="",flush=True)
                time.sleep(random.uniform(1,2))
            print(f"\n[Fase 2] ✅ Provvedimenti trovati: {provv_added} su {len(to_check)} verificate")
            if provv_added>0:
                with open(CACHE_FILE,"w",encoding="utf-8") as f:
                    json.dump(existing,f,ensure_ascii=False,indent=2)
                print(f"💾 Cache aggiornata con provvedimenti: {CACHE_FILE}")
        else:
            print(f"\n[Fase 2] Nessuna gara recente senza provvedimento da verificare.")

if __name__=="__main__":
    main()