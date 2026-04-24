#!/usr/bin/env python3
"""
FIP Sardegna Scraper v6 – Doppia fase: lista per data + dettaglio per numero gara
Garantisce di non perdere nessun provvedimento anche se aggiunto retroattivamente.

Fase 1: scarica per data → raccoglie tutti i numeri gara
Fase 2: per ogni gara senza provvedimento (o in refresh) → ri-scarica per numero_gara

Uso:
  python fip_scraper.py                    # aggiornamento giornaliero normale
  python fip_scraper.py --full-refresh     # riscarica tutto dall'inizio
  python fip_scraper.py --refresh-days 30  # riscarica gli ultimi 30 giorni
  python fip_scraper.py --reprovv          # ri-scarica SOLO le gare senza provvedimento
                                            # (utile dopo un full-refresh vecchio)
"""
import requests, json, os, re, sys, time, random, argparse
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date

BASE_URL   = "https://fip.it/risultati/"
COMITATO   = "RSA"
DATE_START = datetime(2025, 9, 1)
CACHE_FILE = "cache/fip_sardegna_cache.json"
MAX_RETRIES = 5

HEADERS_POOL = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"},
]

MESI_IT = {
    "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
    "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
    "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12",
}


# ─────────────────────────────────────────────
# PARSING
# ─────────────────────────────────────────────

def clean(t):
    return " ".join(t.split()) if t else ""

def parse_date_it(raw):
    parts = raw.lower().split()
    if len(parts) == 3:
        return f"{parts[2]}-{MESI_IT.get(parts[1], '00')}-{int(parts[0]):02d}"
    return raw

def parse_ref(ref_text):
    flat = " ".join(ref_text.split())
    tokens = flat.split()
    num_gara = tokens[0] if tokens else ""
    campionato = girone = fase = ""
    m = re.search(r"-\s*COMITATO REGIONALE SARDEGNA\s+(.*)", flat, re.IGNORECASE)
    if m:
        rest = m.group(1)
        gm = re.search(r"Girone:\s*(.+?)(?:,\s*Fase:|$)", rest)
        fm = re.search(r"Fase:\s*(.+)", rest)
        cm = re.match(r"(.+?)\s*(?:[MF],|Girone:|$)", rest)
        campionato = clean(cm.group(1)) if cm else ""
        girone     = clean(gm.group(1)) if gm else ""
        fase       = clean(fm.group(1)) if fm else ""
    return num_gara, campionato, girone, fase

def get_info(match_div, label_text):
    for info in match_div.find_all("div", class_="info"):
        lbl = info.find("div", class_="label")
        val = info.find(class_="value")
        if lbl and val and label_text.lower() in lbl.get_text().lower():
            v = clean(val.get_text())
            if v.lower() in ("", "designazione in attesa di conferma.", "n/d"):
                return ""
            return v
    return ""

def parse_match(m):
    teams   = m.find_all("div", class_="team")
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
        "Stato Gara": "", "Campo": get_info(m, "campo di gioco"),
        "Arbitro 1": get_info(m, "1° arbitro"), "Arbitro 2": get_info(m, "2° arbitro"),
        "Arbitro 3": get_info(m, "3° arbitro"),
        "Segnapunti": get_info(m, "segnapunti"), "Cronometrista": get_info(m, "cronometrista"),
        "24 Secondi": get_info(m, "24 secondi"), "Addetto Referto": get_info(m, "addetto referto"),
        "Osservatore": get_info(m, "osservatore"), "Provvedimenti": get_info(m, "provvedimenti"),
    }

def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    if "numero eccessivo" in soup.get_text().lower():
        return None
    return [parse_match(m) for m in soup.find_all("div", class_="results-matches__match")]


# ─────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────

def fetch_by_date(session, da):
    """Scarica tutte le gare di una data."""
    params = {
        "search": "true", "data_singola": da, "data_da": "", "data_a": "",
        "comitato": COMITATO, "numero_gara": "", "codice_societa": "",
        "nome_squadra": "", "codice_campo": "", "codice_arbitro": "", "cognome_arbitro": "",
    }
    return _fetch(session, params)

def fetch_by_numero(session, numero_gara):
    """Scarica il dettaglio di una singola gara per numero."""
    params = {
        "search": "true", "data_singola": "", "data_da": "", "data_a": "",
        "comitato": COMITATO, "numero_gara": numero_gara, "codice_societa": "",
        "nome_squadra": "", "codice_campo": "", "codice_arbitro": "", "cognome_arbitro": "",
    }
    return _fetch(session, params)

def _fetch(session, params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=20)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                t = int(resp.headers.get("Retry-After", 20))
                print(f"\n[429] attendo {t}s")
                time.sleep(t)
            else:
                print(f"\n[HTTP {resp.status_code}] tentativo {attempt}")
        except Exception as e:
            print(f"\n[ERR] {e} tentativo {attempt}")
        if attempt < MAX_RETRIES:
            time.sleep(random.uniform(2, 5) * attempt)
    return None


# ─────────────────────────────────────────────
# FASE 1 – scarica per data
# ─────────────────────────────────────────────

def fase1(session, args, existing, key_to_idx):
    yesterday  = (date.today() - timedelta(days=1)).isoformat()
    future_end = (date.today() + timedelta(days=args.future_days)).isoformat()

    scraped_days = set(g["Data"] for g in existing)

    refresh_cutoff   = (date.today() - timedelta(days=args.refresh_days)).isoformat()
    refresh_days_set = set()
    if not args.full_refresh and args.refresh_days > 0:
        for ds in scraped_days:
            if ds >= refresh_cutoff:
                refresh_days_set.add(ds)

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
        print("✅ [Fase 1] Cache già aggiornata — nessun nuovo giorno da scaricare.")
        return 0, 0

    print(f"\n[Fase 1] Giorni da scaricare: {len(days_to_fetch)} ({days_to_fetch[0]} → {days_to_fetch[-1]})")

    added = 0; updated = 0; empty = 0

    for i, da in enumerate(days_to_fetch, 1):
        resp = fetch_by_date(session, da)
        if resp is None:
            print(f"\n[{i}/{len(days_to_fetch)}] {da} FALLITO")
            empty = 0; continue
        rows = parse_page(resp.text)
        if rows is None:
            print(f"\n[{i}/{len(days_to_fetch)}] {da} ⚠️  troppi risultati")
            empty = 0; continue

        nuovi = 0; aggiornati = 0
        for r in rows:
            key = r.get("Numero Gara") or (r["Data"] + r["Squadra Casa"] + r["Squadra Ospite"])
            if not key:
                continue
            if key in key_to_idx:
                if da in refresh_days_set or args.full_refresh:
                    # Preserva campi già valorizzati che la lista-per-data non restituisce
                    old_rec = existing[key_to_idx[key]]
                    for pf in ["Provvedimenti", "Arbitro 1", "Arbitro 2", "Arbitro 3",
                               "Segnapunti", "Cronometrista", "24 Secondi",
                               "Addetto Referto", "Osservatore"]:
                        if not (r.get(pf) or "").strip() and (old_rec.get(pf) or "").strip():
                            r[pf] = old_rec[pf]
                    existing[key_to_idx[key]] = r
                    aggiornati += 1; updated += 1
            else:
                key_to_idx[key] = len(existing)
                existing.append(r)
                nuovi += 1; added += 1

        if nuovi > 0 or aggiornati > 0:
            if empty > 0:
                print()
            tag = f"{nuovi} nuove" + (f", {aggiornati} agg." if aggiornati else "")
            print(f"[{i}/{len(days_to_fetch)}] {da} → {tag} (tot: {len(existing)})")
            empty = 0
        else:
            print(".", end="", flush=True)
            empty += 1

        time.sleep(random.uniform(0.8, 1.8))

    if empty > 0:
        print()
    print(f"\n[Fase 1] ✅ Aggiunte {added} nuove | Aggiornate {updated} | Totale: {len(existing)}")
    return added, updated


# ─────────────────────────────────────────────
# FASE 2 – ri-scarica per numero_gara
# ─────────────────────────────────────────────

def fase2(session, args, existing, key_to_idx):
    """
    Per ogni gara già in cache che NON ha provvedimenti valorizzati,
    ri-scarica la pagina dettaglio con numero_gara e aggiorna il record.

    In modalità --full-refresh o --reprovv: processa TUTTE le gare passate.
    Altrimenti: solo quelle degli ultimi refresh_days giorni.
    """
    today_str = date.today().isoformat()

    # Determina quali gare processare
    if args.full_refresh or args.reprovv:
        # Tutte le gare (passate E future) con provvedimenti vuoti.
        # Le future vengono incluse perché FIP potrebbe già avere arbitri/campo assegnati.
        candidates = [
            i for i, g in enumerate(existing)
            if not (g.get("Provvedimenti") or "").strip()
        ]
        print(f"\n[Fase 2] Modalità {'full-refresh' if args.full_refresh else 'reprovv'}: "
              f"{len(candidates)} gare senza provvedimenti su {len(existing)} totali")
    else:
        # Gare recenti (ultimi refresh_days giorni) senza provvedimenti, passate O future.
        # Le future senza risultato vengono comunque ri-scaricate: FIP può aggiungere
        # designazioni arbitrali o provvedimenti anche su gare non ancora disputate.
        cutoff = (date.today() - timedelta(days=args.refresh_days)).isoformat()
        future_end = (date.today() + timedelta(days=args.future_days)).isoformat()
        candidates = [
            i for i, g in enumerate(existing)
            if cutoff <= g.get("Data", "") <= future_end
            and not (g.get("Provvedimenti") or "").strip()
        ]
        print(f"\n[Fase 2] Controllo provvedimenti gare recenti ({args.refresh_days}gg): "
              f"{len(candidates)} gare da verificare")

    if not candidates:
        print("[Fase 2] ✅ Nessuna gara da verificare.")
        return 0

    aggiornati = 0
    trovati    = 0
    empty      = 0

    for i, idx in enumerate(candidates, 1):
        g      = existing[idx]
        num    = g.get("Numero Gara", "")
        if not num:
            print(f"  [{i}/{len(candidates)}] SKIP (nessun numero gara)")
            continue

        resp = fetch_by_numero(session, num)
        if resp is None:
            print(f"\n  [{i}/{len(candidates)}] {num} FALLITO")
            empty = 0; continue

        rows = parse_page(resp.text)
        if not rows:
            print(f"  [{i}/{len(candidates)}] {num} → nessun risultato HTML")
            empty = 0; continue

        # La risposta dovrebbe contenere solo quella gara
        match = next((r for r in rows if r.get("Numero Gara") == num), rows[0] if rows else None)
        if not match:
            continue

        provv = (match.get("Provvedimenti") or "").strip()
        aggiornati += 1

        # Aggiorna sempre i campi che potrebbero essere stati integrati da FIP
        for field in ["Provvedimenti", "Arbitro 1", "Arbitro 2", "Arbitro 3",
                      "Segnapunti", "Cronometrista", "24 Secondi",
                      "Addetto Referto", "Osservatore", "Campo"]:
            nuovo = (match.get(field) or "").strip()
            if nuovo:
                existing[idx][field] = nuovo

        if provv:
            trovati += 1
            if empty > 0:
                print()
            print(f"  [{i}/{len(candidates)}] {num} ({g.get('Data','')}) ✅ PROVV: {provv[:80]}")
            empty = 0
        else:
            print("·", end="", flush=True)
            empty += 1

        # Pausa più breve in fase 2 (richieste mirate, non bulk)
        time.sleep(random.uniform(0.5, 1.2))

    if empty > 0:
        print()

    print(f"\n[Fase 2] ✅ Verificate {aggiornati} gare | Provvedimenti trovati: {trovati}")
    return trovati


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FIP Sardegna Scraper v6 – doppia fase")
    parser.add_argument("--refresh-days", type=int, default=7,
        help="Ri-scarica anche gli ultimi N giorni già in cache (default: 7)")
    parser.add_argument("--from-date", type=str, default=None,
        help="Scarica da questa data (YYYY-MM-DD). Override DATE_START.")
    parser.add_argument("--full-refresh", action="store_true",
        help="Ri-scarica TUTTO dal DATE_START e ri-verifica tutti i provvedimenti")
    parser.add_argument("--future-days", type=int, default=14,
        help="Scarica anche i prossimi N giorni (default: 14)")
    parser.add_argument("--reprovv", action="store_true",
        help="Salta la Fase 1, ri-scarica solo le gare senza provvedimenti (veloce)")
    parser.add_argument("--skip-fase2", action="store_true",
        help="Esegue solo la Fase 1 (come lo scraper vecchio)")
    args = parser.parse_args()

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    print(f"=== FIP Sardegna v6 – {yesterday} ===")
    if args.full_refresh:
        print("    Modalità: FULL REFRESH (fase 1 + fase 2 su tutte le gare)")
    elif args.reprovv:
        print("    Modalità: REPROVV (solo fase 2 – ri-scarica gare senza provvedimenti)")
    else:
        print(f"    Modalità: aggiornamento giornaliero (refresh ultimi {args.refresh_days}gg)")

    # Carica cache esistente
    existing = []
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            existing = json.load(f)
        print(f"Cache esistente: {len(existing)} gare")
    else:
        os.makedirs("cache", exist_ok=True)

    # Indice key → posizione in lista
    key_to_idx = {}
    for idx, g in enumerate(existing):
        key = g.get("Numero Gara") or (g["Data"] + g["Squadra Casa"] + g["Squadra Ospite"])
        key_to_idx[key] = idx

    session = requests.Session()
    session.headers.update(random.choice(HEADERS_POOL))

    # ── FASE 1 ──────────────────────────────────
    if not args.reprovv:
        fase1(session, args, existing, key_to_idx)
        # Salva intermedio (in caso la fase 2 venga interrotta)
        _save(existing)

    # ── FASE 2 ──────────────────────────────────
    if not args.skip_fase2:
        fase2(session, args, existing, key_to_idx)
    else:
        print("\n[Fase 2] Saltata (--skip-fase2).")

    # Salva finale
    _save(existing)
    print(f"\n✅ Completato. Cache: {len(existing)} gare totali.")
    print("▶ Per aggiornare il dashboard: python scripts/build.py")


def _save(existing):
    os.makedirs("cache", exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    size = os.path.getsize(CACHE_FILE)
    print(f"💾 Cache salvata: {CACHE_FILE} ({size // 1024} KB)")


if __name__ == "__main__":
    main()
