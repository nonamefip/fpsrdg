#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fip_scraper_sarda.py  (riscritta 11/04/2026)
============================================
Scarica classifiche, risultati e prossime gare di tutti i campionati
regionali sardi da fip.it e produce fip_sarda_data.json.

Tecnica: requests + BeautifulSoup su HTML SSR (niente Selenium).
fip.it è server-side rendered: tutti i dati (classifica, partite,
select dei campionati) sono già nell'HTML restituito dal server.

Struttura HTML rilevante scoperta via ispezione diretta:
  - select[2]  → campionati (C1, D, PM, U19G/M …)
  - select[3]  → fasi (1=Qualificazione/regular, 2=Playoff QT …)
  - select[4]  → gironi (codici numerici tipo 75185)
  - div.results-ranking-full > table  → classifica completa
      colonne: "" | Squadra | Punti | G | V | P | PF | PS
  - div.results-matches__match  → singola partita
      div.team__name  → nome squadra
      div.team__points → punteggio (presente solo se risultato noto)
      div.date / div.time → data e ora

Uso:
  python fip_scraper_sarda.py

Output: fip_sarda_data.json nella cartella di lavoro corrente.

Dipendenze:
  pip install requests beautifulsoup4
"""

import json
import re
import time
import sys
import os
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("[fip_scraper_sarda] ERRORE: installa requests e beautifulsoup4")
    print("  pip install requests beautifulsoup4")
    sys.exit(1)

# ── Costanti ──────────────────────────────────────────────────────────
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'it-IT,it;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

BASE_URL = (
    "https://fip.it/risultati/"
    "?group=campionati-regionali"
    "&regione_codice=SA"
    "&comitato_codice=RSA"
)

OUTPUT_FILE = "fip_sarda_data.json"

# Pausa tra richieste HTTP (secondi) per non sovraccaricare il server
SLEEP_BETWEEN = 0.4

# Campionati da escludere (minibasket, esordienti — poco utili in classifica)
# M: ES, TAQB, TAQS, TSCB, TSCS
# F: TFE (Esordienti), TGAB, TGAS (Gazzelle), TLIB (Libellule)
ESCLUDI_CODICI = {'ES', 'TAQB', 'TAQS', 'TSCB', 'TSCS', 'TFE', 'TGAB', 'TGAS', 'TLIB'}


# ── Logging ──────────────────────────────────────────────────────────
def log(msg):
    print(f"[fip_scraper_sarda] {msg}", flush=True)


# ── HTTP ──────────────────────────────────────────────────────────────
def fetch(url, retry=2):
    """GET con retry. Ritorna BeautifulSoup o None in caso di errore."""
    for attempt in range(retry + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return BeautifulSoup(r.text, 'html.parser')
        except Exception as e:
            if attempt < retry:
                log(f"  Errore ({e}), riprovo ({attempt+1}/{retry})…")
                time.sleep(1.5)
            else:
                log(f"  FALLITO dopo {retry+1} tentativi: {e}")
                return None


# ── Lettura select ────────────────────────────────────────────────────
def get_select_options(soup, index):
    """
    Ritorna lista di (value, label) per il select all'indice dato.
    I select nella pagina sono nell'ordine:
      0 = comitato  1 = sesso  2 = campionato  3 = fase  4 = girone
    """
    selects = soup.find_all('select')
    if index >= len(selects):
        return []
    options = []
    for opt in selects[index].find_all('option'):
        val = opt.get('value', '').strip()
        txt = opt.get_text(strip=True)
        if val:
            options.append((val, txt))
    return options


def get_selected_value(soup, index):
    """Ritorna il value dell'opzione selezionata per il select dato."""
    selects = soup.find_all('select')
    if index >= len(selects):
        return None
    for opt in selects[index].find_all('option'):
        if opt.get('selected') is not None:
            return opt.get('value', '').strip()
    # Se nessuna ha selected, ritorna la prima
    opts = selects[index].find_all('option')
    return opts[0].get('value', '').strip() if opts else None


# ── Parser classifica ─────────────────────────────────────────────────
def parse_classifica(soup):
    """
    Estrae la classifica dalla div.results-ranking-full.
    Colonne attese: "" | Squadra | Punti | G | V | P | PF | PS
    """
    div = soup.find('div', class_='results-ranking-full')
    if not div:
        return []
    table = div.find('table')
    if not table:
        return []

    rows = table.find_all('tr')
    if len(rows) < 2:
        return []

    def sint(s, default=0):
        try:
            return int(re.sub(r'[^\d]', '', s))
        except Exception:
            return default

    classifica = []
    for i, tr in enumerate(rows[1:], 1):
        cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
        if len(cells) < 3:
            continue

        # celle: [pos, squadra, punti, G, V, P, PF, PS]
        pos    = sint(cells[0], i)
        squadra = cells[1] if len(cells) > 1 else ''
        punti  = sint(cells[2]) if len(cells) > 2 else 0
        pg     = sint(cells[3]) if len(cells) > 3 else 0
        vinte  = sint(cells[4]) if len(cells) > 4 else 0
        perse  = sint(cells[5]) if len(cells) > 5 else 0
        pf     = sint(cells[6]) if len(cells) > 6 else 0
        ps     = sint(cells[7]) if len(cells) > 7 else 0

        if not squadra:
            continue

        classifica.append({
            'pos':     pos,
            'squadra': squadra,
            'punti':   punti,
            'pg':      pg,
            'vinte':   vinte,
            'perse':   perse,
            'pf':      pf,
            'ps':      ps,
        })

    return classifica


# ── Parser partite ────────────────────────────────────────────────────
def parse_partite(soup):
    """
    Estrae risultati (con punteggio) e prossime gare (senza punteggio).
    Struttura div:
      div.results-matches__match
        div.teams
          div.team
            div.team__name
            div.team__points   ← assente se gara non ancora giocata
        div.results-matches__match__info
          div.date
          div.time
    """
    risultati = []
    prossime  = []

    for match in soup.find_all('div', class_='results-matches__match'):
        teams = match.find_all('div', class_='team')
        if len(teams) < 2:
            continue

        def team_txt(t, cls):
            el = t.find('div', class_=cls)
            return el.get_text(strip=True) if el else ''

        casa_nome    = team_txt(teams[0], 'team__name')
        ospite_nome  = team_txt(teams[1], 'team__name')
        casa_pts_txt = team_txt(teams[0], 'team__points')
        ospite_pts_txt = team_txt(teams[1], 'team__points')

        date_el = match.find('div', class_='date')
        time_el = match.find('div', class_='time')
        data = date_el.get_text(strip=True) if date_el else ''
        ora  = time_el.get_text(strip=True) if time_el else ''

        if not casa_nome or not ospite_nome:
            continue

        if casa_pts_txt and ospite_pts_txt:
            risultati.append({
                'data':      data,
                'ora':       ora,
                'casa':      casa_nome,
                'ospite':    ospite_nome,
                'punteggio': f"{casa_pts_txt}-{ospite_pts_txt}",
            })
        else:
            prossime.append({
                'data':   data,
                'ora':    ora,
                'casa':   casa_nome,
                'ospite': ospite_nome,
            })

    return risultati, prossime


# ── Scraper singolo campionato ─────────────────────────────────────────
def scrape_campionato(sesso, camp_code, camp_nome):
    """
    Scarica classifica e partite per un campionato.
    Strategia:
      1. Carica URL con codice_campionato → la pagina seleziona
         automaticamente la fase corrente e il girone di default.
      2. Se non c'è classifica (playoff senza standings) → ricarica
         con codice_fase=1 (regular season) che ha sempre la classifica.
      3. Per le partite usa la pagina della fase corrente.
    """
    url_base = f"{BASE_URL}&sesso={sesso}&codice_campionato={camp_code}"
    soup = fetch(url_base)
    if not soup:
        return None

    # Classifica dalla pagina di default
    classifica = parse_classifica(soup)

    # Se non c'è classifica, prova a caricare la regular season (fase 1)
    if not classifica:
        url_fase1 = f"{url_base}&codice_fase=1"
        soup_f1 = fetch(url_fase1)
        if soup_f1:
            classifica = parse_classifica(soup_f1)
        time.sleep(SLEEP_BETWEEN)

    # Partite dalla pagina di default (fase corrente)
    risultati, prossime = parse_partite(soup)

    # Se la pagina default non ha partite, prova gironi multipli
    if not risultati and not prossime:
        girone_opts = get_select_options(soup, 4)
        for gcode, gnome in girone_opts[:4]:  # max 4 gironi
            url_g = f"{url_base}&codice_girone={gcode}"
            soup_g = fetch(url_g)
            if soup_g:
                r2, p2 = parse_partite(soup_g)
                risultati.extend(r2)
                prossime.extend(p2)
            time.sleep(SLEEP_BETWEEN)

    if not classifica and not risultati and not prossime:
        log(f"    Nessun dato per {camp_nome} — saltato")
        return None

    return {
        'nome':       camp_nome,
        'codice':     camp_code,
        'sesso':      sesso,
        'classifica': classifica,
        'risultati':  risultati[-20:],   # ultimi 20 risultati
        'prossime':   prossime[:15],     # max 15 prossime gare
    }


# ── Scraper per sesso ──────────────────────────────────────────────────
def scrape_sesso(sesso):
    """
    Scarica tutti i campionati per un sesso (M o F).
    Legge la lista dei campionati dal select nell'HTML SSR della pagina base.
    """
    label = 'Maschili' if sesso == 'M' else 'Femminili'
    url0  = f"{BASE_URL}&sesso={sesso}"

    log(f"\n--- Campionati {label}: lettura lista campionati ---")
    soup0 = fetch(url0)
    if not soup0:
        log(f"  ERRORE: impossibile caricare la pagina base per {label}")
        return []

    # select[2] = lista campionati (SSR: tutte le opzioni sono nell'HTML)
    camp_opts = get_select_options(soup0, 2)
    if not camp_opts:
        log(f"  ERRORE: nessun campionato trovato nel select per {label}")
        return []

    log(f"  Trovati {len(camp_opts)} campionati: {[c[0] for c in camp_opts]}")

    campionati = []
    for camp_code, camp_nome in camp_opts:
        if camp_code in ESCLUDI_CODICI:
            log(f"  Salto {camp_nome} ({camp_code}) — minibasket/esordienti")
            continue

        log(f"  → Scraping: {camp_nome} ({camp_code})")
        dati = scrape_campionato(sesso, camp_code, camp_nome)
        if dati:
            n_class = len(dati['classifica'])
            n_ris   = len(dati['risultati'])
            n_pros  = len(dati['prossime'])
            log(f"     ✓ classifica:{n_class} squadre  risultati:{n_ris}  prossime:{n_pros}")
            campionati.append(dati)
        time.sleep(SLEEP_BETWEEN)

    return campionati


# ── Main ───────────────────────────────────────────────────────────────
def main():
    log("=== FIP Scraper Sarda (v2) — avvio ===")
    log(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log(f"Output: {OUTPUT_FILE}")

    all_data = {
        'aggiornato':  datetime.now().strftime('%Y-%m-%d %H:%M'),
        'campionati':  [],
    }

    for sesso in ('M', 'F'):
        campionati = scrape_sesso(sesso)
        all_data['campionati'].extend(campionati)

    # Salva JSON
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    total = len(all_data['campionati'])
    masch = sum(1 for c in all_data['campionati'] if c['sesso'] == 'M')
    femm  = sum(1 for c in all_data['campionati'] if c['sesso'] == 'F')
    log(f"\n=== Completato: {total} campionati ({masch} M + {femm} F) → {out_path} ===")
    return all_data


if __name__ == '__main__':
    main()
