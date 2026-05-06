#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fip_scraper_sarda.py  (v3 — 12/04/2026)
========================================
Scarica classifiche e TUTTE le giornate di tutti i campionati
regionali sardi da fip.it -> produce fip_sarda_data.json.

Struttura output compatibile con il template dashboard:
  {
    "aggiornato": "2026-04-12 10:00",
    "campionati": [
      {
        "nome": "Serie C", "codice": "C1", "sesso": "M",
        "gironi": [
          {
            "nome": "Girone Unico",
            "classifica": [{"pos":1,"squadra":"...","punti":34,"pg":20,"vinte":17,"perse":3,"pf":1638,"ps":1488}],
            "risultati":  [{"giornata":1,"data":"...","ora":"...","casa":"...","ospite":"...","punteggio":"89-69"}],
            "prossime":   [{"giornata":21,"data":"...","ora":"...","casa":"...","ospite":"..."}]
          }
        ]
      }
    ]
  }

Uso:  python fip_scraper_sarda.py
Deps: pip install requests beautifulsoup4
"""

import json, re, time, sys, os
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERRORE: pip install requests beautifulsoup4")
    sys.exit(1)

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept-Language': 'it-IT,it;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

BASE_URL = ("https://fip.it/risultati/?group=campionati-regionali"
            "&regione_codice=SA&comitato_codice=RSA")

OUTPUT_FILE = "fip_sarda_data.json"
SLEEP = 0.35

ESCLUDI = {'ES','TAQB','TAQS','TSCB','TSCS','TFE','TGAB','TGAS','TLIB'}


def log(msg): print(f"[fip] {msg}", flush=True)


def fetch(url, retry=2):
    for attempt in range(retry + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return BeautifulSoup(r.text, 'html.parser')
        except Exception as e:
            if attempt < retry:
                time.sleep(1.5)
            else:
                log(f"  FAIL: {url[-80:]} ({e})")
                return None


def build_url(**kw):
    url = BASE_URL
    for k, v in kw.items():
        if v is not None and v != '':
            url += f"&{k}={v}"
    return url


def select_opts(soup, idx):
    sels = soup.find_all('select')
    if idx >= len(sels):
        return []
    return [(o.get('value','').strip(), o.get_text(strip=True))
            for o in sels[idx].find_all('option')
            if o.get('value','').strip()]


def parse_classifica(soup):
    div = soup.find('div', class_='results-ranking-full')
    if not div:
        return []
    tbl = div.find('table')
    if not tbl:
        return []

    def sint(s, d=0):
        try: return int(re.sub(r'[^\d]', '', s or ''))
        except: return d

    rows = []
    for i, tr in enumerate(tbl.find_all('tr')[1:], 1):
        cells = [td.get_text(strip=True) for td in tr.find_all(['td','th'])]
        if len(cells) < 3 or not cells[1]:
            continue
        rows.append({
            'pos':     sint(cells[0], i),
            'squadra': cells[1],
            'punti':   sint(cells[2]) if len(cells)>2 else 0,
            'pg':      sint(cells[3]) if len(cells)>3 else 0,
            'vinte':   sint(cells[4]) if len(cells)>4 else 0,
            'perse':   sint(cells[5]) if len(cells)>5 else 0,
            'pf':      sint(cells[6]) if len(cells)>6 else 0,
            'ps':      sint(cells[7]) if len(cells)>7 else 0,
        })
    return rows


def parse_partite(soup, giornata=None):
    risultati, prossime = [], []
    for m in soup.find_all('div', class_='results-matches__match'):
        teams = m.find_all('div', class_='team')
        if len(teams) < 2:
            continue

        def txt(t, cls):
            el = t.find('div', class_=cls)
            return el.get_text(strip=True) if el else ''

        casa   = txt(teams[0], 'team__name')
        ospite = txt(teams[1], 'team__name')
        pt_c   = txt(teams[0], 'team__points')
        pt_o   = txt(teams[1], 'team__points')
        de     = m.find('div', class_='date')
        te     = m.find('div', class_='time')
        data   = de.get_text(strip=True) if de else ''
        ora    = te.get_text(strip=True) if te else ''

        if not casa or not ospite:
            continue

        entry = {'casa': casa, 'ospite': ospite, 'data': data, 'ora': ora}
        if giornata is not None:
            entry['giornata'] = giornata

        if pt_c and pt_o:
            entry['punteggio'] = f"{pt_c}-{pt_o}"
            risultati.append(entry)
        else:
            prossime.append(entry)

    return risultati, prossime


def scrape_girone(sesso, camp_code, fase_code, girone_code, girone_nome, classifica):
    """Scarica TUTTE le partite di un girone iterando le giornate."""
    n_sq = len(classifica)
    # Round robin: 2*(N-1) giornate; aggiunge un po' di margine
    max_g = max((n_sq - 1) * 2 + 4, 34) if n_sq > 1 else 34

    base_kw = dict(sesso=sesso, codice_campionato=camp_code,
                   codice_fase=fase_code or None,
                   codice_girone=girone_code or None)

    tutti_ris, tutte_pro = [], []
    vuote = 0

    for g in range(1, max_g + 1):
        soup = fetch(build_url(**base_kw, giornata=g))
        time.sleep(SLEEP)
        if not soup:
            vuote += 1
            if vuote >= 3: break
            continue

        ris, pro = parse_partite(soup, giornata=g)
        if not ris and not pro:
            vuote += 1
            if vuote >= 3: break
            continue

        vuote = 0
        tutti_ris.extend(ris)
        tutte_pro.extend(pro)

    log(f"       '{girone_nome}': ris={len(tutti_ris)} pros={len(tutte_pro)}")
    return {
        'nome':       girone_nome,
        'classifica': classifica,
        'risultati':  tutti_ris,
        'prossime':   tutte_pro,
    }


def scrape_campionato(sesso, camp_code, camp_nome):
    log(f"  -> {camp_nome} ({camp_code})")

    soup_base = fetch(build_url(sesso=sesso, codice_campionato=camp_code))
    if not soup_base:
        return None

    # Tutte le fasi disponibili
    fase_opts = select_opts(soup_base, 3)
    if not fase_opts:
        fase_opts = [('', '')]

    log(f"     fasi: {[f[0] for f in fase_opts]}")

    gironi_out = []
    cl_globale = []   # prima classifica valida trovata (fallback)
    multi_fasi = len(fase_opts) > 1

    # ── Itera TUTTE le fasi ────────────────────────────────────
    for fase_code, fase_nome in fase_opts:
        if fase_code:
            soup_fase = fetch(build_url(sesso=sesso, codice_campionato=camp_code,
                                        codice_fase=fase_code))
            time.sleep(SLEEP)
        else:
            soup_fase = soup_base

        if not soup_fase:
            continue

        cl_fase = parse_classifica(soup_fase)
        if not cl_globale and cl_fase:
            cl_globale = cl_fase

        # Gironi di questa fase
        girone_opts = select_opts(soup_fase, 4)
        if not girone_opts:
            girone_opts = [('', 'Girone Unico')]

        log(f"     fase={fase_code!r} '{fase_nome}' gironi:{[g[0] for g in girone_opts]} sq:{len(cl_fase)}")

        for girone_code, girone_nome in girone_opts:
            # Classifica specifica per questo girone
            if girone_code and len(girone_opts) > 1:
                sg = fetch(build_url(sesso=sesso, codice_campionato=camp_code,
                                     codice_fase=fase_code or None,
                                     codice_girone=girone_code))
                time.sleep(SLEEP)
                cl_g = parse_classifica(sg) if sg else cl_fase
                if not cl_g:
                    cl_g = cl_fase
            else:
                cl_g = cl_fase
            if not cl_g:
                cl_g = cl_globale

            # Nome del girone: prefissa con fase se ci sono più fasi
            if multi_fasi and fase_nome and fase_nome.strip():
                fn = fase_nome.strip()
                full_nome = f"{fn} – {girone_nome}" if girone_nome and girone_nome != 'Girone Unico' else fn
            else:
                full_nome = girone_nome

            g_data = scrape_girone(sesso, camp_code, fase_code, girone_code, full_nome, cl_g)

            # Includi il girone se ha dati (risultati, prossime o classifica)
            if g_data['risultati'] or g_data['prossime'] or g_data['classifica']:
                gironi_out.append(g_data)

    if not gironi_out:
        log(f"     Nessun dato — saltato")
        return None

    return {'nome': camp_nome, 'codice': camp_code, 'sesso': sesso, 'gironi': gironi_out}


def scrape_sesso(sesso):
    label = 'Maschili' if sesso == 'M' else 'Femminili'
    log(f"\n=== {label} ===")
    soup = fetch(build_url(sesso=sesso))
    if not soup:
        return []

    opts = select_opts(soup, 2)
    log(f"  Trovati: {[c[0] for c in opts]}")

    out = []
    for code, nome in opts:
        if code in ESCLUDI:
            log(f"  Salto {code}")
            continue
        d = scrape_campionato(sesso, code, nome)
        if d:
            out.append(d)
        time.sleep(SLEEP)
    return out


def main():
    t0 = time.time()
    log("=== FIP Scraper Sarda v4 (tutte le fasi) ===")

    data = {
        'aggiornato': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'campionati': [],
    }
    for sesso in ('M', 'F'):
        data['campionati'].extend(scrape_sesso(sesso))

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    tot = len(data['campionati'])
    nm  = sum(1 for c in data['campionati'] if c['sesso']=='M')
    nf  = sum(1 for c in data['campionati'] if c['sesso']=='F')
    log(f"\n=== FINE: {tot} camp ({nm}M+{nf}F) in {int(time.time()-t0)}s -> {out} ===")


if __name__ == '__main__':
    main()
