#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fip_scraper_sarda.py  (v5 — 02/05/2026)
========================================
Scarica classifiche e TUTTE le giornate di tutti i campionati
regionali sardi da fip.it -> produce fip_sarda_data.json.

NOVITÀ v5:
  - Estrae il numero gara (gara_id) per ogni partita (risultati e prossime)
  - Modalità --aggiorna: riscarica solo i gironi che hanno prossime,
    confronta data/ora con il JSON esistente e aggiorna le gare spostate
  - Rileva nuove fasi/gironi (es. Finale appena pubblicata) anche su
    campionati che prima non avevano prossime
  - Log migliorato: mostra gare diventate risultato, date cambiate,
    gare nuove e gare rimosse

Struttura output:
  {
    "aggiornato": "2026-05-02 10:00",
    "campionati": [
      {
        "nome": "Serie C", "codice": "C1", "sesso": "M",
        "gironi": [
          {
            "nome": "Girone Unico",
            "classifica": [...],
            "risultati":  [{"gara_id":"001234","giornata":1,"data":"...","ora":"...",
                            "casa":"...","ospite":"...","punteggio":"89-69"}],
            "prossime":   [{"gara_id":"001235","giornata":21,"data":"...","ora":"...",
                            "casa":"...","ospite":"..."}]
          }
        ]
      }
    ]
  }

Uso:
  python fip_scraper_sarda.py              # scaricamento completo
  python fip_scraper_sarda.py --aggiorna   # aggiorna solo gironi con prossime

Deps: pip install requests beautifulsoup4
"""

import json, re, time, sys, os, argparse
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


def estrai_gara_id(match_div):
    """Estrae il numero gara (es. '005297') dal div di una partita.

    FIP mostra il numero gara in vari posti a seconda della pagina:
      1. Attributo data-* sul div principale
      2. Href di un <a> con pattern numero_gara=XXXXXX o /gara/XXXXXX/
      3. Testo di un <div> con classe contenente 'number' o 'numero'
      4. Testo di un <a> con 6 cifre consecutive
      5. Fallback: pattern 0XXXXX nel testo grezzo del div
    """
    # 1. Attributi data- sul div
    for attr in ('data-match-id', 'data-game-id', 'data-id', 'data-numero'):
        val = match_div.get(attr, '').strip()
        if re.match(r'^\d{5,6}$', val):
            return val.zfill(6)

    # 2. Link con numero_gara=XXXXXX o /gara/XXXXXX/ nell'href
    for a in match_div.find_all('a', href=True):
        m = re.search(r'numero_gara[=_](\d{5,6})', a['href'])
        if m:
            return m.group(1).zfill(6)
        m = re.search(r'/gara/(\d{5,6})/', a['href'])
        if m:
            return m.group(1).zfill(6)

    # 3. Div con classe che contiene 'number' o 'numero'
    for cls in ('match-number', 'game-number', 'numero-gara', 'match__number'):
        el = match_div.find(class_=re.compile(cls, re.I))
        if el:
            val = re.sub(r'[^\d]', '', el.get_text())
            if len(val) in (5, 6):
                return val.zfill(6)

    # 4. Testo di un <a> con esattamente 6 cifre
    for a in match_div.find_all('a'):
        val = re.sub(r'[^\d]', '', a.get_text())
        if re.match(r'^\d{6}$', val):
            return val

    # 5. Fallback: pattern 0XXXXX nel testo grezzo (es. "001569")
    raw = match_div.get_text()
    m = re.search(r'\b0\d{5}\b', raw)
    if m:
        return m.group(0)

    return ''


def parse_partite(soup, giornata=None):
    """Parsa le partite da una pagina FIP.

    Ogni entry contiene: gara_id, casa, ospite, data, ora, giornata,
    e punteggio (solo per i risultati).
    """
    risultati, prossime = [], []
    for m in soup.find_all('div', class_='results-matches__match'):
        teams = m.find_all('div', class_='team')
        if len(teams) < 2:
            continue

        def txt(t, cls):
            el = t.find('div', class_=cls)
            return el.get_text(strip=True) if el else ''

        casa    = txt(teams[0], 'team__name')
        ospite  = txt(teams[1], 'team__name')
        pt_c    = txt(teams[0], 'team__points')
        pt_o    = txt(teams[1], 'team__points')
        de      = m.find('div', class_='date')
        te      = m.find('div', class_='time')
        data    = de.get_text(strip=True) if de else ''
        ora     = te.get_text(strip=True) if te else ''
        gara_id = estrai_gara_id(m)

        if not casa or not ospite:
            continue

        entry = {
            'gara_id': gara_id,
            'casa':    casa,
            'ospite':  ospite,
            'data':    data,
            'ora':     ora,
        }
        if giornata is not None:
            entry['giornata'] = giornata

        if pt_c and pt_o:
            entry['punteggio'] = f"{pt_c}-{pt_o}"
            risultati.append(entry)
        else:
            prossime.append(entry)

    return risultati, prossime


def scrape_girone(sesso, camp_code, fase_code, girone_code, girone_nome, classifica):
    """Scarica TUTTE le partite di un girone con numerazione sequenziale."""
    n_sq    = len(classifica)
    leg_len = max(n_sq - 1, 1) if n_sq > 1 else 18
    max_g   = leg_len * 2 + 4

    base_kw = dict(sesso=sesso, codice_campionato=camp_code,
                   codice_fase=fase_code or None,
                   codice_girone=girone_code or None)

    ris, pro = [], []
    vuote = 0
    for g in range(1, max_g + 1):
        soup = fetch(build_url(**base_kw, giornata=g))
        time.sleep(SLEEP)
        if not soup:
            vuote += 1
            if vuote >= 3: break
            continue
        r, p = parse_partite(soup, giornata=g)
        if not r and not p:
            vuote += 1
            if vuote >= 3: break
            continue
        vuote = 0
        ris.extend(r)
        pro.extend(p)

    log(f"       '{girone_nome}': {len(ris)}r+{len(pro)}p  (legLen={leg_len}, maxG={max_g})")
    return {
        'nome':       girone_nome,
        'classifica': classifica,
        'risultati':  ris,
        'prossime':   pro,
    }


def scrape_campionato(sesso, camp_code, camp_nome):
    log(f"  -> {camp_nome} ({camp_code})")

    soup_base = fetch(build_url(sesso=sesso, codice_campionato=camp_code))
    if not soup_base:
        return None

    fase_opts = select_opts(soup_base, 3)
    if not fase_opts:
        fase_opts = [('', '')]

    log(f"     fasi: {[f[0] for f in fase_opts]}")

    gironi_out = []
    cl_globale = []
    multi_fasi = len(fase_opts) > 1

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

        girone_opts = select_opts(soup_fase, 4)
        if not girone_opts:
            girone_opts = [('', 'Girone Unico')]

        log(f"     fase={fase_code!r} '{fase_nome}' gironi:{[g[0] for g in girone_opts]} sq:{len(cl_fase)}")

        for girone_code, girone_nome in girone_opts:
            if girone_code and len(girone_opts) > 1:
                sg   = fetch(build_url(sesso=sesso, codice_campionato=camp_code,
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

            if multi_fasi and fase_nome and fase_nome.strip():
                fn        = fase_nome.strip()
                full_nome = f"{fn} – {girone_nome}" if girone_nome and girone_nome != 'Girone Unico' else fn
            else:
                full_nome = girone_nome

            g_data = scrape_girone(sesso, camp_code, fase_code, girone_code, full_nome, cl_g)

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


# ═══════════════════════════════════════════════════════════════════
# MODALITÀ --aggiorna
# ═══════════════════════════════════════════════════════════════════

def _chiave_gara(g):
    """Chiave univoca per identificare una gara nel confronto.

    Preferisce gara_id (affidabile, non cambia mai).
    Fallback: casa + ospite + giornata (meno affidabile, usato solo
    quando gara_id è assente — es. vecchi JSON senza estrazione id).
    """
    gid = g.get('gara_id', '').strip()
    if gid:
        return ('id', gid)
    return ('match', g.get('casa',''), g.get('ospite',''), str(g.get('giornata','')))


def aggiorna_girone_esistente(girone_vec, sesso, camp_code, fase_code, girone_code, classifica_nuova):
    """Riscarica un girone e confronta con i dati esistenti.

    Logica di aggiornamento per ogni prossima nel JSON salvato:
      - Gara ora RISULTATO  -> spostata in risultati (era designata, ora giocata)
      - Gara PROSSIMA con data/ora cambiata -> aggiornata (rinvio o anticipo)
      - Gara non trovata con gara_id noto -> rimossa (annullata)
      - Gara non trovata senza gara_id -> mantenuta per sicurezza
    Nuove gare non presenti prima -> aggiunte.
    """
    girone_nome = girone_vec['nome']
    n_sq        = len(classifica_nuova) if classifica_nuova else len(girone_vec['classifica'])
    leg_len     = max(n_sq - 1, 1) if n_sq > 1 else 18
    max_g       = leg_len * 2 + 4

    base_kw = dict(sesso=sesso, codice_campionato=camp_code,
                   codice_fase=fase_code or None,
                   codice_girone=girone_code or None)

    # ── Scarica tutte le giornate ──
    ris_new, pro_new = [], []
    vuote = 0
    for g in range(1, max_g + 1):
        soup = fetch(build_url(**base_kw, giornata=g))
        time.sleep(SLEEP)
        if not soup:
            vuote += 1
            if vuote >= 3: break
            continue
        r, p = parse_partite(soup, giornata=g)
        if not r and not p:
            vuote += 1
            if vuote >= 3: break
            continue
        vuote = 0
        ris_new.extend(r)
        pro_new.extend(p)

    # Indici per ricerca rapida
    ris_new_idx = {_chiave_gara(g): g for g in ris_new}
    pro_new_idx = {_chiave_gara(g): g for g in pro_new}

    n_diventate_ris = 0
    n_data_cambiata = 0
    n_rimosse       = 0
    n_nuove         = 0

    # ── Aggiorna i risultati già salvati con dati freschi ──
    chiavi_ris_vec = {_chiave_gara(r) for r in girone_vec['risultati']}
    ris_out = []
    for r in girone_vec['risultati']:
        chiave = _chiave_gara(r)
        ris_out.append(ris_new_idx.get(chiave, r))   # aggiorna se disponibile

    # Aggiungi nuovi risultati non presenti prima
    for r in ris_new:
        chiave = _chiave_gara(r)
        if chiave not in chiavi_ris_vec:
            ris_out.append(r)
            n_nuove += 1

    # ── Aggiorna le prossime ──
    pro_out = []
    for p in girone_vec['prossime']:
        chiave = _chiave_gara(p)

        if chiave in ris_new_idx:
            # Era prossima, ora è risultato
            ris_out.append(ris_new_idx[chiave])
            n_diventate_ris += 1
            punteggio = ris_new_idx[chiave].get('punteggio', '?')
            log(f"         ✅ {p['casa']} vs {p['ospite']} -> {punteggio}")

        elif chiave in pro_new_idx:
            # Ancora prossima: controlla data/ora
            p_new = pro_new_idx[chiave]
            if p_new['data'] != p['data'] or p_new['ora'] != p['ora']:
                log(f"         📅 {p['casa']} vs {p['ospite']}: "
                    f"{p['data']} {p['ora']} -> {p_new['data']} {p_new['ora']}")
                n_data_cambiata += 1
            pro_out.append(p_new)

        else:
            # Gara non trovata nel nuovo scaricamento
            if _chiave_gara(p)[0] == 'id':
                # Aveva gara_id noto ma non è più online
                log(f"         ❌ rimossa: {p['casa']} vs {p['ospite']} [{p.get('gara_id','')}]")
                n_rimosse += 1
            else:
                # Nessun id: mantieni per sicurezza
                pro_out.append(p)

    # Aggiungi nuove prossime non presenti prima
    chiavi_pro_vec = {_chiave_gara(p) for p in girone_vec['prossime']}
    for p in pro_new:
        chiave = _chiave_gara(p)
        if chiave not in chiavi_pro_vec and chiave not in chiavi_ris_vec:
            pro_out.append(p)
            n_nuove += 1
            log(f"         ➕ nuova gara: {p['casa']} vs {p['ospite']} "
                f"{p['data']} {p['ora']}")

    log(f"       '{girone_nome}': "
        f"+{n_diventate_ris}✅  {n_data_cambiata}📅  -{n_rimosse}❌  +{n_nuove}➕")

    return {
        'nome':       girone_nome,
        'classifica': classifica_nuova or girone_vec['classifica'],
        'risultati':  ris_out,
        'prossime':   pro_out,
    }


def aggiorna_campionato(camp_vec, sesso):
    """Aggiorna un campionato: riscarica i gironi con prossime,
    controlla nuovi gironi/fasi, mantiene invariati i gironi completati."""
    camp_code = camp_vec['codice']
    camp_nome = camp_vec['nome']
    log(f"  ⟳ {camp_nome} ({camp_code})")

    soup_base = fetch(build_url(sesso=sesso, codice_campionato=camp_code))
    if not soup_base:
        log(f"     FAIL — mantengo dati esistenti")
        return camp_vec

    fase_opts = select_opts(soup_base, 3)
    if not fase_opts:
        fase_opts = [('', '')]

    gironi_vec_idx = {g['nome']: g for g in camp_vec['gironi']}
    multi_fasi     = len(fase_opts) > 1
    gironi_out     = []
    cl_globale     = []

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

        girone_opts = select_opts(soup_fase, 4)
        if not girone_opts:
            girone_opts = [('', 'Girone Unico')]

        for girone_code, girone_nome in girone_opts:
            # Calcola nome pieno (identico a scrape_campionato)
            if multi_fasi and fase_nome and fase_nome.strip():
                fn        = fase_nome.strip()
                full_nome = f"{fn} – {girone_nome}" if girone_nome and girone_nome != 'Girone Unico' else fn
            else:
                full_nome = girone_nome

            # Classifica specifica per questo girone
            if girone_code and len(girone_opts) > 1:
                sg   = fetch(build_url(sesso=sesso, codice_campionato=camp_code,
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

            girone_vec = gironi_vec_idx.get(full_nome)

            if girone_vec is None:
                # Girone nuovo: scaricalo completo
                log(f"     ➕ girone nuovo: '{full_nome}'")
                g_data = scrape_girone(sesso, camp_code, fase_code, girone_code, full_nome, cl_g or [])
                if g_data['risultati'] or g_data['prossime'] or g_data['classifica']:
                    gironi_out.append(g_data)

            elif girone_vec['prossime']:
                # Girone con prossime: aggiorna
                g_data = aggiorna_girone_esistente(
                    girone_vec, sesso, camp_code, fase_code, girone_code, cl_g)
                gironi_out.append(g_data)

            else:
                # Girone completato: mantieni, aggiorna solo classifica
                gironi_out.append({**girone_vec, 'classifica': cl_g or girone_vec['classifica']})

    if not gironi_out:
        log(f"     Nessun girone trovato — mantengo esistente")
        return camp_vec

    return {'nome': camp_nome, 'codice': camp_code, 'sesso': sesso, 'gironi': gironi_out}


def _controlla_nuovi_gironi(camp_vec, sesso):
    """Per campionati senza prossime: controlla se sono comparse nuove fasi
    (es. Finale Serie C appena pubblicata). Se sì, scarica i gironi nuovi."""
    camp_code = camp_vec['codice']
    camp_nome = camp_vec['nome']

    soup_base = fetch(build_url(sesso=sesso, codice_campionato=camp_code))
    if not soup_base:
        return camp_vec

    fase_opts = select_opts(soup_base, 3)
    if not fase_opts:
        fase_opts = [('', '')]

    multi_fasi = len(fase_opts) > 1
    nomi_vec   = {g['nome'] for g in camp_vec['gironi']}
    gironi_out = list(camp_vec['gironi'])
    cl_globale = []

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

        girone_opts = select_opts(soup_fase, 4)
        if not girone_opts:
            girone_opts = [('', 'Girone Unico')]

        for girone_code, girone_nome in girone_opts:
            if multi_fasi and fase_nome and fase_nome.strip():
                fn        = fase_nome.strip()
                full_nome = f"{fn} – {girone_nome}" if girone_nome and girone_nome != 'Girone Unico' else fn
            else:
                full_nome = girone_nome

            if full_nome not in nomi_vec:
                log(f"     ➕ nuova fase/girone in {camp_nome}: '{full_nome}'")
                if girone_code and len(girone_opts) > 1:
                    sg   = fetch(build_url(sesso=sesso, codice_campionato=camp_code,
                                           codice_fase=fase_code or None,
                                           codice_girone=girone_code))
                    time.sleep(SLEEP)
                    cl_g = parse_classifica(sg) if sg else cl_fase
                else:
                    cl_g = cl_fase
                if not cl_g:
                    cl_g = cl_globale

                g_data = scrape_girone(sesso, camp_code, fase_code, girone_code, full_nome, cl_g or [])
                if g_data['risultati'] or g_data['prossime'] or g_data['classifica']:
                    gironi_out.append(g_data)

    return {'nome': camp_nome, 'codice': camp_code, 'sesso': sesso, 'gironi': gironi_out}


def modalita_aggiorna(json_path):
    """Carica il JSON esistente e aggiorna in modo intelligente."""
    log(f"=== Modalità AGGIORNA — carico {json_path} ===")

    if not os.path.exists(json_path):
        log("ERRORE: file JSON non trovato. Esegui prima uno scaricamento completo.")
        sys.exit(1)

    with open(json_path, encoding='utf-8') as f:
        data_vec = json.load(f)

    tot_pro_prima = sum(len(g['prossime']) for c in data_vec['campionati'] for g in c['gironi'])
    log(f"  JSON caricato: {len(data_vec['campionati'])} campionati — "
        f"prossime totali: {tot_pro_prima} — aggiornato: {data_vec.get('aggiornato','?')}")

    camp_nuovi = []
    for camp in data_vec['campionati']:
        sesso  = camp['sesso']
        ha_pro = any(g['prossime'] for g in camp['gironi'])
        if ha_pro:
            camp_ag = aggiorna_campionato(camp, sesso)
        else:
            camp_ag = _controlla_nuovi_gironi(camp, sesso)
        camp_nuovi.append(camp_ag)
        time.sleep(SLEEP)

    return {
        'aggiornato': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'campionati': camp_nuovi,
    }


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='FIP Scraper Sarda v5')
    parser.add_argument('--aggiorna', action='store_true',
                        help='Aggiorna solo gironi con prossime (veloce, ~2-5 min)')
    args = parser.parse_args()

    t0  = time.time()
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)

    if args.aggiorna:
        log("=== FIP Scraper Sarda v5 — AGGIORNAMENTO VELOCE ===")
        data = modalita_aggiorna(out)
    else:
        log("=== FIP Scraper Sarda v5 — SCARICAMENTO COMPLETO ===")
        data = {
            'aggiornato': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'campionati': [],
        }
        for sesso in ('M', 'F'):
            data['campionati'].extend(scrape_sesso(sesso))

    with open(out, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    tot     = len(data['campionati'])
    nm      = sum(1 for c in data['campionati'] if c['sesso'] == 'M')
    nf      = sum(1 for c in data['campionati'] if c['sesso'] == 'F')
    tot_ris = sum(len(g['risultati']) for c in data['campionati'] for g in c['gironi'])
    tot_pro = sum(len(g['prossime'])  for c in data['campionati'] for g in c['gironi'])

    log(f"\n=== FINE: {tot} camp ({nm}M+{nf}F) | "
        f"{tot_ris} risultati | {tot_pro} prossime | "
        f"{int(time.time()-t0)}s -> {out} ===")


if __name__ == '__main__':
    main()
