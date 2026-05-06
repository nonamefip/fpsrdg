#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_cache_da_live.py — 03/05/2026
====================================
Allinea cache/fip_sardegna_cache.json (fonte storica usata da gen_data.py)
con i dati live di fip_sarda_data.json (aggiornato dallo scraper v5).

Logica:
 - Per ogni gara nel live con gara_id valido:
   * Se gara_id già in cache: aggiorna SOLO Data, Ora, Risultato, Punti
     (mantenendo Campionato, Girone, Fase, Campo, Arbitri, Provvedimenti)
   * Se gara_id NON in cache: aggiunge entry nuova con campi base
 - Le gare in cache che non hanno corrispondenza nel live vengono mantenute
 - Salva un backup della cache prima di sovrascrivere

Risolve i problemi:
 1. Playout Div. Reg. 1 mancante      → aggiunge gare nuove
 2. Finale Serie C mancante           → aggiunge gare nuove
 3. Ritorno U15 / U17 incompleti      → aggiunge gare nuove
 4. Gara 004568 con data sbagliata    → aggiorna Data/Ora dalla live

Uso:
  python sync_cache_da_live.py
  python sync_cache_da_live.py --dry-run    (mostra solo cosa farebbe)
"""

import json, os, sys, shutil, argparse
from datetime import datetime

LIVE_FILE  = "fip_sarda_data.json"
CACHE_FILE = "cache/fip_sardegna_cache.json"

MESI_IT = {
    "gennaio":"01","febbraio":"02","marzo":"03","aprile":"04",
    "maggio":"05","giugno":"06","luglio":"07","agosto":"08",
    "settembre":"09","ottobre":"10","novembre":"11","dicembre":"12",
    "gen":"01","feb":"02","mar":"03","apr":"04","mag":"05","giu":"06",
    "lug":"07","ago":"08","set":"09","ott":"10","nov":"11","dic":"12",
}


def parse_data_it(raw):
    """Converte '30 Giugno 2026' / '30 giu 2026' → '2026-06-30'."""
    if not raw:
        return ""
    parts = raw.lower().strip().split()
    if len(parts) != 3:
        return raw  # già in formato ISO o sconosciuto
    g, m, a = parts
    mese = MESI_IT.get(m, "")
    if not mese:
        return raw
    try:
        return f"{a}-{mese}-{int(g):02d}"
    except ValueError:
        return raw


def split_fase_girone(nome_girone_live):
    """Divide il nome del girone live in (fase, girone) per la cache.

    Esempi:
      'Fase Playout'                       → ('Fase Playout',     'Girone Unico')
      'Qualificazione'                     → ('Qualificazione',   'Girone Unico')
      'Playoff Finale – Finale'            → ('Playoff Finale',   'Finale')
      'Seconda Fase Sud – Girone Arancio'  → ('Seconda Fase Sud', 'Girone Arancio')
      'Qualificazione – Girone Nord'       → ('Qualificazione',   'Girone Nord')
    """
    if " – " in nome_girone_live:
        fase, girone = nome_girone_live.split(" – ", 1)
        return fase.strip(), girone.strip()
    if " - " in nome_girone_live:
        fase, girone = nome_girone_live.split(" - ", 1)
        return fase.strip(), girone.strip()
    return nome_girone_live.strip(), "Girone Unico"


def build_entry_nuova(camp_nome, girone_nome_live, gara_live):
    """Crea una nuova entry per la cache da una gara del live."""
    fase, girone = split_fase_girone(girone_nome_live)
    pc = ""
    po = ""
    risultato = ""
    if gara_live.get("punteggio"):
        pp = gara_live["punteggio"].split("-")
        if len(pp) == 2:
            pc = pp[0].strip()
            po = pp[1].strip()
            risultato = f"{pc}-{po}"
    return {
        "Data":           parse_data_it(gara_live.get("data", "")),
        "Ora":            gara_live.get("ora", ""),
        "Numero Gara":    gara_live.get("gara_id", ""),
        "Campionato":     camp_nome,
        "Girone":         girone,
        "Fase":           fase,
        "Squadra Casa":   gara_live.get("casa", ""),
        "Squadra Ospite": gara_live.get("ospite", ""),
        "Punti Casa":     pc,
        "Punti Ospite":   po,
        "Risultato":      risultato,
        "Stato Gara":     "",
        "Campo":          "",
        "Arbitro 1":      "",
        "Arbitro 2":      "",
        "Arbitro 3":      "",
        "Segnapunti":     "",
        "Cronometrista":  "",
        "24 Secondi":     "",
        "Addetto Referto":"",
        "Osservatore":    "",
        "Provvedimenti":  "",
    }


def main():
    parser = argparse.ArgumentParser(description="Sync cache da dati live FIP")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra solo cosa farebbe senza modificare la cache")
    args = parser.parse_args()

    if not os.path.exists(LIVE_FILE):
        print(f"ERRORE: {LIVE_FILE} non trovato. Esegui prima fip_scraper_sarda.py")
        sys.exit(1)
    if not os.path.exists(CACHE_FILE):
        print(f"ERRORE: {CACHE_FILE} non trovato.")
        sys.exit(1)

    with open(LIVE_FILE, encoding="utf-8") as f:
        live = json.load(f)
    with open(CACHE_FILE, encoding="utf-8") as f:
        cache = json.load(f)

    print(f"Live:  {len(live['campionati'])} campionati, "
          f"aggiornato {live.get('aggiornato','?')}")
    print(f"Cache: {len(cache)} gare")

    # Indice cache per Numero Gara
    cache_idx = {}
    for i, g in enumerate(cache):
        ng = (g.get("Numero Gara") or "").strip()
        if ng:
            cache_idx[ng] = i

    # Statistiche
    n_aggiornate = 0
    n_data_cambiata = 0
    n_diventate_ris = 0
    n_aggiunte = 0
    n_skipped_no_id = 0

    nuove_entries = []

    for camp in live["campionati"]:
        camp_nome = camp["nome"]
        for girone in camp["gironi"]:
            girone_nome = girone["nome"]
            tutte_gare = [(g, "ris") for g in girone["risultati"]] + \
                         [(g, "pro") for g in girone["prossime"]]

            for g_live, kind in tutte_gare:
                gara_id = (g_live.get("gara_id") or "").strip()
                if not gara_id:
                    n_skipped_no_id += 1
                    continue

                # Calcola Data ISO e punteggio dal live
                data_new = parse_data_it(g_live.get("data", ""))
                ora_new  = g_live.get("ora", "")
                pc_new = po_new = ""
                ris_new = ""
                if g_live.get("punteggio"):
                    pp = g_live["punteggio"].split("-")
                    if len(pp) == 2:
                        pc_new = pp[0].strip()
                        po_new = pp[1].strip()
                        ris_new = f"{pc_new}-{po_new}"

                if gara_id in cache_idx:
                    # Aggiorna entry esistente
                    e = cache[cache_idx[gara_id]]
                    cambiato = False
                    if data_new and data_new != e.get("Data", ""):
                        print(f"  📅 [{gara_id}] {e.get('Squadra Casa')} - "
                              f"{e.get('Squadra Ospite')}: data "
                              f"{e.get('Data')} → {data_new}")
                        e["Data"] = data_new
                        n_data_cambiata += 1
                        cambiato = True
                    if ora_new and ora_new != e.get("Ora", ""):
                        e["Ora"] = ora_new
                        cambiato = True
                    if ris_new and ris_new != e.get("Risultato", ""):
                        # Era prossima/senza risultato, ora ha punteggio
                        if not (e.get("Risultato") or "").strip():
                            print(f"  ✅ [{gara_id}] {e.get('Squadra Casa')} - "
                                  f"{e.get('Squadra Ospite')}: {ris_new}")
                            n_diventate_ris += 1
                        e["Punti Casa"]   = pc_new
                        e["Punti Ospite"] = po_new
                        e["Risultato"]    = ris_new
                        cambiato = True
                    if cambiato:
                        n_aggiornate += 1
                else:
                    # Gara nuova → aggiungila
                    nuova = build_entry_nuova(camp_nome, girone_nome, g_live)
                    nuove_entries.append(nuova)
                    print(f"  ➕ [{gara_id}] {nuova['Campionato']} | "
                          f"{nuova['Fase']} / {nuova['Girone']}: "
                          f"{nuova['Squadra Casa']} - {nuova['Squadra Ospite']} "
                          f"({nuova['Data']} {nuova['Ora']})")
                    n_aggiunte += 1

    # Aggiungi le nuove entries
    cache.extend(nuove_entries)

    print()
    print("═" * 60)
    print(f"Riepilogo sync:")
    print(f"  Gare aggiornate (totale):       {n_aggiornate}")
    print(f"     di cui data cambiata:        {n_data_cambiata}")
    print(f"     di cui diventate risultato:  {n_diventate_ris}")
    print(f"  Gare nuove aggiunte:            {n_aggiunte}")
    print(f"  Gare live senza gara_id (skip): {n_skipped_no_id}")
    print(f"  Cache finale:                   {len(cache)} gare")
    print("═" * 60)

    if args.dry_run:
        print("\n[DRY RUN] Nessun file scritto.")
        return

    # Backup cache
    backup = CACHE_FILE.replace(".json", f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    shutil.copy2(CACHE_FILE, backup)
    print(f"\n💾 Backup salvato: {backup}")

    # Salva cache aggiornata
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    print(f"💾 Cache aggiornata: {CACHE_FILE}")
    print(f"\n▶ Ora esegui: python scripts/gen_data.py && python build.py")


if __name__ == "__main__":
    main()
