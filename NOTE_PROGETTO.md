# FIP Sardegna Dashboard — Note di progetto

> Riepilogo creato il 03/05/2026 dopo sessione di fix dei calendari e gare riprogrammate.
> Da usare come contesto in nuove chat.

## 1. Cosa è il progetto

Dashboard statico in HTML che mostra classifiche, risultati, calendari e statistiche di tutti i campionati di basket regionali della Sardegna (FIP — Federazione Italiana Pallacanestro, comitato RSA).

- **Cartella:** `C:\Users\fylo2\Desktop\fpsrdg`
- **Output:** `docs/index.html` (single-file, ~31 MB) pubblicato su GitHub Pages
- **Dati:** scrapati da `fip.it/risultati/?...&comitato_codice=RSA`

## 2. Architettura del flusso dati

Ci sono **DUE catene di scraping in parallelo** che convergono in `docs/index.html`:

```
                                              ┌─────────────────────────┐
[FIP fip.it]                                  │                         │
   │                                          ▼                         │
   ├─ fip_scraper.py ───► cache/fip_sardegna_cache.json (gare per data) │
   │                            │                                       │
   │                            ▼                                       │
   │                     fip_calendar_scraper.py ─► cache/fip_calendari.json
   │                            │                                       │
   │                            ▼                                       │
   │                     scripts/gen_data.py ─► cache/data_v5_new.json  │
   │                                                  │                 │
   │                                                  ▼                 │
   │                                              build.py ─► docs/index.html
   │                                                  ▲                 │
   └─ fip_scraper_sarda.py ─► fip_sarda_data.json ────┘                 │
      (scraper "live" v5 — classifiche + giornate per fase/girone)      │
                                                                         │
   sync_cache_da_live.py: allinea cache/fip_sardegna_cache.json ◄────────┘
   con i dati live (necessario perché le 2 catene divergono)
```

**Punto chiave:** la dashboard ha 2 sezioni principali con 2 fonti dati diverse:
- "FIP Live" = `fip_sarda_data.json` (sempre aggiornato)
- Tutto il resto (squadre, persone, gironi strutturati, calendari) = `cache/data_v5_new.json` (alimentato da `fip_sardegna_cache.json` via gen_data.py)

## 3. File principali

| File | Ruolo |
|---|---|
| `fip_scraper.py` | Scarica gare per data (data_singola=YYYY-MM-DD), aggiorna cache/fip_sardegna_cache.json. **Fase 3 NUOVA**: verifica per Numero Gara le gare future senza risultato per rilevare riprogrammazioni. |
| `fip_scraper_sarda.py` | Scraper v5 "live": scarica classifiche + giornate per ogni fase/girone con gara_id. Modalità `--aggiorna`. |
| `fip_calendar_scraper.py` | Costruisce calendari strutturati (fase → girone → giornate). **Algoritmo NUOVO**: round-robin per coppia di squadre, separato per leg (andata/ritorno). |
| `scripts/gen_data.py` | Aggrega tutto in `cache/data_v5_new.json` (squadre, persone, h2h, calendari, ecc.). |
| `build.py` | Inserisce data_v5_new.json + fip_sarda_data.json in scripts/template.html → docs/index.html |
| `sync_cache_da_live.py` | **NUOVO**: allinea fip_sardegna_cache.json con fip_sarda_data.json (per gare con gara_id). |
| `aggiorna_fip.bat` | Sequenza completa: scraper_sarda --aggiorna + build + git push |
| `fix_completo.bat` | **NUOVO**: applica tutti i fix in sequenza (sync + Fase 3 + calendari + gen_data + build) |

## 4. Problemi risolti il 02-03/05/2026

### Problemi segnalati
1. **Screen 1**: Divisione regionale 1 — manca la fase Playout
2. **Screen 3**: Serie C — manca la Finale  
3. **Screen 4-5**: Under 15/17 — manca metà del girone di ritorno
4. **Screen 6**: Gara 004568 (A.S.D. AMPURIAS - P.G.S. CONDOR) elencata 18/04/2026 ma su FIP è 30/06/2026
5. **Successivamente**: gare di andata/ritorno tutte sfasate nei gironi (DR1 mostrava 33 giornate invece di 26, DR2 Sud 44 invece di 30, ecc.)

### Cause
- I dati live (`fip_sarda_data.json`) erano aggiornati e corretti
- La cache storica (`fip_sardegna_cache.json`) era disallineata: gare riprogrammate non venivano rilevate (lo scraper riscaricava per data, ma se una gara veniva spostata oltre il range future-days, restava con la vecchia data)
- `fip_calendar_scraper.py` raggruppava le giornate con finestra temporale fissa di 6 giorni → recuperi tardivi creavano "giornate extra" fittizie

### Fix applicati
1. **`sync_cache_da_live.py`** — Script di sync che usa fip_sarda_data.json (con gara_id affidabile) per allineare fip_sardegna_cache.json. Al primo run: 50 gare aggiornate (37 con data cambiata, 13 diventate risultato), 46 gare nuove aggiunte.

2. **`fip_calendar_scraper.py` riscritto** — Nuovo algoritmo `build_giornate()`:
   - Raggruppa per coppia di squadre
   - Separa per leg (andata = 1ª occorrenza, ritorno = 2ª)
   - Ordina per Numero Gara (calendario FIP originale) — recuperi tardivi finiscono nella giornata corretta
   - Ogni giornata ha campo `leg: 1|2|3+`

3. **`fip_scraper.py` patchato con Fase 3** — Per ogni gara futura senza risultato, fa query per Numero Gara su FIP e rileva: spostamenti data, ora cambiata, gare diventate risultato, gare scomparse. Default ON, disattivabile con `--no-check-spostate`. Limite: 250 gare per run (priorità alle date più imminenti/vecchie).

   Risultato primo run: 38 gare spostate, 24 ora cambiata, 1 diventata risultato, 14 scomparse.

### Risultato calendari (giornate per girone)
| Campionato/Girone | Prima | Dopo | Atteso |
|---|---|---|---|
| Serie C Girone Unico (11sq) | 23 | 22 | 22 |
| DR1 Girone Unico (14sq) | 33 | 27 | 26 |
| DR2 Girone Sud (16sq) | 44 | 31 | 30 |
| DR2 Girone Nord A (8sq) | 18 | 14 | 14 |
| U17 Reg. Girone Regionale (8sq) | — | 14 | 14 |

Restano 1-3 giornate extra in alcuni gironi a causa di recuperi multipli con date molto distanti — non perfetto ma molto migliorato.

## 5. Comandi utili

```bash
# Aggiornamento veloce (solo classifiche + prossime live)
aggiorna_fip.bat

# Aggiornamento completo + Fase 3 + calendari + dashboard
fix_completo.bat

# Singoli step
python sync_cache_da_live.py
python sync_cache_da_live.py --dry-run        # mostra solo cosa farebbe
python fip_scraper.py                         # default: --check-spostate ON
python fip_scraper.py --no-check-spostate     # disabilita Fase 3
python fip_scraper.py --full-refresh          # ri-scarica TUTTO
python fip_scraper.py --all-provv             # Fase 2 su tutte le gare
python fip_calendar_scraper.py --rebuild
python scripts/gen_data.py
python build.py
python fip_scraper_sarda.py                   # scraper live completo
python fip_scraper_sarda.py --aggiorna        # solo aggiornamento veloce

# Pubblicazione GitHub Pages
git add . && git commit -m "..." && git push
# oppure
commit_push.bat
```

## 6. Convenzioni dati

### `cache/fip_sardegna_cache.json` (lista di gare)
```json
{
  "Data": "2026-04-18",         // ISO YYYY-MM-DD
  "Ora": "18:00",
  "Numero Gara": "004568",      // ID univoco FIP, padding zeri 6 cifre
  "Campionato": "Under 17 Maschile Regionale",
  "Girone": "Classificazione",
  "Fase": "Seconda fase",
  "Squadra Casa": "A.S.D. AMPURIAS",
  "Squadra Ospite": "P.G.S. CONDOR",
  "Punti Casa": "", "Punti Ospite": "",
  "Risultato": "",              // "" se non ancora giocata
  "Stato Gara": "",
  "Campo": "...", "Arbitro 1": "...", ...
  "Provvedimenti": ""           // PRESERVATO sempre, non sovrascritto
}
```

### `fip_sarda_data.json` (live, struttura annidata)
```json
{
  "aggiornato": "2026-05-03 00:33",
  "campionati": [{
    "nome": "Serie C", "codice": "C1", "sesso": "M",
    "gironi": [{
      "nome": "Playoff Finale – Finale",   // formato "Fase – Girone"
      "classifica": [...],
      "risultati": [...],
      "prossime": [{
        "gara_id": "004568",
        "casa": "...", "ospite": "...",
        "data": "30 Giugno 2026",          // formato ITALIANO
        "ora": "18:00",
        "giornata": 3
      }]
    }]
  }]
}
```

### `cache/fip_calendari.json` (struttura per dashboard sezione "Gironi")
```json
{
  "Serie C": {
    "fasi": [{
      "nome": "Qualificazione",
      "tipo": "girone",  // o "playoff"
      "gironi": [{
        "nome": "Girone Unico",
        "n_squadre": 11,
        "giornate": [{
          "n": 1,
          "leg": 1,                        // 1 = andata, 2 = ritorno
          "gare": [...],
          "completa": true,
          "data_label": "27 set–28 set"
        }],
        "classifica": [...],
        "serie": []                        // popolato per playoff
      }]
    }]
  }
}
```

## 7. Note sui campionati

Codici campionati FIP regionali Sardegna:
- C1 = Serie C M
- D = Divisione regionale 1 M
- PM = Divisione regionale 2 M
- B/F = Serie B Femminile
- U19G/M, U17/E, U17G/M, U17S/M, U15/E, U15S/M, U14S/M, U13S/M (M)
- U19/F, U17/F, U15/F, U14/F, U13/F (F)
- ESCLUSI da scraping: ES, TAQB, TAQS, TSCB, TSCS, TFE, TGAB, TGAS, TLIB (minibasket non competitivo)

## 8. Possibili miglioramenti futuri

- Fase 3 di `fip_scraper.py`: limite 250 gare per run → estendere a 500 o iterare su più giorni
- `fip_calendar_scraper.py`: post-processing per fondere giornate adiacenti compatibili (squadre disgiunte) — eliminerebbe le 1-3 giornate residue
- `sync_cache_da_live.py`: fa skip di 1754 gare senza gara_id (dati legacy del live) — implementare matching per (casa, ospite, data) come fallback
- Le gare "scomparse" da FIP (Fase 3) non vengono rimosse dalla cache, solo segnalate — decidere policy
- I file `BUILD VARI/`, `ALTRO TUTTO/`, `Altri Scraper e Gendata/`, `DEPLOY Raccolta 08 04 2026/`, `FILES Raccolta 08 04 2026/`, `Template Raccolta 08 04 2026/` sono backup storici — possono essere archiviati altrove

## 9. File di backup creati durante i fix

- `cache/fip_sardegna_cache.bak_20260503_193325.json` (sync 1)
- `cache/fip_sardegna_cache.bak_20260503_215616.json` (sync 2)
- I backup vengono creati automaticamente da `sync_cache_da_live.py` ad ogni run.

## 10. Stato al 03/05/2026 22:00

✅ Cache allineata con dati live  
✅ 38 gare riprogrammate corrette (gara 004568 inclusa)  
✅ Calendari ricostruiti con algoritmo round-robin (leg 1/2 separati)  
✅ data_v5_new.json: 26.9 MB rigenerato  
✅ docs/index.html: 31.2 MB ricostruito  
⏳ Pubblicazione GitHub Pages: l'utente deve eseguire `commit_push.bat` o `aggiorna_fip.bat`
