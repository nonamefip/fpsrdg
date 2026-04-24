# Resoconto sessione — Dashboard FIP Sardegna

## Stato attuale
Template funzionante, deployato su GitHub Pages tramite `deploy.bat`.
Ultimo file consegnato: `template.html` (≈616KB).

---

## Cosa è stato implementato

### 1. Tabella gare principale (pagina Gare)
- **Filtri pill** — 4 pulsanti rettangolari con bordo sempre visibili: Campionati / Province / Arbitri / Campi. Si aprono/chiudono con slide-down, multi-select, contatore attivo sul pulsante, **✕ Azzera tutto** globale.
- **Colonna Prov.A** — provincia squadra di casa, separata, a sinistra di Casa, ordinabile
- **Colonna Prov.O** — provincia squadra ospite, separata, a destra di Ospite, ordinabile
- **N° gara** — stile monospace con sfondo navy e testo celeste
- **Click sulla riga** → apre modal gara
- **Click su squadra** → apre scheda squadra (stopPropagation)
- **Click su arbitro** → apre scheda arbitro (stopPropagation)
- **Click su campionato** → apre scheda campionato (stopPropagation)
- **Click su campo** → apre scheda campo (stopPropagation)

### 2. Modal giornata (openGiornoModal)
- **5 pulsanti filtro**: Campionati / Province / Arbitri / Campi / **Città** — stessa logica pill multi-select
- **Colonna Prov.A** — badge provincia squadra di casa, prima della colonna Casa
- **Colonna Prov.O** — badge provincia squadra ospite, dopo la colonna Ospite
- **Colonna Città** — città della palestra con badge provincia **prima** del nome (es. "SS Sassari")
- **data-citta** su ogni riga per il filtro Città
- Arbitro 1 e Arbitro 2 in colonne separate
- Province arbitri mostrate inline dopo il nome

### 3. Modal giornata — scheda campo (openCampoModal / mo-campo)
- **Finestra a tutto schermo** — larghezza `min(98vw,1400px)`, pulsante **⛶ Espandi** (usa `toggleMoFullscreen`)
- **Provincia nel titolo** — badge colorato accanto al nome campo
- **Squadre di casa/ospitate collassabili** — toggle con click, con sigla provincia accanto ad ogni squadra; click su squadra filtra la tabella sottostante; pulsante **← Tutte le gare** per tornare
- **Campionati ospitati rimosso** — era ridondante, già nei pill
- **Gare passate collassate** — riga "▶ Gare precedenti (N) — clicca per espandere", si apre al click
- **Intestazioni ordinabili** — frecce ↑↓ cliccabili su ogni colonna
- **Colonne ridimensionabili** — handle trascinabile sul bordo destro di ogni th
- **Colonne tabella**: # / Ora / Data / N° Gara / Campionato / **Prov.A** / Casa / Ris. / Ospite / **Prov.B** / Arbitro 1 / Arbitro 2
- **Prov.A e Prov.B** — badge più grandi (11px), rinominati da Prov.C/Prov.O
- **N° Gara** — stile navy/celeste monospace
- **Filtri Campionati + Arbitri** — stessi pill multi-select

### 4. Sistema deploy
- **deploy.bat** — script Windows che carica `template.html` su GitHub e aspetta il build
- **token.txt** — token GitHub nella cartella `fpsrdg/` sul PC
- Struttura cartella: `fpsrdg/token.txt`, `fpsrdg/deploy.py`, `fpsrdg/deploy.bat`, `fpsrdg/template.html`

---

## Struttura dati chiave

```
D.gare[]          — gare disputate
D.gare_future[]   — gare future
D.persons{}       — arbitri/udc/osservatori (chiave = nome)
  .provincia      — sigla provincia (CA/SS/NU/OR/SU)
D.squads{}        — squadre (chiave = nome)
  .prov           — sigla provincia
D.campi{}         — CAMPI (scheda campo)
CAMPO_GPS{}       — coordinate + city per ogni campo
PC{}              — colori province: CA=#d63031, SS=#0984e3, NU=#e17055, OR=#00b894, SU=#6c5ce7
```

## Funzioni chiave aggiunte/modificate

| Funzione | Dove | Cosa fa |
|---|---|---|
| `gFcatToggle(type)` | globale | apre/chiude pannello pill nella tabella gare |
| `gTogglePill(type,val)` | globale | seleziona/deseleziona pill nella tabella gare |
| `gResetGrp(type)` | globale | resetta gruppo pill |
| `gResetAll()` | globale | resetta tutti i gruppi |
| `_gBuildPillBars()` | globale | popola pill da GARE[] |
| `gmToggleGrp(type)` | globale | apre/chiude pannello pill nel modal giornata |
| `gmTogglePill(btn,type,val)` | globale | seleziona pill nel modal giornata |
| `gmResetGrp(type)` | globale | resetta gruppo nel modal giornata |
| `gmResetAll()` | globale | resetta tutti i gruppi nel modal giornata |
| `gmApplyPillFilters()` | globale | applica filtri (camp/prov/arb/campo/citta) |
| `mcaSort(col)` | globale | ordina tabella scheda campo |
| `mcaStartResize(e,handle)` | globale | ridimensiona colonna scheda campo |
| `mcaTogglePast(btn)` | globale | espande/collassa gare passate in scheda campo |
| `mcaToggleSq(type)` | globale | espande/collassa lista squadre in scheda campo |
| `mcaFilterSq(sq)` | globale | filtra gare per squadra in scheda campo |
| `_gSqCell(n)` | globale | cella squadra con stopPropagation |
| `_gArbCell(n,hi)` | globale | cella arbitro con stopPropagation |

## Colonne _gCOLS (tabella gare principale)
`n, data, ora, num, camp, cpv, gir, **pvcasa**, casa, osp, **pvosp**, ptc, pto, stato, narb, a1, a2, sgp, cro, s24, are, oss, campo`

Novità: `pvcasa` (Prov.A, prima di casa) e `pvosp` (Prov.O, dopo osp).

---

## Cose da fare / possibili prossimi passi
*(da aggiornare ad ogni sessione)*

- Nessuna richiesta in sospeso al momento della chiusura sessione.

---

## Note tecniche
- Script 5 (principale) è ~7600 righe nel template
- Tutti i 7 script passano `node --check` senza errori
- Bug risolti in questa sessione: scope `allProvs`, `gmToggleGrp` non definito, `data-citta` quoting rotto, `_gSqCell` senza stopPropagation
