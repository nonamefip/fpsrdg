# CONTESTO PROGETTO — FIP Sardegna Dashboard
> Incolla questo all'inizio di ogni nuova sessione AI per riprendere il lavoro

---

## Chi sono
Lavoro al **COSC (Centro Operativo Sicurezza Cibernetica) di Sassari**.  
Gestisco anche la dashboard arbitri per la **FIP Sardegna** (Federazione Italiana Pallacanestro, Comitato Regionale Sardegna).

---

## Il progetto: `template.html`

### Cos'è
Un **singolo file HTML autocontenuto** (~16.000 righe) che è la dashboard completa degli arbitri FIP Sardegna.  
Viene generato da un `build.py` che inietta i dati JSON dentro il template, poi deployato su GitHub Pages.

### URL live
`https://nonamefip.github.io/fpsrdg/`

### Repository GitHub
`https://github.com/nonamefip/fpsrdg`

### Deploy
- Script Python: `deploy.py` nella root del progetto
- Flusso: copia `template.html` → `scripts/template.html` → `git pull --rebase` → `git add` → `git commit` → `git push` → GitHub Actions rebuilda il sito
- Il `deploy.bat` è un collegamento che lancia `deploy.py`

---

## Struttura dati principale

I dati sono iniettati come `const D = { ... }` all'inizio dello script.  
Oggetti principali:
- `D.gare` — gare disputate
- `D.gare_future` — gare programmate
- `D.persone` — arbitri, UDC, osservatori
- `D.camp_meta` — metadati campionati (gruppo, sort_key, genere)
- `D.generated` — timestamp generazione file (NON usare per "oggi" — usare sempre `new Date()`)

### Oggetti globali calcolati al caricamento
- `P` — mappa nome→persona con stats calcolate
- `CAMP_META` — metadati campionati
- `CAMPO_GPS` — coordinate GPS di ogni campo
- `PC` — colori province: `{CA:'#d63031', SS:'#0984e3', SU:'#6c5ce7', OR:'#00b894', NU:'#e17055'}`

### Province
`CA` = Cagliari, `SS` = Sassari, `SU` = Sud Sardegna, `NU` = Nuoro, `OR` = Oristano

---

## Campionati (gerarchia per importanza)
Serie B Femminile → Serie C → Div. Reg. 1 → Div. Reg. 2 → Under 19 → Under 17 → Under 15 → Under 14 → Under 13 → Esordienti/Aquilotti/Scoiattoli/Gazzelle/Libellule

---

## Funzioni chiave

| Funzione | Cosa fa |
|---|---|
| `openPersona(nome)` | Apre la scheda modale di un arbitro |
| `openGiornoModal(ds)` | Apre modal con le gare di un giorno (ds = 'YYYY-MM-DD') |
| `openGiornoRange(d1,d2,arbFilter)` | Modal gare in un range di date |
| `openTrsGareModal(nome,tipo)` | Modal trasferte (tipo: 'sede'/'corta'/'lunga') |
| `calcTrasferte(p)` | Calcola km trasferte per un arbitro |
| `renderArbPerSerie(filterPv)` | Tabella arbitri per serie/campionato |
| `renderTimeline()` | Timeline grafica delle giornate |
| `calDayClick(ds,cnt)` | Click su giorno nel calendario |
| `campColor(cn)` | Colore esadecimale di un campionato |
| `cpv(campo)` | Provincia di un campo |
| `pns(str)` | Normalizza nome (trim, uppercase) |
| `escH(str)` | HTML-escape una stringa |
| `esc(str)` | JS-escape per uso in onclick inline |
| `haversineKm(lat1,lng1,lat2,lng2)` | Distanza in km tra due coordinate |

---

## Soglie trasferte
- **In sede**: campo nella stessa città dell'arbitro (coordinate entro 0.05° lat/lng)
- **Corta (`<50km`)**: km A/R < 50
- **Lunga (`>50km`)**: km A/R ≥ 50

*(Nota: in precedenti versioni la soglia era 100km — ora è 50km)*

---

## Calendario arbitro (scheda persona)

Il calendario è nella tab **Statistiche** della scheda arbitro.  
Ha due viste:
- **A — Heatmap**: griglia settimanale per righe DOW, colonne = date
- **B — Strip**: riga per ogni mese, colonne = giorni 1-31

### Variabili importanti nel rendering calA
```js
const todayDs = new Date().toISOString().split('T')[0]; // SEMPRE data reale
const streak = _streakMap[ds] || 0; // DEVE essere dichiarato prima di usarlo
const _inSerie5 = streak >= 5;
const _serieBg5 = ...; // sfondo colorato per serie >=5 giorni
```

### Bug noti già risolti
- `streak is not defined` → aggiunto `const streak = _streakMap[ds]||0` prima dell'uso
- `todayDs` usava `D.generated` invece di `new Date()` → corretto
- Freeze calendario dopo selezione range → listener `click` fuori ora resetta sia `outline` che `background`

---

## Modal trasferte (`openTrsGareModal`)

Aperto cliccando i box "In sede / <50km / >50km" nella scheda arbitro.  
Caratteristiche attuali:
- Ridimensionabile (resize handle)
- Trascinabile (drag sull'header)
- Pulsante ⛶ per fullscreen
- Colonne: Data, Campionato, Partita, **Città**, **Prov.**, Km A/R
- Colonne ordinabili cliccando l'header (funzione `_trsTblSort`)

---

## Tabella arbitri principale

Header a due livelli:
- Gruppo **ATTIVITÀ SVOLTA** (colspan=9): N°gare, Top mese, ⚠️ Provv., IQA, % Cop., In sede, %, <50km, %, >50km, %
- Gruppo **COV.** (colspan=2): ⭐ IQA, % Cop.
- Gruppo **TRASFERTE**: In sede, %, <50km, %, >50km, %
- Gruppo **RIMBORSI**: Km tot., Rimborso

---

## CSS design tokens principali
```css
--bl:#1e3a6e;  --bl2:#2456a4;  --bl3:#deeaff;  --bl4:#f0f5ff;
--gold:#c98a00; --green:#1a7a3c; --red:#c0392b;
--bg:#f0f2f7;  --wh:#ffffff;   --br:#d8e0ee;
--tx:#1c2840;  --mu:#6b7a99;
--radius:8px;  --shadow:0 3px 12px rgba(0,0,50,.10);
```

---

## File del progetto

```
fpsrdg/
├── template.html          ← il file che si modifica
├── scripts/
│   ├── template.html      ← copia per build.py
│   └── build.py           ← genera il sito dai dati
├── deploy.py              ← script deploy (pull→add→commit→push)
├── deploy.bat             ← collegamento per lanciare deploy.py
└── deploy_ts.txt          ← timestamp per forzare diff git
```

---

## Come riprendere il lavoro

1. Incolla questo file all'inizio della nuova sessione
2. Carica il `template.html` attuale se ci sono modifiche da fare
3. Descrivi cosa non funziona o cosa vuoi aggiungere

---

## Sessione precedente — fix applicati (aprile 2026)

- ✅ Click arbitro in classifica serie → apre scheda (fix encoding con `data-nome`)
- ✅ Box trasferte cliccabili → aprono lista gare filtrate
- ✅ Box LIVELLO → mostra posizione nell'elenco (es. "2°/23 in DR2")
- ✅ Intestazioni tabella arbitri riorganizzate
- ✅ Soglia trasferte cambiata da 100km a 50km
- ✅ Modal trasferte e modal range → fullscreen, ridimensionabile
- ✅ Separatori mesi nel calendario (calB) → badge colorato con totale gare
- ✅ Serie >=5 giorni → tutti i giorni evidenziati con sfondo colorato
- ✅ Partite timeline → stessa larghezza (8px), tick marker per giorno con data
- ✅ Modal trasferte → colonne Città + Prov., ordinabili, drag, resize
- ✅ Freeze calendario → risolto (reset background + outline su click esterno)
- ✅ Oggi segnato come ieri → risolto (`new Date()` invece di `D.generated`)
- ✅ `streak is not defined` → risolto (`const streak = _streakMap[ds]||0`)
- ✅ `deploy.py` → aggiunto `git pull --rebase` prima del commit/push
