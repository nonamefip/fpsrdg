# 🏀 FIP Sardegna Dashboard

Dashboard automatica per gli ufficiali di gara del Comitato Regionale Sardegna.
Si aggiorna **ogni notte alle 02:30** scaricando i dati da fip.it.

---

## 📱 Come accedere alla dashboard

Una volta configurato, il tuo URL sarà:

```
https://TUO-NOME-GITHUB.github.io/fip-sardegna/
```

---

## 🚀 Configurazione iniziale (una volta sola — 10 minuti)

### Passo 1 — Crea un account GitHub gratuito

1. Vai su **https://github.com**
2. Clicca **Sign up** (in alto a destra)
3. Inserisci email, password, username (es. `mario-rossi`)
4. Verifica l'email
5. Scegli il piano **Free** (gratuito)

---

### Passo 2 — Carica questo progetto su GitHub

1. Accedi a **https://github.com**
2. Clicca il **+** in alto a destra → **New repository**
3. Nome repository: `fip-sardegna`
4. Visibilità: **Public** ✓ (necessario per GitHub Pages gratuito)
5. Clicca **Create repository**
6. Nella pagina che si apre, clicca **uploading an existing file**
7. **Trascina TUTTI i file** di questa cartella zip nel browser
8. Clicca **Commit changes**

---

### Passo 3 — Attiva GitHub Pages

1. Nel repository, clicca **Settings** (in alto)
2. Nel menu a sinistra clicca **Pages**
3. Sotto "Source" seleziona **GitHub Actions**
4. Salva

---

### Passo 4 — Esegui il primo aggiornamento manuale

1. Clicca la scheda **Actions** nel repository
2. Clicca **🏀 Aggiorna Dashboard FIP Sardegna** nella lista a sinistra
3. Clicca **Run workflow** → **Run workflow** (bottone verde)
4. Aspetta 3-5 minuti che finisca (vedrai una spunta verde ✅)
5. La tua dashboard è online!

---

### Passo 5 — Trova il tuo URL

Vai su **Settings → Pages** e vedrai l'URL della tua dashboard.
Salvalo nei preferiti su tutti i tuoi dispositivi!

---

## ⚙️ Come funziona

```
Ogni notte alle 02:30
        ↓
GitHub scarica le gare da fip.it
(dal 01/09/2025 fino a oggi + 14 giorni)
        ↓
Calcola tutte le statistiche
        ↓
Compila la dashboard HTML
        ↓
Pubblica online automaticamente
        ↓
Apri l'URL da qualsiasi dispositivo ✅
```

## 🔄 Aggiornamento manuale

Se vuoi aggiornare subito senza aspettare la notte:
1. Vai sul repository GitHub
2. **Actions** → **🏀 Aggiorna Dashboard** → **Run workflow**

---

## ❓ Problemi?

- **Il workflow fallisce**: controlla che GitHub Pages sia attivato (Passo 3)
- **Dati vecchi**: prova un aggiornamento manuale (Passo 4)
- **URL non funziona**: aspetta 5 minuti dopo il primo deploy
