#!/usr/bin/env python3
"""
deploy.py — Carica template.html su GitHub, aspetta il build e apre la dashboard.
Metti questo file nella stessa cartella di template.html e token.txt.
"""

import os, sys, base64, json, time, webbrowser
import urllib.request, urllib.error

# ── Configurazione ───────────────────────────────────────────────
GITHUB_USER   = "nonamefip"
GITHUB_REPO   = "fpsrdg"
FILE_PATH     = "scripts/template.html"
BRANCH        = "main"
DASHBOARD_URL = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/"
ACTIONS_URL   = f"https://github.com/{GITHUB_USER}/{GITHUB_REPO}/actions"
POLL_INTERVAL = 8    # secondi tra un controllo e l'altro
MAX_WAIT      = 600  # secondi massimi di attesa (10 minuti)
# ────────────────────────────────────────────────────────────────

def leggi_file(nome):
    cartella = os.path.dirname(os.path.abspath(__file__))
    percorso = os.path.join(cartella, nome)
    if not os.path.exists(percorso):
        print(f"\n❌  File non trovato: {percorso}")
        sys.exit(1)
    with open(percorso, "r", encoding="utf-8") as f:
        return f.read().strip()

def api(metodo, endpoint, token, dati=None):
    url = f"https://api.github.com{endpoint}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "fpsrdg-deploy"
    }
    body = json.dumps(dati).encode() if dati else None
    req = urllib.request.Request(url, data=body, headers=headers, method=metodo)
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"\n❌  Errore GitHub API {e.code}: {err}")
        sys.exit(1)

def spinner(msg, i):
    chars = ["|", "/", "-", "\\"]
    print(f"\r   {chars[i % len(chars)]}  {msg}   ", end="", flush=True)

def scarica_index():
    """Scarica il nuovo index.html da GitHub Pages nella cartella locale."""
    cartella = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(cartella, "index.html")
    url = DASHBOARD_URL
    print(f"\n  Download index aggiornato da GitHub Pages...")
    try:
        req = urllib.request.Request(url, headers={"Cache-Control": "no-cache", "Pragma": "no-cache"})
        with urllib.request.urlopen(req, timeout=30) as r:
            content = r.read()
        with open(index_path, "wb") as f:
            f.write(content)
        size_kb = len(content) // 1024
        print(f"  ✅ index.html scaricato ({size_kb} KB) → {index_path}")
        return index_path
    except Exception as e:
        print(f"  ⚠️  Download fallito ({e}) — apro il sito online")
        return None

def main():
    print()
    print("  FIP Sardegna — Deploy automatico")
    print("  " + "-" * 42)

    token    = leggi_file("token.txt")
    template = leggi_file("template.html")

    # Verifica token
    print("\n  Verifica connessione GitHub...")
    utente = api("GET", "/user", token)
    print(f"  Connesso come: {utente.get('login','?')}")

    # SHA attuale del file
    print(f"\n  Lettura file remoto...")
    info = api("GET", f"/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/{FILE_PATH}?ref={BRANCH}", token)
    sha_attuale = info.get("sha", "")

    # Carica il template
    print(f"\n  Caricamento template.html...")
    contenuto_b64 = base64.b64encode(template.encode("utf-8")).decode("ascii")
    risposta = api("PUT",
        f"/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/{FILE_PATH}",
        token,
        {
            "message": "Aggiornamento template via deploy.py",
            "content": contenuto_b64,
            "sha":     sha_attuale,
            "branch":  BRANCH
        }
    )
    commit_sha = risposta.get("commit", {}).get("sha", "")[:10]
    print(f"  OK - Commit: {commit_sha}")

    # Aspetta che il workflow parta
    print(f"\n  Build avviato - attendo GitHub Actions...")
    time.sleep(8)

    # Polling stato workflow
    inizio = time.time()
    run_id = None
    stato_finale = None
    i = 0

    while time.time() - inizio < MAX_WAIT:
        runs = api("GET",
            f"/repos/{GITHUB_USER}/{GITHUB_REPO}/actions/runs?branch={BRANCH}&per_page=5",
            token
        )
        run_list = runs.get("workflow_runs", [])

        if not run_id and run_list:
            run_id = run_list[0].get("id")

        if run_id:
            run = api("GET", f"/repos/{GITHUB_USER}/{GITHUB_REPO}/actions/runs/{run_id}", token)
            status     = run.get("status", "")
            conclusion = run.get("conclusion", "")
            elapsed    = int(time.time() - inizio)

            if status == "completed":
                stato_finale = conclusion
                print(f"\r" + " " * 55)
                break
            else:
                label = "in coda..." if status == "queued" else f"in esecuzione... ({elapsed}s)"
                spinner(label, i)
        else:
            spinner("in attesa che il workflow parta...", i)

        i += 1
        time.sleep(POLL_INTERVAL)

    # Risultato
    print()
    print("  " + "-" * 42)

    if stato_finale == "success":
        print("\n  BUILD COMPLETATO!")
        # Aspetta qualche secondo che GitHub Pages propague il nuovo file
        print("  Attendo propagazione GitHub Pages (15s)...")
        time.sleep(15)
        # Scarica il nuovo index localmente
        index_locale = scarica_index()
        if index_locale:
            print(f"\n  Apertura dashboard locale...")
            webbrowser.open(f"file:///{index_locale.replace(os.sep, '/')}")
        else:
            print(f"\n  Apertura dashboard online...")
            webbrowser.open(DASHBOARD_URL)
        print(f"  {DASHBOARD_URL}")
    elif stato_finale:
        print(f"\n  ERRORE nel build: {stato_finale}")
        print(f"  Controlla: {ACTIONS_URL}")
        webbrowser.open(ACTIONS_URL)
    else:
        print(f"\n  TIMEOUT ({MAX_WAIT}s) - il build sta ancora girando.")
        print(f"  Segui qui: {ACTIONS_URL}")
        webbrowser.open(ACTIONS_URL)

    print()
    input("  Premi Invio per chiudere...")

if __name__ == "__main__":
    main()
