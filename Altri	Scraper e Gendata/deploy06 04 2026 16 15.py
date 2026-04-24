#!/usr/bin/env python3
"""
deploy.py — Carica template.html su GitHub via git (nessun limite di dimensione).
Metti questo file nella stessa cartella di template.html e token.txt.
Richiede git installato sul PC (https://git-scm.com/download/win).
"""

import os, sys, time, webbrowser, subprocess, shutil, tempfile
import urllib.request, urllib.error, json

# ── Configurazione ───────────────────────────────────────────────
GITHUB_USER   = "nonamefip"
GITHUB_REPO   = "fpsrdg"
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

def leggi_file_bytes(nome):
    cartella = os.path.dirname(os.path.abspath(__file__))
    percorso = os.path.join(cartella, nome)
    if not os.path.exists(percorso):
        print(f"\n❌  File non trovato: {percorso}")
        sys.exit(1)
    return percorso  # ritorna il path, non il contenuto

def git(*args, cwd=None, check=True):
    """Esegue un comando git e ritorna l'output."""
    cmd = ["git"] + list(args)
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"\n❌  git {' '.join(args)} fallito:")
        print(result.stderr or result.stdout)
        sys.exit(1)
    return result.stdout.strip()

def git_disponibile():
    try:
        subprocess.run(["git", "--version"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False

def api(metodo, endpoint, token, dati=None):
    """Chiamata GitHub API (solo per GET — polling workflow)."""
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
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"\n❌  Errore GitHub API {e.code}: {err}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌  Errore connessione: {e}")
        sys.exit(1)

def spinner(msg, i):
    chars = ["|", "/", "-", "\\"]
    print(f"\r   {chars[i % len(chars)]}  {msg}   ", end="", flush=True)

def scarica_index():
    """Scarica il nuovo index.html da GitHub Pages nella cartella locale."""
    cartella = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(cartella, "index.html")
    print(f"\n  Download index aggiornato da GitHub Pages...")
    try:
        req = urllib.request.Request(
            DASHBOARD_URL,
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache"}
        )
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

def upload_via_git(token, template_path):
    """Carica il file su GitHub usando git clone → copy → commit → push."""
    repo_url = f"https://{GITHUB_USER}:{token}@github.com/{GITHUB_USER}/{GITHUB_REPO}.git"
    cartella = os.path.dirname(os.path.abspath(template_path))
    tmpdir = tempfile.mkdtemp(prefix="fpsrdg_deploy_")
    try:
        print(f"  Clone repository...")
        git("clone", "--depth=1", "--branch", BRANCH, repo_url, tmpdir, check=True)

        # Copia il template nella cartella scripts/
        dest = os.path.join(tmpdir, "scripts", "template.html")
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copy2(template_path, dest)

        size_kb = os.path.getsize(dest) // 1024
        print(f"  File copiato ({size_kb} KB)")

        # Copia data_v5_new.json nella cartella cache/ se esiste in locale
        data_src = os.path.join(cartella, "data_v5_new.json")
        if os.path.exists(data_src):
            data_dest = os.path.join(tmpdir, "cache", "data_v5_new.json")
            os.makedirs(os.path.dirname(data_dest), exist_ok=True)
            shutil.copy2(data_src, data_dest)
            size_kb2 = os.path.getsize(data_dest) // 1024
            print(f"  data_v5_new.json copiato ({size_kb2} KB)")

        # Configura git identity (necessario per il commit)
        git("config", "user.email", "deploy@fpsrdg.local", cwd=tmpdir)
        git("config", "user.name", "FPS Deploy", cwd=tmpdir)

        # Controlla se ci sono modifiche
        status = git("status", "--porcelain", cwd=tmpdir)
        if not status:
            print("  ℹ️  Nessuna modifica rilevata — il file è già aggiornato.")
            return None

        git("add", "scripts/template.html", cwd=tmpdir)
        git("add", "cache/data_v5_new.json", cwd=tmpdir, check=False)
        result_commit = subprocess.run(
            ["git", "commit", "-m", "Aggiornamento template via deploy.py"],
            cwd=tmpdir, capture_output=True, text=True
        )
        if result_commit.returncode != 0:
            if "nothing to commit" in result_commit.stdout or "nothing to commit" in result_commit.stderr:
                print("  ℹ️  Il file su GitHub è già aggiornato — nessun commit necessario.")
                return None
            else:
                print(f"\n❌  git commit fallito:\n{result_commit.stderr}")
                sys.exit(1)
        print(f"  Commit creato")

        print(f"  Push su GitHub...")
        git("push", "origin", BRANCH, cwd=tmpdir)

        # Ritorna il commit SHA
        sha = git("rev-parse", "--short", "HEAD", cwd=tmpdir)
        return sha
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def upload_via_api_fallback(token, template_path):
    """Fallback: usa GitHub API con chunked encoding (per file < 50MB)."""
    import base64
    print("  Uso GitHub API (fallback)...")
    with open(template_path, "rb") as f:
        contenuto_b64 = base64.b64encode(f.read()).decode("ascii")

    info = api("GET", f"/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/scripts/template.html?ref={BRANCH}", token)
    sha_attuale = info.get("sha", "")

    risposta = api("PUT",
        f"/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/scripts/template.html",
        token,
        {
            "message": "Aggiornamento template via deploy.py",
            "content": contenuto_b64,
            "sha":     sha_attuale,
            "branch":  BRANCH
        }
    )
    return risposta.get("commit", {}).get("sha", "")[:10]

def main():
    print()
    print("  FIP Sardegna — Deploy automatico")
    print("  " + "-" * 42)

    token = leggi_file("token.txt")
    cartella = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(cartella, "template.html")

    if not os.path.exists(template_path):
        print(f"\n❌  template.html non trovato in {cartella}")
        sys.exit(1)

    size_kb = os.path.getsize(template_path) // 1024
    print(f"\n  Template: {size_kb} KB")

    # Verifica connessione GitHub
    print("\n  Verifica connessione GitHub...")
    utente = api("GET", "/user", token)
    print(f"  Connesso come: {utente.get('login','?')}")

    # Scegli metodo di upload
    print(f"\n  Caricamento template.html...")
    commit_sha = None

    if git_disponibile():
        print("  Metodo: git (nessun limite dimensione)")
        commit_sha = upload_via_git(token, template_path)
    else:
        print("  ⚠️  git non trovato — uso GitHub API")
        print("  💡 Suggerimento: installa git da https://git-scm.com/download/win")
        commit_sha = upload_via_api_fallback(token, template_path)

    if commit_sha is None:
        print("\n  ℹ️  Nessuna modifica da deployare.")
        input("\n  Premi Invio per chiudere...")
        return

    print(f"  ✅ Upload completato — commit: {commit_sha}")

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
        print("\n  ✅ BUILD COMPLETATO!")
        print("  Attendo propagazione GitHub Pages (15s)...")
        time.sleep(15)
        index_locale = scarica_index()
        if index_locale:
            print(f"\n  Apertura dashboard locale...")
            webbrowser.open(f"file:///{index_locale.replace(os.sep, '/')}")
        else:
            print(f"\n  Apertura dashboard online...")
            webbrowser.open(DASHBOARD_URL)
        print(f"  {DASHBOARD_URL}")
    elif stato_finale:
        print(f"\n  ❌ ERRORE nel build: {stato_finale}")
        print(f"  Controlla: {ACTIONS_URL}")
        webbrowser.open(ACTIONS_URL)
    else:
        print(f"\n  ⏳ TIMEOUT ({MAX_WAIT}s) - il build sta ancora girando.")
        print(f"  Segui qui: {ACTIONS_URL}")
        webbrowser.open(ACTIONS_URL)

    print()
    input("  Premi Invio per chiudere...")

if __name__ == "__main__":
    main()
