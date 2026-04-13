import subprocess, shutil, os, sys, time, datetime, webbrowser

# ── CONFIGURAZIONE ──────────────────────────────────────────────────────────
SITO_URL    = "https://nonamefip.github.io/fpsrdg/"
TEMPLATE_SRC = "template.html"          # file che metti nella cartella fpsrdg
TEMPLATE_DST = "scripts/template.html"  # dove il build.py lo legge
DEPLOY_DURATA = 65                       # secondi attesi per GitHub Actions
# ────────────────────────────────────────────────────────────────────────────

def banner(testo, colore="\033[1;36m"):
    reset = "\033[0m"
    linea = "=" * 54
    print(f"\n{colore}{linea}")
    print(f"  {testo}")
    print(f"{linea}{reset}")

def ok(msg):   print(f"\033[1;32m  OK  {msg}\033[0m")
def info(msg): print(f"\033[0;37m  --  {msg}\033[0m")
def err(msg):  print(f"\033[1;31m  !!  {msg}\033[0m")

def run(cmd, check=True):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and r.returncode != 0:
        err(f"Comando fallito: {cmd}")
        err(r.stderr.strip())
        input("\nPremi Invio per uscire...")
        sys.exit(1)
    return r

def countdown(secondi):
    """Countdown live con barra di avanzamento"""
    print()
    larghezza = 40
    for i in range(secondi, -1, -1):
        rimanenti = i
        trascorsi = secondi - i
        pct = trascorsi / secondi
        blocchi = int(pct * larghezza)
        barra = "█" * blocchi + "░" * (larghezza - blocchi)
        fine_ora = datetime.datetime.now() + datetime.timedelta(seconds=rimanenti)
        ora_fine = fine_ora.strftime("%H:%M:%S")

        if rimanenti == 0:
            print(f"\r  \033[1;32m[{barra}] PRONTO! Apro il sito...\033[0m          ", end="", flush=True)
        else:
            print(f"\r  \033[1;33m[{barra}]\033[0m {rimanenti:>3}s  →  pronto alle {ora_fine}", end="", flush=True)
            time.sleep(1)
    print()

# ══════════════════════════════════════════════════════════
banner("DEPLOY FIP SARDEGNA", "\033[1;34m")

# 1) Copia template
if not os.path.exists(TEMPLATE_SRC):
    err(f"File non trovato: {TEMPLATE_SRC}")
    err("Assicurati che template.html sia in questa cartella.")
    input("\nPremi Invio per uscire...")
    sys.exit(1)

os.makedirs("scripts", exist_ok=True)
shutil.copy(TEMPLATE_SRC, TEMPLATE_DST)
ok(f"template.html copiato in {TEMPLATE_DST}")

# 2) Genera timestamp per forzare diff
ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
with open("deploy_ts.txt", "w") as f:
    f.write(ts)
info(f"Timestamp: {ts}")

# 3) Sincronizza con remote PRIMA di fare add/commit
banner("Sincronizzazione con remote...", "\033[1;33m")
# Stash temporaneo per le modifiche locali non ancora committate
stash = run("git stash --include-untracked", check=False)
stashed = "No local changes" not in stash.stdout and stash.returncode == 0
if stashed:
    info("Modifiche locali messe in stash temporaneo")

pull = run("git pull --rebase origin main", check=False)
if pull.returncode != 0:
    # Ripristina lo stash in caso di errore
    if stashed:
        run("git stash pop", check=False)
    err("Pull fallito. Risolvi i conflitti manualmente e riprova.")
    err(pull.stderr.strip())
    input("\nPremi Invio per uscire...")
    sys.exit(1)
ok("Sincronizzazione completata")

# Ripristina le modifiche locali (template + timestamp già copiati)
if stashed:
    run("git stash pop", check=False)
    info("Modifiche locali ripristinate")

# 4) Git add
run("git add template.html scripts/template.html deploy_ts.txt")
ok("git add completato")

# 5) Check se c'e' qualcosa da committare
status = run("git status --porcelain", check=False)
if not status.stdout.strip():
    info("Nessuna modifica rilevata da git.")
    info("Il template e' identico all'ultima versione deployata.")
    input("\nPremi Invio per uscire...")
    sys.exit(0)

# 6) Commit
msg = f"deploy {ts}"
run(f'git commit -m "{msg}"')
ok(f"Commit: {msg}")

# 7) Push
banner("Invio a GitHub...", "\033[1;33m")
run("git push")
ok("Push completato!")

# 8) Countdown + apertura sito
banner(f"GitHub Actions in corso — attesa {DEPLOY_DURATA}s", "\033[1;35m")
info(f"Sito: {SITO_URL}")
print()

countdown(DEPLOY_DURATA)

# 9) Apri il browser
banner("Apro il sito!", "\033[1;32m")
webbrowser.open(SITO_URL)
ok(f"Browser aperto su {SITO_URL}")
info("Se non vedi le modifiche subito, premi Ctrl+Shift+R per forzare il refresh.")
print()
input("Premi Invio per chiudere...")
