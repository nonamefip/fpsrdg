import subprocess, shutil, os, sys, time, datetime, webbrowser

# ── CONFIGURAZIONE ──────────────────────────────────────────────────────────
SITO_URL     = "https://nonamefip.github.io/fpsrdg/"
TEMPLATE_SRC = "template.html"
TEMPLATE_DST = "scripts/template.html"
DEPLOY_DURATA = 65
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

# 2) Scraping campionati sardi da fip.it
banner("Scraping campionati sardi da fip.it...", "\033[1;36m")
if os.path.exists("fip_scraper_sarda.py"):
    subprocess.run([sys.executable, "fip_scraper_sarda.py"])
    ok("Scraping completato")
else:
    info("fip_scraper_sarda.py non trovato — salto scraping.")

# 3) Build: inserisce __DATA__ + __FIP_SARDA__ e produce docs/index.html
banner("Build dashboard...", "\033[1;36m")
r = subprocess.run([sys.executable, "build.py"], capture_output=False, text=True)
if r.returncode != 0:
    err("build.py ha restituito errori — controlla l'output sopra.")
    input("\nPremi Invio per uscire...")
    sys.exit(1)
ok("Build completato → docs/index.html")

# 4) Genera timestamp
ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
with open("deploy_ts.txt", "w") as f:
    f.write(ts)
info(f"Timestamp: {ts}")

# 5) Sincronizza con remote
banner("Sincronizzazione con remote...", "\033[1;33m")
stash = run("git stash --include-untracked", check=False)
stashed = "No local changes" not in stash.stdout and stash.returncode == 0
if stashed:
    info("Modifiche locali messe in stash temporaneo")

pull = run("git pull --rebase origin main", check=False)
if pull.returncode != 0:
    if stashed:
        run("git stash pop", check=False)
    err("Pull fallito. Risolvi i conflitti manualmente e riprova.")
    input("\nPremi Invio per uscire...")
    sys.exit(1)
ok("Sincronizzazione completata")

if stashed:
    run("git stash pop", check=False)
    info("Modifiche locali ripristinate")

# 6) Git add
run("git add template.html scripts/template.html docs/index.html deploy_ts.txt")
if os.path.exists("fip_sarda_data.json"):
    run("git add fip_sarda_data.json")
ok("git add completato")

# 7) Check modifiche
status = run("git status --porcelain", check=False)
if not status.stdout.strip():
    info("Nessuna modifica rilevata da git.")
    input("\nPremi Invio per uscire...")
    sys.exit(0)

# 8) Commit
msg = f"deploy {ts}"
run(f'git commit -m "{msg}"')
ok(f"Commit: {msg}")

# 9) Push
banner("Invio a GitHub...", "\033[1;33m")
run("git push")
ok("Push completato!")

# 10) Countdown + apertura sito
banner(f"GitHub Actions in corso — attesa {DEPLOY_DURATA}s", "\033[1;35m")
info(f"Sito: {SITO_URL}")
countdown(DEPLOY_DURATA)

# 11) Apri browser
banner("Apro il sito!", "\033[1;32m")
webbrowser.open(SITO_URL)
ok(f"Browser aperto su {SITO_URL}")
info("Se non vedi le modifiche subito, premi Ctrl+Shift+R per forzare il refresh.")
print()
input("Premi Invio per chiudere...")
