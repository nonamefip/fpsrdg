import subprocess, shutil, os, sys, time, datetime, webbrowser

SITO_URL     = "https://nonamefip.github.io/fpsrdg/"
TEMPLATE_SRC = "template.html"
TEMPLATE_DST = "scripts/template.html"
DEPLOY_DURATA = 65

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

print("\n\033[1;34m======================================================")
print("  DEPLOY FIP SARDEGNA (veloce)")
print("======================================================\033[0m\n")

# 1) Copia template
shutil.copy(TEMPLATE_SRC, TEMPLATE_DST)
ok(f"template.html → {TEMPLATE_DST}")

# 2) Build
print("\n  Build dashboard...")
r = subprocess.run([sys.executable, "build.py"], text=True)
if r.returncode != 0:
    err("build.py ha restituito errori.")
    input("\nPremi Invio per uscire...")
    sys.exit(1)
ok("Build completato → docs/index.html")

# 3) Timestamp
ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
with open("deploy_ts.txt", "w") as f:
    f.write(ts)

# 4) Git
run("git stash --include-untracked", check=False)
run("git pull --rebase origin main", check=False)
run("git stash pop", check=False)
run("git add template.html scripts/template.html docs/index.html deploy_ts.txt")
if os.path.exists("fip_sarda_data.json"):
    run("git add fip_sarda_data.json")

status = run("git status --porcelain", check=False)
if not status.stdout.strip():
    info("Nessuna modifica da committare.")
    input("\nPremi Invio per uscire...")
    sys.exit(0)

run(f'git commit -m "deploy {ts}"')
ok("Commit fatto")
run("git push")
ok("Push completato!")

print(f"\n  Attendo {DEPLOY_DURATA}s per GitHub Pages...")
for i in range(DEPLOY_DURATA, 0, -1):
    print(f"\r  {i}s...", end="", flush=True)
    time.sleep(1)
print()

webbrowser.open(SITO_URL)
ok(f"Sito aperto: {SITO_URL}")
input("\nPremi Invio per chiudere...")
