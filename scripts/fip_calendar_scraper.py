#!/usr/bin/env python3
"""
FIP Calendar Scraper — Sardegna RSA
Costruisce la struttura COMPLETA dei calendari da cache/fip_sardegna_cache.json:
  - Fasi (Qualificazione, Seconda Fase Nord/Sud, Playoff, Playout, ecc.)
  - Gironi per fase
  - Giornate numerate con squadre
  - Classifica per girone (punti, V/S/P, PF/PS, diff)

Legge:  cache/fip_sardegna_cache.json  (già esistente)
Scrive: cache/fip_calendari.json       (nuovo)

gen_data.py legge fip_calendari.json e lo inietta come D['calendari']

Uso:
    python3 fip_calendar_scraper.py
    python3 fip_calendar_scraper.py --rebuild
    python3 fip_calendar_scraper.py --camp "UNDER 17 MASCHILE GOLD"
"""

import json, os, re, sys, time, random, argparse
from datetime import date
from collections import defaultdict, Counter

CACHE_IN  = "cache/fip_sardegna_cache.json"
CACHE_OUT = "cache/fip_calendari.json"

# Ordine logico delle fasi
FASE_ORDER = [
    "Qualificazione", "Prima Fase", "Fase 1",
    "Seconda Fase", "Seconda fase",
    "Seconda Fase Nord", "Seconda Fase Sud",
    "Terza Fase", "Fase Orologio", "Fase Regionale",
    "Fase di Classificazione", "Fase di Classificazi",
    "Playoff Semifinale", "Semifinale",
    "Quarti di Finale", "Quarto",
    "Finale Playoff", "Finale",
    "Playout",
]

FASE_TIPO = {
    "Qualificazione": "girone", "Prima Fase": "girone", "Fase 1": "girone",
    "Seconda Fase": "girone", "Seconda fase": "girone",
    "Seconda Fase Nord": "girone", "Seconda Fase Sud": "girone",
    "Terza Fase": "girone", "Fase Orologio": "girone",
    "Fase Regionale": "girone",
    "Fase di Classificazione": "girone", "Fase di Classificazi": "girone",
    "Playoff Semifinale": "playoff", "Semifinale": "playoff",
    "Quarti di Finale": "playoff", "Quarto": "playoff",
    "Finale Playoff": "playoff", "Finale": "playoff",
    "Playout": "playoff",
}

MESI_SHORT = {
    "01":"gen","02":"feb","03":"mar","04":"apr","05":"mag","06":"giu",
    "07":"lug","08":"ago","09":"set","10":"ott","11":"nov","12":"dic"
}


def fase_sort_key(nome):
    for i, f in enumerate(FASE_ORDER):
        if f.lower() in nome.lower():
            return i
    return 99

def tipo_fase(nome):
    for f, t in FASE_TIPO.items():
        if f.lower() in nome.lower():
            return t
    return "girone"

def fmt_date(d):
    try:
        y, m, dd = d.split("-")
        return f"{int(dd)} {MESI_SHORT.get(m, m)}"
    except Exception:
        return d

def date_label(gare):
    dates = sorted(set(g.get("Data","") for g in gare if g.get("Data","")))
    if not dates:
        return ""
    if len(dates) == 1:
        return fmt_date(dates[0])
    return f"{fmt_date(dates[0])}–{fmt_date(dates[-1])}"


def build_giornate(gare_g):
    """
    Raggruppa le gare in 'giornate' con finestra 7 giorni (weekend FIP).
    Partite del ven-dom (o recuperi ravvicinati) vanno nella stessa giornata.
    Ogni squadra appare max 1 volta per giornata.
    """
    from datetime import datetime, timedelta

    gare_sorted = sorted(gare_g, key=lambda x: (x.get("Data",""), x.get("Ora","")))

    gare_left = list(gare_sorted)
    giornate = []
    # Punti cumulativi per squadra (per annotare pt progressivi)
    squadre_all = set(g["Squadra Casa"] for g in gare_sorted) | set(g["Squadra Ospite"] for g in gare_sorted)
    pt_cum = {sq: 0 for sq in squadre_all}

    safety = 0
    WINDOW_DAYS = 6  # venerdì → domenica + eventuale lunedì

    while gare_left and safety < 500:
        safety += 1

        first_date_str = gare_left[0].get("Data", "")
        try:
            first_date = datetime.strptime(first_date_str, "%Y-%m-%d")
            window_end = first_date + timedelta(days=WINDOW_DAYS)
        except Exception:
            first_date = window_end = None

        turno = []
        sq_usate = set()
        da_rimuovere = []

        for idx, g in enumerate(gare_left):
            c, o = g["Squadra Casa"], g["Squadra Ospite"]
            if c in sq_usate or o in sq_usate:
                continue
            # Controlla finestra temporale
            if window_end:
                try:
                    g_date = datetime.strptime(g.get("Data",""), "%Y-%m-%d")
                    if g_date > window_end:
                        continue
                except Exception:
                    pass
            turno.append(g)
            sq_usate.add(c)
            sq_usate.add(o)
            da_rimuovere.append(idx)

        if not turno:
            # Forza prima gara rimanente
            turno = [gare_left[0]]
            da_rimuovere = [0]

        da_rimuovere.reverse()
        for idx in da_rimuovere:
            gare_left.pop(idx)

        # Annota punti cumulativi prima di questa giornata
        for g in turno:
            g["_pt_cum_casa"] = pt_cum.get(g["Squadra Casa"], 0)
            g["_pt_cum_osp"]  = pt_cum.get(g["Squadra Ospite"], 0)

        # Aggiorna pt cumulativi con questa giornata
        for g in turno:
            if not g.get("Risultato"):
                continue
            try:
                pc, po = int(g["Punti Casa"]), int(g["Punti Ospite"])
                if pc > po:
                    pt_cum[g["Squadra Casa"]] = pt_cum.get(g["Squadra Casa"], 0) + 2
                elif po > pc:
                    pt_cum[g["Squadra Ospite"]] = pt_cum.get(g["Squadra Ospite"], 0) + 2
            except Exception:
                pass

        giornate.append({
            "n": len(giornate) + 1,
            "gare": turno,
            "completa": all(g.get("Risultato") for g in turno),
            "data_label": date_label(turno),
        })

    return giornate


def build_classifica(gare_g, squadre):
    stats = {sq: {"sq":sq,"g":0,"v":0,"s":0,"par":0,"pf":0,"ps":0} for sq in squadre}
    for g in gare_g:
        if not g.get("Risultato"):
            continue
        try:
            pc, po = int(g["Punti Casa"]), int(g["Punti Ospite"])
        except (ValueError, KeyError, TypeError):
            continue
        c, o = g["Squadra Casa"], g["Squadra Ospite"]
        if c in stats:
            stats[c]["g"]+=1; stats[c]["pf"]+=pc; stats[c]["ps"]+=po
            if pc>po: stats[c]["v"]+=1
            elif pc<po: stats[c]["s"]+=1
            else: stats[c]["par"]+=1
        if o in stats:
            stats[o]["g"]+=1; stats[o]["pf"]+=po; stats[o]["ps"]+=pc
            if po>pc: stats[o]["v"]+=1
            elif po<pc: stats[o]["s"]+=1
            else: stats[o]["par"]+=1

    def punti(st): return st["v"]*2 + st["par"]

    cl = sorted(stats.values(), key=lambda st: (-punti(st), -(st["pf"]-st["ps"]), -st["pf"]))
    for i, st in enumerate(cl):
        st["punti"] = punti(st)
        st["diff"]  = st["pf"] - st["ps"]
        st["pos"]   = i + 1
    return cl


def build_calendario(gare_camp):
    """
    Costruisce la struttura completa fase → girone → giornate + classifica
    per tutte le gare di un campionato.
    """
    fasi_dict = defaultdict(lambda: defaultdict(list))
    for g in gare_camp:
        fase  = (g.get("Fase","") or "").strip() or "Qualificazione"
        giron = (g.get("Girone","") or "").strip() or "Girone Unico"
        fasi_dict[fase][giron].append(g)

    fasi_out = []
    for fase_nome, gironi_dict in fasi_dict.items():
        tipo = tipo_fase(fase_nome)
        gironi_out = []

        for girone_nome, gare_g in gironi_dict.items():
            squadre = sorted(set(
                g["Squadra Casa"] for g in gare_g
            ) | set(
                g["Squadra Ospite"] for g in gare_g
            ))

            giornate  = build_giornate(gare_g)
            classifica = build_classifica(gare_g, squadre) if tipo == "girone" else []

            # Per i playoff: serie (accoppiamenti)
            serie = []
            if tipo == "playoff":
                accoppiamenti = defaultdict(list)
                for g in sorted(gare_g, key=lambda x: x.get("Data","")):
                    key = tuple(sorted([g["Squadra Casa"], g["Squadra Ospite"]]))
                    accoppiamenti[key].append(g)
                for (sq_a, sq_b), gare_s in sorted(accoppiamenti.items()):
                    vA=vB=0
                    for g in gare_s:
                        if not g.get("Risultato"): continue
                        try:
                            pc,po=int(g["Punti Casa"]),int(g["Punti Ospite"])
                            mA = pc if g["Squadra Casa"]==sq_a else po
                            mB = po if g["Squadra Casa"]==sq_a else pc
                            if mA>mB: vA+=1
                            elif mB>mA: vB+=1
                        except: pass
                    serie.append({
                        "sq_a": sq_a, "sq_b": sq_b,
                        "vA": vA, "vB": vB,
                        "gare": [{"d":g.get("Data",""),"pc":g.get("Punti Casa",""),
                                  "po":g.get("Punti Ospite",""),"ris":g.get("Risultato",""),
                                  "num":g.get("Numero Gara","")} for g in gare_s],
                    })

            gironi_out.append({
                "nome":       girone_nome,
                "squadre":    squadre,
                "n_squadre":  len(squadre),
                "n_gare":     len(gare_g),
                "giornate":   giornate,
                "n_giornate": len(giornate),
                "classifica": classifica,
                "serie":      serie,
            })

        gironi_out.sort(key=lambda x: x["nome"])
        fasi_out.append({
            "nome":     fase_nome,
            "tipo":     tipo,
            "sort_key": fase_sort_key(fase_nome),
            "n_gironi": len(gironi_out),
            "gironi":   gironi_out,
        })

    fasi_out.sort(key=lambda x: x["sort_key"])
    return fasi_out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--camp", type=str, default=None)
    args = parser.parse_args()

    print(f"=== FIP Calendar Scraper — {date.today()} ===")

    if not os.path.exists(CACHE_IN):
        print(f"❌ {CACHE_IN} non trovato. Esegui prima fip_scraper.py")
        sys.exit(1)

    with open(CACHE_IN, encoding="utf-8") as f:
        gare_raw = json.load(f)

    gare = [g for g in gare_raw if g.get("Data","") >= "2025-09-01"]
    print(f"Gare in cache: {len(gare)}")

    tutti_camp = sorted(set(g["Campionato"] for g in gare if g.get("Campionato","")))
    if args.camp:
        tutti_camp = [c for c in tutti_camp if args.camp.lower() in c.lower()]
        if not tutti_camp:
            print(f"❌ Campionato '{args.camp}' non trovato"); sys.exit(1)

    print(f"Campionati: {len(tutti_camp)}")

    cal_existing = {}
    if not args.rebuild and os.path.exists(CACHE_OUT):
        with open(CACHE_OUT, encoding="utf-8") as f:
            cal_existing = json.load(f)
        print(f"Cache esistente: {len(cal_existing)} campionati")

    calendari = dict(cal_existing)

    for i, camp in enumerate(tutti_camp, 1):
        gare_camp = [g for g in gare if g.get("Campionato","") == camp]
        if not gare_camp:
            continue

        fasi  = build_calendario(gare_camp)
        fasi_n = [f["nome"] for f in fasi]
        date_list = sorted(g.get("Data","") for g in gare_camp if g.get("Data",""))
        gare_con_ris = sum(1 for g in gare_camp if g.get("Risultato",""))

        calendari[camp] = {
            "campionato":    camp,
            "n_gare":        len(gare_camp),
            "n_giocate":     gare_con_ris,
            "n_programmate": len(gare_camp) - gare_con_ris,
            "data_inizio":   date_list[0] if date_list else "",
            "data_fine":     date_list[-1] if date_list else "",
            "n_fasi":        len(fasi),
            "fasi_nomi":     fasi_n,
            "fasi":          fasi,
            "last_update":   str(date.today()),
        }

        # Log
        print(f"[{i}/{len(tutti_camp)}] {camp[:55]}")
        for fase in fasi:
            for g in fase["gironi"]:
                cl_top = [r["sq"][:20] for r in g["classifica"][:3]] if g["classifica"] else []
                print(f"  {fase['nome'][:25]:25} | {g['nome'][:20]:20} | {g['n_squadre']:2}sq | {g['n_giornate']:2}gg | Top: {cl_top}")

    os.makedirs("cache", exist_ok=True)
    with open(CACHE_OUT, "w", encoding="utf-8") as f:
        json.dump(calendari, f, ensure_ascii=False, indent=2)

    size = os.path.getsize(CACHE_OUT)
    print(f"\n✅ {CACHE_OUT}: {size//1024} KB — {len(calendari)} campionati")

    # Riepilogo fasi trovate
    all_fasi = Counter()
    for cal in calendari.values():
        for fase in cal["fasi"]:
            all_fasi[fase["nome"]] += 1
    print("\nFasi trovate:")
    for f, n in sorted(all_fasi.items(), key=lambda x: -x[1]):
        print(f"  {n:3d}x  {f}")


if __name__ == "__main__":
    main()
