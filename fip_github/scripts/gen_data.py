import json, re, datetime
from collections import defaultdict, Counter
from difflib import SequenceMatcher

with open('cache/fip_sardegna_cache.json', encoding='utf-8') as f:
    RAW = json.load(f)

TODAY     = datetime.date.today().isoformat()
YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
RAW_ALL   = [g for g in RAW if g['Data'] >= '2025-09-01']
from datetime import date, timedelta
TODAY_STR = str(date.today())
FUTURE_END = str(date.today() + timedelta(days=14))
RAW_FUTURE = [g for g in RAW if TODAY_STR <= g['Data'] <= FUTURE_END]
print(f"Gare filtrate (dal 01/09/2025): {len(RAW_ALL)}, fino a {YESTERDAY}")
print(f"Gare future (prossimi 14gg): {len(RAW_FUTURE)}")

# ══════════════════════════════════════════════════════════════
# REGOLE MANUALI DI FUSIONE SQUADRE
# ══════════════════════════════════════════════════════════════
MANUAL_MERGES = {
    'BASKET 90':              ['A.S.DIL. BASKET 90'],
    'BASKET GHILARZA':        ['A.S.D. BASKET GHILARZA'],
    'POL. IL GABBIANO':       ['A.POL. DIL. IL GABBIANO'],
    'ASTRO SSDRL':            ['ASTRO SSDRL A','ASTRO SSDRL B'],
    'S. GIOVANNI BATTISTA':   ['GS SAN GIOVANNI BATTISTA A.S.D'],
    'SHARDANA BASKET':        ['SHARDANA BASKET SMdP'],
    'BASKET IGLESIAS':        ['ASD BASKET IGLESIAS 2010'],
    'BASKET LANUSEI':         ['BASKET LANUSEI (JERZU)'],
    'EOS SPORT BLAZERS':      ['ASD EOS SPORT'],
    'AZZURRA BASKET':         ['AZZURRA BASKET ORISTANO A.S.D.'],
    'ROOSTERS TEMPIO':        ['ROOSTERS AMPURIAS ROCKETS'],
    'PANDA 76ERS':            ['PANDA PISTONS'],
    'ICHNOS NUORO':           ['ICHNOS TIMBERWOLVES'],
    'PALL. OROSEI':           ['OROSEI PELICANS'],
    'POL. SERRAMANNA':        ['ASD POL. ATLETICA SERRAMANNA'],
    'AUREA SASSARI':          ['SCUOLA BASKET SS WARRIORS','FENIX SASSARI A.S.D.','MASTERS SASSARI','FISIOKONS AUREA SASSARI'],
}

prov_re = re.compile(r'\bdi\s+.+?\s+\((\w{2,3})\)\s*$', re.IGNORECASE)

# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
def parse_person(s):
    if not s: return None
    s = s.strip()
    sl = s.lower()
    # Use word boundaries - avoid matching 'nd' inside names like ANDREA, ALESSANDRA
    if any(x in sl for x in ('attesa','designazione','n.d.')): return None
    if re.search(r'\bn\.?d\.?\b', sl): return None
    if sl.strip() in ('','-'): return None
    m = prov_re.search(s)
    idx = s.lower().rfind(' di ')
    if m and idx > 0:
        nome = s[:idx].strip()
        rest = s[idx+4:].strip()
        cm2  = re.match(r'(.+?)\s*\((\w{2,3})\)', rest)
        return {'nome': nome, 'citta': cm2.group(1).strip() if cm2 else '', 'provincia': m.group(1).upper()}
    return None

def campo_prov(campo):
    m = re.search(r'\(\s*(\w{2,3})\s*\)\s*$', campo or '')
    return m.group(1).upper() if m else ''

def is_femminile(camp):
    c = (camp or '').upper()
    return any(x in c for x in ['FEMM','FEMMINILE','DONNE','WOMEN'])

def is_minibasket(camp):
    c = (camp or '').upper()
    return any(x in c for x in ['AQUILOTTI','SCOIATTOLI','GAZZELLE','LIBELLULE','ESORDIENTI','TROFEO'])

def norm_sq(s):
    s = s.upper().strip()
    for r in ['A.S.D.','ASD','S.S.D.','SSD','S.S.','A.S.','A.P.D.','APD','P.G.S.','PGS',
              'PALLACANESTRO','PALL.','BASKET','ASSOCIAZIONE SPORTIVA','POL.','POLISPORTIVA',
              'A.S.DIL.','DIL.','G.S.DIL.','GSD','G.S.','A.POL.','NEW','NUOVA','NUOVO',
              'SPORT','CLUB','SCUOLA']:
        s = s.replace(r,'').strip()
    s = re.sub(r'[.\-_/()]',' ', s)
    return re.sub(r'\s+', ' ', s).strip()

# ══════════════════════════════════════════════════════════════
# CLASSIFICAZIONE CAMPIONATI
# ══════════════════════════════════════════════════════════════
def classify_camp(c):
    cu = c.upper()
    gen = 'F' if is_femminile(c) else ('X' if is_minibasket(c) else 'M')
    if   'SERIE A' in cu:               grp='Serie A';      lv=''
    elif 'SERIE B' in cu:               grp='Serie B';      lv=''
    elif 'SERIE C' in cu:               grp='Serie C';      lv=''
    elif re.search(r'DIV.*REG.*1|DIVISIONE.*1', cu): grp='Div. Reg. 1'; lv=''
    elif re.search(r'DIV.*REG.*2|DIVISIONE.*2', cu): grp='Div. Reg. 2'; lv=''
    elif 'UNDER 19' in cu: grp='Under 19'; lv='Gold' if 'GOLD' in cu else ('Eccellenza' if 'ECCEL' in cu else 'Regionale')
    elif 'UNDER 17' in cu: grp='Under 17'; lv='Gold' if 'GOLD' in cu else ('Eccellenza' if 'ECCEL' in cu else 'Regionale')
    elif 'UNDER 15' in cu: grp='Under 15'; lv='Gold' if 'GOLD' in cu else ('Eccellenza' if 'ECCEL' in cu else 'Regionale')
    elif 'UNDER 14' in cu: grp='Under 14'; lv='Gold' if 'GOLD' in cu else ('Eccellenza' if 'ECCEL' in cu else 'Regionale')
    elif 'UNDER 13' in cu: grp='Under 13'; lv='Gold' if 'GOLD' in cu else ('Eccellenza' if 'ECCEL' in cu else 'Regionale')
    elif 'ESORDIENTI' in cu or 'TROFEO' in cu: grp='Esordienti'; lv='Big' if 'BIG' in cu else ('Small' if 'SMALL' in cu else 'Trofeo' if 'TROFEO' in cu else '')
    elif 'AQUILOTTI' in cu:  grp='Aquilotti';  lv='Big' if 'BIG' in cu else 'Small'
    elif 'SCOIATTOLI' in cu: grp='Scoiattoli'; lv='Big' if 'BIG' in cu else 'Small'
    elif 'GAZZELLE' in cu:   grp='Gazzelle';   lv='Big' if 'BIG' in cu else 'Small'
    elif 'LIBELLULE' in cu:  grp='Libellule';  lv='Big' if 'BIG' in cu else 'Small'
    else: grp=c; lv=''
    ORDER = {'Serie A':0,'Serie B':1,'Serie C':2,'Div. Reg. 1':3,'Div. Reg. 2':4,
             'Under 19':5,'Under 17':6,'Under 15':7,'Under 14':8,'Under 13':9,
             'Esordienti':10,'Aquilotti':11,'Scoiattoli':12,'Gazzelle':13,'Libellule':14}
    sk = (ORDER.get(grp,99), 0 if gen=='M' else (1 if gen=='F' else 2), lv)
    return grp, gen, lv, sk

all_camp_names = sorted(set(g['Campionato'] for g in RAW_ALL if g.get('Campionato')))
camp_meta = {}
for c in all_camp_names:
    grp, gen, lv, sk = classify_camp(c)
    camp_meta[c] = {'gruppo': grp, 'genere': gen, 'livello': lv, 'sort_key': list(sk)}

# ══════════════════════════════════════════════════════════════
# SQUAD GROUPING
# ══════════════════════════════════════════════════════════════
squad_campos = defaultdict(set)
for g in RAW_ALL:
    squad_campos[g['Squadra Casa']].add(g.get('Campo',''))

squad_home_prov  = {}
squad_main_campo = {}
for g in RAW_ALL:
    pv = campo_prov(g.get('Campo',''))
    if pv:
        if g['Squadra Casa'] not in squad_home_prov: squad_home_prov[g['Squadra Casa']] = Counter()
        squad_home_prov[g['Squadra Casa']][pv] += 1
    if g.get('Campo',''):
        if g['Squadra Casa'] not in squad_main_campo: squad_main_campo[g['Squadra Casa']] = Counter()
        squad_main_campo[g['Squadra Casa']][g['Campo']] += 1
squad_home_prov  = {sq: c.most_common(1)[0][0] for sq,c in squad_home_prov.items()}
squad_main_campo = {sq: c.most_common(1)[0][0] for sq,c in squad_main_campo.items()}

all_squads = sorted(set(g['Squadra Casa'] for g in RAW_ALL) | set(g['Squadra Ospite'] for g in RAW_ALL))

manual_map = {}
for canonical, aliases in MANUAL_MERGES.items():
    for alias in aliases:
        manual_map[alias] = canonical

squad_groups = {}; all_groups = {}; used = set()

for alias, canonical in manual_map.items():
    if alias in all_squads:
        squad_groups[alias] = canonical
        used.add(alias)

for i, s1 in enumerate(all_squads):
    if s1 in used or s1 in squad_groups: continue
    group = [s1]
    n1 = norm_sq(s1); c1 = squad_campos.get(s1, set()) - {''}
    main_c1 = squad_main_campo.get(s1,'')
    for s2 in all_squads[i+1:]:
        if s2 in used or s2 in squad_groups: continue
        n2 = norm_sq(s2); c2 = squad_campos.get(s2, set()) - {''}
        main_c2 = squad_main_campo.get(s2,'')
        ratio = SequenceMatcher(None, n1, n2).ratio()
        shared = c1 & c2
        cnb1=main_c1.split(',')[0].strip().upper() if main_c1 else ''
        cnb2=main_c2.split(',')[0].strip().upper() if main_c2 else ''
        same_main_campo = bool(cnb1 and cnb2 and cnb1==cnb2)
        words1 = set(n1.split()) - {'','A','B','C','1','2','E','O','DI','DA','DEL','DELLA','SRL','DILS','SS','SRL','ASD','ASDS'}
        words2 = set(n2.split()) - {'','A','B','C','1','2','E','O','DI','DA','DEL','DELLA','SRL','DILS','SS','SRL','ASD','ASDS'}
        common = words1 & words2
        # Raggruppa se: nome molto simile E stesso campo principale
        # OPPURE nome quasi identico (>0.85) indipendentemente dal campo
        if (ratio > 0.82) or \
           (ratio > 0.60 and same_main_campo) or \
           (len(common) >= 2 and ratio > 0.50 and (shared or same_main_campo)):
            group.append(s2); used.add(s2)
    if len(group) > 1: used.add(s1)
    canonical = sorted(group, key=lambda x: (len(x), x))[0]
    all_groups[canonical] = group
    for sq in group: squad_groups[sq] = canonical

for alias, canonical in manual_map.items():
    if canonical not in all_groups: all_groups[canonical] = [canonical]
    if alias in all_squads and alias not in all_groups.get(canonical, []):
        all_groups[canonical].append(alias); squad_groups[alias] = canonical
    if alias in all_groups and alias != canonical: del all_groups[alias]

for sq in all_squads:
    if sq not in squad_groups:
        squad_groups[sq] = sq
        if sq not in all_groups: all_groups[sq] = [sq]

print(f"Gruppi squadra multi-variante: {len({k:v for k,v in all_groups.items() if len(v)>1})}")

# ══════════════════════════════════════════════════════════════
# HOME PROV & CAMPO
# ══════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════
# ROLE FIELDS
# ══════════════════════════════════════════════════════════════
ROLE_FIELDS = {
    'Arbitro 1':'Arbitro','Arbitro 2':'Arbitro',
    'Segnapunti':'UDC','Cronometrista':'UDC','24 Secondi':'UDC','Addetto Referto':'UDC',
    'Osservatore':'Osservatore',
}

def campo_nome_breve(campo):
    import re as _re
    m=_re.match(r'^([^,]+)',campo)
    return m.group(1).strip() if m else campo

GENDER_MAP = {
    'AGNESE':'F','ALESSANDRA':'F','ALESSIA':'F','ALICE':'F','BEATRICE':'F',
    'BENEDETTA':'F','CARLA':'F','CHANTAL':'F','CHIARA':'F','CHRISTINE':'F',
    'CLAUDIA':'F','CRISTINA':'F','DALILA':'F','DENISE':'F','ELEKTRA':'F',
    'ELENA':'F','ELISA':'F','EMMYLOU':'F','FEDERICA':'F','FRANCESCA':'F',
    'GABRIELLA':'F','GINEVRA':'F','GIORGIA':'F','GIULIA':'F','GIUSEPPINA':'F',
    'IGNAZIA':'F','ILARIA':'F','ILENIA':'F','IRENE':'F','ISIDORA':'F',
    'JHASMINE':'F','LUDOVICA':'F','MADDALENA':'F','MARIA':'F','MARTINA':'F',
    'MICHELA':'F','MIRIAM':'F','NICOLE':'F','NICOLETTA':'F','PAOLA':'F',
    'ROBERTA':'F','SABRINA':'F','SARA':'F','STEFANIA':'F','VALENTINA':'F',
    'VANESSA':'F',
    'ADRIANO':'M','ALBERTO':'M','ALESSANDRO':'M','ALESSIO':'M','ANDREA':'M',
    'ANTONIO':'M','ARTURO':'M','CHENG':'M','CRISTIAN':'M','DANIELE':'M',
    'DAVIDE':'M','DIEGO':'M','EDOARDO':'M','ELIAS':'M','EMANUEL':'M',
    'ENRICO':'M','ERMANNO':'M','FABIO':'M','FEDELE':'M','FEDERICO':'M',
    'FILIPPO':'M','FRANCESCO':'M','FRANCO':'M','GABRIELE':'M','GIANFRANCO':'M',
    'GIANLUCA':'M','GIANMARCO':'M','GIORDANO':'M','GIOVANNI':'M',
    'HUNTER':'M','LEONARDO':'M','LIBERO':'M','LORENZO':'M','LUCA':'M',
    'MARCO':'M','MARIO':'M','MASSIMILIANO':'M','MASSIMO':'M','MATIJA':'M',
    'MATTEO':'M','MATTIA':'M','MICHELE':'M','NICOLA':'M','NICOLAS':'M',
    'RICCARDO':'M','ROBERTO':'M','SALVATORE':'M','SAMUELE':'M',
    'SILVIO':'M','SIMONE':'M','STEFANO':'M','TOMMASO':'M',
}

def get_genere(nome_full):
    """Estrae il genere dal nome proprio (ultimo token del nome completo)."""
    parts = nome_full.strip().upper().split()
    # Il nome proprio è l'ultimo token (es: CARRUS FABIO → FABIO)
    # Ma per nomi con 3 token tipo MURRU ANDREA SALVATORE prova tutti dal fondo
    for part in reversed(parts[1:]):  # salta il cognome (primo token)
        g = GENDER_MAP.get(part)
        if g: return g
    return '?'

# ══════════════════════════════════════════════════════════════
# PERSONS
# ══════════════════════════════════════════════════════════════
persons = {}
for g in RAW_ALL:
    for field, cat in ROLE_FIELDS.items():
        p = parse_person(g.get(field,''))
        if not p: continue
        pid = p['nome']
        if pid not in persons:
            persons[pid] = {
                'nome':p['nome'],'provincia':p['provincia'],'citta':p['citta'],
                'genere':get_genere(p['nome']),
                'ruoli':set(),'categorie':set(),
                'gare_arbitro':[],'gare_udc':[],'gare_osservatore':[],
                'campionati':Counter(),'giorni':Counter(),'mesi':Counter(),
                'colleghi':Counter(),'campi':Counter(),'squadre':Counter(),
                'squadre_incontrate':defaultdict(lambda:{'gare':0,'vinte':0,'perse':0,'pareggi':0,'gare_list':[]}),
            }
        persons[pid]['ruoli'].add(field)
        persons[pid]['categorie'].add(cat)
        ge = {**g,'_ruolo':field,'_cat':cat}
        if cat=='Arbitro':     persons[pid]['gare_arbitro'].append(ge)
        elif cat=='UDC':       persons[pid]['gare_udc'].append(ge)
        else:                  persons[pid]['gare_osservatore'].append(ge)
        persons[pid]['campionati'][g['Campionato']] += 1
        persons[pid]['giorni'][g['Data']] += 1
        persons[pid]['mesi'][g['Data'][:7]] += 1
        persons[pid]['campi'][g.get('Campo','')] += 1
        persons[pid]['squadre'][g['Squadra Casa']] += 1
        persons[pid]['squadre'][g['Squadra Ospite']] += 1
        for f2 in ROLE_FIELDS:
            if f2==field: continue
            p2 = parse_person(g.get(f2,''))
            if p2 and p2['nome']!=pid: persons[pid]['colleghi'][p2['nome']] += 1
        for sq_key in ['Squadra Casa','Squadra Ospite']:
            sq=g[sq_key]; si=persons[pid]['squadre_incontrate'][sq]
            si['gare']+=1
            si['gare_list'].append({'data':g['Data'],'camp':g['Campionato'],
                'casa':g['Squadra Casa'],'ospite':g['Squadra Ospite'],
                'ris':g.get('Risultato',''),'pc':g.get('Punti Casa',''),
                'po':g.get('Punti Ospite',''),'num':g.get('Numero Gara',''),'sq_role':sq_key})
            if g.get('Risultato'):
                try:
                    pc,po=int(g['Punti Casa']),int(g['Punti Ospite'])
                    sp,ap=(pc,po) if sq_key=='Squadra Casa' else (po,pc)
                    if sp>ap: si['vinte']+=1
                    elif ap>sp: si['perse']+=1
                    else: si['pareggi']+=1
                except: pass

# Categoria = ruolo con più gare (FIX: bi-ruolo)
for pid,p in persons.items():
    counts={'Arbitro':len(p['gare_arbitro']),'UDC':len(p['gare_udc']),'Osservatore':len(p['gare_osservatore'])}
    p['categoria']=max(counts,key=counts.get)

# ══════════════════════════════════════════════════════════════
# SQUADS
# ══════════════════════════════════════════════════════════════
squads = {}
for g in RAW_ALL:
    for role,sq in [('casa',g['Squadra Casa']),('ospite',g['Squadra Ospite'])]:
        if sq not in squads:
            squads[sq]={
                'nome':sq,'prov':squad_home_prov.get(sq,''),
                'campo_principale':squad_main_campo.get(sq,''),
                'gruppo':squad_groups.get(sq,sq),'gare':[],
                'campionati':Counter(),'campionati_meta':{},'gironi_per_camp':defaultdict(set),
                'avversari':Counter(),'avv_vinte':Counter(),'avv_perse':Counter(),
                'arbitri_stats':defaultdict(lambda:{'gare':0,'vinte':0,'perse':0,'pareggi':0,'gare_list':[]}),
                'vinte_casa':0,'perse_casa':0,'par_casa':0,
                'vinte_osp':0,'perse_osp':0,'par_osp':0,
                'punti_fatti':0,'punti_subiti':0,'giorni':Counter(),'mesi':Counter(),'is_femminile':False,
            }
        squads[sq]['gare'].append({**g,'_ruolo':role})
        squads[sq]['campionati'][g['Campionato']]+=1
        if g['Campionato'] not in squads[sq]['campionati_meta']:
            squads[sq]['campionati_meta'][g['Campionato']]=camp_meta.get(g['Campionato'],{})
        if g.get('Girone'): squads[sq]['gironi_per_camp'][g['Campionato']].add(g['Girone'])
        if is_femminile(g['Campionato']): squads[sq]['is_femminile']=True
        squads[sq]['giorni'][g['Data']]+=1; squads[sq]['mesi'][g['Data'][:7]]+=1
        avv=g['Squadra Ospite'] if role=='casa' else g['Squadra Casa']
        squads[sq]['avversari'][avv]+=1
        for f in ['Arbitro 1','Arbitro 2']:
            arb=parse_person(g.get(f,''))
            if arb:
                an=arb['nome']; squads[sq]['arbitri_stats'][an]['gare']+=1
                squads[sq]['arbitri_stats'][an]['gare_list'].append({
                    'data':g['Data'],'camp':g['Campionato'],'girone':g.get('Girone',''),
                    'casa':g['Squadra Casa'],'ospite':g['Squadra Ospite'],
                    'ris':g.get('Risultato',''),'pc':g.get('Punti Casa',''),
                    'po':g.get('Punti Ospite',''),'ruolo':role,'num':g.get('Numero Gara','')
                })
        if g.get('Risultato'):
            try:
                pc,po=int(g['Punti Casa']),int(g['Punti Ospite'])
                mio=pc if role=='casa' else po; av=po if role=='casa' else pc
                squads[sq]['punti_fatti']+=mio; squads[sq]['punti_subiti']+=av
                for f in ['Arbitro 1','Arbitro 2']:
                    arb=parse_person(g.get(f,''))
                    if arb:
                        an=arb['nome']
                        if mio>av: squads[sq]['arbitri_stats'][an]['vinte']+=1
                        elif mio<av: squads[sq]['arbitri_stats'][an]['perse']+=1
                        else: squads[sq]['arbitri_stats'][an]['pareggi']+=1
                if mio>av:
                    if role=='casa': squads[sq]['vinte_casa']+=1
                    else: squads[sq]['vinte_osp']+=1
                    squads[sq]['avv_vinte'][avv]+=1
                elif mio<av:
                    if role=='casa': squads[sq]['perse_casa']+=1
                    else: squads[sq]['perse_osp']+=1
                    squads[sq]['avv_perse'][avv]+=1
                else:
                    if role=='casa': squads[sq]['par_casa']+=1
                    else: squads[sq]['par_osp']+=1
            except: pass

# ══════════════════════════════════════════════════════════════
# H2H
# ══════════════════════════════════════════════════════════════
h2h=defaultdict(lambda:{'sq1':'','sq2':'','gare':[],'sq1_vinte':0,'sq2_vinte':0,'pareggi':0,'sq1_pt':0,'sq2_pt':0})
for g in RAW_ALL:
    c,o=g['Squadra Casa'],g['Squadra Ospite']; key=tuple(sorted([c,o]))
    h2h[key]['sq1']=key[0]; h2h[key]['sq2']=key[1]; h2h[key]['gare'].append(g)
    if g.get('Risultato'):
        try:
            pc,po=int(g['Punti Casa']),int(g['Punti Ospite'])
            s1p=pc if c==key[0] else po; s2p=po if c==key[0] else pc
            h2h[key]['sq1_pt']+=s1p; h2h[key]['sq2_pt']+=s2p
            if s1p>s2p: h2h[key]['sq1_vinte']+=1
            elif s2p>s1p: h2h[key]['sq2_vinte']+=1
            else: h2h[key]['pareggi']+=1
        except: pass

# ══════════════════════════════════════════════════════════════
# COPERTURA
# ══════════════════════════════════════════════════════════════
copertura={'totale':0,'con_2_arbitri':0,'con_1_arbitro':0,'senza_arbitri':0,
           'con_udc':0,'con_osservatore':0,'copertura_completa':0,'solo_arbitri':0,'gare_con_tutte_figure':0}
for g in RAW_ALL:
    a1=bool(parse_person(g.get('Arbitro 1',''))); a2=bool(parse_person(g.get('Arbitro 2','')))
    udc=any(parse_person(g.get(f,'')) for f in ['Segnapunti','Cronometrista','24 Secondi','Addetto Referto'])
    oss=bool(parse_person(g.get('Osservatore','')))
    copertura['totale']+=1
    if a1 and a2: copertura['con_2_arbitri']+=1
    elif a1 or a2: copertura['con_1_arbitro']+=1
    else: copertura['senza_arbitri']+=1
    if udc: copertura['con_udc']+=1
    if oss: copertura['con_osservatore']+=1
    if (a1 or a2) and udc and oss: copertura['copertura_completa']+=1
    if (a1 or a2) and not udc and not oss: copertura['solo_arbitri']+=1
    if a1 and a2 and udc and oss: copertura['gare_con_tutte_figure']+=1

# ══════════════════════════════════════════════════════════════
# GLOBAL STATS
# ══════════════════════════════════════════════════════════════
days_counter=Counter(); months_counter=Counter()
days_squads=defaultdict(set); days_camps=defaultdict(set)
months_squads=defaultdict(set); months_camps=defaultdict(set)
for g in RAW_ALL:
    d=g['Data']; mo=d[:7]
    days_counter[d]+=1; months_counter[mo]+=1
    days_squads[d].add(g['Squadra Casa']); days_squads[d].add(g['Squadra Ospite'])
    days_camps[d].add(g['Campionato'])
    months_squads[mo].add(g['Squadra Casa']); months_squads[mo].add(g['Squadra Ospite'])
    months_camps[mo].add(g['Campionato'])

global_stats={
    'per_giorno':{d:{'gare':n,'squadre':len(days_squads[d]),'campionati':len(days_camps[d]),
                     'camps_list':sorted(days_camps[d])} for d,n in sorted(days_counter.items())},
    'per_mese':{mo:{'gare':n,'squadre':len(months_squads[mo]),'campionati':len(months_camps[mo])}
                for mo,n in sorted(months_counter.items())},
    'tot_gare':len(RAW_ALL),'tot_giorni':len(days_counter),
    'media_gare_giorno':round(len(RAW_ALL)/max(len(days_counter),1),1),
}

all_province=sorted(set(campo_prov(g.get('Campo','')) for g in RAW_ALL if campo_prov(g.get('Campo',''))))

# ══════════════════════════════════════════════════════════════
# PROVINCE
# ══════════════════════════════════════════════════════════════
PROV_COORDS={
    'CA':{'lat':39.2238,'lng':9.1217,'nome':'Cagliari'},
    'SS':{'lat':40.7259,'lng':8.5556,'nome':'Sassari'},
    'SU':{'lat':39.3110,'lng':9.0150,'nome':'Sud Sardegna'},
    'NU':{'lat':40.3223,'lng':9.3311,'nome':'Nuoro'},
    'OR':{'lat':39.9036,'lng':8.5915,'nome':'Oristano'},
    'OT':{'lat':40.9164,'lng':9.5379,'nome':'Olbia-Tempio'},
    'OG':{'lat':39.8989,'lng':9.5293,'nome':'Ogliastra'},
    'VS':{'lat':39.5833,'lng':8.8667,'nome':'Medio Campidano'},
    'CI':{'lat':39.1147,'lng':8.4937,'nome':'Carbonia-Iglesias'},
}

squad_home_prov_map={}
for sq,s in squads.items():
    pv=campo_prov(s.get('campo_principale',''))
    if pv: squad_home_prov_map[sq]=pv

province_out={}
for pv in all_province:
    pg=[g for g in RAW_ALL if campo_prov(g.get('Campo',''))==pv]
    sq_res=sorted(set(sq for sq,hp in squad_home_prov_map.items() if hp==pv))
    # squadre ospiti (giocano in questa provincia ma non ci abitano)
    sq_all_in_pv=sorted(set(g['Squadra Casa'] for g in pg)|set(g['Squadra Ospite'] for g in pg))
    sq_ospiti=sorted(set(sq for sq in sq_all_in_pv if sq not in sq_res))
    # persone residenti per categoria
    arbs_res=[pid for pid,p in persons.items() if p['provincia']==pv and 'Arbitro' in p['categorie']]
    udc_res=[pid for pid,p in persons.items() if p['provincia']==pv and 'UDC' in p['categorie'] and 'Arbitro' not in p['categorie']]
    oss_res=[pid for pid,p in persons.items() if p['provincia']==pv and 'Osservatore' in p['categorie'] and 'Arbitro' not in p['categorie']]
    province_out[pv]={
        'n_gare':len(pg),'sq_residenti':sq_res,'sq_ospiti':sq_ospiti,
        'arbitri_residenti':arbs_res,'udc_residenti':udc_res,'oss_residenti':oss_res,
        'campionati':dict(Counter(g['Campionato'] for g in pg)),
        'coord':PROV_COORDS.get(pv,{}),'nome':PROV_COORDS.get(pv,{}).get('nome',pv),
        'gare':pg,
    }

# ══════════════════════════════════════════════════════════════
# CAMP_STATS
# ══════════════════════════════════════════════════════════════
camp_stats={}
for camp in all_camp_names:
    cg=[g for g in RAW_ALL if g['Campionato']==camp]
    gironi=sorted(set(g.get('Girone','') for g in cg if g.get('Girone','')))
    sqs_m=sorted(set(g['Squadra Casa'] for g in cg)|set(g['Squadra Ospite'] for g in cg))
    sq_stats={}
    for sq in sqs_m:
        sg=[g for g in cg if g['Squadra Casa']==sq or g['Squadra Ospite']==sq]
        v=s_v=par=pf=ps=0
        for g in sg:
            if g.get('Risultato'):
                try:
                    pc,po=int(g['Punti Casa']),int(g['Punti Ospite'])
                    mio=pc if g['Squadra Casa']==sq else po; av=po if g['Squadra Casa']==sq else pc
                    pf+=mio; ps+=av
                    if mio>av: v+=1
                    elif mio<av: s_v+=1
                    else: par+=1
                except: pass
        sq_stats[sq]={'gare':len(sg),'v':v,'s':s_v,'p':par,'pf':pf,'ps':ps}
    meta=camp_meta.get(camp,{})
    camp_stats[camp]={
        'n_gare':len(cg),'squadre':sqs_m,'sq_stats':sq_stats,'gironi':gironi,
        'n_squadre':len(sqs_m),'gare':cg,
        'gruppo':meta.get('gruppo',camp),'genere':meta.get('genere','M'),
        'livello':meta.get('livello',''),
        'is_femminile':meta.get('genere','')=='F','is_minibasket':is_minibasket(camp),
    }

# ══════════════════════════════════════════════════════════════
# CAMPI (raggruppati per nome breve + provincia)
# ══════════════════════════════════════════════════════════════
raw_campi=defaultdict(lambda:{'campo':'','prov':'','gare':0,'squadre':set(),'sq_casa':set(),'sq_ospiti':set(),'campionati':set()})
for g in RAW_ALL:
    campo=g.get('Campo','')
    if campo:
        pv=campo_prov(campo)
        raw_campi[campo]['campo']=campo; raw_campi[campo]['prov']=pv
        raw_campi[campo]['gare']+=1
        raw_campi[campo]['squadre'].add(g['Squadra Casa']); raw_campi[campo]['squadre'].add(g['Squadra Ospite'])
        raw_campi[campo]['sq_casa'].add(g['Squadra Casa'])
        raw_campi[campo]['sq_ospiti'].add(g['Squadra Ospite'])
        raw_campi[campo]['campionati'].add(g['Campionato'])

def campo_nome_breve(campo):
    m=re.match(r'^([^,]+)',campo)
    return m.group(1).strip() if m else campo

def campo_location_key(campo):
    """Estrae via/indirizzo per raggruppare palestre nella stessa ubicazione."""
    # Es: "Palestra Luca Simula, Via Poligono 2 07100 SASSARI ( SS)"
    # → normalizza il nome breve rimuovendo articoli e parole comuni
    nome = campo_nome_breve(campo).upper()
    # Rimuovi prefissi comuni
    for pref in ['PALESTRA COMUNALE', 'PALESTRA', 'PALA', 'CAMPO', 'CENTRO SPORTIVO', 'PALAZZETTO']:
        if nome.startswith(pref):
            nome = nome[len(pref):].strip()
            break
    # Normalizza spazi
    return re.sub(r'\s+', ' ', nome).strip()

campo_gruppi=defaultdict(list)
for campo,info in raw_campi.items():
    key=(campo_nome_breve(campo),info['prov'])
    campo_gruppi[key].append(campo)

campi_out={}
for (nome_b,pv),varianti in campo_gruppi.items():
    gare_tot=sum(raw_campi[c]['gare'] for c in varianti)
    sq_tot=set(); ca_tot=set()
    for c in varianti: sq_tot|=raw_campi[c]['squadre']; ca_tot|=raw_campi[c]['campionati']
    canon=max(varianti,key=lambda c:raw_campi[c]['gare'])
    campi_out[canon]={
        'campo':canon,'nome_breve':nome_b,'prov':pv,'gare':gare_tot,
        'n_squadre':len(sq_tot),'n_campionati':len(ca_tot),
        'varianti':varianti,'squadre':sorted(sq_tot),'campionati':sorted(ca_tot),
        'sq_casa':sorted(set().union(*[raw_campi[c]['sq_casa'] for c in varianti])),
        'sq_ospiti':sorted(set().union(*[raw_campi[c]['sq_ospiti'] for c in varianti])),
    }
print(f"Campi: {len(campi_out)} (da {len(raw_campi)} varianti)")

# ══════════════════════════════════════════════════════════════
# PROVVEDIMENTI
# ══════════════════════════════════════════════════════════════
provvedimenti_list=[]
for g in RAW_ALL:
    pv_val=(g.get('Provvedimenti','') or '').strip()
    if pv_val and pv_val.lower() not in ('','n/d','nessuno','-'):
        provvedimenti_list.append({
            'data':g['Data'],'camp':g['Campionato'],'girone':g.get('Girone',''),
            'casa':g['Squadra Casa'],'ospite':g['Squadra Ospite'],
            'num':g.get('Numero Gara',''),'ris':g.get('Risultato',''),'testo':pv_val,
            'arbitro1':g.get('Arbitro 1',''),'arbitro2':g.get('Arbitro 2',''),
        })
print(f"Provvedimenti: {len(provvedimenti_list)}")

# ══════════════════════════════════════════════════════════════
# SERIALIZE
# ══════════════════════════════════════════════════════════════
def serialize_persons():
    out={}
    for pid,p in persons.items():
        gare_tot=len(p['gare_arbitro'])+len(p['gare_udc'])+len(p['gare_osservatore'])
        max_streak=0; streak_cur=0
        if p['gare_arbitro']:
            sg=sorted(p['gare_arbitro'],key=lambda x:x['Data']); cur=1
            for i in range(1,len(sg)):
                d1=datetime.date.fromisoformat(sg[i-1]['Data']); d2=datetime.date.fromisoformat(sg[i]['Data'])
                if (d2-d1).days<=14: cur+=1
                else: max_streak=max(max_streak,cur); cur=1
            max_streak=max(max_streak,cur); streak_cur=cur
        forma=[{'d':ga['Data'],'c':ga['Campionato'],'h':ga['Squadra Casa'],
                'a':ga['Squadra Ospite'],'r':ga.get('Risultato','')}
               for ga in sorted(p['gare_arbitro'],key=lambda x:x['Data'])[-10:]]
        all_gare=sorted(p['gare_arbitro']+p['gare_udc']+p['gare_osservatore'],key=lambda x:x['Data'])
        out[pid]={
            'nome':p['nome'],'provincia':p['provincia'],'citta':p['citta'],
            'categoria':p['categoria'],'categorie':list(p['categorie']),'ruoli':sorted(p['ruoli']),
            'genere':p.get('genere','?'),
            'n_gare':gare_tot,'n_gare_arbitro':len(p['gare_arbitro']),
            'n_gare_udc':len(p['gare_udc']),'n_gare_osservatore':len(p['gare_osservatore']),
            'n_singolo':sum(1 for ga in p['gare_arbitro'] if not (ga.get('Arbitro 2','') or '').strip()),
            'tot':gare_tot,'arb':len(p['gare_arbitro']),'udc':len(p['gare_udc']),'oss':len(p['gare_osservatore']),
            'campionati':dict(p['campionati']),'giorni':dict(p['giorni']),'mesi':dict(p['mesi']),
            'colleghi':dict(p['colleghi']),'campi':dict(p['campi']),
            'squadre':dict(p['squadre']),
            'streak_max':max_streak,'streak_cur':streak_cur,'forma_recente':forma,
            'gare':[{
                'Data':ga['Data'],'Ora':ga.get('Ora',''),'Numero Gara':ga.get('Numero Gara',''),
                'Campionato':ga['Campionato'],'Girone':ga.get('Girone',''),
                'Squadra Casa':ga.get('Squadra Casa',''),'Squadra Ospite':ga.get('Squadra Ospite',''),
                'Punti Casa':ga.get('Punti Casa',''),'Punti Ospite':ga.get('Punti Ospite',''),
                'Risultato':ga.get('Risultato',''),'Campo':ga.get('Campo',''),
                '_ruolo':ga.get('_ruolo',''),'_cat':ga.get('_cat',''),
                'Arbitro 1':ga.get('Arbitro 1',''),'Arbitro 2':ga.get('Arbitro 2',''),
                'Segnapunti':ga.get('Segnapunti',''),'Cronometrista':ga.get('Cronometrista',''),
                '24 Secondi':ga.get('24 Secondi',''),'Addetto Referto':ga.get('Addetto Referto',''),
                'Osservatore':ga.get('Osservatore',''),
            } for ga in all_gare],
            'gare_arbitro':[{'d':ga['Data'],'c':ga['Campionato'],'h':ga['Squadra Casa'],
                'a':ga['Squadra Ospite'],'r':ga.get('Risultato',''),'pc':ga.get('Punti Casa',''),
                'po':ga.get('Punti Ospite',''),'campo':ga.get('Campo',''),
                'num':ga.get('Numero Gara',''),'girone':ga.get('Girone',''),'ruolo':ga.get('_ruolo','')}
               for ga in p['gare_arbitro']],
            'gare_udc':[{'d':ga['Data'],'c':ga['Campionato'],'h':ga['Squadra Casa'],
                'a':ga['Squadra Ospite'],'r':ga.get('Risultato',''),'campo':ga.get('Campo',''),
                'num':ga.get('Numero Gara',''),'ruolo':ga.get('_ruolo','')}
               for ga in p['gare_udc']],
            'gare_osservatore':[{'d':ga['Data'],'c':ga['Campionato'],'h':ga['Squadra Casa'],
                'a':ga['Squadra Ospite'],'r':ga.get('Risultato',''),'campo':ga.get('Campo',''),
                'num':ga.get('Numero Gara','')} for ga in p['gare_osservatore']],
            'squadre_incontrate':{sq:{'gare':v['gare'],'vinte':v['vinte'],'perse':v['perse'],
                'pareggi':v['pareggi'],'gare_list':v['gare_list']}
                for sq,v in p['squadre_incontrate'].items()},
        }
    return out

def serialize_squads():
    out={}
    for sq,s in squads.items():
        gare_con_ris=[g for g in s['gare'] if g.get('Risultato')]
        max_sv=0; max_ss=0; cur_v=0; cur_s=0
        for g in sorted(gare_con_ris,key=lambda x:x['Data']):
            try:
                pc,po=int(g['Punti Casa']),int(g['Punti Ospite'])
                mio=pc if g['_ruolo']=='casa' else po; av=po if g['_ruolo']=='casa' else pc
                if mio>av: cur_v+=1; cur_s=0; max_sv=max(max_sv,cur_v)
                elif mio<av: cur_s+=1; cur_v=0; max_ss=max(max_ss,cur_s)
                else: cur_v=0; cur_s=0
            except: pass
        forma=[]
        for g in sorted(gare_con_ris,key=lambda x:x['Data'])[-5:]:
            try:
                pc,po=int(g['Punti Casa']),int(g['Punti Ospite'])
                mio=pc if g['_ruolo']=='casa' else po; av=po if g['_ruolo']=='casa' else pc
                forma.append({'d':g['Data'],'res':'V' if mio>av else ('S' if mio<av else 'P'),'score':f"{mio}-{av}"})
            except: forma.append({'d':g['Data'],'res':'?','score':''})
        out[sq]={
            'nome':s['nome'],'prov':s['prov'],'campo_principale':s['campo_principale'],
            'gruppo':s['gruppo'],'is_femminile':s['is_femminile'],'n_gare':len(s['gare']),
            'gare':s['gare'],
            'campionati':dict(s['campionati']),'campionati_meta':s['campionati_meta'],
            'gironi_per_camp':{k:list(v) for k,v in s['gironi_per_camp'].items()},
            'avversari':dict(s['avversari']),
            'avv_vinte':dict(s['avv_vinte']),'avv_perse':dict(s['avv_perse']),
            'vinte':s['vinte_casa']+s['vinte_osp'],'perse':s['perse_casa']+s['perse_osp'],
            'pareggi':s['par_casa']+s['par_osp'],'pareggiate':s['par_casa']+s['par_osp'],
            'vinte_casa':s['vinte_casa'],'perse_casa':s['perse_casa'],'par_casa':s['par_casa'],
            'vinte_osp':s['vinte_osp'],'perse_osp':s['perse_osp'],'par_osp':s['par_osp'],
            'punti_fatti':s['punti_fatti'],'punti_subiti':s['punti_subiti'],
            'giorni':dict(s['giorni']),'mesi':dict(s['mesi']),
            'streak_v_max':max_sv,'streak_s_max':max_ss,'streak_v_cur':cur_v,'streak_s_cur':cur_s,
            'forma_recente':forma,
            'arbitri_stats':{an:{'gare':v['gare'],'vinte':v['vinte'],'perse':v['perse'],
                'pareggi':v['pareggi'],'gare_list':v['gare_list']} for an,v in s['arbitri_stats'].items()},
        }
    return out

print("Serializzazione persons..."); pers_out=serialize_persons()
print("Serializzazione squads...");  sq_out=serialize_squads()

h2h_out={}
for key,v in h2h.items():
    if len(v['gare'])>=2:
        h2h_out[f"{key[0]}||{key[1]}"]={
            'sq1':v['sq1'],'sq2':v['sq2'],'n_gare':len(v['gare']),
            'sq1_vinte':v['sq1_vinte'],'sq2_vinte':v['sq2_vinte'],'pareggi':v['pareggi'],
            'sq1_pt':v['sq1_pt'],'sq2_pt':v['sq2_pt'],
            'gare':[{'d':g['Data'],'c':g['Campionato'],'h':g['Squadra Casa'],'a':g['Squadra Ospite'],
                     'pc':g.get('Punti Casa',''),'po':g.get('Punti Ospite',''),'num':g.get('Numero Gara','')} for g in v['gare']]
        }

all_dates=sorted(g['Data'] for g in RAW_ALL if g.get('Data'))
D={
    'meta':{'generated':TODAY,'yesterday':YESTERDAY,'tot_gare':len(RAW_ALL),
            'data_min':all_dates[0] if all_dates else '','data_max':all_dates[-1] if all_dates else TODAY,
            'tot_persone':len(pers_out),'tot_squadre':len(sq_out),'tot_campi':len(campi_out),
            'n_m':sum(1 for p in pers_out.values() if p.get('genere')=='M'),
            'n_f':sum(1 for p in pers_out.values() if p.get('genere')=='F'),
            'future_end':FUTURE_END},
    'persons':pers_out,'squads':sq_out,
    'all_groups':dict(all_groups),'squad_groups':squad_groups,
    'h2h':h2h_out,'copertura':copertura,'global_stats':global_stats,
    'all_campionati':all_camp_names,'camp_meta':camp_meta,
    'all_province':all_province,'prov_coords':PROV_COORDS,'campi':campi_out,
    'gare_future':RAW_FUTURE,
    # compatibilità v4
    'generated':TODAY,'today':TODAY,'yesterday':YESTERDAY,
    'totale_gare':len(RAW_ALL),'gare':RAW_ALL,
    'campionati':all_camp_names,'province':province_out,
    'camp_stats':camp_stats,'provvedimenti':provvedimenti_list,
}

with open('cache/data_v5_new.json','w',encoding='utf-8') as f:
    json.dump(D,f,ensure_ascii=False,separators=(',',':'))

import os; size=os.path.getsize('data_v5_new.json')
print(f"\n✅ data_v5_new.json: {size//1024} KB ({size/1024/1024:.1f} MB)")
print(f"   Persons:{len(pers_out)} | Squads:{len(sq_out)} | H2H:{len(h2h_out)} | Campi:{len(campi_out)}")
print(f"   Province:{sorted(all_province)} | Campionati:{len(all_camp_names)}")
print(f"   Gare:{len(RAW_ALL)} | Provvedimenti:{len(provvedimenti_list)}")
