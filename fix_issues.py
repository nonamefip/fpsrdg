import sys, re

TMPL = '/sessions/fervent-dreamy-noether/mnt/fpsrdg/scripts/template.html'
with open(TMPL, 'rb') as f:
    content = f.read().decode('utf-8')
done = []

# ── FIX 1: renderUltimeGare — fix g['Arbitri'] and date-filter logic ────────
# Problem A: uses g['Arbitri'] which doesn't exist in GARE; fix to Arbitro 1/2
OLD1A = "    const arbs=(g['Arbitri']||g['Arbitro']||'').split(',').map(s=>s.trim()).filter(Boolean);"
NEW1A = "    const arbs=[g['Arbitro 1']||'',g['Arbitro 2']||''].map(s=>s.trim()).filter(Boolean);"
if OLD1A in content:
    content = content.replace(OLD1A, NEW1A, 1); done.append('1A-arbs-fix')
else:
    print('FIX 1A not found'); sys.exit(1)

# Problem B: date filter applied to ultimaMap makes arbitri show "—"
# Fix: build ultimaMap from ALL GARE, then filter rows by period
OLD1B = (
    "  const ultimaMap={};\n"
    "  (GARE||[]).forEach(g=>{\n"
    "    const d=g['Data']||'';\n"
    "    if(!d)return;\n"
    "    if(fD1&&d<fD1)return;\n"
    "    if(fD2&&d>fD2)return;\n"
    "    if(fM&&!d.startsWith(fM))return;\n"
    "    const arbs=[g['Arbitro 1']||'',g['Arbitro 2']||''].map(s=>s.trim()).filter(Boolean);\n"
    "    arbs.forEach(a=>{\n"
    "      if(!ultimaMap[a]||d>ultimaMap[a].data){\n"
    "        ultimaMap[a]={data:d,camp:g['Campionato']||'',citta:g['Campo']||''};\n"
    "      }\n"
    "    });\n"
    "  });\n"
    "  const ARB=Object.values(P).filter(x=>x.categoria==='Arbitro');\n"
    "  let rows=ARB.map(a=>{\n"
    "    const nome=(a.nome||'').trim();\n"
    "    const pv=(a.provincia||'').toUpperCase();\n"
    "    const last=ultimaMap[nome]||null;\n"
    "    return {nome,pv,last};\n"
    "  }).filter(r=>r.nome);\n"
    "  if(fPv) rows=rows.filter(r=>r.pv===fPv);\n"
    "  rows.sort((a,b)=>{\n"
    "    if(!a.last&&!b.last)return a.nome.localeCompare(b.nome);\n"
    "    if(!a.last)return 1;\n"
    "    if(!b.last)return -1;\n"
    "    return a.last.data.localeCompare(b.last.data);\n"
    "  });\n"
)

NEW1B = (
    "  // Costruisci ultimaMap da TUTTE le gare (nessun filtro data)\n"
    "  const ultimaMap={};\n"
    "  (GARE||[]).forEach(g=>{\n"
    "    const d=g['Data']||'';if(!d)return;\n"
    "    [g['Arbitro 1']||'',g['Arbitro 2']||''].map(s=>s.trim()).filter(Boolean).forEach(a=>{\n"
    "      if(!ultimaMap[a]||d>ultimaMap[a].data)\n"
    "        ultimaMap[a]={data:d,camp:g['Campionato']||'',citta:g['Campo']||''};\n"
    "    });\n"
    "  });\n"
    "  const ARB=Object.values(P).filter(x=>x.categoria==='Arbitro');\n"
    "  let rows=ARB.map(a=>{\n"
    "    const nome=(a.nome||'').trim();\n"
    "    const pv=(a.provincia||'').toUpperCase();\n"
    "    const last=ultimaMap[nome]||null;\n"
    "    return {nome,pv,last};\n"
    "  }).filter(r=>r.nome);\n"
    "  if(fPv) rows=rows.filter(r=>r.pv===fPv);\n"
    "  // Filtro periodo: mostra solo chi ha l'ultima partita nel periodo selezionato\n"
    "  if(fD1||fD2||fM){\n"
    "    rows=rows.filter(r=>{\n"
    "      if(!r.last)return false;\n"
    "      const d=r.last.data;\n"
    "      if(fM&&!d.startsWith(fM))return false;\n"
    "      if(fD1&&d<fD1)return false;\n"
    "      if(fD2&&d>fD2)return false;\n"
    "      return true;\n"
    "    });\n"
    "  }\n"
    "  rows.sort((a,b)=>{\n"
    "    if(!a.last&&!b.last)return a.nome.localeCompare(b.nome);\n"
    "    if(!a.last)return 1;\n"
    "    if(!b.last)return -1;\n"
    "    return a.last.data.localeCompare(b.last.data);\n"
    "  });\n"
)

if OLD1B in content:
    content = content.replace(OLD1B, NEW1B, 1); done.append('1B-filter-logic')
else:
    print('FIX 1B not found')
    # Show context
    idx = content.find('const ultimaMap={}')
    print('ultimaMap at:', idx)
    print(repr(content[idx:idx+200]))
    sys.exit(1)

# ── FIX 2: Territorio — gareFuoriPv use pvComp not arbPv ────────────────────
OLD2 = "            if(pvC&&pvC!==arbPv)gareFuoriPv.push(g);"
NEW2 = "            if(pvC&&!pvComp.has(pvC))gareFuoriPv.push(g);"
if OLD2 in content:
    content = content.replace(OLD2, NEW2, 1); done.append('2-gareFuoriPv-pvComp')
else:
    print('FIX 2 not found'); sys.exit(1)

# ── FIX 3: Territorio SEZ A — add città column + sortable headers ───────────
OLD3 = (
    "          html+='<table style=\"width:100%;border-collapse:collapse;font-size:.72rem\">';\n"
    "          html+='<thead><tr style=\"background:#fff0f0\"><th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6\">Campionato</th><th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6\">Prov.</th><th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6\">Squadre</th></tr></thead><tbody>';\n"
    "          Object.keys(byCamp).sort().forEach(key=>{\n"
    "            const {camp,pv,sqs}=byCamp[key];\n"
    "            const clr=PC[pv]||campColor(camp)||'#555';\n"
    "            html+='<tr style=\"border-bottom:1px solid #fce8e8;vertical-align:top\">';\n"
    "            html+='<td style=\"padding:4px 8px;font-size:.68rem;white-space:nowrap\"><span style=\"'+campBadgeStyle(camp)+'border-radius:3px;padding:1px 6px;font-size:.62rem\">'+escH(camp)+'</span></td>';\n"
    "            html+='<td style=\"padding:4px 8px\"><span style=\"background:'+clr+';color:#fff;border-radius:3px;padding:0 5px;font-size:.62rem;font-weight:700\">'+pv+'</span></td>';\n"
    "            html+='<td style=\"padding:3px 8px\">'+sqs.sort().map(n=>sqPill(n,clr)).join('')+'</td>';\n"
    "            html+='</tr>';\n"
    "          });\n"
    "          html+='</tbody></table>';\n"
)

NEW3 = (
    "          // Tabella con colonne: Campionato | Prov | Città | Squadra\n"
    "          const terrRows=[];\n"
    "          Object.keys(byCamp).sort().forEach(key=>{\n"
    "            const {camp,pv,sqs}=byCamp[key];\n"
    "            sqs.sort().forEach(n=>{\n"
    "              const s=SQ[n]||{};\n"
    "              const cityM=(s.campo_principale||'').match(/\\(([A-Z]{2,3})\\)/);\n"
    "              const city=cityM?cityM[1]:'';\n"
    "              terrRows.push({camp,pv,n,city});\n"
    "            });\n"
    "          });\n"
    "          const terrSortUid='ts_'+Math.random().toString(36).slice(2,5);\n"
    "          let terrSortCol='camp',terrSortAsc=true;\n"
    "          function terrSort(col){\n"
    "            if(terrSortCol===col)terrSortAsc=!terrSortAsc;\n"
    "            else{terrSortCol=col;terrSortAsc=true;}\n"
    "            renderTerrTable();\n"
    "          }\n"
    "          function campImportanza(c){\n"
    "            const rank=_ordFIP?_ordFIP.indexOf(c):-1;\n"
    "            return rank>=0?rank:999;\n"
    "          }\n"
    "          function renderTerrTable(){\n"
    "            const sorted=[...terrRows].sort((a,b)=>{\n"
    "              let va,vb;\n"
    "              if(terrSortCol==='camp'){va=campImportanza(a.camp);vb=campImportanza(b.camp);return terrSortAsc?va-vb:vb-va;}\n"
    "              if(terrSortCol==='pv'){va=a.pv;vb=b.pv;}\n"
    "              else if(terrSortCol==='city'){va=a.city;vb=b.city;}\n"
    "              else{va=a.n;vb=b.n;}\n"
    "              return terrSortAsc?va.localeCompare(vb):vb.localeCompare(va);\n"
    "            });\n"
    "            const th=(col,lbl)=>{\n"
    "              const active=terrSortCol===col;\n"
    "              const arr=active?(terrSortAsc?'▲':'▼'):'';\n"
    "              return '<th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6;cursor:pointer;user-select:none;white-space:nowrap\" onclick=\"terrSort(\\''+col+'\\')\">'+ lbl+(arr?' <span style=\"color:#c0392b\">'+arr+'</span>':'')+'</th>';\n"
    "            };\n"
    "            let t='<table style=\"width:100%;border-collapse:collapse;font-size:.72rem\">';\n"
    "            t+='<thead><tr style=\"background:#fff0f0\">'+th('camp','Campionato')+th('pv','Prov.')+th('city','Città')+th('sq','Squadra')+'</tr></thead><tbody>';\n"
    "            sorted.forEach((r,i)=>{\n"
    "              const clr=PC[r.pv]||campColor(r.camp)||'#555';\n"
    "              t+='<tr style=\"border-bottom:1px solid #fce8e8;background:'+(i%2?'#fffafa':'#fff')+'\">';\n"
    "              t+='<td style=\"padding:3px 8px;white-space:nowrap\"><span style=\"'+campBadgeStyle(r.camp)+'border-radius:3px;padding:1px 5px;font-size:.62rem\">'+(campIsEcc(r.camp)?'\u2b50 ':'')+escH(r.camp.replace('COMITATO REGIONALE SARDEGNA ',''))+'</span></td>';\n"
    "              t+='<td style=\"padding:3px 8px\"><span style=\"background:'+clr+';color:#fff;border-radius:3px;padding:0 5px;font-size:.62rem;font-weight:700\">'+r.pv+'</span></td>';\n"
    "              t+='<td style=\"padding:3px 8px;font-size:.68rem;color:var(--mu)\">'+escH(r.city)+'</td>';\n"
    "              t+='<td style=\"padding:3px 8px\">'+sqPill(r.n,clr)+'</td>';\n"
    "              t+='</tr>';\n"
    "            });\n"
    "            t+='</tbody></table>';\n"
    "            document.getElementById(terrSortUid).innerHTML=t;\n"
    "          }\n"
    "          html+='<div id=\"'+terrSortUid+'\"></div>';\n"
    "          setTimeout(()=>{\n"
    "            if(typeof terrSort==='function')renderTerrTable();\n"
    "            else{const el=document.getElementById(terrSortUid);if(el)renderTerrTable();}\n"
    "          },0);\n"
)

if OLD3 in content:
    content = content.replace(OLD3, NEW3, 1); done.append('3-terr-sortable-table')
else:
    print('FIX 3 not found'); sys.exit(1)

# ── FIX 4: campLvlRows — add gold border for Eccellenza badges ──────────────
OLD4 = (
    "              const short=cn.replace('COMITATO REGIONALE SARDEGNA ','').replace('Divisione regionale','DR').replace('Under ','U');\n"
    "              return `<div style=\"display:flex;align-items:center;gap:6px;font-size:.68rem;padding:1px 0\">\n"
    "                <span style=\"background:${col};color:#fff;border-radius:2px;padding:0 5px;flex-shrink:0;font-size:.6rem\">${escH(short)}</span>\n"
    "                <div style=\"flex:1;height:6px;background:#eee;border-radius:3px;overflow:hidden\">\n"
    "                  <div style=\"height:6px;background:${col};width:${Math.min(pctC,100)}%;border-radius:3px\"></div>\n"
    "                </div>\n"
    "                <span style=\"color:var(--mu);flex-shrink:0;min-width:28px;text-align:right\">${pctC}%</span>\n"
    "              </div>`;"
)

NEW4 = (
    "              const short=cn.replace('COMITATO REGIONALE SARDEGNA ','').replace('Divisione regionale','DR').replace('Under ','U');\n"
    "              const isEccL=campIsEcc(cn);\n"
    "              const badgeStyle=isEccL?`background:${col};color:#fff;border:2px solid #ffd700;border-radius:3px;padding:0 5px;flex-shrink:0;font-size:.6rem`:`background:${col};color:#fff;border-radius:2px;padding:0 5px;flex-shrink:0;font-size:.6rem`;\n"
    "              return `<div style=\"display:flex;align-items:center;gap:6px;font-size:.68rem;padding:1px 0\">\n"
    "                <span style=\"${badgeStyle}\">${isEccL?'\u2b50 ':''}${escH(short)}</span>\n"
    "                <div style=\"flex:1;height:6px;background:#eee;border-radius:3px;overflow:hidden\">\n"
    "                  <div style=\"height:6px;background:${col};width:${Math.min(pctC,100)}%;border-radius:3px${isEccL?';box-shadow:0 0 0 1px #ffd700':''}\"></div>\n"
    "                </div>\n"
    "                <span style=\"color:var(--mu);flex-shrink:0;min-width:28px;text-align:right\">${pctC}%</span>\n"
    "              </div>`;"
)

if OLD4 in content:
    content = content.replace(OLD4, NEW4, 1); done.append('4-campLvlRows-eccellenza')
else:
    print('FIX 4 not found'); sys.exit(1)

# ── Write back ──────────────────────────────────────────────────────────────
with open(TMPL, 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixes applied:', done)
