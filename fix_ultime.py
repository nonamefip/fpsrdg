import sys

TMPL = '/sessions/fervent-dreamy-noether/mnt/fpsrdg/scripts/template.html'
with open(TMPL, 'rb') as f:
    content = f.read().decode('utf-8')
done = []

# ── FIX 1: Replace entire renderUltimeGare function ─────────────────────────
START = 'function renderUltimeGare(){'
END = '\n\nfunction renderArbiFermi(){'
idx_s = content.find(START)
idx_e = content.find(END, idx_s)
if idx_s == -1 or idx_e == -1:
    print('renderUltimeGare markers not found'); sys.exit(1)

NEW_FN = (
    'function renderUltimeGare(){\n'
    '  const selPv=document.getElementById(\'ultime-pv\');\n'
    '  const selM=document.getElementById(\'ultime-m\');\n'
    '  const body=document.getElementById(\'ultime-body\');\n'
    '  if(!body)return;\n'
    '  const MESI_IT=[\'Gen\',\'Feb\',\'Mar\',\'Apr\',\'Mag\',\'Giu\',\'Lug\',\'Ago\',\'Set\',\'Ott\',\'Nov\',\'Dic\'];\n'
    '  if(selM&&selM.options.length<=1){\n'
    '    const ms=new Set();\n'
    '    (GARE||[]).forEach(g=>{if(g[\'Data\'])ms.add(g[\'Data\'].slice(0,7));});\n'
    '    [...ms].sort().reverse().forEach(m=>{\n'
    '      const[y,mo]=m.split(\'-\');\n'
    '      selM.innerHTML+=\'<option value="\'+m+\'">\'+MESI_IT[parseInt(mo)-1]+\' \'+y+\'</option>\';\n'
    '    });\n'
    '  }\n'
    '  const fPv=selPv?selPv.value:\'\';\n'
    '  const fM=selM?selM.value:\'\';\n'
    '  const fD1=(document.getElementById(\'ultime-d1\')||{}).value||\'\';\n'
    '  const fD2=(document.getElementById(\'ultime-d2\')||{}).value||\'\';\n'
    '  // Costruisci ultimaMap da TUTTE le gare (senza filtro data)\n'
    '  const ultimaMap={};\n'
    '  (GARE||[]).forEach(g=>{\n'
    '    const d=g[\'Data\']||\'\';\n'
    '    if(!d)return;\n'
    '    [g[\'Arbitro 1\']||\'\',g[\'Arbitro 2\']||\'\'].map(s=>s.trim()).filter(Boolean).forEach(a=>{\n'
    '      if(!ultimaMap[a]||d>ultimaMap[a].data)\n'
    '        ultimaMap[a]={data:d,camp:g[\'Campionato\']||\'\',citta:g[\'Campo\']||\'\'};\n'
    '    });\n'
    '  });\n'
    '  const ARB=Object.values(P).filter(x=>x.categoria===\'Arbitro\');\n'
    '  let rows=ARB.map(a=>{\n'
    '    const nome=(a.nome||\'\').trim();\n'
    '    const pv=(a.provincia||\'\').toUpperCase();\n'
    '    const last=ultimaMap[nome]||null;\n'
    '    return {nome,pv,last};\n'
    '  }).filter(r=>r.nome);\n'
    '  if(fPv) rows=rows.filter(r=>r.pv===fPv);\n'
    '  // Filtro periodo: mostra arbitri la cui ultima partita cade nel periodo\n'
    '  if(fD1||fD2||fM){\n'
    '    rows=rows.filter(r=>{\n'
    '      if(!r.last)return false;\n'
    '      const d=r.last.data;\n'
    '      if(fM&&!d.startsWith(fM))return false;\n'
    '      if(fD1&&d<fD1)return false;\n'
    '      if(fD2&&d>fD2)return false;\n'
    '      return true;\n'
    '    });\n'
    '  }\n'
    '  rows.sort((a,b)=>{\n'
    '    if(!a.last&&!b.last)return a.nome.localeCompare(b.nome);\n'
    '    if(!a.last)return 1;\n'
    '    if(!b.last)return -1;\n'
    '    return a.last.data.localeCompare(b.last.data);\n'
    '  });\n'
    '  if(!rows.length){body.innerHTML=\'<p style="color:var(--mu);font-size:.8rem;padding:8px">Nessun arbitro trovato.</p>\';return;}\n'
    '  const today=new Date().toISOString().slice(0,10);\n'
    '  const daysDiff=d=>{if(!d)return null;const ms2=new Date(today)-new Date(d);return Math.round(ms2/86400000);};\n'
    '  const rowColor=days=>{\n'
    '    if(days===null)return \'#e9ecef\';\n'
    '    if(days>90)return \'#ffc9c9\';\n'
    '    if(days>60)return \'#ffe8b0\';\n'
    '    if(days>30)return \'#fff3cd\';\n'
    '    return \'#d4edda\';\n'
    '  };\n'
    '  let html=\'<table style="width:100%;border-collapse:collapse;font-size:.75rem">\';\n'
    '  html+=\'<thead><tr style="background:#f0f4ff">\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:left;border-bottom:1px solid var(--br)">#</th>\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:left;border-bottom:1px solid var(--br)">Arbitro</th>\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:left;border-bottom:1px solid var(--br)">Prov.</th>\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:left;border-bottom:1px solid var(--br)">Ultima partita</th>\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:left;border-bottom:1px solid var(--br)">Campionato</th>\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:left;border-bottom:1px solid var(--br)">Citt\u00e0/Campo</th>\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:center;border-bottom:1px solid var(--br)">Giorni fa</th>\';\n'
    '  html+=\'</tr></thead><tbody>\';\n'
    '  rows.forEach(function(r,i){\n'
    '    const days=r.last?daysDiff(r.last.data):null;\n'
    '    const bg=rowColor(days);\n'
    '    const dataStr=r.last?r.last.data:\'\u2014\';\n'
    '    const campStr=r.last?r.last.camp:\'\u2014\';\n'
    '    const cittaStr=r.last?r.last.citta:\'\u2014\';\n'
    '    const daysStr=days!==null?days+\'gg\':\'n/d\';\n'
    '    const nomeSafe=r.nome.replace(/\\x27/g,\'\\\\x27\');\n'
    '    html+=\'<tr style="background:\'+bg+\';border-bottom:1px solid #e8eaf0">\';\n'
    '    html+=\'<td style="padding:3px 8px;color:var(--mu)">\'+(i+1)+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px;font-weight:600;cursor:pointer" onclick="openArbitro(\\\'\'+nomeSafe+\'\\\')">\'+r.nome+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px">\'+(r.pv||\'\u2014\')+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px">\'+dataStr+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px;font-size:.68rem">\'+escH(campStr)+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px;font-size:.67rem;color:var(--mu)">\'+escH(cittaStr)+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px;text-align:center;font-weight:600">\'+daysStr+\'</td>\';\n'
    '    html+=\'</tr>\';\n'
    '  });\n'
    '  html+=\'</tbody></table>\';\n'
    '  const label=fM?(function(){const pt=fM.split(\'-\');return MESI_IT[parseInt(pt[1])-1]+\' \'+pt[0];})():(fD1||fD2?(fD1||\'inizio\')+\' \u2192 \'+(fD2||\'oggi\'):\'Tutta la stagione\');\n'
    '  body.innerHTML=\'<p style="font-size:.7rem;color:var(--mu);margin-bottom:6px">Ultima partita nel periodo: <b>\'+label+\'</b>\'+(fPv?\' \u00b7 Prov. \'+fPv:\'\')+\' \u00b7 \'+rows.length+\' arbitri</p>\'+html;\n'
    '}'
)

content = content[:idx_s] + NEW_FN + content[idx_e:]
done.append('1-renderUltimeGare-rewrite')

# ── FIX 2: Territorio — gareFuoriPv use pvComp not arbPv ────────────────────
OLD2 = "            if(pvC&&pvC!==arbPv)gareFuoriPv.push(g);"
NEW2 = "            if(pvC&&!pvComp.has(pvC))gareFuoriPv.push(g);"
if OLD2 in content:
    content = content.replace(OLD2, NEW2, 1); done.append('2-gareFuoriPv-pvComp')
else:
    print('FIX 2 not found - checking...')
    idx = content.find('gareFuoriPv.push')
    print('gareFuoriPv at:', idx, repr(content[idx-30:idx+50]))

# ── FIX 3: campLvlRows — gold border for Eccellenza ─────────────────────────
OLD4 = (
    "              const short=cn.replace('COMITATO REGIONALE SARDEGNA ','').replace('Divisione regionale','DR').replace('Under ','U');\n"
    "              return `<div style=\"display:flex;align-items:center;gap:6px;font-size:.68rem;padding:1px 0\">\n"
    "                <span style=\"background:${col};color:#fff;border-radius:2px;padding:0 5px;flex-shrink:0;font-size:.6rem\">${escH(short)}</span>"
)
NEW4 = (
    "              const short=cn.replace('COMITATO REGIONALE SARDEGNA ','').replace('Divisione regionale','DR').replace('Under ','U');\n"
    "              const isEccBadge=campIsEcc(cn);\n"
    "              return `<div style=\"display:flex;align-items:center;gap:6px;font-size:.68rem;padding:1px 0\">\n"
    "                <span style=\"background:${col};color:#fff;border-radius:2px;padding:0 5px;flex-shrink:0;font-size:.6rem${isEccBadge?';border:2px solid #ffd700':''};\">${isEccBadge?'\u2b50 ':''}${escH(short)}</span>"
)
if OLD4 in content:
    content = content.replace(OLD4, NEW4, 1); done.append('3-campLvl-eccellenza')
else:
    print('FIX 3 not found')
    idx = content.find("replace('Divisione regionale','DR')")
    print('short at:', idx, repr(content[idx:idx+200]))

# ── FIX 4: Territorio SEZ A — sortable table ────────────────────────────────
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
    "          // Riga per riga con campionato, provincia, città, squadra\n"
    "          const terrRows=[];\n"
    "          Object.keys(byCamp).sort((a,b)=>campFipRank(byCamp[a].camp)-campFipRank(byCamp[b].camp)).forEach(key=>{\n"
    "            const {camp,pv,sqs}=byCamp[key];\n"
    "            sqs.sort().forEach(n=>{\n"
    "              const s=SQ[n]||{};\n"
    "              const cityM=(s.campo_principale||'').match(/\\(([A-Z]{2,3})\\)/);\n"
    "              const city=cityM?cityM[1]:'';\n"
    "              terrRows.push({camp,pv,n,city});\n"
    "            });\n"
    "          });\n"
    "          const terrUid='terr_'+Math.random().toString(36).slice(2,6);\n"
    "          let terrSCol='camp',terrSAsc=true;\n"
    "          function renderTerrTbl(){\n"
    "            const sorted=[...terrRows].sort((a,b)=>{\n"
    "              if(terrSCol==='camp'){const ra=campFipRank(a.camp),rb=campFipRank(b.camp);return terrSAsc?ra-rb:rb-ra;}\n"
    "              if(terrSCol==='pv')return terrSAsc?a.pv.localeCompare(b.pv):b.pv.localeCompare(a.pv);\n"
    "              if(terrSCol==='city')return terrSAsc?a.city.localeCompare(b.city):b.city.localeCompare(a.city);\n"
    "              return terrSAsc?a.n.localeCompare(b.n):b.n.localeCompare(a.n);\n"
    "            });\n"
    "            const mkTh=(col,lbl)=>{'use strict';\n"
    "              const act=terrSCol===col,arr=act?(terrSAsc?'\u25b2':'\u25bc'):'';\n"
    "              return '<th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6;cursor:pointer;user-select:none;white-space:nowrap\" onclick=\"'+terrUid+'_sort(\\''+col+'\\')\">'+ lbl+(arr?' <b style=\"color:#c0392b\">'+arr+'</b>':'')+'</th>';\n"
    "            };\n"
    "            let t='<table style=\"width:100%;border-collapse:collapse;font-size:.72rem\">';\n"
    "            t+='<thead><tr style=\"background:#fff0f0\">'+mkTh('camp','Campionato')+mkTh('pv','Prov.')+mkTh('city','Citt\u00e0')+mkTh('sq','Squadra')+'</tr></thead><tbody>';\n"
    "            sorted.forEach((r,i)=>{\n"
    "              const clr=PC[r.pv]||campColor(r.camp)||'#555';\n"
    "              const isEcc=campIsEcc(r.camp);\n"
    "              t+='<tr style=\"border-bottom:1px solid #fce8e8;background:'+(i%2?'#fffafa':'#fff')+'\">';\n"
    "              t+='<td style=\"padding:3px 8px;white-space:nowrap\"><span style=\"'+campBadgeStyle(r.camp)+'border-radius:3px;padding:1px 5px;font-size:.62rem\">'+(isEcc?'\u2b50 ':'')+escH(r.camp.replace(/COMITATO REGIONALE SARDEGNA /i,''))+'</span></td>';\n"
    "              t+='<td style=\"padding:3px 8px\"><span style=\"background:'+clr+';color:#fff;border-radius:3px;padding:0 5px;font-size:.62rem;font-weight:700\">'+r.pv+'</span></td>';\n"
    "              t+='<td style=\"padding:3px 8px;font-size:.68rem;color:var(--mu)\">'+escH(r.city)+'</td>';\n"
    "              t+='<td style=\"padding:3px 8px\">'+sqPill(r.n,clr)+'</td>';\n"
    "              t+='</tr>';\n"
    "            });\n"
    "            t+='</tbody></table>';\n"
    "            const el=document.getElementById(terrUid);\n"
    "            if(el)el.innerHTML=t;\n"
    "          }\n"
    "          window[terrUid+'_sort']=function(col){if(terrSCol===col)terrSAsc=!terrSAsc;else{terrSCol=col;terrSAsc=true;}renderTerrTbl();};\n"
    "          html+='<div id=\"'+terrUid+'\"></div>';\n"
    "          setTimeout(renderTerrTbl,0);\n"
)
if OLD3 in content:
    content = content.replace(OLD3, NEW3, 1); done.append('4-terr-sortable')
else:
    print('FIX 4 not found')

# ── Write back ──────────────────────────────────────────────────────────────
with open(TMPL, 'w', encoding='utf-8') as f:
    f.write(content)
print('Fixes applied:', done)
