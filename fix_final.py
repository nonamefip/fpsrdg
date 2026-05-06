import sys, re

TMPL = '/sessions/fervent-dreamy-noether/mnt/fpsrdg/scripts/template.html'
with open(TMPL, 'rb') as f:
    content = f.read().decode('utf-8')
done = []

# ── FIX 1: mkPills — add gold border for Eccellenza campionati ───────────────
OLD1 = "      s.style.background=colorFn(v);s.textContent=v;"
NEW1 = (
    "      s.style.background=colorFn(v);\n"
    "      if(type==='camp'&&campIsEcc&&campIsEcc(v)){s.style.border='2px solid #ffd700';s.textContent='\u2b50 '+v;}\n"
    "      else s.textContent=v;"
)
if OLD1 in content:
    content = content.replace(OLD1, NEW1, 1); done.append('1-mkPills-eccellenza')
else:
    print('FIX 1 not found'); sys.exit(1)

# ── FIX 2: renderUltimeGare — pns() normalization for name matching ──────────
# Replace the ultimaMap building + rows lookup section
OLD2A = (
    "  // Costruisci ultimaMap da TUTTE le gare (senza filtro data)\n"
    "  const ultimaMap={};\n"
    "  (GARE||[]).forEach(g=>{\n"
    "    const d=g['Data']||'';\n"
    "    if(!d)return;\n"
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
)
NEW2A = (
    "  // Costruisci ultimaMap da TUTTE le gare — normalizza nomi con pns()\n"
    "  const _pns=typeof pns==='function'?pns:s=>s;\n"
    "  const ultimaMap={};\n"
    "  (GARE||[]).forEach(g=>{\n"
    "    const d=g['Data']||'';\n"
    "    if(!d)return;\n"
    "    [g['Arbitro 1']||'',g['Arbitro 2']||''].forEach(s=>{\n"
    "      const k=_pns(s.trim()).trim().toUpperCase();\n"
    "      if(!k)return;\n"
    "      if(!ultimaMap[k]||d>ultimaMap[k].data)\n"
    "        ultimaMap[k]={data:d,camp:g['Campionato']||'',citta:g['Campo']||'',nomeRaw:s.trim()};\n"
    "    });\n"
    "  });\n"
    "  const ARB=Object.values(P).filter(x=>x.categoria==='Arbitro');\n"
    "  let rows=ARB.map(a=>{\n"
    "    const nome=(a.nome||'').trim();\n"
    "    const nomeKey=_pns(nome).trim().toUpperCase();\n"
    "    const pv=(a.provincia||'').toUpperCase();\n"
    "    const last=ultimaMap[nomeKey]||null;\n"
    "    return {nome,pv,last};\n"
    "  }).filter(r=>r.nome);\n"
)
if OLD2A in content:
    content = content.replace(OLD2A, NEW2A, 1); done.append('2-pns-normalization')
else:
    print('FIX 2 not found')
    idx = content.find('Costruisci ultimaMap')
    print('ultimaMap at:', idx)
    sys.exit(1)

# ── FIX 3: SEZ B — squadre fuori area: table with count + clickable games ────
# Find and replace the entire SEZ B block
SEZ_B_START = "        // ── SEZ B: Squadre arbitrate che NON risiedono in pvComp ──"
SEZ_B_END   = "\n\n        // ── SEZ C:"

idx_b = content.find(SEZ_B_START)
idx_be = content.find(SEZ_B_END, idx_b)
if idx_b == -1 or idx_be == -1:
    print('SEZ B markers not found', idx_b, idx_be); sys.exit(1)

NEW_SEZ_B = (
    "        // ── SEZ B: Squadre arbitrate che NON risiedono in pvComp ──\n"
    "        // Conta gare per squadra fuori area\n"
    "        const sqFuoriCount={};\n"
    "        const sqFuoriGames={};\n"
    "        gaArb.forEach(g=>{\n"
    "          [g.h||'',g.a||''].filter(n=>sqFuoriArea.has(n)).forEach(n=>{\n"
    "            sqFuoriCount[n]=(sqFuoriCount[n]||0)+1;\n"
    "            if(!sqFuoriGames[n])sqFuoriGames[n]=[];\n"
    "            sqFuoriGames[n].push(g);\n"
    "          });\n"
    "        });\n"
    "        html+='<div style=\"margin-bottom:18px\">';\n"
    "        html+='<div style=\"font-size:.75rem;font-weight:700;color:#6f42c1;margin-bottom:8px\">';\n"
    "        html+='\U0001f30d Squadre di FUORI area arbitrate <span style=\"font-size:.65rem;font-weight:400;color:var(--mu)\">'+sqFuoriArea.size+' squadre</span></div>';\n"
    "        if(sqFuoriArea.size===0){\n"
    "          html+='<div style=\"color:var(--mu);font-size:.72rem;padding:6px 10px;background:#f8faff;border-radius:6px\">Nessuna squadra fuori area arbitrata.</div>';\n"
    "        }else{\n"
    "          // Ordina per provincia poi nome\n"
    "          const sqFuoriList=[...sqFuoriArea].sort((a,b)=>{\n"
    "            const pa=(SQ[a]||{}).prov||'?',pb=(SQ[b]||{}).prov||'?';\n"
    "            return pa!==pb?pa.localeCompare(pb):a.localeCompare(b);\n"
    "          });\n"
    "          const fUid='fu_'+Math.random().toString(36).slice(2,6);\n"
    "          html+='<div style=\"overflow-x:auto\"><table style=\"width:100%;border-collapse:collapse;font-size:.73rem\">';\n"
    "          html+='<thead><tr style=\"background:#f3eeff\"><th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #d0b8f0\">Squadra</th><th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #d0b8f0\">Prov.</th><th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #d0b8f0\">Campionato</th><th style=\"padding:4px 8px;text-align:center;border-bottom:1px solid #d0b8f0\">Gare</th></tr></thead><tbody>';\n"
    "          sqFuoriList.forEach((n,i)=>{\n"
    "            const pv2=(SQ[n]||{}).prov||'?';\n"
    "            const clr=PC[pv2]||'#555';\n"
    "            const cnt=sqFuoriCount[n]||0;\n"
    "            const camps=Object.keys((SQ[n]||{}).campionati||{}).sort((a,b)=>((SQ[n]||{}).campionati[b]||0)-((SQ[n]||{}).campionati[a]||0));\n"
    "            const mainCamp=camps[0]||'';\n"
    "            const rowId=fUid+'_'+i;\n"
    "            html+='<tr style=\"border-bottom:1px solid #ede8ff;background:'+(i%2?'#faf8ff':'#fff')+'\">';\n"
    "            html+='<td style=\"padding:3px 8px;font-weight:600;cursor:pointer\" onclick=\"openSquadra(\\''+n.replace(/\\x27/g,'\\\\x27')+'\\')\">'+ escH(n)+'</td>';\n"
    "            html+='<td style=\"padding:3px 8px\"><span style=\"background:'+clr+';color:#fff;border-radius:3px;padding:0 5px;font-size:.62rem;font-weight:700\">'+pv2+'</span></td>';\n"
    "            html+='<td style=\"padding:3px 8px;font-size:.67rem\"><span style=\"'+campBadgeStyle(mainCamp)+'border-radius:3px;padding:1px 5px;font-size:.6rem\">'+(campIsEcc(mainCamp)?'\u2b50 ':'')+escH(mainCamp)+'</span></td>';\n"
    "            html+='<td style=\"padding:3px 8px;text-align:center\"><span style=\"background:#6f42c1;color:#fff;border-radius:10px;padding:1px 8px;font-size:.7rem;cursor:pointer;font-weight:700\" onclick=\"(function(){var el=document.getElementById(\\''+rowId+'\\');if(el)el.style.display=el.style.display===\\'none\\'?\\'table-row\\':\\'none\\';})()\">'+cnt+'</span></td>';\n"
    "            html+='</tr>';\n"
    "            // Sub-row with games (hidden by default)\n"
    "            const gs=sqFuoriGames[n]||[];\n"
    "            const gsHtml=gs.sort((a,b)=>(a.d||'').localeCompare(b.d||'')).map(g=>{\n"
    "              return '<tr style=\"background:#f3eeff\"><td style=\"padding:2px 8px;font-size:.67rem\">'+( g.d||'')+'</td><td style=\"padding:2px 8px;font-size:.67rem;color:var(--mu)\" colspan=\"3\">'+escH(g.h||'')+' vs '+escH(g.a||'')+' &nbsp;<span style=\"'+campBadgeStyle(g.c||'')+';border-radius:3px;padding:0 5px;font-size:.6rem\">'+escH(g.c||'')+'</span> &nbsp;<span style=\"color:var(--mu)\">'+escH(g.campo||g[\\'Campo\\']||'')+'</span></td></tr>';\n"
    "            }).join('');\n"
    "            html+='<tr id=\"'+rowId+'\" style=\"display:none;border-bottom:2px solid #d0b8f0\"><td colspan=\"4\" style=\"padding:0\"><table style=\"width:100%;border-collapse:collapse;font-size:.68rem\"><thead><tr style=\"background:#ede8ff\"><th style=\"padding:2px 8px;text-align:left\">Data</th><th style=\"padding:2px 8px;text-align:left\" colspan=\"3\">Gara</th></tr></thead><tbody>'+gsHtml+'</tbody></table></td></tr>';\n"
    "          });\n"
    "          html+='</tbody></table></div>';\n"
    "        }\n"
    "        html+='</div>';\n"
)

content = content[:idx_b] + NEW_SEZ_B + content[idx_be:]
done.append('3-sezB-table-count')

# ── FIX 4: SEZ D — campi non visitati: table with città column ───────────────
SEZ_D_START = "        // ── SEZ D: Campi in pvComp non ancora visitati ──"
SEZ_D_END   = "\n\n        terrEl.innerHTML=html;"

idx_d = content.find(SEZ_D_START)
idx_de = content.find(SEZ_D_END, idx_d)
if idx_d == -1 or idx_de == -1:
    print('SEZ D markers not found', idx_d, idx_de); sys.exit(1)

NEW_SEZ_D = (
    "        // ── SEZ D: Campi in pvComp non ancora visitati ──\n"
    "        html+='<div style=\"margin-bottom:8px\">';\n"
    "        html+='<div style=\"font-size:.75rem;font-weight:700;color:#16a085;margin-bottom:8px\">';\n"
    "        html+='\U0001f3df\ufe0f Campi in '+pvLabel+' non ancora visitati <span style=\"font-size:.65rem;font-weight:400;color:var(--mu)\">'+campiNonVisitati.length+' su '+campiDiComp.length+'</span></div>';\n"
    "        if(campiNonVisitati.length===0){\n"
    "          html+='<div style=\"color:var(--green);font-size:.72rem;padding:6px 10px;background:#e6f8ee;border-radius:6px\">\u2705 Hai visitato tutti i campi della provincia!</div>';\n"
    "        }else{\n"
    "          // Estrai città dal nome campo (pattern: NNNNN CITTA ( PV ))\n"
    "          function extractCity(campo){\n"
    "            const m=(campo||'').match(/\\d{5}\\s+([A-Z\u00c0-\u00d6\u00d8-\u00de][A-Z\u00c0-\u00d6\u00d8-\u00de\\s.'\\-]+?)\\s*\\(/);\n"
    "            return m?m[1].trim():'';\n"
    "          }\n"
    "          const dUid='dc_'+Math.random().toString(36).slice(2,6);\n"
    "          let dSort='prov',dAsc=true;\n"
    "          const dRows=campiNonVisitati.map(c=>({campo:c.campo||'',prov:c.prov||'?',city:extractCity(c.campo||'')}));\n"
    "          function renderDTable(){\n"
    "            const sorted=[...dRows].sort((a,b)=>{\n"
    "              if(dSort==='city')return dAsc?a.city.localeCompare(b.city):b.city.localeCompare(a.city);\n"
    "              if(dSort==='prov')return dAsc?a.prov.localeCompare(b.prov):b.prov.localeCompare(a.prov);\n"
    "              return dAsc?a.campo.localeCompare(b.campo):b.campo.localeCompare(a.campo);\n"
    "            });\n"
    "            const mkDTh=(col,lbl)=>{\n"
    "              const act=dSort===col,arr=act?(dAsc?'\u25b2':'\u25bc'):'';\n"
    "              return '<th style=\"padding:4px 8px;text-align:left;border-bottom:1px solid #b2dfdb;cursor:pointer;user-select:none;white-space:nowrap\" onclick=\"'+dUid+'_s(\\''+col+'\\')\">'+ lbl+(arr?' <b style=\"color:#16a085\">'+arr+'</b>':'')+'</th>';\n"
    "            };\n"
    "            let t='<div style=\"overflow-x:auto\"><table style=\"width:100%;border-collapse:collapse;font-size:.72rem\">';\n"
    "            t+='<thead><tr style=\"background:#e0f2f1\">'+mkDTh('city','Citt\u00e0')+mkDTh('prov','Prov.')+mkDTh('campo','Campo')+'</tr></thead><tbody>';\n"
    "            sorted.forEach((r,i)=>{\n"
    "              const clr=PC[r.prov]||'#555';\n"
    "              t+='<tr style=\"border-bottom:1px solid #e0f2f1;background:'+(i%2?'#f9fffd':'#fff')+'\">';\n"
    "              t+='<td style=\"padding:3px 8px;font-weight:600;white-space:nowrap\">'+escH(r.city||'\u2014')+'</td>';\n"
    "              t+='<td style=\"padding:3px 8px\"><span style=\"background:'+clr+';color:#fff;border-radius:3px;padding:0 5px;font-size:.62rem;font-weight:700\">'+r.prov+'</span></td>';\n"
    "              t+='<td style=\"padding:3px 8px;font-size:.67rem;color:var(--mu)\">'+escH(r.campo)+'</td>';\n"
    "              t+='</tr>';\n"
    "            });\n"
    "            t+='</tbody></table></div>';\n"
    "            const el=document.getElementById(dUid);if(el)el.innerHTML=t;\n"
    "          }\n"
    "          window[dUid+'_s']=function(col){if(dSort===col)dAsc=!dAsc;else{dSort=col;dAsc=true;}renderDTable();};\n"
    "          html+='<div id=\"'+dUid+'\"></div>';\n"
    "          setTimeout(renderDTable,0);\n"
    "        }\n"
    "        html+='</div>';\n"
)

content = content[:idx_d] + NEW_SEZ_D + content[idx_de:]
done.append('4-sezD-table-city')

# ── Write ────────────────────────────────────────────────────────────────────
with open(TMPL, 'w', encoding='utf-8') as f:
    f.write(content)
print('Fixes applied:', done)
