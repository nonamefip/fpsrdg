import sys

TMPL = '/sessions/fervent-dreamy-noether/mnt/fpsrdg/scripts/template.html'
with open(TMPL, 'rb') as f:
    content = f.read().decode('utf-8')
done = []

# ── FIX A: renderUltimeGare — fix window.GARE / window.ARBITRI / field names ──
OLD_A = '    (window.GARE||[]).forEach(g=>{if(g[\'Data\'])ms.add(g[\'Data\'].slice(0,7));});\n  }\n  const fPv=selPv?selPv.value:\'\''
NEW_A = '    (GARE||[]).forEach(g=>{if(g[\'Data\'])ms.add(g[\'Data\'].slice(0,7));});\n  }\n  const fPv=selPv?selPv.value:\'\''
if OLD_A in content:
    content = content.replace(OLD_A, NEW_A, 1); done.append('A1-GARE-mesi')
else:
    print('FIX A1 not found'); sys.exit(1)

OLD_A2 = '  (window.GARE||[]).forEach(g=>{'
NEW_A2 = '  (GARE||[]).forEach(g=>{'
if OLD_A2 in content:
    content = content.replace(OLD_A2, NEW_A2, 1); done.append('A2-GARE-loop')
else:
    print('FIX A2 not found'); sys.exit(1)

# Fix the arbitri lookup in renderUltimeGare: uses g['Arbitri'] but global GARE has 'Arbitro 1'/'Arbitro 2'
# Also fix the field names for the nome/provincia of persons
OLD_A3 = (
    "    const arbs=(g['Arbitri']||g['Arbitro']||'')"
    ".split(',').map(s=>s.trim()).filter(Boolean);\n"
)
NEW_A3 = (
    "    const arbs=[g['Arbitro 1']||'',g['Arbitro 2']||''].map(s=>s.trim()).filter(Boolean);\n"
)
if OLD_A3 in content:
    content = content.replace(OLD_A3, NEW_A3, 1); done.append('A3-arbs-fields')
else:
    print('FIX A3 not found'); sys.exit(1)

OLD_A4 = "  const ARB=(window.ARBITRI||[]);\n  let rows=ARB.map(a=>{\n    const nome=(a['Nome Cognome']||a['Nome']||'').trim();\n    const pv=(a['Provincia']||'').toUpperCase();"
NEW_A4 = "  const ARB=Object.values(P).filter(x=>x.categoria==='Arbitro');\n  let rows=ARB.map(a=>{\n    const nome=(a.nome||'').trim();\n    const pv=(a.provincia||'').toUpperCase();"
if OLD_A4 in content:
    content = content.replace(OLD_A4, NEW_A4, 1); done.append('A4-ARB-persons')
else:
    print('FIX A4 not found'); sys.exit(1)

# Fix citta field: global GARE has 'Campo' not 'CittaGara'
OLD_A5 = "        ultimaMap[a]={data:d,camp:g['Campionato']||'',citta:g['CittaGara']||g['Citt\u00e0']||''};"
NEW_A5 = "        ultimaMap[a]={data:d,camp:g['Campionato']||'',citta:g['Campo']||''};"
if OLD_A5 in content:
    content = content.replace(OLD_A5, NEW_A5, 1); done.append('A5-citta-campo')
else:
    print('FIX A5 not found'); sys.exit(1)

# ── FIX B: Territorio tab — complete rewrite ────────────────────────────────
OLD_B = """  // Populate mpt-terr (Territorio — province analysis)
  if(p.categoria==='Arbitro'){
    try{
      const terrEl=document.getElementById('mpt-terr-inner');
      if(terrEl){
        const arbPv=p.provincia||'';
        // Province di competenza: SS e NU si coprono a vicenda
        const pvComp=new Set([arbPv]);
        if(arbPv==='SS')pvComp.add('NU');
        if(arbPv==='NU')pvComp.add('SS');
        // Squadre visitate (campo casa o trasferta in gare arbitrate)
        const sqVisitate=new Set();
        const sqFuoriPv=new Set();
        (p.gare||[]).forEach(g=>{
          ['Casa','Ospite'].forEach(f=>{
            const sq=g[f]||'';if(!sq)return;
            const sqData=SQ[sq];const pvSq=sqData?sqData.prov:'';
            if(pvSq&&!pvComp.has(pvSq))sqFuoriPv.add(sq);
            sqVisitate.add(sq);
          });
        });
        // Tutte le squadre nelle province di competenza
        const sqDiComp=Object.entries(SQ).filter(([n,s])=>pvComp.has(s.prov||'')).map(([n])=>n);
        const sqNonVisitate=sqDiComp.filter(n=>!sqVisitate.has(n));
        // Render helper
        function sqPill(nome,clr){
          const s=SQ[nome]||{};const campo=s.campo_principale||'';
          const city=campo?(campo.match(/([A-Z]{2,})\\s*\\(\\s*\\w+\\s*\\)/)||[])[1]||'':'';
          return `<span style="display:inline-flex;align-items:center;gap:3px;background:${clr}18;border:1px solid ${clr}44;border-radius:5px;padding:2px 7px;font-size:.65rem;margin:2px;cursor:pointer" onclick="openSquadra('${nome.replace(/'/g,\"\\'\")}')"><b style="color:${clr}">${escH(nome)}</b>${city?`<span style="color:var(--mu);font-size:.6rem">${city}</span>`:''}</span>`;
        }
        const pvLabel=[...pvComp].join(' + ');
        let html=`<div style="font-size:.72rem;color:var(--mu);margin-bottom:12px">Province di competenza: <b>${pvLabel}</b>${arbPv==='SS'?' (include NU per accordo territoriale)':arbPv==='NU'?' (include SS per accordo territoriale)':''}</div>`;
        // SEZIONE 1: mai stati nelle province di competenza
        html+=`<div style="margin-bottom:16px"><div style="font-size:.75rem;font-weight:700;color:#c0392b;margin-bottom:6px;display:flex;align-items:center;gap:6px">
          ❌ Squadre NON ancora arbitrate (${pvLabel}) <span style="font-size:.65rem;font-weight:400;color:var(--mu)">${sqNonVisitate.length} su ${sqDiComp.length}</span></div>`;
        if(sqNonVisitate.length===0){
          html+=`<div style="color:var(--green);font-size:.72rem;padding:6px 10px;background:#e6f8ee;border-radius:6px">✅ Hai arbitrato almeno una volta tutte le squadre della tua provincia!</div>`;
        }else{
          // Group by prov
          const byPv={};sqNonVisitate.forEach(n=>{const pv=(SQ[n]||{}).prov||'?';if(!byPv[pv])byPv[pv]=[];byPv[pv].push(n);});
          Object.entries(byPv).sort().forEach(([pv,sqs])=>{
            const clr=PC[pv]||'#555';
            html+=`<div style="margin-bottom:6px"><span style="font-size:.62rem;font-weight:700;color:${clr};background:${clr}18;border-radius:4px;padding:1px 7px;margin-right:4px">${pv}</span>`;
            html+=sqs.sort().map(n=>sqPill(n,clr)).join('');
            html+='</div>';
          });
        }
        html+='</div>';
        // SEZIONE 2: squadre arbitrate fuori provincia
        html+=`<div><div style="font-size:.75rem;font-weight:700;color:#0d6efd;margin-bottom:6px;display:flex;align-items:center;gap:6px">
          🌍 Squadre arbitrate FUORI provincia <span style="font-size:.65rem;font-weight:400;color:var(--mu)">${sqFuoriPv.size} squadre</span></div>`;
        if(sqFuoriPv.size===0){
          html+=`<div style="color:var(--mu);font-size:.72rem;padding:6px 10px;background:#f8faff;border-radius:6px">Nessuna gara arbitrata fuori dalle province di competenza.</div>`;
        }else{
          const byPv2={};[...sqFuoriPv].forEach(n=>{const pv=(SQ[n]||{}).prov||'?';if(!byPv2[pv])byPv2[pv]=[];byPv2[pv].push(n);});
          Object.entries(byPv2).sort().forEach(([pv,sqs])=>{
            const clr=PC[pv]||'#555';
            html+=`<div style="margin-bottom:6px"><span style="font-size:.62rem;font-weight:700;color:${clr};background:${clr}18;border-radius:4px;padding:1px 7px;margin-right:4px">${pv}</span>`;
            html+=sqs.sort().map(n=>sqPill(n,clr)).join('');
            html+='</div>';
          });
        }
        html+='</div>';
        terrEl.innerHTML=html;
      }
    }catch(e){const el=document.getElementById('mpt-terr-inner');if(el)el.innerHTML='<p style="color:red;padding:10px">Errore territorio: '+e.message+'</p>';}
  }"""

NEW_B = """  // Populate mpt-terr (Territorio — province analysis)
  if(p.categoria==='Arbitro'){
    try{
      const terrEl=document.getElementById('mpt-terr-inner');
      if(terrEl){
        const arbPv=p.provincia||'';
        // Province di competenza: SS e NU condivise
        const pvComp=new Set([arbPv]);
        if(arbPv==='SS')pvComp.add('NU');
        if(arbPv==='NU')pvComp.add('SS');
        // Gare arbitrate (usa gare_arbitro con chiavi .h/.a/.campo)
        const gaArb=p.gare_arbitro||[];
        // Squadre visitate (qualunque ruolo: casa o ospite)
        const sqVisitate=new Set();
        // Squadre di FUORI area che l'arbitro HA arbitrato (sezione 1)
        const sqFuoriArea=new Set();
        // Campi visitati
        const campiVisitati=new Set();
        // Gare giocate fuori dalla propria provincia (arbPv, non pvComp)
        const gareFuoriPv=[];
        gaArb.forEach(g=>{
          const h=g.h||'', a=g.a||'', campo=g.campo||g['Campo']||'';
          if(h){sqVisitate.add(h);const pvH=(SQ[h]||{}).prov||'';if(pvH&&!pvComp.has(pvH))sqFuoriArea.add(h);}
          if(a){sqVisitate.add(a);const pvA=(SQ[a]||{}).prov||'';if(pvA&&!pvComp.has(pvA))sqFuoriArea.add(a);}
          if(campo){
            campiVisitati.add(campo.trim().toLowerCase());
            const pvC=cpv(campo);
            if(pvC&&pvC!==arbPv)gareFuoriPv.push(g);
          }
        });
        // Tutte le squadre nelle province di competenza
        const sqDiComp=Object.entries(SQ).filter(([n,s])=>pvComp.has(s.prov||'')).map(([n])=>n);
        const sqNonVisitate=sqDiComp.filter(n=>!sqVisitate.has(n));
        // Campi nella provincia di competenza non visitati
        const campiDiComp=Object.values(D.campi||{}).filter(c=>pvComp.has(c.prov||''));
        const campiNonVisitati=campiDiComp.filter(c=>!campiVisitati.has((c.campo||'').trim().toLowerCase()));
        // Helper pill squadra
        function sqPill(nome,clr){
          const s=SQ[nome]||{};
          const campo=s.campo_principale||'';
          const city=campo?(campo.match(/\\(([A-Z]{2,3})\\)/)||[])[1]||'':'';
          const ns=nome.replace(/\x27/g,'\\x27');
          return '<span style="display:inline-flex;align-items:center;gap:3px;background:'+clr+'18;border:1px solid '+clr+'44;border-radius:5px;padding:2px 7px;font-size:.65rem;margin:2px;cursor:pointer" onclick="openSquadra(\''+ns+'\')">'+'<b style="color:'+clr+'">'+escH(nome)+'</b>'+(city?'<span style="color:var(--mu);font-size:.6rem">'+city+'</span>':'')+'</span>';
        }
        const pvLabel=[...pvComp].sort().join(' + ');
        let html='<div style="font-size:.72rem;color:var(--mu);margin-bottom:14px">Province di competenza: <b>'+pvLabel+'</b>'+(arbPv==='SS'?' (include NU per accordo territoriale)':arbPv==='NU'?' (include SS per accordo territoriale)':'')+'</div>';

        // ── SEZIONE A: Squadre NON arbitrate nella provincia di competenza (in colonna con categoria) ──
        html+='<div style="margin-bottom:18px">';
        html+='<div style="font-size:.75rem;font-weight:700;color:#c0392b;margin-bottom:8px;display:flex;align-items:center;gap:6px">';
        html+='\u274c Squadre NON ancora arbitrate ('+pvLabel+') ';
        html+='<span style="font-size:.65rem;font-weight:400;color:var(--mu)">'+sqNonVisitate.length+' su '+sqDiComp.length+'</span></div>';
        if(sqNonVisitate.length===0){
          html+='<div style="color:var(--green);font-size:.72rem;padding:6px 10px;background:#e6f8ee;border-radius:6px">\u2705 Hai arbitrato almeno una volta tutte le squadre della tua provincia!</div>';
        }else{
          // Raggruppa per campionato poi per provincia
          const byCamp={};
          sqNonVisitate.forEach(n=>{
            const s=SQ[n]||{};
            const camps=Object.keys(s.campionati||{});
            const camp=camps.length?camps.sort((a,b)=>(s.campionati[b]||0)-(s.campionati[a]||0))[0]:'Sconosciuto';
            const pv=s.prov||'?';
            const key=camp+'||'+pv;
            if(!byCamp[key])byCamp[key]={camp,pv,sqs:[]};
            byCamp[key].sqs.push(n);
          });
          // Ordina per campionato
          const campKeys=Object.keys(byCamp).sort();
          html+='<table style="width:100%;border-collapse:collapse;font-size:.72rem">';
          html+='<thead><tr style="background:#fff0f0"><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6">Campionato</th><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6">Prov.</th><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #f5c6c6">Squadre</th></tr></thead><tbody>';
          campKeys.forEach(key=>{
            const {camp,pv,sqs}=byCamp[key];
            const clr=PC[pv]||campColor(camp)||'#555';
            html+='<tr style="border-bottom:1px solid #fce8e8;vertical-align:top">';
            html+='<td style="padding:4px 8px;font-size:.68rem;white-space:nowrap"><span style="'+campBadgeStyle(camp)+'border-radius:3px;padding:1px 6px;font-size:.62rem">'+escH(camp)+'</span></td>';
            html+='<td style="padding:4px 8px;font-size:.68rem;white-space:nowrap"><span style="background:'+clr+';color:#fff;border-radius:3px;padding:1px 5px;font-size:.62rem;font-weight:700">'+escH(pv)+'</span></td>';
            html+='<td style="padding:3px 8px">'+sqs.sort().map(n=>sqPill(n,clr)).join('')+'</td>';
            html+='</tr>';
          });
          html+='</tbody></table>';
        }
        html+='</div>';

        // ── SEZIONE B: Squadre arbitrate che NON risiedono nella provincia di competenza ──
        html+='<div style="margin-bottom:18px">';
        html+='<div style="font-size:.75rem;font-weight:700;color:#6f42c1;margin-bottom:8px;display:flex;align-items:center;gap:6px">';
        html+='\U0001f30d Squadre arbitrate di FUORI area ';
        html+='<span style="font-size:.65rem;font-weight:400;color:var(--mu)">'+sqFuoriArea.size+' squadre</span></div>';
        if(sqFuoriArea.size===0){
          html+='<div style="color:var(--mu);font-size:.72rem;padding:6px 10px;background:#f8faff;border-radius:6px">Nessuna squadra fuori area arbitrata.</div>';
        }else{
          const byPvF={};[...sqFuoriArea].forEach(n=>{const pv=(SQ[n]||{}).prov||'?';if(!byPvF[pv])byPvF[pv]=[];byPvF[pv].push(n);});
          Object.entries(byPvF).sort().forEach(function(entry){
            const pv=entry[0],sqs=entry[1];
            const clr=PC[pv]||'#555';
            html+='<div style="margin-bottom:6px"><span style="background:'+clr+';color:#fff;border-radius:4px;padding:1px 7px;margin-right:4px;font-size:.62rem;font-weight:700">'+pv+'</span>';
            html+=sqs.sort().map(n=>sqPill(n,clr)).join('');
            html+='</div>';
          });
        }
        html+='</div>';

        // ── SEZIONE C: Gare disputate FUORI dalla propria provincia ──
        html+='<div style="margin-bottom:18px">';
        html+='<div style="font-size:.75rem;font-weight:700;color:#e67e22;margin-bottom:8px;display:flex;align-items:center;gap:6px">';
        html+='\U0001f3df\ufe0f Gare fuori dalla propria provincia ('+arbPv+') ';
        html+='<span style="font-size:.65rem;font-weight:400;color:var(--mu)">'+gareFuoriPv.length+' gare</span></div>';
        if(gareFuoriPv.length===0){
          html+='<div style="color:var(--mu);font-size:.72rem;padding:6px 10px;background:#fff8f0;border-radius:6px">Nessuna gara disputata fuori dalla provincia '+arbPv+'.</div>';
        }else{
          const byPvG={};gareFuoriPv.forEach(g=>{const pv=cpv(g.campo||g['Campo']||'')||'?';if(!byPvG[pv])byPvG[pv]=[];byPvG[pv].push(g);});
          html+='<table style="width:100%;border-collapse:collapse;font-size:.72rem">';
          html+='<thead><tr style="background:#fff8f0"><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #ffe0b2">Data</th><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #ffe0b2">Campo</th><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #ffe0b2">Prov.</th><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #ffe0b2">Casa</th><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #ffe0b2">Ospite</th><th style="padding:4px 8px;text-align:left;border-bottom:1px solid #ffe0b2">Campionato</th></tr></thead><tbody>';
          gareFuoriPv.sort((a,b)=>(a.d||'').localeCompare(b.d||'')).forEach((g,i)=>{
            const pv=cpv(g.campo||g['Campo']||'')||'?';
            const clr=PC[pv]||'#888';
            html+='<tr style="background:'+(i%2?'#fffaf5':'#fff')+';border-bottom:1px solid #ffe0b2">';
            html+='<td style="padding:3px 8px;white-space:nowrap">'+(g.d||'')+'</td>';
            html+='<td style="padding:3px 8px;font-size:.67rem">'+escH(g.campo||g['Campo']||'—')+'</td>';
            html+='<td style="padding:3px 8px"><span style="background:'+clr+';color:#fff;border-radius:3px;padding:0 5px;font-size:.62rem;font-weight:700">'+pv+'</span></td>';
            html+='<td style="padding:3px 8px;font-size:.68rem">'+escH(g.h||'')+'</td>';
            html+='<td style="padding:3px 8px;font-size:.68rem">'+escH(g.a||'')+'</td>';
            html+='<td style="padding:3px 8px;font-size:.65rem">'+escH(g.c||g['Campionato']||'')+'</td>';
            html+='</tr>';
          });
          html+='</tbody></table>';
        }
        html+='</div>';

        // ── SEZIONE D: Campi in provincia di competenza non visitati ──
        html+='<div style="margin-bottom:8px">';
        html+='<div style="font-size:.75rem;font-weight:700;color:#16a085;margin-bottom:8px;display:flex;align-items:center;gap:6px">';
        html+='\U0001f3df\ufe0f Campi in '+pvLabel+' non ancora visitati ';
        html+='<span style="font-size:.65rem;font-weight:400;color:var(--mu)">'+campiNonVisitati.length+' su '+campiDiComp.length+'</span></div>';
        if(campiNonVisitati.length===0){
          html+='<div style="color:var(--green);font-size:.72rem;padding:6px 10px;background:#e6f8ee;border-radius:6px">\u2705 Hai visitato tutti i campi della tua provincia!</div>';
        }else{
          const byPvCa={};campiNonVisitati.forEach(c=>{const pv=c.prov||'?';if(!byPvCa[pv])byPvCa[pv]=[];byPvCa[pv].push(c);});
          Object.entries(byPvCa).sort().forEach(function(entry){
            const pv=entry[0],cs=entry[1];
            const clr=PC[pv]||'#555';
            html+='<div style="margin-bottom:6px"><span style="background:'+clr+';color:#fff;border-radius:4px;padding:1px 7px;margin-right:4px;font-size:.62rem;font-weight:700">'+pv+'</span>';
            html+=cs.sort((a,b)=>(a.campo||'').localeCompare(b.campo||'')).map(c=>{
              const city=c.campo?(c.campo.match(/\\(([A-Z]{2,3})\\)/)||[])[1]||'':'';
              return '<span style="display:inline-flex;align-items:center;gap:3px;background:'+clr+'12;border:1px solid '+clr+'33;border-radius:5px;padding:2px 8px;font-size:.65rem;margin:2px">'+'<b style="color:'+clr+'">'+escH(c.campo||'')+'</b>'+(city?'<span style="color:var(--mu);font-size:.6rem">'+city+'</span>':'')+'</span>';
            }).join('');
            html+='</div>';
          });
        }
        html+='</div>';

        terrEl.innerHTML=html;
      }
    }catch(e){const el=document.getElementById('mpt-terr-inner');if(el)el.innerHTML='<p style="color:red;padding:10px">Errore territorio: '+e.message+'<br><small>'+e.stack+'</small></p>';}
  }"""

if OLD_B in content:
    content = content.replace(OLD_B, NEW_B, 1); done.append('B-territorio-rewrite')
else:
    print('FIX B not found — checking partial match...')
    # find approximate location
    idx = content.find('// Populate mpt-terr')
    print('mpt-terr comment at:', idx)
    sys.exit(1)

with open(TMPL, 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixes applied:', done)
