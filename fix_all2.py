import sys

TMPL = '/sessions/fervent-dreamy-noether/mnt/fpsrdg/scripts/template.html'

with open(TMPL, 'rb') as f:
    raw = f.read()

content = raw.decode('utf-8')
done = []

# ── FIX 4: Make _fToggle generic ───────────────────────────────────────────
# Find the function by locating its start and next function
FTOGGLE_START = 'function _fToggle(e){'
NEXT_FN = '\nfunction _tmSortClick'

idx_start = content.find(FTOGGLE_START)
idx_end = content.find(NEXT_FN, idx_start)
if idx_start == -1 or idx_end == -1:
    print('FIX 4 markers not found, start=%d end=%d' % (idx_start, idx_end))
    sys.exit(1)

OLD4 = content[idx_start:idx_end]
print('OLD4 length:', len(OLD4))

NEW4 = (
    'function _fToggle(e){\n'
    '  if(e.target.tagName===\'SELECT\'||e.target.tagName===\'BUTTON\'||e.target.tagName===\'INPUT\')return;\n'
    '  const hd=e.currentTarget;const card=hd.closest(\'.db-card\')||hd.parentElement;\n'
    '  const wrap=card?card.querySelector(\'[id$="-wrap"]\'):null;\n'
    '  const tog=card?card.querySelector(\'[id$="-tog"]\'):null;\n'
    '  if(!wrap)return;\n'
    '  const open=wrap.style.display!==\'none\';\n'
    '  wrap.style.display=open?\'none\':\'block\';\n'
    '  if(tog)tog.textContent=open?\'\u25b6\':\'\u25bc\';\n'
    '  if(!open){const wid=wrap.id;\n'
    '    if(wid===\'fermi-wrap\')renderArbiFermi();\n'
    '    else if(wid===\'ultime-wrap\')renderUltimeGare();\n'
    '  }\n'
    '}'
)

content = content.replace(OLD4, NEW4, 1)
done.append('4-_fToggle-generic')

# ── FIX 5: Add "Ultima partita" HTML section BEFORE ROW 4e ─────────────────
MARKER5 = '  <!-- ROW 4e: Arbitri meno attivi -->'

ULTIME_HTML = (
    '  <!-- ROW 4e-pre: Ultima partita arbitri -->\n'
    '  <div class="db-card" style="margin-bottom:12px">\n'
    '    <div class="db-card-hd" style="cursor:pointer;display:flex;align-items:center;gap:8px;flex-wrap:wrap" onclick="_fToggle(event)">\n'
    '      <span>\U0001f4c5 Ultima partita \u2014 tutti gli arbitri</span>\n'
    '      <span id="ultime-tog" style="font-size:.8rem;color:var(--mu)">\u25b6</span>\n'
    '      <span style="font-size:.65rem;font-weight:400;color:var(--mu)">clicca per espandere</span>\n'
    '      <select id="ultime-pv" onchange="renderUltimeGare()" style="font-size:.72rem;padding:2px 6px;border:1px solid var(--br);border-radius:6px">\n'
    '        <option value="">Tutte le province</option>\n'
    '        <option value="CA">CA</option><option value="SS">SS</option><option value="SU">SU</option>\n'
    '        <option value="NU">NU</option><option value="OR">OR</option>\n'
    '      </select>\n'
    '      <select id="ultime-m" onchange="renderUltimeGare()" style="font-size:.72rem;padding:2px 6px;border:1px solid var(--br);border-radius:6px">\n'
    '        <option value="">Tutti i mesi</option>\n'
    '      </select>\n'
    '      <button onclick="document.getElementById(\'ultime-pv\').value=\'\';document.getElementById(\'ultime-m\').value=\'\';document.getElementById(\'ultime-d1\').value=\'\';document.getElementById(\'ultime-d2\').value=\'\';renderUltimeGare()" style="font-size:.68rem;padding:2px 8px;border:1px solid var(--br);border-radius:6px;background:none;cursor:pointer;color:var(--mu)">\u21ba</button>\n'
    '      <span style="font-size:.65rem;color:var(--mu);margin-left:auto">ordinati per data ultima partita</span>\n'
    '    </div>\n'
    '    <div id="ultime-wrap" style="display:none;margin-top:8px">\n'
    '      <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:8px;padding:6px 8px;background:#f8faff;border-radius:6px">\n'
    '        <span style="font-size:.7rem;font-weight:600;color:var(--mu)">Periodo preciso:</span>\n'
    '        <div style="display:flex;align-items:center;gap:4px">\n'
    '          <label style="font-size:.68rem;color:var(--mu)">Dal</label>\n'
    '          <input type="date" id="ultime-d1" onchange="renderUltimeGare()" style="font-size:.68rem;padding:2px 5px;border:1px solid var(--br);border-radius:4px">\n'
    '        </div>\n'
    '        <div style="display:flex;align-items:center;gap:4px">\n'
    '          <label style="font-size:.68rem;color:var(--mu)">Al</label>\n'
    '          <input type="date" id="ultime-d2" onchange="renderUltimeGare()" style="font-size:.68rem;padding:2px 5px;border:1px solid var(--br);border-radius:4px">\n'
    '        </div>\n'
    '        <button onclick="document.getElementById(\'ultime-d1\').value=\'\';document.getElementById(\'ultime-d2\').value=\'\';renderUltimeGare()" style="font-size:.65rem;padding:1px 6px;border:1px solid var(--br);border-radius:4px;background:none;cursor:pointer;color:var(--mu)">\u21ba date</button>\n'
    '        <span style="font-size:.65rem;color:var(--mu2)">oppure usa il selettore mese sopra</span>\n'
    '      </div>\n'
    '      <div id="ultime-body"></div>\n'
    '    </div>\n'
    '  </div>\n\n'
)

if MARKER5 in content:
    content = content.replace(MARKER5, ULTIME_HTML + MARKER5, 1)
    done.append('5-ultime-HTML')
else:
    print('FIX 5 MARKER NOT FOUND')
    sys.exit(1)

# ── FIX 6: Add renderUltimeGare() function BEFORE renderArbiFermi() ─────────
MARKER6 = 'function renderArbiFermi(){'

RENDER_ULTIME = (
    'function renderUltimeGare(){\n'
    '  const selPv=document.getElementById(\'ultime-pv\');\n'
    '  const selM=document.getElementById(\'ultime-m\');\n'
    '  const body=document.getElementById(\'ultime-body\');\n'
    '  if(!body)return;\n'
    '  const MESI_IT=[\'Gen\',\'Feb\',\'Mar\',\'Apr\',\'Mag\',\'Giu\',\'Lug\',\'Ago\',\'Set\',\'Ott\',\'Nov\',\'Dic\'];\n'
    '  if(selM&&selM.options.length<=1){\n'
    '    const ms=new Set();\n'
    '    (window.GARE||[]).forEach(g=>{if(g[\'Data\'])ms.add(g[\'Data\'].slice(0,7));});\n'
    '    [...ms].sort().reverse().forEach(m=>{\n'
    '      const[y,mo]=m.split(\'-\');\n'
    '      selM.innerHTML+=\'<option value="\'+m+\'">\'+MESI_IT[parseInt(mo)-1]+\' \'+y+\'</option>\';\n'
    '    });\n'
    '  }\n'
    '  const fPv=selPv?selPv.value:\'\';\n'
    '  const fM=selM?selM.value:\'\';\n'
    '  const fD1=(document.getElementById(\'ultime-d1\')||{}).value||\'\';\n'
    '  const fD2=(document.getElementById(\'ultime-d2\')||{}).value||\'\';\n'
    '  const ultimaMap={};\n'
    '  (window.GARE||[]).forEach(g=>{\n'
    '    const d=g[\'Data\']||\'\';\n'
    '    if(!d)return;\n'
    '    if(fD1&&d<fD1)return;\n'
    '    if(fD2&&d>fD2)return;\n'
    '    if(fM&&!d.startsWith(fM))return;\n'
    '    const arbs=(g[\'Arbitri\']||g[\'Arbitro\']||\'\')'
    '.split(\',\').map(s=>s.trim()).filter(Boolean);\n'
    '    arbs.forEach(a=>{\n'
    '      if(!ultimaMap[a]||d>ultimaMap[a].data){\n'
    '        ultimaMap[a]={data:d,camp:g[\'Campionato\']||\'\',citta:g[\'CittaGara\']||g[\'Citt\u00e0\']||\'\'};\n'
    '      }\n'
    '    });\n'
    '  });\n'
    '  const ARB=(window.ARBITRI||[]);\n'
    '  let rows=ARB.map(a=>{\n'
    '    const nome=(a[\'Nome Cognome\']||a[\'Nome\']||\'\').trim();\n'
    '    const pv=(a[\'Provincia\']||\'\').toUpperCase();\n'
    '    const last=ultimaMap[nome]||null;\n'
    '    return {nome,pv,last};\n'
    '  }).filter(r=>r.nome);\n'
    '  if(fPv) rows=rows.filter(r=>r.pv===fPv);\n'
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
    '  html+=\'<th style="padding:4px 8px;text-align:left;border-bottom:1px solid var(--br)">Citt\u00e0</th>\';\n'
    '  html+=\'<th style="padding:4px 8px;text-align:center;border-bottom:1px solid var(--br)">Giorni fa</th>\';\n'
    '  html+=\'</tr></thead><tbody>\';\n'
    '  rows.forEach(function(r,i){\n'
    '    const days=r.last?daysDiff(r.last.data):null;\n'
    '    const bg=rowColor(days);\n'
    '    const dataStr=r.last?r.last.data:\'\u2014\';\n'
    '    const campStr=r.last?r.last.camp:\'\u2014\';\n'
    '    const cittaStr=r.last?r.last.citta:\'\u2014\';\n'
    '    const daysStr=days!==null?days+\'gg\':\'n/d\';\n'
    '    const nomeSafe=r.nome.replace(/\x27/g,"\\x27");\n'
    '    html+=\'<tr style="background:\'+bg+\';border-bottom:1px solid #e8eaf0">\';\n'
    '    html+=\'<td style="padding:3px 8px;color:var(--mu)">\'+( i+1)+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px;font-weight:600;cursor:pointer" onclick="openArbitro(\\\'\'+nomeSafe+\'\\\')">\'+r.nome+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px">\'+( r.pv||\'\u2014\')+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px">\'+dataStr+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px;font-size:.68rem">\'+escH(campStr)+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px">\'+escH(cittaStr)+\'</td>\';\n'
    '    html+=\'<td style="padding:3px 8px;text-align:center;font-weight:600">\'+daysStr+\'</td>\';\n'
    '    html+=\'</tr>\';\n'
    '  });\n'
    '  html+=\'</tbody></table>\';\n'
    '  const label=fM?(function(){const pt=fM.split(\'-\');return MESI_IT[parseInt(pt[1])-1]+\' \'+pt[0];})():(fD1||fD2?(fD1||\'inizio\')+\' \u2192 \'+(fD2||\'oggi\'):\'Tutta la stagione\');\n'
    '  body.innerHTML=\'<p style="font-size:.7rem;color:var(--mu);margin-bottom:6px">Periodo: <b>\'+label+\'</b>\'+(fPv?\' \u00b7 Prov. \'+fPv:\'\')+\' \u00b7 \'+rows.length+\' arbitri</p>\'+html;\n'
    '}\n\n'
)

if MARKER6 in content:
    content = content.replace(MARKER6, RENDER_ULTIME + MARKER6, 1)
    done.append('6-renderUltimeGare')
else:
    print('FIX 6 MARKER NOT FOUND')
    sys.exit(1)

# ── Write back ──────────────────────────────────────────────────────────────
with open(TMPL, 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixes applied:', done)
