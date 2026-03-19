"""
Relatório Gerencial — Aderência de Ciclo PM/SD
Premissa: quantidades estão corretas. Check = ciclo (PM/SD vs AM).
"""
import subprocess, sys, os, requests
sys.stdout.reconfigure(encoding='utf-8')
from collections import defaultdict
from datetime import date, datetime

GCLOUD = r'C:\Users\gmreis\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
token = subprocess.run([GCLOUD, 'auth', 'application-default', 'print-access-token'],
                       capture_output=True, text=True).stdout.strip()
H = {'Authorization': f'Bearer {token}', 'x-goog-user-project': 'meli-bi-data'}

from google.oauth2.credentials import Credentials
from google.cloud import bigquery
bq_client = bigquery.Client(project='meli-bi-data', credentials=Credentials(token=token))

MLP_MAP = {
    'HAWK':'Hawk Transportes','ALC':'A L C TRANSPORTES','BLD':'BLD LOGÍSTICA',
    'PARCEIRO SPOT':'PARCEIRO SPOT SOLUCOES','ADO':'ADO TRANSPORTADORA',
    'TORRES':'TORRESTRANSP','JM TRANSP.':'JM Transportes','UNICA':'UNICA TRANSPORTES',
    'ONTIME':'ON TIME SERVICOS','DECARGO':'DECARGO','FLIGHT CARGO':'Flight Cargo',
    'DHL':'DHL','FLASHLOG':'FLASHLOG','COOPMETRO':'COOPMETRO','RODALOG':'RODALOG',
    'ALTOVALE':'ALTOVALE','ECO EXPRESS':'ECO EXPRESS','REDE FRETE':'Rede Frete',
    'RENNER':'RENNER LOCADORA','MURICI':'Murici','LOG SERVICOS':'LOG SERVICOS',
    'ATITUDE':'ATITUDE','AR CARGO':'AR CARGO',
}
def norm(n): return MLP_MAP.get(n.strip().upper(), n.strip())

WEEK_TO_DATE = {f'W{w}': date.fromisocalendar(2026, w, 1) for w in range(1, 25)}
SEMANAS      = ['W7','W8','W9','W10','W11','W12','W13','W14','W15','W16','W17','W18']
COL_IDX      = {w: 15+i for i, w in enumerate(SEMANAS)}
W_ATUAL      = 12
W_ATUALIZADO = 13

# ── Cronograma: coleta pares SVC×MLP únicos de Expansão PM/SD ─────────────────
resp = requests.get(
    'https://sheets.googleapis.com/v4/spreadsheets/'
    '1yETyJzRyl-4c1ZjORlVXAVPKfHUHD5nsu_OEhY9GTUk/values/Cronograma', headers=H)
cr_rows = resp.json().get('values', [])[2:]

pares    = set()          # (svc, mlp_bq)
neg_semana = {w: 0 for w in SEMANAS}  # quantidade negociada por semana

for row in cr_rows:
    if len(row) < 3 or row[0].strip() != 'Expansão PM/SD':
        continue
    svc = row[2].strip()
    mlp = norm(row[3].strip() if len(row) > 3 else '')
    tem_entrada = False
    for w in SEMANAS:
        ci = COL_IDX[w]
        if len(row) > ci and row[ci] not in ('', '0'):
            try:
                neg_semana[w] += int(row[ci])
                tem_entrada = True
            except ValueError:
                pass
    if tem_entrada:
        pares.add((svc, mlp))

print(f'Pares SVC×MLP PM/SD: {len(pares)}')

# ── BigQuery: VALOR segundas-feiras W7–W18 por (SVC, MLP, CICLO, semana) ──────
svcs = ', '.join(f"'{s}'" for s, _ in pares)
q = f"""
SELECT SVC, MLP, CICLO,
       EXTRACT(ISOWEEK FROM DATA_DIA_DA_SEMANA) AS sem,
       SUM(VALOR) AS v
FROM meli-bi-data.SBOX_ANALYTICSLASTMILE.SDDCALENDARICAOHISTORICO_T
WHERE DATA_DIA_DA_SEMANA BETWEEN '2026-02-09' AND '2026-04-06'
  AND DIA_DA_SEMANA = 'SEGUNDA_FEIRA'
  AND SVC IN ({svcs})
GROUP BY 1,2,3,4
"""
# (svc, mlp, semana) -> {ciclo: veiculos}
bq = defaultdict(lambda: defaultdict(int))
for row in bq_client.query(q).result():
    bq[(row.SVC, row.MLP, int(row.sem))][row.CICLO] = int(row.v)

def pmsd(d): return d.get('PM', 0) + d.get('SD', 0)
def am(d):   return d.get('AM', 0) + d.get('AM1', 0) + d.get('AM2', 0)

# ── Agregado por semana (calendarizado por ciclo) ─────────────────────────────
agg = {}
for w in SEMANAS:
    wn = int(w[1:])
    v_pmsd = sum(pmsd(bq[(svc, mlp, wn)]) for svc, mlp in pares)
    v_am   = sum(am  (bq[(svc, mlp, wn)]) for svc, mlp in pares)
    total  = v_pmsd + v_am
    agg[w] = {
        'pmsd':  v_pmsd,
        'am':    v_am,
        'total': total,
        'pct':   round(v_pmsd / total * 100, 1) if total > 0 else 0,
    }

# ── Tabela completa: todos os pares por semana (W7–W_ATUALIZADO) ───────────────
tabela = []
for svc, mlp in sorted(pares):
    for w in SEMANAS:
        wn = int(w[1:])
        if wn > W_ATUALIZADO:
            continue
        d   = bq[(svc, mlp, wn)]
        v_p = pmsd(d); v_a = am(d); tot = v_p + v_a
        if tot == 0:
            continue
        pct = round(v_p / tot * 100)
        status = 'Correto' if pct >= 85 else 'Incorreto'
        tabela.append({
            'svc': svc, 'mlp': mlp, 'w': w,
            'pmsd': v_p, 'am': v_a, 'tot': tot,
            'pct': pct, 'status': status,
        })

tabela.sort(key=lambda r: (r['status'], r['pct'], r['svc'], r['mlp']))

n_correto   = sum(1 for r in tabela if r['status'] == 'Correto')
n_incorreto = sum(1 for r in tabela if r['status'] == 'Incorreto')
print(f'Tabela: {len(tabela)} linhas | Correto: {n_correto} | Incorreto: {n_incorreto}')

# Resumo geral W7-W13
total_pmsd = sum(agg[w]['pmsd'] for w in SEMANAS if int(w[1:]) <= W_ATUALIZADO)
total_am   = sum(agg[w]['am']   for w in SEMANAS if int(w[1:]) <= W_ATUALIZADO)
total_cal  = total_pmsd + total_am
pct_ader   = round(total_pmsd / total_cal * 100) if total_cal > 0 else 0
cor_ader   = '#22c55e' if pct_ader >= 80 else '#f97316' if pct_ader >= 50 else '#ef4444'

# ── JS arrays ─────────────────────────────────────────────────────────────────
labels_js   = str(SEMANAS)
pmsd_js     = str([agg[w]['pmsd'] for w in SEMANAS])
am_js       = str([agg[w]['am']   for w in SEMANAS])
neg_js      = str([neg_semana[w]  for w in SEMANAS])
ader_js     = str([agg[w]['pct']  for w in SEMANAS])
pct_am_js   = str([round(agg[w]['am']  / agg[w]['total'] * 100) if agg[w]['total'] > 0 else 0 for w in SEMANAS])
pct_pmsd_js = str([round(agg[w]['pmsd'] / agg[w]['total'] * 100) if agg[w]['total'] > 0 else 0 for w in SEMANAS])

# ── Tabela HTML ────────────────────────────────────────────────────────────────
def pct_bar(pct):
    cor = '#22c55e' if pct >= 85 else '#f97316' if pct >= 50 else '#ef4444'
    return (f'<div style="display:flex;align-items:center;gap:8px">'
            f'<div style="flex:1;background:#f1f5f9;border-radius:4px;height:8px;overflow:hidden">'
            f'<div style="width:{max(pct,2)}%;background:{cor};height:100%;border-radius:4px"></div></div>'
            f'<span style="font-size:12px;font-weight:700;color:{cor};min-width:36px">{pct}%</span></div>')

def badge_st(status):
    if status == 'Correto':
        return '<span class="badge badge-ok">Correto</span>'
    return '<span class="badge badge-nok">Incorreto</span>'

svcs_uniq = sorted(set(r['svc'] for r in tabela))
semanas_uniq = sorted(set(r['w'] for r in tabela), key=lambda w: int(w[1:]))

tab_rows = ''
for r in tabela:
    rowbg = '#f0fdf4' if r['status'] == 'Correto' else '#fef2f2'
    tab_rows += (
        f'<tr class="data-row" data-status="{r["status"]}" '
        f'data-svc="{r["svc"]}" data-w="{r["w"]}">'
        f'<td>{r["w"]}</td>'
        f'<td>{r["svc"]}</td>'
        f'<td>{r["mlp"]}</td>'
        f'<td style="text-align:center;font-weight:700;color:#16a34a">{r["pmsd"]}</td>'
        f'<td style="text-align:center;font-weight:700;color:#dc2626">{r["am"]}</td>'
        f'<td style="text-align:center;color:#475569">{r["tot"]}</td>'
        f'<td style="min-width:160px">{pct_bar(r["pct"])}</td>'
        f'<td>{badge_st(r["status"])}</td>'
        f'</tr>\n'
    )

svc_opts   = '\n'.join(f'<option value="{s}">{s}</option>' for s in svcs_uniq)
semana_opts = '\n'.join(f'<option value="{w}">{w}</option>' for w in semanas_uniq)

now_str = datetime.now().strftime('%d/%m/%Y %H:%M')

HTML = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Aderência PM/SD — SDD</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
      background:#f1f5f9;color:#0f172a;font-size:14px;line-height:1.5}}

/* header */
.header{{background:#0f172a;padding:22px 36px;border-bottom:3px solid #FFE600;
          display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}}
.header h1{{color:#fff;font-size:20px;font-weight:800;letter-spacing:-.3px}}
.header h1 em{{color:#FFE600;font-style:normal}}
.header .meta{{color:#64748b;font-size:12px;text-align:right;line-height:1.9}}

/* cards */
.cards{{display:flex;gap:14px;padding:20px 36px;background:#fff;
         border-bottom:1px solid #e2e8f0;flex-wrap:wrap}}
.card{{flex:1;min-width:130px;padding:16px 18px;border-radius:10px;
        background:#f8fafc;border:1px solid #e2e8f0}}
.cv{{font-size:30px;font-weight:800;line-height:1.1}}
.cl{{font-size:10px;text-transform:uppercase;letter-spacing:.7px;color:#64748b;font-weight:700;margin-top:5px}}
.cs{{font-size:11px;color:#94a3b8;margin-top:2px}}

/* body */
.body{{padding:24px 36px;display:grid;grid-template-columns:1fr 1fr;gap:20px}}
@media(max-width:900px){{.body{{grid-template-columns:1fr}}}}
.box{{background:#fff;border-radius:12px;border:1px solid #e2e8f0;
       padding:22px;box-shadow:0 1px 4px rgba(0,0,0,.05)}}
.box.full{{grid-column:1/-1}}
.box-title{{font-size:13px;font-weight:700;color:#0f172a;margin-bottom:16px;
             display:flex;align-items:center;gap:8px;flex-wrap:wrap}}
.tag{{font-size:11px;font-weight:600;padding:2px 8px;border-radius:4px;
       background:#f1f5f9;color:#64748b}}
.chart-wrap{{position:relative}}

/* table */
table{{width:100%;border-collapse:collapse;font-size:13px}}
thead th{{padding:9px 12px;font-size:10px;color:#475569;font-weight:700;
           text-transform:uppercase;letter-spacing:.6px;
           border-bottom:2px solid #e2e8f0;background:#f8fafc;white-space:nowrap}}
td{{padding:9px 12px;border-bottom:1px solid #f8fafc;vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#fafafa}}

/* nota */
.nota{{font-size:11px;color:#94a3b8;margin-top:10px;font-style:italic}}

/* filtros */
.filters{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;align-items:center}}
.filters select, .filters input{{
  padding:7px 12px;border:1px solid #e2e8f0;border-radius:8px;
  font-size:13px;color:#0f172a;background:#fff;outline:none;
  font-family:inherit;cursor:pointer}}
.filters select:focus,.filters input:focus{{border-color:#94a3b8}}
.filter-count{{font-size:12px;color:#64748b;margin-left:auto;white-space:nowrap}}

/* badges */
.badge{{padding:3px 12px;border-radius:20px;font-size:11px;font-weight:700;white-space:nowrap}}
.badge-ok{{background:#dcfce7;color:#15803d}}
.badge-nok{{background:#fef2f2;color:#b91c1c}}

/* hidden row */
tr.hidden{{display:none}}
</style>
</head>
<body>

<div class="header">
  <h1>Aderência de Ciclo <em>PM/SD</em> — Expansão</h1>
  <div class="meta">
    W{W_ATUAL} atual &nbsp;·&nbsp; Atualizado até W{W_ATUALIZADO} &nbsp;·&nbsp; {now_str}
  </div>
</div>

<div class="cards">
  <div class="card">
    <div class="cv">{len(pares)}</div>
    <div class="cl">Pares SVC×MLP</div>
    <div class="cs">Expansão PM/SD</div>
  </div>
  <div class="card">
    <div class="cv" style="color:#22c55e">{total_pmsd}</div>
    <div class="cl">Rotas PM/SD</div>
    <div class="cs">Ciclo correto (W7–W{W_ATUALIZADO})</div>
  </div>
  <div class="card">
    <div class="cv" style="color:#ef4444">{total_am}</div>
    <div class="cl">Rotas AM</div>
    <div class="cs">Ciclo incorreto</div>
  </div>
  <div class="card">
    <div class="cv" style="color:{cor_ader}">{pct_ader}%</div>
    <div class="cl">Aderência PM/SD</div>
    <div class="cs">PM+SD / total calendarizado</div>
  </div>
  <div class="card">
    <div class="cv" style="color:#ef4444">{n_incorreto}</div>
    <div class="cl">Incorretos</div>
    <div class="cs">Registros &lt; 85% PM/SD</div>
  </div>
</div>

<div class="body">

  <!-- Gráfico principal empilhado -->
  <div class="box full">
    <div class="box-title">
      Calendarizadas por ciclo e semana
      <span class="tag">Segundas-feiras · Expansão PM/SD</span>
    </div>
    <div class="chart-wrap" style="height:300px">
      <canvas id="chartMain"></canvas>
    </div>
    <p class="nota">Área cinza = semanas futuras (ainda não atualizadas na planilha de volume)</p>
  </div>

  <!-- Aderência % -->
  <div class="box">
    <div class="box-title">Aderência ao ciclo PM/SD por semana <span class="tag">meta ≥ 80%</span></div>
    <div class="chart-wrap" style="height:230px">
      <canvas id="chartAder"></canvas>
    </div>
  </div>

  <!-- Composição % empilhada -->
  <div class="box">
    <div class="box-title">Composição % por semana <span class="tag">PM/SD vs AM</span></div>
    <div class="chart-wrap" style="height:230px">
      <canvas id="chartComp"></canvas>
    </div>
  </div>

  <!-- Tabela completa com filtros -->
  <div class="box full">
    <div class="box-title">
      Calendarização por SVC × MLP × Semana
      <span class="tag">{len(tabela)} registros · W7–W{W_ATUALIZADO}</span>
      <span class="tag" style="background:#dcfce7;color:#15803d">{n_correto} corretos</span>
      <span class="tag" style="background:#fef2f2;color:#b91c1c">{n_incorreto} incorretos</span>
    </div>

    <div class="filters">
      <select id="f-status" onchange="applyFilters()">
        <option value="">Todos os status</option>
        <option value="Correto">Correto</option>
        <option value="Incorreto">Incorreto</option>
      </select>
      <select id="f-svc" onchange="applyFilters()">
        <option value="">Todos os SVCs</option>
        {svc_opts}
      </select>
      <select id="f-semana" onchange="applyFilters()">
        <option value="">Todas as semanas</option>
        {semana_opts}
      </select>
      <input id="f-mlp" type="text" placeholder="Buscar MLP..." oninput="applyFilters()" style="width:180px"/>
      <button onclick="clearFilters()" style="padding:7px 14px;border:1px solid #e2e8f0;border-radius:8px;
        background:#f8fafc;font-size:12px;font-weight:600;color:#64748b;cursor:pointer">
        Limpar filtros
      </button>
      <span class="filter-count" id="filter-count">{len(tabela)} de {len(tabela)} registros</span>
    </div>

    <table id="main-table">
      <thead><tr>
        <th style="cursor:pointer" onclick="sortTable(0)">Semana ↕</th>
        <th style="cursor:pointer" onclick="sortTable(1)">SVC ↕</th>
        <th style="cursor:pointer" onclick="sortTable(2)">MLP ↕</th>
        <th style="text-align:center">PM/SD</th>
        <th style="text-align:center">AM</th>
        <th style="text-align:center">Total</th>
        <th style="min-width:160px;cursor:pointer" onclick="sortTable(6)">% PM/SD ↕</th>
        <th>Status</th>
      </tr></thead>
      <tbody id="table-body">{tab_rows}</tbody>
    </table>
    <p class="nota">Valores de segunda-feira de cada semana. Correto = ≥ 85% no ciclo PM/SD.</p>
  </div>

</div>

<script>
const labels    = {labels_js};
const calPmsd   = {pmsd_js};
const calAm     = {am_js};
const negociado = {neg_js};
const ader      = {ader_js};
const pctPmsd   = {pct_pmsd_js};
const pctAm     = {pct_am_js};
const wAtualiz  = {W_ATUALIZADO};

const gc = 'rgba(0,0,0,.06)';
Chart.defaults.font.family = "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
Chart.defaults.color = '#475569';

const futureShade = {{
  id: 'futureShade',
  beforeDraw(chart) {{
    const {{ctx, chartArea, scales}} = chart;
    if (!chartArea || !scales.x) return;
    ctx.save(); ctx.fillStyle = 'rgba(241,245,249,.65)';
    labels.forEach((lbl, i) => {{
      if (parseInt(lbl.slice(1)) > wAtualiz) {{
        const x0 = scales.x.getPixelForValue(i - .5);
        const x1 = scales.x.getPixelForValue(i + .5);
        ctx.fillRect(x0, chartArea.top, x1 - x0, chartArea.bottom - chartArea.top);
      }}
    }});
    ctx.restore();
  }}
}};

// Gráfico principal
new Chart(document.getElementById('chartMain'), {{
  type: 'bar',
  plugins: [futureShade],
  data: {{
    labels,
    datasets: [
      {{label:'PM/SD (correto)', data:calPmsd, backgroundColor:'rgba(34,197,94,.85)', stack:'s',
        borderRadius:{{topLeft:0,topRight:0,bottomLeft:4,bottomRight:4}}}},
      {{label:'AM (incorreto)',  data:calAm,   backgroundColor:'rgba(239,68,68,.80)',  stack:'s',
        borderRadius:{{topLeft:4,topRight:4,bottomLeft:0,bottomRight:0}}}},
    ]
  }},
  options: {{
    responsive:true, maintainAspectRatio:false,
    interaction:{{mode:'index',intersect:false}},
    plugins:{{
      legend:{{position:'bottom',labels:{{boxWidth:12,padding:18}}}},
      tooltip:{{callbacks:{{
        footer: items => {{
          const pm  = items.find(i => i.dataset.label.startsWith('PM'))?.raw  ?? 0;
          const am  = items.find(i => i.dataset.label.startsWith('AM'))?.raw  ?? 0;
          const tot = pm + am;
          return tot > 0 ? `Aderência ciclo: ${{Math.round(pm/tot*100)}}%` : '';
        }}
      }}}}
    }},
    scales:{{
      x:{{stacked:true, grid:{{color:gc}}}},
      y:{{stacked:true, beginAtZero:true, grid:{{color:gc}},
          title:{{display:true,text:'Veículos (segunda-feira)'}}}}
    }}
  }}
}});

// Aderência %
new Chart(document.getElementById('chartAder'), {{
  type: 'line',
  plugins: [futureShade],
  data: {{
    labels,
    datasets: [
      {{label:'% PM/SD', data:ader,
        borderColor:'#0ea5e9', backgroundColor:'rgba(14,165,233,.1)',
        fill:true, tension:.3, pointRadius:5, pointBorderColor:'#fff', pointBorderWidth:2,
        pointBackgroundColor: ader.map(v => v>=80?'#22c55e':v>=50?'#f97316':'#ef4444')}},
      {{label:'Meta 80%', data:labels.map(()=>80),
        borderColor:'rgba(34,197,94,.45)', borderDash:[6,4],
        borderWidth:1.5, pointRadius:0, fill:false}},
    ]
  }},
  options:{{
    responsive:true, maintainAspectRatio:false,
    plugins:{{legend:{{position:'bottom',labels:{{boxWidth:12,padding:14}}}},
              tooltip:{{callbacks:{{label: c => ` ${{c.parsed.y}}%`}}}}}},
    scales:{{x:{{grid:{{color:gc}}}}, y:{{min:0,max:110,grid:{{color:gc}},ticks:{{callback:v=>v+'%'}}}}}}
  }}
}});

// ── Filtros e ordenação da tabela ─────────────────────────────────────────────
function applyFilters() {{
  const fStatus = document.getElementById('f-status').value;
  const fSvc    = document.getElementById('f-svc').value;
  const fSemana = document.getElementById('f-semana').value;
  const fMlp    = document.getElementById('f-mlp').value.toLowerCase().trim();
  const rows    = document.querySelectorAll('#table-body .data-row');
  let visible   = 0;
  rows.forEach(row => {{
    const ok = (
      (!fStatus || row.dataset.status === fStatus) &&
      (!fSvc    || row.dataset.svc    === fSvc)    &&
      (!fSemana || row.dataset.w      === fSemana) &&
      (!fMlp    || row.cells[2].textContent.toLowerCase().includes(fMlp))
    );
    row.classList.toggle('hidden', !ok);
    if (ok) visible++;
  }});
  document.getElementById('filter-count').textContent =
    visible + ' de ' + rows.length + ' registros';
}}

function clearFilters() {{
  ['f-status','f-svc','f-semana'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('f-mlp').value = '';
  applyFilters();
}}

let sortDir = {{}};
function sortTable(col) {{
  sortDir[col] = !sortDir[col];
  const tbody = document.getElementById('table-body');
  const rows  = Array.from(tbody.querySelectorAll('.data-row'));
  rows.sort((a, b) => {{
    let va = a.cells[col].textContent.trim();
    let vb = b.cells[col].textContent.trim();
    const na = parseFloat(va); const nb = parseFloat(vb);
    const cmp = isNaN(na) ? va.localeCompare(vb) : na - nb;
    return sortDir[col] ? cmp : -cmp;
  }});
  rows.forEach(r => tbody.appendChild(r));
  applyFilters();
}}

// Composição %
new Chart(document.getElementById('chartComp'), {{
  type: 'bar',
  plugins: [futureShade],
  data: {{
    labels,
    datasets: [
      {{label:'PM/SD (%)', data:pctPmsd, backgroundColor:'rgba(34,197,94,.85)', stack:'s',
        borderRadius:{{topLeft:0,topRight:0,bottomLeft:4,bottomRight:4}}}},
      {{label:'AM (%)',    data:pctAm,   backgroundColor:'rgba(239,68,68,.80)',  stack:'s',
        borderRadius:{{topLeft:4,topRight:4,bottomLeft:0,bottomRight:0}}}},
    ]
  }},
  options:{{
    responsive:true, maintainAspectRatio:false,
    plugins:{{legend:{{position:'bottom',labels:{{boxWidth:12,padding:14}}}},
              tooltip:{{callbacks:{{label: c => ` ${{c.dataset.label}}: ${{c.parsed.y}}%`}}}}}},
    scales:{{
      x:{{stacked:true, grid:{{color:gc}}}},
      y:{{stacked:true, min:0, max:100, grid:{{color:gc}}, ticks:{{callback:v=>v+'%'}}}}
    }}
  }}
}});
</script>
</body>
</html>"""

out = r'C:\Users\gmreis\Documents\Claude Code\Validacao SDD\relatorio_pmsd_ciclos.html'
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f'\nRelatório: {out}  ({len(HTML)//1024} KB)')
print(f'Aderência geral W7–W{W_ATUALIZADO}: {pct_ader}%  |  PM/SD: {total_pmsd}  |  AM: {total_am}')
print(f'Tabela: {len(tabela)} linhas | Correto: {n_correto} | Incorreto: {n_incorreto}')
