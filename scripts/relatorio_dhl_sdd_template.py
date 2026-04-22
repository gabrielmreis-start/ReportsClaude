#!/usr/bin/env python3
"""
Gera relatorio DHL SDD com dados de W16 usando PLAN ACUMULADO.
W16 completo (semana 2026-04-13 a 2026-04-19).
"""
import re
import json
import subprocess
import urllib.request
from pathlib import Path

GCLOUD_PATH = r"C:\Users\gmreis\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
PLANILHA_ID = "1yETyJzRyl-4c1ZjORlVXAVPKfHUHD5nsu_OEhY9GTUk"
PLANILHA_RANGE = "Cronograma!A1:AJ200"

HTML_SOURCE = Path(r"C:\Users\gmreis\Documents\ReportsClaude\relatorio_dhl_sdd_W15.html")
HTML_OUTPUT = Path(r"C:\Users\gmreis\Documents\ReportsClaude\relatorio_dhl_sdd_W16.html")
HTML_BASE   = Path(r"C:\Users\gmreis\Documents\ReportsClaude\relatorio_dhl_sdd.html")

# Baselines fixos (veiculos que a DHL ja tinha antes da expansao)
BASELINES = {"SSC3": 17, "SSP29": 37, "SSP30": 10}
BASELINE_TOTAL = sum(BASELINES.values())  # 64

# Plan INCREMENTAL por SVC (W7..W22) - fonte: planilha Cronograma
# SSP48 target=20 (W8=12+W9=8). SSP29/SSP38/SSP20 tem entradas futuras
# armazenadas mas fora de SEMANAS (nao aparecem no Gantt ate revisao).
PLAN_SEMANAL = {
    #         W7   W8   W9  W10  W11  W12  W13  W14  W15  W16  W17  W18  W19  W20  W21  W22
    "SMG15": [13,   7,  10,   9,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0],
    "SSC2":  [ 0,  10,   5,   5,  15,   6,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0],
    "SSC3":  [ 3,   3,   3,   4,   3,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0],
    "SSC4":  [ 5,   5,  10,   5,   5,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0],
    "SSP29": [ 0,   5,   5,   9,   0,   0,   0,   0,   0,   0,   3,   3,   4,   5,   5,   5],
    "SSP30": [10,  13,   3,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0],
    "SSP38": [ 6,   5,   9,  10,   0,   0,   0,   0,   0,   0,   3,   3,   2,   0,   0,   0],
    "SSP4":  [15,   5,   5,   5,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0],
    "SSP48": [ 0,  12,   8,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0],
    "SSP20": [ 0,   0,   0,   0,   0,   0,   0,   5,   3,   3,   3,   0,   0,   0,   0,   0],
}
SEMANAS = ["W7", "W8", "W9", "W10", "W11", "W12", "W13", "W14", "W15", "W16"]
SVCS_EXPANSAO = ["SMG15", "SSC2", "SSC3", "SSC4", "SSP29", "SSP30", "SSP38", "SSP4", "SSP48", "SSP20"]

# Exec BRUTO (veiculos = rotas/6) por SVC por semana
# W16: BQ meli-bi-data.WHOWNER.BT_BASEROTAS_LASTMILE, WEEKISO=16, CARRIER_NAME='DHL'
# Dados parciais ate 2026-04-16 (quarta-feira, W16 em andamento)
EXEC_BRUTO = {
    "SMG15": {"W7":  6.2, "W8":  6.3, "W9":  8.5, "W10": 10.3, "W11": 13.2, "W12": 18.3, "W13": 17.3, "W14": 21.0, "W15": 26.7, "W16": 31.2},
    "SSC2":  {"W7":  5.3, "W8":  8.5, "W9":  9.0, "W10": 13.5, "W11": 13.0, "W12": 11.3, "W13": 16.7, "W14": 20.7, "W15": 25.8, "W16": 29.5},
    "SSC3":  {"W7": 18.7, "W8": 20.8, "W9": 21.3, "W10": 26.3, "W11": 25.5, "W12": 25.5, "W13": 28.5, "W14": 25.8, "W15": 26.3, "W16": 27.0},
    "SSC4":  {"W7":  2.3, "W8":  2.7, "W9":  5.7, "W10":  6.7, "W11":  7.8, "W12":  8.7, "W13":  9.3, "W14":  7.7, "W15": 10.2, "W16": 11.7},
    "SSP29": {"W7": 33.7, "W8": 40.2, "W9": 39.0, "W10": 43.3, "W11": 41.7, "W12": 40.5, "W13": 40.7, "W14": 39.0, "W15": 46.5, "W16": 47.7},
    "SSP30": {"W7": 12.0, "W8": 11.5, "W9": 15.7, "W10": 15.7, "W11": 19.7, "W12": 16.8, "W13": 18.7, "W14": 16.7, "W15": 15.5, "W16": 18.8},
    "SSP38": {"W7":  5.7, "W8":  4.3, "W9":  6.0, "W10":  8.5, "W11": 11.8, "W12": 12.3, "W13": 11.5, "W14": 12.5, "W15": 14.0, "W16": 16.2},
    "SSP4":  {"W7": 13.7, "W8": 14.0, "W9": 14.5, "W10": 16.7, "W11": 19.8, "W12": 23.5, "W13": 27.8, "W14": 25.3, "W15": 29.5, "W16": 29.8},
    "SSP48": {"W7":  0.0, "W8":  2.8, "W9":  2.8, "W10":  2.7, "W11":  5.0, "W12":  7.5, "W13":  7.3, "W14":  9.2, "W15": 12.2, "W16": 15.8},
    "SSP20": {"W7":  0.0, "W8":  0.0, "W9":  0.0, "W10":  0.0, "W11":  0.0, "W12":  0.0, "W13":  0.0, "W14":  3.0, "W15":  4.3, "W16": 11.2},
}

# Mensal (BQ) - Nov/2025 a Mar/2026 (abril incompleto, nao incluido)
MENSAL = [
    {"periodo": "Nov", "ano": 2025, "cal":  5355, "exec": 5017},
    {"periodo": "Dez", "ano": 2025, "cal":  6391, "exec": 6038},
    {"periodo": "Jan", "ano": 2026, "cal":  6063, "exec": 5842},
    {"periodo": "Fev", "ano": 2026, "cal":  8574, "exec": 6510},
    {"periodo": "Mar", "ano": 2026, "cal": 12581, "exec": 8477},
]

# Semanal (BQ) - ultimas 4 semanas (W13-W16)
# W16: cal=2934 (aprox W15), exec=2560 (parcial - dados ate 2026-04-16)
SEMANAL = [
    {"periodo": "W13", "cal": 2946, "exec": 2011},
    {"periodo": "W14", "cal": 2976, "exec": 1980},
    {"periodo": "W15", "cal": 2934, "exec": 2208},
    {"periodo": "W16", "cal": 2952, "exec": 2372},
]

# Abertura por SVC W16
# CAL: aproximado de W15 (tabela SDDCALENDARICAOHISTORICO_T sem acesso)
# EXEC: BQ BT_BASEROTAS_LASTMILE, WEEKISO=16, CARRIER_NAME='DHL', parcial ate 16/04
ABERTURA_W16_CAL = {
    "SSP29": 336, "SSC2":  246, "SMG15": 234, "SSP30": 216,
    "SSC3":  198, "SSC4":  180, "SSP4":  180, "SSP38": 180,
    "SPR6":  168, "SPR1":  162, "SPE1":  150, "SSP48": 120,
    "SCE1":  120, "SPR3":  114, "SSP18":  90, "SAL1":   84,
    "SSP20":  66, "SSP26":  60, "SGO1":   48,
}
ABERTURA_W16_EXEC = {
    "SSP29": 286, "SMG15": 187, "SSP4":  179, "SSC2":  177,
    "SPR1":  164, "SSC3":  162, "SPE1":  149, "SPR6":  125,
    "SCE1":  120, "SSP30": 113, "SSP38":  97, "SSP48":  95,
    "SPR3":   95, "SSP18":  85, "SAL1":   84, "SSC4":   70,
    "SSP20":  67, "SSP26":  66, "SGO1":   50,
}


def get_access_token():
    result = subprocess.run(
        [GCLOUD_PATH, "auth", "print-access-token"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def carregar_plan_planilha_dhl():
    """Lê plan incremental DHL da planilha Cronograma; retorna {SVC: {week_num: val}}."""
    try:
        token = get_access_token()
        encoded_range = PLANILHA_RANGE.replace("!", "%21").replace(":", "%3A")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{PLANILHA_ID}/values/{encoded_range}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
        rows = data.get("values", [])
        if len(rows) < 2:
            return None
        header = rows[1]
        mlp_col = next((i for i, h in enumerate(header) if str(h).strip().upper() == "MLP"), None)
        svc_col = next((i for i, h in enumerate(header) if str(h).strip().upper() == "SVC"), None)
        if mlp_col is None or svc_col is None:
            return None
        week_cols = {}
        for i, h in enumerate(header):
            h_str = str(h).strip().upper()
            if h_str.startswith("W") and h_str[1:].isdigit():
                w_num = int(h_str[1:])
                if 7 <= w_num <= 27:
                    week_cols[w_num] = i
        result = {}
        for row in rows[2:]:
            mlp = str(row[mlp_col]).strip().upper() if len(row) > mlp_col else ""
            if "DHL" not in mlp:
                continue
            svc = str(row[svc_col]).strip().upper() if len(row) > svc_col else ""
            if not svc:
                continue
            vals = {}
            for w_num, col_idx in week_cols.items():
                raw = row[col_idx].strip() if len(row) > col_idx else ""
                try:
                    vals[w_num] = float(raw) if raw else 0.0
                except ValueError:
                    vals[w_num] = 0.0
            result[svc] = vals
        return result
    except Exception as e:
        print(f"  AVISO planilha: {e}")
        return None


def validar_plan_vs_planilha_dhl():
    """Compara PLAN_SEMANAL com a planilha e imprime divergencias."""
    print("\n[VALIDAÇÃO] Verificando plan DHL vs. planilha...")
    sheet_data = carregar_plan_planilha_dhl()
    if sheet_data is None:
        print("  AVISO: Não foi possível ler a planilha. Pulando validação.")
        return
    semanas_num = [int(w[1:]) for w in SEMANAS]
    encontrou_diff = False
    for svc, incremental in PLAN_SEMANAL.items():
        if svc not in sheet_data:
            continue
        sheet_vals = sheet_data[svc]
        diffs = []
        for i, w_num in enumerate(semanas_num):
            script_val = incremental[i] if i < len(incremental) else 0
            sheet_val = sheet_vals.get(w_num, 0)
            if abs(script_val - sheet_val) > 0.5:
                diffs.append(f"W{w_num}: script={int(script_val)} planilha={int(sheet_val)}")
        if diffs:
            print(f"  ⚠️  {svc}: {', '.join(diffs)}")
            encontrou_diff = True
    if not encontrou_diff:
        print("  ✅ Nenhuma divergência encontrada.")
    print("  Validação concluída.")


def exec_liquido(svc, semana):
    bruto = EXEC_BRUTO.get(svc, {}).get(semana, 0)
    baseline = BASELINES.get(svc, 0)
    return max(0, bruto - baseline)


def plan_acumulado(svc):
    acum, s = [], 0
    for v in PLAN_SEMANAL[svc]:
        s += v
        acum.append(s)
    return acum


def cor_er(val):
    if val >= 95: return "er-meta"
    if val >= 90: return "er-verde"
    if val >= 80: return "er-amarelo"
    return "er-vermelho"


def cor_ponto(v):
    if v >= 95: return '"#348338"'
    if v >= 90: return '"#7ED321"'
    return '"#E60000"'


def main():
    validar_plan_vs_planilha_dhl()

    html = HTML_SOURCE.read_text(encoding="utf-8")

    # === 1. Aderencia ao plano ===
    er_w16 = round(SEMANAL[-1]["exec"] / SEMANAL[-1]["cal"] * 100, 1)
    er_w16_str = f"{er_w16:.1f}".replace(".", ",")
    html = re.sub(
        r'<p class="info-aderencia">.*?</p>',
        f'<p class="info-aderencia">Aderencia ao plano na semana passada (W16): <strong>{er_w16_str}%</strong></p>',
        html
    )

    # === 2. Abertura por SVC - W16 (com data-svc para Grid SDK) ===
    svcs_aber = sorted(
        ABERTURA_W16_CAL.keys(),
        key=lambda s: ABERTURA_W16_CAL[s] - ABERTURA_W16_EXEC.get(s, 0),
        reverse=True
    )
    aber_rows = []
    for svc in svcs_aber:
        cal = ABERTURA_W16_CAL[svc]
        ex  = ABERTURA_W16_EXEC.get(svc, 0)
        gap = cal - ex
        er  = round(ex / cal * 100, 2) if cal > 0 else 0
        er_str = f"{er:.2f}".replace(".", ",") + "%"
        cls = cor_er(er)
        aber_rows.append(
            f"  <tr><td>{svc}</td><td class='num'>{cal}</td><td class='num'>{ex}</td>"
            f"<td class='num top10-gap'>{gap}</td><td class='num {cls}'>{er_str}</td></tr>"
        )
    aber_html = "\n".join(aber_rows)
    html = re.sub(
        r'(<section class="section top10-box">\s*<h2>)Abertura por SVC - W\d+.*?(</h2>.*?<tbody>)\s*.*?(</tbody>)',
        rf'\g<1>Abertura por SVC - W16\g<2>\n{aber_html}\n      \g<3>',
        html, flags=re.DOTALL
    )

    # === 3. Proposta DHL 2026 - PLAN ACUMULADO vs EXEC SEMANAL ===
    th_cols = "".join(
        f"<th class='num gantt-sort' data-col='{i+1}'>{w}</th>"
        for i, w in enumerate(SEMANAS)
    )
    gantt_rows = []
    total_plan = [0] * len(SEMANAS)
    total_exec = [0] * len(SEMANAS)

    for svc in SVCS_EXPANSAO:
        plan_acum = plan_acumulado(svc)
        cells = []
        for i, w in enumerate(SEMANAS):
            p = plan_acum[i]
            e = int(round(exec_liquido(svc, w)))
            gap = e - p
            total_plan[i] += p
            total_exec[i] += e
            cls = "gantt-vermelho" if gap < 0 else ("gantt-cinza" if gap == 0 else "gantt-verde")
            gap_str = str(gap) if gap != 0 else "0"
            cells.append(
                f"<td class='num gantt-pe {cls}' title='Plan (acum.) / Real (sem.)'>"
                f"{p} / {e}  <span class='gantt-gap'>({gap_str})</span></td>"
            )
        gantt_rows.append(
            f"  <tr><td class='gantt-svc'>{svc}</td>" + "".join(cells) + "</tr>"
        )

    total_cells = []
    for i in range(len(SEMANAS)):
        p, e = total_plan[i], total_exec[i]
        gap = e - p
        cls = "gantt-vermelho" if gap < 0 else ("gantt-cinza" if gap == 0 else "gantt-verde")
        gap_str = str(gap) if gap != 0 else "0"
        total_cells.append(
            f"<td class='num gantt-pe gantt-total {cls}' title='Plan (acum.) / Real (sem.)'>"
            f"{p} / {e}  <span class='gantt-gap'>({gap_str})</span></td>"
        )
    gantt_rows.append(
        f"  <tr><td class='gantt-svc gantt-total-row'>Total</td>"
        + "".join(total_cells) + "</tr>"
    )

    new_gantt = (
        f'\n  <div class="gantt-scroll">\n'
        f'  <table class="gantt-tabela gantt-compact" id="tabelaGantt">\n'
        f'    <thead><tr><th class="gantt-sort" data-col="0">SVC</th>{th_cols}</tr></thead>\n'
        f'    <tbody>\n'
        + "".join(gantt_rows) +
        f'\n    </tbody>\n  </table>\n  </div>'
    )
    html = re.sub(
        r'<div class="gantt-scroll">.*?</table>\s*</div>',
        new_gantt, html, flags=re.DOTALL
    )
    html = re.sub(
        r'<p class="gantt-subtitulo">.*?</p>',
        '<p class="gantt-subtitulo">Planejado (acum.) / Executado (sem.) / Gap</p>',
        html
    )

    # === 4. Charts JS ===
    carros_neg_mes = []
    er_mes = []
    for m in MENSAL:
        divisor = 24 if m["periodo"] == "Fev" else 26
        carros_neg_mes.append(round(m["cal"] / divisor, 1))
        er_mes.append(round(m["exec"] / m["cal"] * 100, 1))

    carros_neg_sem, er_sem, er_sem_display = [], [], []
    for s in SEMANAL:
        veic = round((s["cal"] - BASELINE_TOTAL) / 6, 1)
        carros_neg_sem.append(veic)
        er_val = round(s["exec"] / s["cal"] * 100, 1)
        er_sem.append(er_val)
        er_sem_display.append(f"{er_val:.1f}".replace(".", ",") + "%")

    cores_mes = [cor_ponto(v) for v in er_mes]
    cores_sem = [cor_ponto(v) for v in er_sem]

    html = re.sub(r'var carrosNegMes = \[.*?\];', f'var carrosNegMes = {carros_neg_mes};', html)
    html = re.sub(r'var carrosNegSem = \[.*?\];', f'var carrosNegSem = {carros_neg_sem};', html)
    html = re.sub(r'var erSemDisplay = \[.*?\];', f'var erSemDisplay = {er_sem_display};', html)

    labels_mes = ["Nov/2025", "Dez/2025", "Jan/2026", "Fev/2026", "Mar/2026"]
    html = re.sub(
        r'(new Chart\(document\.getElementById\("chartMes"\).*?labels: )\[.*?\]',
        rf'\g<1>{labels_mes}', html, flags=re.DOTALL
    )
    html = re.sub(
        r'(new Chart\(document\.getElementById\("chartMes"\).*?data: carrosNegMes.*?"ER %".*?data: )\[.*?\]',
        rf'\g<1>{er_mes}', html, flags=re.DOTALL
    )
    html = re.sub(
        r'(new Chart\(document\.getElementById\("chartMes"\).*?pointBackgroundColor: )\[.*?\](.*?pointBorderColor: )\[.*?\]',
        rf'\g<1>[{",".join(cores_mes)}]\g<2>[{",".join(cores_mes)}]',
        html, flags=re.DOTALL
    )

    labels_sem = [s["periodo"] for s in SEMANAL]
    html = re.sub(
        r'(new Chart\(document\.getElementById\("chartSem"\).*?labels: )\[.*?\]',
        rf'\g<1>{labels_sem}', html, flags=re.DOTALL
    )
    html = re.sub(
        r'(new Chart\(document\.getElementById\("chartSem"\).*?data: carrosNegSem.*?"ER %".*?data: )\[.*?\]',
        rf'\g<1>{er_sem}', html, flags=re.DOTALL
    )
    html = re.sub(
        r'(new Chart\(document\.getElementById\("chartSem"\).*?pointBackgroundColor: )\[.*?\](.*?pointBorderColor: )\[.*?\]',
        rf'\g<1>[{",".join(cores_sem)}]\g<2>[{",".join(cores_sem)}]',
        html, flags=re.DOTALL
    )

    # === 5. TSV para copiar ===
    tsv_lines = ["Periodo\tAno\tVeiculos negociados\tVeiculos executados\tER%"]
    for m in MENSAL:
        divisor = 24 if m["periodo"] == "Fev" else 26
        vn = round(m["cal"] / divisor, 1)
        ve = round(m["exec"] / 6, 1)
        er = round(m["exec"] / m["cal"] * 100, 1)
        tsv_lines.append(
            f"{m['periodo']}\t{m['ano']}\t"
            f"{str(vn).replace('.', ',')}\t"
            f"{str(ve).replace('.', ',')}\t"
            f"{str(er).replace('.', ',')}%"
        )
    tsv_lines.append("")
    tsv_lines.append("Semana\tVeiculos negociados\tVeiculos executados\tER%")
    for s in SEMANAL:
        vn = round((s["cal"] - BASELINE_TOTAL) / 6, 1)
        ve = round(s["exec"] / 6, 1)
        er = round(s["exec"] / s["cal"] * 100, 1)
        tsv_lines.append(
            f"{s['periodo']}\t"
            f"{str(vn).replace('.', ',')}\t"
            f"{str(ve).replace('.', ',')}\t"
            f"{str(er).replace('.', ',')}%"
        )
    tsv_text = "\\n".join(tsv_lines)
    html = re.sub(r'var tsv = `.*?`;', f'var tsv = `{tsv_text}`;', html, flags=re.DOTALL)

    # === 6. Remover scroll do gantt ===
    html = re.sub(
        r'(\.gantt-scroll\s*\{[^}]*?)max-height:\s*\d+px;\s*overflow-y:\s*auto;',
        r'\1overflow-y: visible;',
        html
    )

    # === 7. CSS: overflow, fontes e centralização ===
    html = re.sub(r'overflow-x:\s*\w+;', 'overflow-x: visible;', html, count=1)
    html = re.sub(r'overflow-y:\s*\w+;', 'overflow-y: visible;', html, count=1)

    html = re.sub(
        r'(\.gantt-tabela \{ font-size: )[\d.]+rem;',
        r'\g<1>0.82rem;', html
    )
    html = re.sub(
        r'(\.gantt-tabela th, \.gantt-tabela td \{ padding: )[\d px]+;',
        r'\g<1>5px 6px;', html
    )
    html = re.sub(
        r'(\.gantt-tabela \.gantt-pe \{ font-size: )[\d.]+em;',
        r'\g<1>0.90em;', html
    )
    html = re.sub(
        r'(\.gantt-gap \{ font-size: )[\d.]+em;',
        r'\g<1>0.78em;', html
    )

    if '.gantt-tabela td.num { text-align: center; }' not in html:
        html = html.replace(
            '.gantt-tabela thead th:first-child { text-align: left; }',
            '.gantt-tabela thead th:first-child { text-align: left; }'
            '\n    .gantt-tabela td.num { text-align: center; }'
            '\n    .top10-tabela td.num, .top10-tabela th.num { text-align: center; }'
        )

    html = re.sub(
        r'(\.top10-tabela \{ font-size: )[\d.]+rem;',
        r'\g<1>1rem;', html
    )
    html = re.sub(
        r'(\.top10-tabela th,\s*\.top10-tabela td\s*\{[^}]*?padding:\s*)[\d px]+;',
        r'\g<1>8px 12px;', html
    )

    html = re.sub(r'(datalabels:[^}]*font: \{ size: )\d+', r'\g<1>13', html)
    html = re.sub(
        r'(scales: \{ x: \{ grid: \{ display: false \}[^}]*\})',
        lambda m: m.group(0) if 'ticks' in m.group(0)
            else m.group(0).replace(
                'x: { grid: { display: false } }',
                'x: { grid: { display: false }, ticks: { font: { size: 13 } } }'
            ),
        html
    )
    html = re.sub(
        r'(legend: \{ display: true[^}]*\})',
        lambda m: m.group(0) if 'labels' in m.group(0)
            else m.group(0).rstrip('}') + ', labels: { font: { size: 13 } } }',
        html
    )


    # === Salvar ===
    HTML_OUTPUT.write_text(html, encoding="utf-8")
    HTML_BASE.write_text(html, encoding="utf-8")
    print(f"HTML salvo: {HTML_OUTPUT}")
    print(f"Base atualizada: {HTML_BASE}")

    # === Resumo ===
    print(f"\n=== RESUMO W16 (semana completa 13/04-19/04) ===")
    print(f"Aderencia W16: {er_w16_str}%")
    print(f"\nProposta DHL - Plan acum / Exec sem / Gap:")
    for i, w in enumerate(SEMANAS):
        p, e = total_plan[i], total_exec[i]
        par = " (parcial)" if w == "W16" else ""
        print(f"  {w}: Plan={p}, Exec={e}, Gap={e-p}{par}")
    print(f"\nAbertura W16 parcial - TOP 5 gaps:")
    svcs_sorted = sorted(ABERTURA_W16_CAL.keys(),
                         key=lambda s: ABERTURA_W16_CAL[s] - ABERTURA_W16_EXEC.get(s, 0),
                         reverse=True)
    for svc in svcs_sorted[:5]:
        gap = ABERTURA_W16_CAL[svc] - ABERTURA_W16_EXEC.get(svc, 0)
        print(f"  {svc}: Cal={ABERTURA_W16_CAL[svc]}, Exec={ABERTURA_W16_EXEC.get(svc,0)}, Gap={gap}")


if __name__ == "__main__":
    main()
