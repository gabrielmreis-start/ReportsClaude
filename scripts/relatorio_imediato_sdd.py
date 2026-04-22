#!/usr/bin/env python3
"""
Relatorio Gerencial Imediato - SDD.
Base: rotas executadas x calendarizacao.
KPI: ER (Taxa de Execucao) = rotas executadas / rotas calendarizadas.

Saidas:
- Ultimas 4 semanas: ER semanal (grafico).
- Abertura por SVC: ultima semana executada.
- Plano Total: todas SVCs x todas semanas (W11-W32), passado plan/exec, futuro em azul.
"""

import subprocess
import json
import urllib.request
from datetime import datetime, timedelta, date
from pathlib import Path
from google.oauth2 import credentials as oauth2_credentials
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = "meli-bi-data"
GCLOUD_PATH = r"C:\Users\gmreis\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"

# Filtros BigQuery
CARRIER_LIKE = "%imediato%"
MLP_LIKE = "%imediato%"

# Plano de expansao: entrada, target e ramp por SVC
# Cada SVC ramps (ramp veiculos/semana) ate o target. Total em W32 = 580.
SVC_PLAN = {
    # Ja rodando (calendarizado desde W11)
    "SSP4":   {"entry": 11, "target": 25, "ramp": 5},
    # Onda W16
    "SSP22":  {"entry": 16, "target": 25, "ramp": 5},
    "SMG1":   {"entry": 16, "target": 25, "ramp": 5},
    "SSP26":  {"entry": 16, "target": 25, "ramp": 5},
    # Onda W17
    "SDF1":   {"entry": 17, "target": 25, "ramp": 5},
    # Onda W18 — SPR8 entra com 10 carros no W18 (nao 5); schedule sobrepoe formula
    "SPR8":   {"entry": 18, "target": 25, "ramp": 5, "schedule": {18: 10, 19: 15, 20: 20, 21: 25}},
    # Onda W19 — SMG2 atualizado na planilha: entrada movida de W18 para W19
    "SMG2":   {"entry": 19, "target": 25, "ramp": 5},
    # Onda W19 — SSP46 substituiu SSP15 (mesmo perfil); SSC1 antecipado de W25 para W19
    "SSP46":  {"entry": 19, "target": 15, "ramp": 3},
    "SSC1":   {"entry": 19, "target": 25, "ramp": 5},
    # Onda W20
    "SSP25":  {"entry": 20, "target": 25, "ramp": 5},
    # Onda W21
    "SSP38":  {"entry": 21, "target": 25, "ramp": 5},
    "SPR3":   {"entry": 21, "target": 25, "ramp": 5},
    # Onda W22
    "SSP20":  {"entry": 22, "target": 25, "ramp": 5},
    # Onda W23
    "SSC8":   {"entry": 23, "target": 25, "ramp": 5},
    "SSP48":  {"entry": 23, "target": 15, "ramp": 3},
    # Onda W24
    "SMN1":   {"entry": 24, "target": 25, "ramp": 5},
    "SRS4":   {"entry": 24, "target": 25, "ramp": 5},
    "SMG10":  {"entry": 24, "target": 25, "ramp": 5},
    # Onda W25 — SSC2 adiado de W19 para W25
    "SSC2":   {"entry": 25, "target": 25, "ramp": 5},
    # Onda W26
    "SBA1":   {"entry": 26, "target": 25, "ramp": 5},
    "SPR1":   {"entry": 26, "target": 25, "ramp": 5},
    # Onda W27
    "SMG3":   {"entry": 27, "target": 25, "ramp": 5},
    # Onda W28 (ultima — chega a 25 na W32)
    "SSC3":   {"entry": 28, "target": 25, "ramp": 5},
    "SSC9":   {"entry": 28, "target": 25, "ramp": 5},
}

SVC_ORDER = list(SVC_PLAN.keys())
SEMANA_INICIO = 11
SEMANA_FIM = 32

# Abertura por tipo de veiculo (Utilitarios, Van, Vuc) por SVC
SVC_VEICULOS = {
    "SSP4":  {"util": 18, "van": 2, "vuc": 5},
    "SSP22": {"util": 18, "van": 0, "vuc": 7},
    "SMG1":  {"util": 18, "van": 0, "vuc": 7},
    "SSP26": {"util": 18, "van": 0, "vuc": 7},
    "SDF1":  {"util": 18, "van": 0, "vuc": 7},
    "SPR8":  {"util": 18, "van": 0, "vuc": 7},
    "SMG2":  {"util": 18, "van": 0, "vuc": 7},
    "SSP46": {"util": 11, "van": 0, "vuc": 4},
    "SSC2":  {"util": 18, "van": 0, "vuc": 7},
    "SSP25": {"util": 18, "van": 0, "vuc": 7},
    "SSP38": {"util": 18, "van": 0, "vuc": 7},
    "SPR3":  {"util": 18, "van": 0, "vuc": 7},
    "SSP20": {"util": 18, "van": 0, "vuc": 7},
    "SSC8":  {"util": 18, "van": 0, "vuc": 7},
    "SSP48": {"util": 11, "van": 0, "vuc": 4},
    "SMN1":  {"util": 18, "van": 0, "vuc": 7},
    "SRS4":  {"util": 18, "van": 0, "vuc": 7},
    "SMG10": {"util": 18, "van": 0, "vuc": 7},
    "SSC1":  {"util": 18, "van": 0, "vuc": 7},
    "SBA1":  {"util": 18, "van": 0, "vuc": 7},
    "SPR1":  {"util": 18, "van": 0, "vuc": 7},
    "SMG3":  {"util": 18, "van": 0, "vuc": 7},
    "SSC3":  {"util": 18, "van": 0, "vuc": 7},
    "SSC9":  {"util": 18, "van": 0, "vuc": 7},
}

BASE_RELATORIOS = Path(r"c:\Users\gmreis\.cursor\relatorios_imediato_sdd")
RELATORIO_HTML_LINK2 = Path(__file__).resolve().parent / "relatorio_imediato_sdd_abrir.html"

HOJE = datetime.now().date()
ANO_ATUAL = HOJE.year

CHART_JS_URL = "https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"
CHART_PLUGIN_URL = "https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"


def _fetch(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  AVISO: {url}: {e}")
        return ""


def _script_embed(url):
    js = _fetch(url)
    if not js.strip():
        return f'<script src="{url}"></script>'
    js = js.replace("</script>", "<\\/script>")
    return f"<script>\n{js}\n</script>"


def get_access_token():
    result = subprocess.run(
        [GCLOUD_PATH, "auth", "print-access-token"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def run_query(query):
    try:
        access_token = get_access_token()
        creds = oauth2_credentials.Credentials(token=access_token)
        client = bigquery.Client(project=PROJECT_ID, credentials=creds)
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        print(f"ERRO BigQuery: {e}")
        import traceback
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Gerar grid de plano ideal (veiculos por SVC por semana, cumulativo com cap)
# ---------------------------------------------------------------------------
PLANILHA_ID = "1yETyJzRyl-4c1ZjORlVXAVPKfHUHD5nsu_OEhY9GTUk"
PLANILHA_RANGE = "Cronograma!A1:AJ200"
PLANILHA_MLP_FILTER = "IMEDIATO"
PLANILHA_W_START = 11
PLANILHA_W_END = 27


def gerar_plan_grid():
    """Retorna DataFrame com SVC, semana_label, plan (fleet size por semana)."""
    rows = []
    for svc, cfg in SVC_PLAN.items():
        for w in range(SEMANA_INICIO, SEMANA_FIM + 1):
            if "schedule" in cfg:
                sched = cfg["schedule"]
                min_week = min(sched.keys())
                if w < min_week:
                    plan = 0
                elif w in sched:
                    plan = sched[w]
                else:
                    plan = cfg["target"]
            else:
                weeks_since = w - cfg["entry"]
                if weeks_since < 0:
                    plan = 0
                else:
                    plan = min((weeks_since + 1) * cfg["ramp"], cfg["target"])
            rows.append({"SVC": svc, "semana_label": f"W{w}", "plan": plan})
    return pd.DataFrame(rows)


def carregar_plan_planilha():
    """Lê plan incremental da planilha Cronograma; retorna {SVC: {week_num: val}}."""
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
        # Row 0 é totais; header real está na linha 1
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
                if PLANILHA_W_START <= w_num <= PLANILHA_W_END:
                    week_cols[w_num] = i
        result = {}
        for row in rows[2:]:
            mlp = str(row[mlp_col]).strip().upper() if len(row) > mlp_col else ""
            if PLANILHA_MLP_FILTER not in mlp:
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


def validar_plan_vs_planilha():
    """Compara SVC_PLAN com planilha e imprime divergencias de entry ou target."""
    print("\n[VALIDAÇÃO] Verificando plan vs. planilha...")
    sheet_data = carregar_plan_planilha()
    if sheet_data is None:
        print("  AVISO: Não foi possível ler a planilha. Pulando validação.")
        return
    encontrou_diff = False
    for svc, cfg in SVC_PLAN.items():
        if svc not in sheet_data:
            continue
        sheet_vals = sheet_data[svc]
        cumulative = 0
        sheet_cumul = {}
        for w in range(PLANILHA_W_START, PLANILHA_W_END + 1):
            cumulative += sheet_vals.get(w, 0)
            sheet_cumul[w] = cumulative
        sheet_target = sheet_cumul.get(PLANILHA_W_END, 0)
        sheet_entry = next((w for w in range(PLANILHA_W_START, PLANILHA_W_END + 1) if sheet_vals.get(w, 0) > 0), None)
        diffs = []
        if sheet_entry is not None and sheet_entry != cfg["entry"]:
            diffs.append(f"entry: script={cfg['entry']} planilha={sheet_entry}")
        if sheet_target > 0 and abs(sheet_target - cfg["target"]) > 1:
            diffs.append(f"target: script={cfg['target']} planilha={int(sheet_target)}")
        if diffs:
            print(f"  ⚠️  {svc}: {', '.join(diffs)}")
            encontrou_diff = True
    if not encontrou_diff:
        print("  ✅ Nenhuma divergência encontrada.")
    print("  Validação concluída.")


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------
def obter_colunas_calendar():
    query = """
    SELECT column_name
    FROM `meli-bi-data.SBOX_ANALYTICSLASTMILE.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'SDDCALENDARICAOHISTORICO_T'
    ORDER BY ordinal_position
    """
    df = run_query(query)
    if df is None or len(df) == 0:
        return None, None, None, None
    cols = [str(c).strip() for c in df["column_name"].tolist()]
    data_col = next((c for c in cols if c.upper() == "DATA_DIA_DA_SEMANA"), None)
    if not data_col:
        data_col = next((c for c in cols if "data" in c.lower()), cols[0])
    valor_col = next((c for c in cols if "valor" in c.lower() or "qtd" in c.lower()), cols[1] if len(cols) > 1 else None)
    mlp_col = next((c for c in cols if c.upper() == "MLP"), None)
    svc_col = next((c for c in cols if c.upper().strip() == "SVC"), None)
    return data_col, valor_col, mlp_col, svc_col


def buscar_calendarizacao_simples(inicio, fim):
    data_col, valor_col, mlp_col, _ = obter_colunas_calendar()
    if not data_col:
        return None
    if not valor_col:
        valor_col = "valor"
    mlp_filter = f"\n      AND LOWER(TRIM(COALESCE(CAST(`{mlp_col}` AS STRING), ''))) LIKE '{MLP_LIKE}'" if mlp_col else ""
    query = f"""
    SELECT *
    FROM `meli-bi-data.SBOX_ANALYTICSLASTMILE.SDDCALENDARICAOHISTORICO_T`
    WHERE DATE(`{data_col}`) >= DATE('{inicio}')
      AND DATE(`{data_col}`) <= DATE('{fim}')
{mlp_filter}
    """
    df = run_query(query)
    if df is None:
        return None
    cols_lower = {c.lower(): c for c in df.columns}
    dc = data_col if data_col in df.columns else (cols_lower.get("data") or next((c for c in df.columns if "data" in c.lower()), data_col))
    vc = cols_lower.get("valor") or next((c for c in df.columns if "valor" in c.lower() or "qtd" in c.lower()), valor_col)
    df = df.rename(columns={dc: "data", vc: "valor"})
    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0).astype(int)
    if not mlp_col:
        for c in df.columns:
            if c in ("data", "valor"):
                continue
            if "carrier" in c.lower() or "transportadora" in c.lower():
                df = df[df[c].astype(str).str.lower().str.contains("imediato", na=False)]
                break
    return df


def buscar_rotas_imediato_sdd(inicio, fim):
    query = f"""
    SELECT DATE(DATA_FIM) AS data, COUNT(*) AS rotas_executadas
    FROM `meli-bi-data.WHOWNER.BT_BASEROTAS_LASTMILE`
    WHERE DATE(DATA_FIM) BETWEEN DATE('{inicio}') AND DATE('{fim}')
      AND LOWER(COALESCE(CAST(Carrier_name AS STRING), '')) LIKE '{CARRIER_LIKE}'
      AND COALESCE(CAST(Plate AS STRING), '') LIKE '%SDD%'
    GROUP BY 1 ORDER BY 1
    """
    return run_query(query)


def buscar_calend_por_svc_semana(inicio, fim, semanas):
    data_col, valor_col, mlp_col, svc_col = obter_colunas_calendar()
    if not svc_col or not data_col or not valor_col:
        return None
    mlp_filter = f" AND LOWER(TRIM(COALESCE(CAST(`{mlp_col}` AS STRING), ''))) LIKE '{MLP_LIKE}'" if mlp_col else ""
    query = f"""
    SELECT `{data_col}` AS data, `{svc_col}` AS SVC, SUM(`{valor_col}`) AS valor
    FROM `meli-bi-data.SBOX_ANALYTICSLASTMILE.SDDCALENDARICAOHISTORICO_T`
    WHERE DATE(`{data_col}`) >= DATE('{inicio}') AND DATE(`{data_col}`) <= DATE('{fim}')
    {mlp_filter}
    GROUP BY 1, 2
    """
    df = run_query(query)
    if df is None or len(df) == 0:
        return None
    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
    rows = []
    for label, (seg, dom) in semanas:
        mask = (df["data"] >= seg) & (df["data"] <= dom)
        sub = df.loc[mask].groupby("SVC", as_index=False)["valor"].sum()
        sub["semana_label"] = label
        sub["calend_veic"] = (sub["valor"] / 6).round(1)
        rows.append(sub[["SVC", "semana_label", "calend_veic"]])
    out = pd.concat(rows, ignore_index=True) if rows else None
    return out


def buscar_rotas_por_svc_semana(inicio, fim, semanas):
    query = f"""
    SELECT DATE(DATA_FIM) AS data, SVC, COUNT(*) AS rotas
    FROM `meli-bi-data.WHOWNER.BT_BASEROTAS_LASTMILE`
    WHERE DATE(DATA_FIM) BETWEEN DATE('{inicio}') AND DATE('{fim}')
      AND LOWER(COALESCE(CAST(Carrier_name AS STRING), '')) LIKE '{CARRIER_LIKE}'
      AND COALESCE(CAST(Plate AS STRING), '') LIKE '%SDD%'
    GROUP BY 1, 2
    """
    df = run_query(query)
    if df is None or len(df) == 0:
        return None
    df["data"] = pd.to_datetime(df["data"]).dt.date
    rows = []
    for label, (seg, dom) in semanas:
        mask = (df["data"] >= seg) & (df["data"] <= dom)
        sub = df.loc[mask].groupby("SVC", as_index=False)["rotas"].sum()
        sub["semana_label"] = label
        sub["exec_veic"] = (sub["rotas"] / 6).round(1)
        rows.append(sub[["SVC", "semana_label", "exec_veic"]])
    return pd.concat(rows, ignore_index=True) if rows else None


# ---------------------------------------------------------------------------
# Semanas helpers
# ---------------------------------------------------------------------------
def semanas_range(w_inicio, w_fim):
    """Lista de (label, (seg, dom)) para semanas ISO do ano atual."""
    semanas = []
    for w in range(w_inicio, w_fim + 1):
        try:
            seg = date.fromisocalendar(ANO_ATUAL, w, 1)
        except ValueError:
            continue
        dom = seg + timedelta(days=6)
        semanas.append((f"W{w}", (seg, dom)))
    return semanas


def buscar_rotas_por_tipo_veiculo(inicio, fim):
    """Rotas executadas por tipo de veiculo (Tipo_veiculo) na ultima semana."""
    query = f"""
    SELECT
      CASE
        WHEN UPPER(TRIM(COALESCE(CAST(Tipo_veiculo AS STRING), ''))) LIKE '%UTIL%' THEN 'Utilitarios SDD'
        WHEN UPPER(TRIM(COALESCE(CAST(Tipo_veiculo AS STRING), ''))) LIKE '%VAN%' THEN 'Van SDD'
        WHEN UPPER(TRIM(COALESCE(CAST(Tipo_veiculo AS STRING), ''))) LIKE '%VUC%' THEN 'Vuc SDD'
        ELSE 'Outro'
      END AS tipo_veiculo,
      COUNT(*) AS rotas_executadas
    FROM `meli-bi-data.WHOWNER.BT_BASEROTAS_LASTMILE`
    WHERE DATE(DATA_FIM) BETWEEN DATE('{inicio}') AND DATE('{fim}')
      AND LOWER(COALESCE(CAST(Carrier_name AS STRING), '')) LIKE '{CARRIER_LIKE}'
      AND COALESCE(CAST(Plate AS STRING), '') LIKE '%SDD%'
    GROUP BY 1 ORDER BY 1
    """
    return run_query(query)


def buscar_calend_por_tipo_veiculo(inicio, fim):
    """Calendarizacao por tipo de veiculo (CADASTRO)."""
    data_col, valor_col, mlp_col, _ = obter_colunas_calendar()
    if not data_col or not valor_col:
        return None
    mlp_filter = f" AND LOWER(TRIM(COALESCE(CAST(`{mlp_col}` AS STRING), ''))) LIKE '{MLP_LIKE}'" if mlp_col else ""
    query = f"""
    SELECT
      CASE
        WHEN UPPER(TRIM(COALESCE(CAST(CADASTRO AS STRING), ''))) LIKE '%UTIL%' THEN 'Utilitarios SDD'
        WHEN UPPER(TRIM(COALESCE(CAST(CADASTRO AS STRING), ''))) LIKE '%VAN%' THEN 'Van SDD'
        WHEN UPPER(TRIM(COALESCE(CAST(CADASTRO AS STRING), ''))) LIKE '%VUC%' THEN 'Vuc SDD'
        ELSE 'Outro'
      END AS tipo_veiculo,
      SUM(`{valor_col}`) AS calendarizado
    FROM `meli-bi-data.SBOX_ANALYTICSLASTMILE.SDDCALENDARICAOHISTORICO_T`
    WHERE DATE(`{data_col}`) >= DATE('{inicio}') AND DATE(`{data_col}`) <= DATE('{fim}')
    {mlp_filter}
    GROUP BY 1 ORDER BY 1
    """
    return run_query(query)


def ultimas_4_semanas():
    seg_atual = HOJE - timedelta(days=HOJE.weekday())
    semanas = []
    for i in range(1, 5):
        seg = seg_atual - timedelta(days=7 * i)
        dom = seg + timedelta(days=6)
        iso_week = seg.isocalendar()[1]
        semanas.append((f"W{iso_week}", (seg, dom)))
    return semanas


def agregar_calend_por_semana(df_calend, datas_seg_dom):
    if df_calend is None or len(df_calend) == 0:
        return pd.DataFrame(columns=["semana_label", "seg", "dom", "calendarizado"])
    df = df_calend.copy()
    df["data"] = pd.to_datetime(df["data"]).dt.date
    res = []
    for label, (seg, dom) in datas_seg_dom:
        s = df[(df["data"] >= seg) & (df["data"] <= dom)]["valor"].sum()
        res.append({"semana_label": label, "seg": seg, "dom": dom, "calendarizado": int(s)})
    return pd.DataFrame(res)


def agregar_rotas_por_semana(df_rotas, datas_seg_dom):
    if df_rotas is None or len(df_rotas) == 0:
        return pd.DataFrame(columns=["semana_label", "seg", "dom", "rotas_executadas"])
    df = df_rotas.copy()
    df["data"] = pd.to_datetime(df["data"]).dt.date
    res = []
    for label, (seg, dom) in datas_seg_dom:
        s = df[(df["data"] >= seg) & (df["data"] <= dom)]["rotas_executadas"].sum()
        res.append({"semana_label": label, "seg": seg, "dom": dom, "rotas_executadas": int(s)})
    return pd.DataFrame(res)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("RELATORIO GERENCIAL IMEDIATO - SDD")
    print("=" * 60)

    validar_plan_vs_planilha()

    # Semanas completas: W11 ate a ultima semana completada
    seg_atual = HOJE - timedelta(days=HOJE.weekday())
    seg_ultima = seg_atual - timedelta(days=7)
    ultima_iso = seg_ultima.isocalendar()[1]
    print(f"  Ultima semana completa: W{ultima_iso}")

    # Todas as semanas W11..W32 para o Plano Total
    todas_semanas = semanas_range(SEMANA_INICIO, SEMANA_FIM)
    # Semanas passadas (executadas)
    semanas_passadas = [(l, d) for l, d in todas_semanas if int(l[1:]) <= ultima_iso]
    if not semanas_passadas:
        print("ERRO: Nenhuma semana completada desde W11.")
        return

    seg_1 = semanas_passadas[0][1][0]
    dom_last = semanas_passadas[-1][1][1]
    inicio = seg_1.strftime("%Y-%m-%d")
    fim = dom_last.strftime("%Y-%m-%d")

    # [1] Calendarizacao total (para ER semanal)
    print(f"\n[1] Calendarizacao ({inicio} a {fim})...")
    df_calend = buscar_calendarizacao_simples(inicio, fim)
    print(f"    {len(df_calend) if df_calend is not None else 0} registros")

    # [2] Rotas executadas
    print(f"\n[2] Rotas executadas ({inicio} a {fim})...")
    df_rotas = buscar_rotas_imediato_sdd(inicio, fim)
    print(f"    {len(df_rotas) if df_rotas is not None else 0} registros")

    # [3] Ultimas 4 semanas para grafico ER
    semanas_4 = ultimas_4_semanas()
    seg_4, dom_4 = semanas_4[-1][1][0], semanas_4[0][1][1]
    inicio_4s, fim_4s = seg_4.strftime("%Y-%m-%d"), dom_4.strftime("%Y-%m-%d")
    print(f"\n[3] Ultimas 4 semanas ({inicio_4s} a {fim_4s})...")
    df_calend_sem = buscar_calendarizacao_simples(inicio_4s, fim_4s)
    df_rotas_sem = buscar_rotas_imediato_sdd(inicio_4s, fim_4s)
    cal_sem = agregar_calend_por_semana(df_calend_sem, semanas_4)
    rot_sem = agregar_rotas_por_semana(df_rotas_sem, semanas_4)
    merge_sem = pd.merge(cal_sem, rot_sem, on=["semana_label", "seg", "dom"], how="outer").fillna(0)
    merge_sem["rotas_executadas"] = merge_sem["rotas_executadas"].astype(int)
    merge_sem["calendarizado"] = merge_sem["calendarizado"].astype(int)
    merge_sem["ER_%"] = (
        (merge_sem["rotas_executadas"] / merge_sem["calendarizado"] * 100).round(1)
        .where(merge_sem["calendarizado"] > 0, 0)
    )
    merge_sem["ER_exibir"] = merge_sem.apply(
        lambda r: "N/D" if r["calendarizado"] == 0 else f"{r['ER_%']:.1f}".replace(".", ",") + "%", axis=1,
    )
    merge_sem["veiculos_negociados"] = (merge_sem["calendarizado"] / 6).round(1)
    merge_sem["veiculos_executados"] = (merge_sem["rotas_executadas"] / 6).round(1)
    # Ordem cronologica
    semanas_order_table = list(reversed([s[0] for s in semanas_4]))
    merge_sem["_ord"] = merge_sem["semana_label"].map({w: i for i, w in enumerate(semanas_order_table)})
    merge_sem = merge_sem.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)

    # [4] ER por tipo de veiculo (ultima semana)
    seg_ult = semanas_passadas[-1][1][0]
    dom_ult = semanas_passadas[-1][1][1]
    inicio_ult = seg_ult.strftime("%Y-%m-%d")
    fim_ult = dom_ult.strftime("%Y-%m-%d")
    print(f"\n[4] ER por tipo de veiculo ({inicio_ult} a {fim_ult})...")
    df_rotas_tipo = buscar_rotas_por_tipo_veiculo(inicio_ult, fim_ult)
    df_calend_tipo = buscar_calend_por_tipo_veiculo(inicio_ult, fim_ult)

    # [5] Dados por SVC para Plano Total e Abertura
    print("\n[5] Dados por SVC (calend + exec)...")
    df_cal_svc = buscar_calend_por_svc_semana(inicio, fim, semanas_passadas)
    df_rot_svc = buscar_rotas_por_svc_semana(inicio, fim, semanas_passadas)

    # Gerar plano ideal
    plan_grid = gerar_plan_grid()

    # Merge: plan + calend + exec
    # Para semanas passadas: plan = calend/6 (se nao tem calend, plan = 0)
    # Para semanas futuras: plan = plan ideal (SVC_PLAN ramp)
    all_weeks = [f"W{w}" for w in range(SEMANA_INICIO, SEMANA_FIM + 1)]
    merged = plan_grid.copy()
    passadas_set = {s[0] for s in semanas_passadas}
    if df_cal_svc is not None:
        merged = merged.merge(df_cal_svc[["SVC", "semana_label", "calend_veic"]], on=["SVC", "semana_label"], how="left")
    if "calend_veic" not in merged.columns:
        merged["calend_veic"] = 0.0
    merged["calend_veic"] = merged["calend_veic"].fillna(0).astype(float)
    merged["plan"] = merged["plan"].astype(float)
    # Semanas passadas: plan = calendarizacao. Sem calend = 0.
    mask_passada = merged["semana_label"].isin(passadas_set)
    merged.loc[mask_passada, "plan"] = merged.loc[mask_passada, "calend_veic"]
    if df_rot_svc is not None:
        merged = merged.merge(df_rot_svc[["SVC", "semana_label", "exec_veic"]], on=["SVC", "semana_label"], how="left")
    else:
        merged["exec_veic"] = 0
    merged["exec_veic"] = merged["exec_veic"].fillna(0)

    print("\n" + "=" * 60)
    print("ULTIMAS 4 SEMANAS")
    print("=" * 60)
    cols_print = merge_sem[["semana_label", "calendarizado", "rotas_executadas"]].copy()
    cols_print["ER_%"] = merge_sem["ER_exibir"]
    print(cols_print.to_string(index=False))

    # Verificar total W32
    w32_total = plan_grid[plan_grid["semana_label"] == f"W{SEMANA_FIM}"]["plan"].sum()
    print(f"\n  Plano W{SEMANA_FIM} total: {int(w32_total)} veiculos (esperado: 580)")

    pasta_dia = BASE_RELATORIOS / HOJE.strftime("%Y-%m-%d")
    pasta_dia.mkdir(parents=True, exist_ok=True)

    gerar_dashboard_html(merge_sem, merged, pasta_dia, ultima_iso, df_rotas_tipo, df_calend_tipo)

    link_html = pasta_dia / "relatorio_imediato_sdd.html"
    print(f"\nDashboard gerado: {link_html.as_uri()}")


def cor_er(val):
    if pd.isna(val) or val == 0:
        return "er-null"
    if val >= 95:
        return "er-meta"
    if val >= 90:
        return "er-verde"
    if val >= 80:
        return "er-amarelo"
    return "er-vermelho"


def gerar_dashboard_html(merge_sem, merged, pasta_dia, ultima_iso, df_rotas_tipo=None, df_calend_tipo=None):
    def fmt_br(x):
        if pd.isna(x):
            return "-"
        if isinstance(x, (int, float)) and x != int(x):
            p = f"{x:.1f}".split(".")
            return f"{int(p[0])},{p[1]}"
        return f"{int(x):,}".replace(",", ".")

    def fmt_pct(x):
        if pd.isna(x):
            return "-"
        return f"{float(x):.1f}".replace(".", ",") + "%"

    er_ultima = merge_sem["ER_%"].iloc[-1] if len(merge_sem) else 0
    er_ultima_exibir = merge_sem["ER_exibir"].iloc[-1] if len(merge_sem) and "ER_exibir" in merge_sem.columns else fmt_pct(er_ultima)

    def cor_barra(v):
        if v >= 95: return "#348338"
        if v >= 90: return "#7ED321"
        if v >= 80: return "#F5A623"
        return "#E60000"

    labels_sem = merge_sem["semana_label"].tolist()
    valores_er_sem = merge_sem["ER_%"].fillna(0).tolist()
    carros_neg_sem = merge_sem["veiculos_negociados"].fillna(0).tolist()
    carros_exec_sem = merge_sem["veiculos_executados"].fillna(0).tolist()
    er_sem_display = merge_sem["ER_exibir"].tolist() if "ER_exibir" in merge_sem.columns else [fmt_pct(v) for v in valores_er_sem]
    cores_sem = [cor_barra(v) if merge_sem["calendarizado"].iloc[i] > 0 else "#999" for i, v in enumerate(valores_er_sem)]

    max_veiculos = max(max(carros_neg_sem, default=0), max(carros_exec_sem, default=0))

    def nice_ceil(val, margin=1.3):
        target = val * margin
        for step in [10, 15, 20, 25, 30, 40, 50, 60, 75, 100, 125, 150, 200, 250, 300, 500]:
            if target <= step:
                return step
        import math
        return int(math.ceil(target / 50) * 50)

    escala_y = nice_ceil(max_veiculos) if max_veiculos > 0 else 30

    min_er_sem = min(valores_er_sem) if valores_er_sem else 0
    er_axis_min = max(0, (int(min_er_sem / 10) - 1) * 10)
    max_er_sem = 100

    # Dados por tipo de veiculo (grafico)
    tipos_ordem = ["Utilitarios SDD", "Van SDD", "Vuc SDD"]
    tipos_label = ["Utilitario", "Van", "Vuc"]
    tipo_calend = {}
    tipo_exec = {}
    tipo_er = {}
    if df_rotas_tipo is not None and len(df_rotas_tipo) > 0:
        for t in tipos_ordem:
            row_e = df_rotas_tipo[df_rotas_tipo["tipo_veiculo"] == t]
            tipo_exec[t] = int(row_e["rotas_executadas"].sum()) if len(row_e) else 0
    if df_calend_tipo is not None and len(df_calend_tipo) > 0:
        for t in tipos_ordem:
            row_c = df_calend_tipo[df_calend_tipo["tipo_veiculo"] == t]
            tipo_calend[t] = int(row_c["calendarizado"].sum()) if len(row_c) else 0

    for t in tipos_ordem:
        c = tipo_calend.get(t, 0)
        e = tipo_exec.get(t, 0)
        tipo_er[t] = round(e / c * 100, 1) if c > 0 else 0

    max_tipo_rotas = max(
        max((tipo_calend.get(t, 0) for t in tipos_ordem), default=0),
        max((tipo_exec.get(t, 0) for t in tipos_ordem), default=0),
    )
    escala_tipo = nice_ceil(max_tipo_rotas) if max_tipo_rotas > 0 else 110

    js_tipo_labels = json.dumps(tipos_label)
    js_tipo_calend = json.dumps([tipo_calend.get(t, 0) for t in tipos_ordem])
    js_tipo_exec = json.dumps([tipo_exec.get(t, 0) for t in tipos_ordem])
    js_tipo_er = json.dumps([tipo_er.get(t, 0) for t in tipos_ordem])

    # TSV para copiar
    tsv_linhas = ["Semana\tVeiculos plan\tVeiculos exec\tER%"]
    for _, r in merge_sem.iterrows():
        er_tsv = r["ER_exibir"] if r["calendarizado"] > 0 else "N/D"
        tsv_linhas.append(f"{r['semana_label']}\t{fmt_br(r['veiculos_negociados'])}\t{fmt_br(r['veiculos_executados'])}\t{er_tsv}")
    tsv_text = "\n".join(tsv_linhas).replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")

    # ----------------------------------------------------------------
    # Abertura por SVC (ultima semana executada) — usa calendarizacao
    # ----------------------------------------------------------------
    ultima_semana_label = f"W{ultima_iso}"
    abertura_svc_html = ""
    ult_data = merged[merged["semana_label"] == ultima_semana_label].copy()
    # Usar calend_veic (calendarizacao BigQuery) como referencia
    ult_data["calend"] = pd.to_numeric(ult_data["calend_veic"], errors="coerce").fillna(0)
    ult_data["exec"] = pd.to_numeric(ult_data["exec_veic"], errors="coerce").fillna(0)
    # Mostrar apenas SVCs com calendarizacao > 0
    ult_data = ult_data[ult_data["calend"] > 0]
    if len(ult_data) > 0:
        ult_data["rotas_calend"] = (ult_data["calend"] * 6).round(0).astype(int)
        ult_data["rotas_exec"] = (ult_data["exec"] * 6).round(0).astype(int)
        ult_data["gap_rotas"] = ult_data["rotas_calend"] - ult_data["rotas_exec"]
        ult_data["er_pct"] = (ult_data["rotas_exec"] / ult_data["rotas_calend"] * 100).round(1).where(ult_data["rotas_calend"] > 0, 0)
        ult_data = ult_data.sort_values("gap_rotas", ascending=False)
        linhas_ab = []
        for _, r in ult_data.iterrows():
            er_val = float(r["er_pct"])
            er_cls = "er-meta" if er_val >= 95 else ("er-verde" if er_val >= 90 else ("er-amarelo" if er_val >= 80 else "er-vermelho"))
            er_str = f"{er_val:.1f}".replace(".", ",") + "%"
            linhas_ab.append(
                f"  <tr><td>{r['SVC']}</td><td class='num'>{int(r['rotas_calend'])}</td>"
                f"<td class='num'>{int(r['rotas_exec'])}</td>"
                f"<td class='num top10-gap'>{int(r['gap_rotas'])}</td>"
                f"<td class='num {er_cls}'>{er_str}</td></tr>"
            )
        linhas_ab_str = "\n".join(linhas_ab)
        abertura_svc_html = f"""
  <section class="section top10-box">
    <h2>Abertura por SVC - {ultima_semana_label}</h2>
    <table class="top10-tabela">
      <thead><tr><th>SVC</th><th class="num">Calendarizado</th><th class="num">Executado SDD</th><th class="num">Gap (rotas)</th><th class="num">ER%</th></tr></thead>
      <tbody>
{linhas_ab_str}
      </tbody>
    </table>
  </section>"""

    # ----------------------------------------------------------------
    # PLANO TOTAL: todas SVCs x todas semanas (W11-W32)
    # Passado: plan / exec | Futuro: plan (azul)
    # ----------------------------------------------------------------
    all_weeks = [f"W{w}" for w in range(SEMANA_INICIO, SEMANA_FIM + 1)]
    passadas_set = {f"W{w}" for w in range(SEMANA_INICIO, ultima_iso + 1)}

    th_cols = "<th class='num' style='background:#f0f0f0;'>Util</th><th class='num' style='background:#f0f0f0;'>Van</th><th class='num' style='background:#f0f0f0;'>Vuc</th><th class='num' style='background:#f0f0f0;'>Total</th>"
    for w in all_weeks:
        is_futuro = w not in passadas_set
        th_cols += f"<th class='num'{' style=\"background:#e3f2fd;color:#1565c0;\"' if is_futuro else ''}>{w}</th>"

    gantt_linhas = []
    for svc in SVC_ORDER:
        v = SVC_VEICULOS.get(svc, {"util": 0, "van": 0, "vuc": 0})
        v_total = v["util"] + v["van"] + v["vuc"]
        tipo_cells = (
            f"<td class='num gantt-pe' style='background:#f9f9f9;'>{v['util']}</td>"
            f"<td class='num gantt-pe' style='background:#f9f9f9;'>{v['van'] if v['van'] > 0 else '-'}</td>"
            f"<td class='num gantt-pe' style='background:#f9f9f9;'>{v['vuc']}</td>"
            f"<td class='num gantt-pe' style='background:#f0f0f0;font-weight:600;'>{v_total}</td>"
        )
        sub = merged[merged["SVC"] == svc]
        cells = []
        for w in all_weeks:
            row = sub[sub["semana_label"] == w]
            p = float(row["plan"].sum()) if len(row) else 0
            e = float(row["exec_veic"].sum()) if len(row) else 0
            p_int = int(round(p))
            e_int = int(round(e))
            is_futuro = w not in passadas_set

            if p_int == 0 and e_int == 0:
                cells.append("<td class='num gantt-pe gantt-cinza'>-</td>")
            elif is_futuro:
                cells.append(f"<td class='num gantt-pe gantt-futuro'>{p_int}</td>")
            else:
                gap = e_int - p_int
                if gap < 0:
                    cls = "gantt-vermelho"
                elif gap > 0:
                    cls = "gantt-verde"
                else:
                    cls = "gantt-cinza"
                gap_str = str(gap) if gap != 0 else "0"
                cells.append(f"<td class='num gantt-pe {cls}'>{p_int} / {e_int} <span class='gantt-gap'>({gap_str})</span></td>")
        gantt_linhas.append(f"  <tr><td class='gantt-svc'>{svc}</td>{tipo_cells}" + "".join(cells) + "</tr>")

    # Total row
    total_cells = []
    for w in all_weeks:
        sub = merged[merged["semana_label"] == w]
        p_total = sub["plan"].sum()
        e_total = sub["exec_veic"].sum()
        p_int = int(round(p_total))
        e_int = int(round(e_total))
        is_futuro = w not in passadas_set

        if is_futuro:
            total_cells.append(f"<td class='num gantt-pe gantt-total gantt-futuro'><strong>{p_int}</strong></td>")
        else:
            gap = e_int - p_int
            cls = "gantt-vermelho" if gap < 0 else ("gantt-verde" if gap > 0 else "gantt-cinza")
            gap_str = str(gap) if gap != 0 else "0"
            total_cells.append(f"<td class='num gantt-pe gantt-total {cls}'><strong>{p_int} / {e_int} ({gap_str})</strong></td>")
    # Totais por tipo
    total_util = sum(SVC_VEICULOS.get(s, {}).get("util", 0) for s in SVC_ORDER)
    total_van = sum(SVC_VEICULOS.get(s, {}).get("van", 0) for s in SVC_ORDER)
    total_vuc = sum(SVC_VEICULOS.get(s, {}).get("vuc", 0) for s in SVC_ORDER)
    total_veic = total_util + total_van + total_vuc
    tipo_total_cells = (
        f"<td class='num gantt-pe' style='background:#f0f0f0;font-weight:700;'>{total_util}</td>"
        f"<td class='num gantt-pe' style='background:#f0f0f0;font-weight:700;'>{total_van}</td>"
        f"<td class='num gantt-pe' style='background:#f0f0f0;font-weight:700;'>{total_vuc}</td>"
        f"<td class='num gantt-pe' style='background:#e8e8e8;font-weight:700;'>{total_veic}</td>"
    )
    gantt_linhas.append(f"  <tr class='gantt-total-row'><td class='gantt-svc gantt-total-row'>Total</td>{tipo_total_cells}" + "".join(total_cells) + "</tr>")

    plano_total_html = f"""
  <div class="chart-box gantt-box" id="planoTotalBox">
    <h2 class="gantt-titulo">Plano Total Imediato 2026</h2>
    <p class="gantt-subtitulo">Veiculos por SVC/semana | Passado: Plan / Exec (gap) | <span style="color:#1565c0;font-weight:600;">Futuro: Plan</span></p>
    <div class="gantt-scroll">
    <table class="gantt-tabela gantt-compact" id="tabelaPlanoTotal">
      <thead><tr><th class="gantt-sort" data-col="0">SVC</th>{th_cols}</tr></thead>
      <tbody>
{"".join(gantt_linhas)}
      </tbody>
    </table>
    </div>
  </div>"""

    print("    Inserindo Chart.js...")
    script_chart = _script_embed(CHART_JS_URL)
    script_plugin = _script_embed(CHART_PLUGIN_URL)

    js_labels_sem = json.dumps(labels_sem)
    js_valores_sem = json.dumps(valores_er_sem)
    js_carros_neg_sem = json.dumps(carros_neg_sem)
    js_carros_exec_sem = json.dumps(carros_exec_sem)
    js_er_sem_display = json.dumps(er_sem_display)
    js_cores_sem = json.dumps(cores_sem)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Relatorio Gerencial Imediato – SDD | Meli Last Mile</title>
  <link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;600;700&display=swap" rel="stylesheet">
  {script_chart}
  {script_plugin}
  <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
  <style>
    :root {{ --meli-yellow: #FFE600; --meli-black: #000; --meli-gray-dark: #333; --meli-gray: #666; --meli-gray-light: #eee; --meli-gray-bg: #F5F5F5; --meli-blue: #2D3277; --er-meta: #348338; --er-verde: #7ED321; --er-amarelo: #F5A623; --er-vermelho: #E60000; }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'Source Sans 3', 'Proxima Nova', -apple-system, sans-serif; background: var(--meli-gray-bg); color: var(--meli-black); line-height: 1.4; padding: 24px 24px 48px; max-width: 100%; margin: 0 auto; }}
    .header {{ background: var(--meli-black); color: var(--meli-yellow); padding: 20px 24px; margin: 0 -32px 24px -32px; text-align: center; }}
    .header h1 {{ font-size: 1.8rem; font-weight: 700; }}
    .header .subtitulo {{ font-size: 0.85rem; color: rgba(255,230,0,0.6); margin-top: 4px; }}
    .info-aderencia {{ background: #fff; border-radius: 8px; padding: 14px 20px; margin-bottom: 24px; box-shadow: 0 1px 4px rgba(0,0,0,.08); border-left: 4px solid var(--meli-yellow); font-size: 1.1rem; }}
    .info-aderencia strong {{ color: var(--er-vermelho); }}
    .z-charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }}
    @media (max-width: 900px) {{ .z-charts {{ grid-template-columns: 1fr; }} }}
    .chart-box {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); border-left: 4px solid var(--meli-yellow); }}
    .chart-box h2 {{ font-size: 1rem; color: var(--meli-gray-dark); margin-bottom: 16px; padding-bottom: 8px; border-bottom: 1px solid var(--meli-gray-light); }}
    .chart-container {{ position: relative; height: 380px; }}
    .gantt-box {{ margin-bottom: 24px; }}
    .gantt-scroll {{ border: 1px solid var(--meli-gray-light); border-radius: 6px; overflow-x: auto; overflow-y: visible; }}
    .gantt-tabela {{ font-size: 0.85rem; margin-top: 0; border-collapse: collapse; min-width: 1600px; }}
    .gantt-tabela th, .gantt-tabela td {{ padding: 5px 8px; border: 1px solid var(--meli-gray-light); white-space: nowrap; }}
    .gantt-tabela thead th {{ text-align: center; background: var(--meli-gray-bg); font-weight: 600; color: var(--meli-gray-dark); position: sticky; top: 0; z-index: 1; }}
    .gantt-tabela thead th:first-child {{ text-align: left; position: sticky; left: 0; z-index: 3; background: var(--meli-gray-bg); }}
    .gantt-tabela .gantt-svc {{ font-weight: 600; background: var(--meli-gray-bg); position: sticky; left: 0; z-index: 2; }}
    .gantt-tabela th.gantt-sort {{ cursor: pointer; user-select: none; }}
    .gantt-tabela .gantt-pe {{ font-size: 0.85em; text-align: center; }}
    .gantt-gap {{ font-size: 0.7em; color: var(--meli-gray); }}
    .gantt-vermelho {{ background: #ffebee !important; }}
    .gantt-verde {{ background: #e8f5e9 !important; }}
    .gantt-cinza {{ background: #f5f5f5 !important; }}
    .gantt-futuro {{ background: #e3f2fd !important; color: #1565c0; font-weight: 600; }}
    .gantt-total-row {{ font-weight: 700; border-top: 2px solid var(--meli-gray-dark); }}
    .gantt-total-row td {{ background: var(--meli-gray-bg) !important; }}
    .gantt-total-row td.gantt-futuro {{ background: #bbdefb !important; }}
    .gantt-titulo {{ font-size: 1rem; color: var(--meli-gray-dark); margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid var(--meli-gray-light); }}
    .gantt-subtitulo {{ font-size: 0.85rem; color: var(--meli-gray); margin: -4px 0 12px 0; }}
    .top10-box {{ margin-bottom: 24px; }}
    .top10-tabela {{ font-size: 0.85rem; width: 100%; border-collapse: collapse; }}
    .top10-tabela th, .top10-tabela td {{ padding: 6px 10px; border-bottom: 1px solid var(--meli-gray-light); text-align: center; }}
    .top10-tabela th {{ background: var(--meli-gray-bg); font-weight: 600; color: var(--meli-gray); }}
    .top10-tabela td.num {{ text-align: center; font-variant-numeric: tabular-nums; }}
    .top10-tabela .top10-gap {{ font-weight: 600; color: var(--er-vermelho); }}
    .section {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
    .section h2 {{ font-size: 1rem; color: var(--meli-gray-dark); margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid var(--meli-gray-light); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
    th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--meli-gray-light); }}
    th {{ font-weight: 600; color: var(--meli-gray); background: var(--meli-gray-bg); }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .er-meta {{ font-weight: 700; color: var(--er-meta) !important; }}
    .er-verde {{ font-weight: 600; color: var(--er-verde) !important; }}
    .er-amarelo {{ font-weight: 600; color: var(--er-amarelo) !important; }}
    .er-vermelho {{ font-weight: 600; color: var(--er-vermelho) !important; }}
    .er-null {{ color: var(--meli-gray); }}
    .legenda-er {{ margin-top: 16px; padding: 12px; background: var(--meli-gray-bg); border-radius: 6px; font-size: 0.8rem; color: var(--meli-gray); display: flex; flex-wrap: wrap; gap: 12px 20px; }}
    .legenda-er span {{ display: inline-flex; align-items: center; gap: 6px; }}
    .dot {{ width: 10px; height: 10px; border-radius: 50%; display: inline-block; }}
    .btn {{ background: var(--meli-yellow); color: var(--meli-black); border: none; padding: 10px 20px; font-size: 14px; font-weight: 600; cursor: pointer; border-radius: 6px; margin-right: 8px; font-family: inherit; }}
    .btn:hover {{ filter: brightness(0.97); }}
    .btn-wrap {{ margin-top: 24px; padding-top: 16px; border-top: 1px solid var(--meli-gray-light); }}
    #msg {{ font-size: 13px; margin-top: 8px; font-weight: 600; }}
    .ok {{ color: var(--er-meta); }}
    .erro {{ color: var(--er-vermelho); }}
  </style>
</head>
<body>
  <div id="relatorioCompleto">
  <header class="header">
    <h1>Relatorio Gerencial Imediato – SDD</h1>
  </header>
  <p class="info-aderencia">ER ultima semana: <strong>{er_ultima_exibir}</strong> | Meta: 95%</p>
  <div class="z-charts">
    <div class="chart-box"><h2>ER x Veiculos – Ultimas 4 Semanas</h2><div class="chart-container"><canvas id="chartSem"></canvas></div></div>
    <div class="chart-box"><h2>ER por Tipo de Veiculo – {ultima_semana_label}</h2><div class="chart-container"><canvas id="chartTipo"></canvas></div></div>
  </div>
  <div class="legenda-er">
    <span><span class="dot" style="background:var(--er-meta)"></span> Meta 95%+</span>
    <span><span class="dot" style="background:var(--er-verde)"></span> Alto 90-95%</span>
    <span><span class="dot" style="background:var(--er-amarelo)"></span> Atencao 80-90%</span>
    <span><span class="dot" style="background:var(--er-vermelho)"></span> Abaixo 80%</span>
    <span><span class="dot" style="background:#1565c0"></span> Futuro (planejado)</span>
  </div>
{abertura_svc_html}
{plano_total_html}
  <div class="btn-wrap">
    <button class="btn" id="btnPlanilha">Copiar para planilha</button>
    <button class="btn" id="btnImagem">Exportar imagem</button>
    <button class="btn" id="btnPdf">Baixar PDF</button>
    <p id="msg"></p>
  </div>
  </div>
  <script>
    (function init() {{
      function run() {{
        var tsv = `{tsv_text}`;
        document.getElementById("btnPlanilha").onclick = function () {{ navigator.clipboard.writeText(tsv).then(function () {{ document.getElementById("msg").textContent = "Copiado!"; document.getElementById("msg").className = "ok"; }}).catch(function () {{ document.getElementById("msg").textContent = "Erro."; document.getElementById("msg").className = "erro"; }}); }};
        document.getElementById("btnImagem").onclick = function () {{
          var box = document.getElementById("relatorioCompleto");
          var msgEl = document.getElementById("msg");
          if (typeof html2canvas === "undefined") {{ msgEl.textContent = "Recarregue com internet."; msgEl.className = "erro"; return; }}
          msgEl.textContent = "Gerando..."; msgEl.className = "";
          // Esconder botoes e expandir tabela para imagem
          var btnWrap = document.querySelector(".btn-wrap");
          var ganttScroll = document.querySelectorAll(".gantt-scroll");
          if (btnWrap) btnWrap.style.display = "none";
          ganttScroll.forEach(function(el) {{ el.style.overflow = "visible"; el.style.maxHeight = "none"; }});
          html2canvas(box, {{ scale: 2, useCORS: true, logging: false }}).then(function(canvas) {{
            var a = document.createElement("a"); a.href = canvas.toDataURL("image/png"); a.download = "relatorio_Imediato_SDD.png"; a.click();
            msgEl.textContent = "Imagem baixada."; msgEl.className = "ok";
            // Restaurar
            if (btnWrap) btnWrap.style.display = "";
            ganttScroll.forEach(function(el) {{ el.style.overflow = ""; el.style.maxHeight = ""; }});
          }}).catch(function() {{
            msgEl.textContent = "Erro."; msgEl.className = "erro";
            if (btnWrap) btnWrap.style.display = "";
            ganttScroll.forEach(function(el) {{ el.style.overflow = ""; el.style.maxHeight = ""; }});
          }});
        }};
        document.getElementById("btnPdf").onclick = function () {{ document.getElementById("msg").textContent = "Use Salvar como PDF."; document.getElementById("msg").className = "ok"; window.print(); }};
        if (typeof Chart === "undefined") return;
        if (typeof ChartDataLabels !== "undefined") Chart.register(ChartDataLabels);
        Chart.defaults.font.family = "'Source Sans 3', sans-serif";
        Chart.defaults.font.size = 14;
        Chart.defaults.color = '#333';
        var erSemDisplay = {js_er_sem_display};
        new Chart(document.getElementById("chartSem"), {{ type: "bar", data: {{ labels: {js_labels_sem}, datasets: [
          {{ label: "Calendarizado", data: {js_carros_neg_sem}, backgroundColor: "rgba(255,230,0,0.8)", borderColor: "#FFE600", borderWidth: 1, yAxisID: "yCarros", order: 2 }},
          {{ label: "Executado", data: {js_carros_exec_sem}, backgroundColor: "rgba(51,51,51,0.75)", borderColor: "#333", borderWidth: 1, yAxisID: "yCarros", order: 1 }},
          {{ label: "ER %", data: {js_valores_sem}, type: "line", borderColor: "#2D3277", backgroundColor: "transparent", borderWidth: 3, fill: false, yAxisID: "yEr", pointBackgroundColor: {js_cores_sem}, pointBorderColor: {js_cores_sem}, pointRadius: 7, order: 0 }}
        ] }}, options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: true, position: "bottom", labels: {{ font: {{ size: 14 }} }} }}, tooltip: {{ callbacks: {{ label: function(ctx) {{ if (ctx.dataset.yAxisID === "yEr") return erSemDisplay[ctx.dataIndex]; return ctx.raw + " veiculos"; }} }} }}, datalabels: {{ display: true, formatter: function(v, ctx) {{ return ctx.datasetIndex === 2 ? Math.round(v) + "%" : Math.round(v); }}, anchor: "end", align: "top", color: "#333", font: {{ size: 14, weight: "bold" }} }} }}, scales: {{ x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 14 }} }} }}, yCarros: {{ type: "linear", position: "left", min: 0, max: {escala_y}, grid: {{ color: "#eee" }}, title: {{ display: true, text: "Veiculos", font: {{ size: 13 }} }}, ticks: {{ stepSize: {max(1, escala_y // 5)}, font: {{ size: 13 }} }} }}, yEr: {{ type: "linear", position: "right", min: {er_axis_min}, max: {max_er_sem}, ticks: {{ callback: function(v) {{ return v + "%"; }}, font: {{ size: 13 }} }}, grid: {{ display: false }}, title: {{ display: true, text: "ER %", font: {{ size: 13 }} }} }} }} }} }});
        // Grafico por tipo de veiculo
        var tipoCalend = {js_tipo_calend};
        var tipoExec = {js_tipo_exec};
        var tipoEr = {js_tipo_er};
        var tipoColors = ["#FFE600", "#999", "#333"];
        var tipoExecColors = ["rgba(51,51,51,0.75)", "rgba(51,51,51,0.75)", "rgba(51,51,51,0.75)"];
        new Chart(document.getElementById("chartTipo"), {{ type: "bar", data: {{ labels: {js_tipo_labels}, datasets: [
          {{ label: "Calendarizado", data: tipoCalend, backgroundColor: "rgba(255,230,0,0.8)", borderColor: "#FFE600", borderWidth: 1, yAxisID: "yRotas", order: 2 }},
          {{ label: "Executado", data: tipoExec, backgroundColor: "rgba(51,51,51,0.75)", borderColor: "#333", borderWidth: 1, yAxisID: "yRotas", order: 1 }},
          {{ label: "ER %", data: tipoEr, type: "line", borderColor: "#2D3277", backgroundColor: "transparent", borderWidth: 3, fill: false, yAxisID: "yEr", pointRadius: 7, pointBackgroundColor: tipoEr.map(function(v) {{ return v >= 95 ? "#348338" : (v >= 90 ? "#7ED321" : (v >= 80 ? "#F5A623" : "#E60000")); }}), order: 0 }}
        ] }}, options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: true, position: "bottom", labels: {{ font: {{ size: 14 }} }} }}, datalabels: {{ display: true, formatter: function(v, ctx) {{ return ctx.datasetIndex === 2 ? Math.round(v) + "%" : Math.round(v); }}, anchor: "end", align: "top", color: "#333", font: {{ size: 14, weight: "bold" }} }} }}, scales: {{ x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 14 }} }} }}, yRotas: {{ type: "linear", position: "left", min: 0, max: {escala_tipo}, grid: {{ color: "#eee" }}, title: {{ display: true, text: "Rotas", font: {{ size: 13 }} }}, ticks: {{ stepSize: {max(1, escala_tipo // 5)}, font: {{ size: 13 }} }} }}, yEr: {{ type: "linear", position: "right", min: 0, max: 100, ticks: {{ callback: function(v) {{ return v + "%"; }}, font: {{ size: 13 }} }}, grid: {{ display: false }}, title: {{ display: true, text: "ER %", font: {{ size: 13 }} }} }} }} }} }});
        // Sorting
        var tbl = document.getElementById("tabelaPlanoTotal");
        if (tbl) {{
          tbl.querySelectorAll("thead th").forEach(function(th, col) {{
            th.style.cursor = "pointer";
            th.addEventListener("click", function() {{
              var tbody = tbl.querySelector("tbody");
              var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr:not(.gantt-total-row)"));
              var asc = th.getAttribute("data-order") !== "asc";
              th.setAttribute("data-order", asc ? "asc" : "desc");
              rows.sort(function(a, b) {{
                var av, bv;
                if (col === 0) {{ av = a.cells[0].textContent; bv = b.cells[0].textContent; return asc ? (av < bv ? -1 : 1) : (bv < av ? -1 : 1); }}
                var am = a.cells[col].textContent.match(/\\d+/), bm = b.cells[col].textContent.match(/\\d+/);
                av = am ? parseInt(am[0]) : 0; bv = bm ? parseInt(bm[0]) : 0;
                return asc ? av - bv : bv - av;
              }});
              rows.forEach(function(r) {{ tbody.appendChild(r); }});
              var tr = tbody.querySelector("tr.gantt-total-row"); if (tr) tbody.appendChild(tr);
            }});
          }});
        }}
      }}
      if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", run); else run();
    }})();
  </script>
</body>
</html>"""

    out_path = pasta_dia / "relatorio_imediato_sdd.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML: {out_path}")
    copy_path = Path(__file__).resolve().parent / "relatorio_imediato_sdd.html"
    try:
        import shutil
        shutil.copy2(out_path, copy_path)
        shutil.copy2(out_path, RELATORIO_HTML_LINK2)
        print(f"Copia: {copy_path}")
    except Exception as e:
        print(f"AVISO: {e}")


if __name__ == "__main__":
    main()
