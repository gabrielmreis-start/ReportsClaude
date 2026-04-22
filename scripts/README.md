# Scripts — Relatórios Fleet SDD

Scripts para geração dos relatórios semanais DHL SDD e Imediato SDD.

## Pré-requisitos (instalar uma única vez)

```bash
# 1. Python 3.10+
# Download: https://www.python.org/downloads/

# 2. Bibliotecas Python
pip install google-cloud-bigquery google-auth pandas

# 3. Google Cloud SDK (gcloud)
# Download: https://cloud.google.com/sdk/docs/install
# Após instalar:
gcloud auth application-default login
# → abrirá o browser, fazer login com @mercadolivre.com

# 4. Clonar este repositório (se ainda não tiver)
git clone https://github.com/gabrielmreis-start/ReportsClaude.git
cd ReportsClaude
```

**Acessos necessários** (solicitar ao gestor):
- BigQuery: projeto `meli-bi-data` — dataset `SBOX_ANALYTICSLASTMILE`
- GitHub: repositório `gabrielmreis-start/ReportsClaude` (para git push)

---

## Relatório DHL SDD — Execução Semanal

**Arquivo:** `scripts/relatorio_dhl_sdd_template.py`  
**Frequência:** Toda segunda ou terça-feira  
**Resultado:** HTML publicado no GitHub Pages

### Passo a Passo

#### PASSO 1 — Criar script da semana
```
Copiar:  scripts/relatorio_dhl_sdd_template.py
     →   scripts/gerar_novo_html_WXX.py   (ex: gerar_novo_html_w17.py)
```

#### PASSO 2 — Atualizar paths no novo script

No início do arquivo, atualizar:
```python
HTML_SOURCE = Path(r"C:\caminho\ReportsClaude\relatorio_dhl_sdd_W[semana anterior].html")
HTML_OUTPUT = Path(r"C:\caminho\ReportsClaude\relatorio_dhl_sdd_WXX.html")
HTML_BASE   = Path(r"C:\caminho\ReportsClaude\relatorio_dhl_sdd.html")
```

> **ATENÇÃO:** Adaptar o caminho para o seu computador. O arquivo base do HTML fica em `ReportsClaude/` (raiz do repo).

#### PASSO 3 — Adicionar semana ao array SEMANAS
```python
# Adicionar a nova semana no final do array
SEMANAS = ["W7", "W8", ..., "W16", "W17"]  # ← adicionar WXX
```

#### PASSO 4 — Atualizar PLAN_SEMANAL (plan incremental por SVC)

Abrir `scripts/plan_semana_dhl.csv` e pegar os valores da nova semana para cada SVC.

```python
# Cada lista tem um valor por semana (na mesma ordem de SEMANAS)
# Ex: SSP29 no W17 = 3 → adicionar 3 no final da lista de SSP29
PLAN_SEMANAL = {
    "SSP29": [0, 5, 5, 9, 0, 0, 0, 0, 0, 0, 3, ...],  # ← +valor da nova semana
    ...
}
```

> **REGRA CRÍTICA:** Valores são INCREMENTAIS (não acumulados). O script acumula.  
> Se a nova semana não estiver no CSV: **duplicar o valor da semana anterior**.

#### PASSO 5 — Atualizar EXEC_BRUTO (execução por SVC)

Rodar no BigQuery (console: https://console.cloud.google.com/bigquery):

```sql
-- Trocar WEEKISO pelo número da semana (ex: 17)
-- e as datas pelo intervalo da semana (seg a dom)
SELECT
  svc_name AS svc,
  ROUND(COUNT(DISTINCT CONCAT(svc_name, placa, CAST(data_rota AS STRING))) / 6.0, 1) AS veiculos
FROM `meli-bi-data.SBOX_ANALYTICSLASTMILE.BT_BASEROTAS_LASTMILE`
WHERE EXTRACT(ISOWEEK FROM data_rota) = [NUMERO_SEMANA]
  AND EXTRACT(YEAR FROM data_rota) = [ANO]
  AND LOWER(carrier_name) LIKE '%dhl%'
GROUP BY svc_name
ORDER BY veiculos DESC
```

Copiar os valores para o dicionário `EXEC_BRUTO` no script:
```python
EXEC_BRUTO = {
    "SMG15": {"W7": 6.2, ..., "W17": [NOVO_VALOR]},
    ...
}
```

#### PASSO 6 — Atualizar SEMANAL (resumo últimas 4 semanas)

Manter só as últimas 4 semanas. Remover a mais antiga e adicionar a nova:
```python
SEMANAL = [
    {"periodo": "W14", "cal": 2976, "exec": 1980},  # ← remover linha mais antiga
    {"periodo": "W15", "cal": 2934, "exec": 2208},
    {"periodo": "W16", "cal": 2952, "exec": 2372},
    {"periodo": "W17", "cal": XXXX, "exec": XXXX},  # ← adicionar nova semana
]
# cal  = total calendarizado da semana (soma do PLAN_ACUMULADO de todos SVCs × 6)
# exec = total executado (soma de EXEC_BRUTO de todos SVCs × 6)
```

#### PASSO 7 — Atualizar ABERTURA (detalhamento por SVC)

Com os mesmos dados do BigQuery (passo 5), preencher:
```python
ABERTURA_WXX_CAL  = {"SSP29": 336, "SSC2": 246, ...}  # calendarizado por SVC
ABERTURA_WXX_EXEC = {"SSP29": 286, "SMG15": 187, ...}  # executado por SVC
```

#### PASSO 8 — Rodar o script
```bash
cd ReportsClaude/scripts
python gerar_novo_html_wXX.py
```
Script salva automaticamente em `ReportsClaude/relatorio_dhl_sdd_WXX.html` e `relatorio_dhl_sdd.html`.

#### PASSO 9 — Publicar e enviar
```bash
cd ReportsClaude
git add relatorio_dhl_sdd_WXX.html relatorio_dhl_sdd.html
git commit -m "Relatório DHL SDD WXX"
git push
```

**Link para enviar:**  
`https://gabrielmreis-start.github.io/ReportsClaude/relatorio_dhl_sdd_WXX.html`

### Regras Críticas
- 🚫 **NUNCA** usar `relatorio_dhl_sdd.py` para a proposta (plan semanal → gráfico sempre verde)
- 🚫 **NUNCA** sobrescrever HTMLs de semanas anteriores (sempre novo arquivo `_WXX.html`)
- ✅ PLAN = **ACUMULADO** (não semanal) — o script acumula a partir dos valores incrementais
- ✅ **BASELINES FIXOS** (não mudam): `SSC3=17`, `SSP29=37`, `SSP30=10`
- ✅ Se semana sem plan no CSV → duplicar semana anterior

---

## Relatório Imediato SDD — Execução Semanal

**Arquivo:** `scripts/relatorio_imediato_sdd.py`  
**Frequência:** Toda semana  
**Resultado:** HTML publicado no GitHub Pages

### Passo a Passo

#### PASSO 1 — Rodar o script

O Imediato busca dados direto do BigQuery (automático, sem editar variáveis):

```bash
cd ReportsClaude/scripts
python relatorio_imediato_sdd.py
```

> O script usa `gcloud auth application-default` para autenticar no BQ automaticamente.

#### PASSO 2 — Copiar HTML com nome da semana
```bash
# Copiar o HTML gerado para um arquivo com nome da semana
cp relatorio_imediato_sdd.html ../relatorio_imediato_sdd_WXX.html
```

#### PASSO 3 — Publicar e enviar
```bash
cd ReportsClaude
git add relatorio_imediato_sdd_WXX.html relatorio_imediato_sdd.html
git commit -m "Relatório Imediato SDD WXX"
git push
```

**Link para enviar:**  
`https://gabrielmreis-start.github.io/ReportsClaude/relatorio_imediato_sdd_WXX.html`

### Contexto do relatório Imediato
- **24 SVCs** em ramp de entrada: W16 a W28
- **Escala final W32:** ~580 veículos
- Carrier BigQuery: `LIKE '%imediato%'`
- O script calcula automaticamente o plano por semana com base na tabela `SVC_PLAN` (entry/target/ramp)
- **Sem baselines fixos** (modelo novo, volume crescente por SVC)

### Atualizar SVC_PLAN (quando entrar novo SVC)
```python
# Adicionar no dicionário SVC_PLAN no início do script:
"NOVO_SVC": {"entry": [SEMANA_ENTRADA], "target": [VEICULOS_ALVO], "ramp": [VEICULOS_POR_SEMANA]},
```

---

## Estrutura do repositório

```
ReportsClaude/
├── scripts/
│   ├── README.md                        ← este arquivo
│   ├── relatorio_dhl_sdd_template.py    ← template DHL (copiar a cada semana)
│   ├── plan_semana_dhl.csv              ← plan incremental DHL por SVC
│   └── relatorio_imediato_sdd.py        ← script Imediato (rodar direto)
├── relatorio_dhl_sdd.html               ← relatório DHL mais recente (base)
├── relatorio_dhl_sdd_WXX.html           ← relatórios DHL por semana
└── relatorio_imediato_sdd_WXX.html      ← relatórios Imediato por semana
```
