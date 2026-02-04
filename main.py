# =========================
# AUTOVOLT LAKEHOUSE (BRONZE) - Cloud Run HTTP
# - Lake-first (GCS JSONL)
# - BigQuery "hot layer" só ano atual (economia/free)
# - Histórico (backfill) grava só no GCS
# - Estado em state/state.json (migração automática)
# - raw_vendas contém lote_id + ordem_producao_id (via mapeamento lote->OP)
# - raw_cliente é append-only (auditável): gera nova linha com data_ultima_compra quando houver venda
# - NÃO toca em raw_usuario / raw_controle_acesso
# =========================

import functions_framework
import warnings
warnings.simplefilter("ignore", category=FutureWarning)

import io
import json
import uuid
import random
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

# -------------------------
# CONFIG
# -------------------------
PROJECT_ID = "autovolt-analytics-479417"
DATASET_ID = "autovolt_bronze"
BUCKET_NAME = "bucket_ingestao"

TZ_BR = timezone(timedelta(hours=-3))  # America/Recife
HORAS_POR_LOTE = 1

STATE_FILE = "state/state.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# -------------------------
# SCHEMAS (Bronze)
# -------------------------
SCHEMAS = {
    "raw_linha": [
        SchemaField("linha_id", "STRING"),
        SchemaField("descricao", "STRING"),
        SchemaField("turnos_operacionais", "STRING"),
    ],
    "raw_metas_vendas": [
        SchemaField("meta_id", "STRING"),
        SchemaField("ano_mes_id", "STRING"),
        SchemaField("meta_quantidade", "STRING"),
        SchemaField("meta_valor", "STRING"),
    ],
    "raw_tempo": [
        SchemaField("ano_mes_id", "STRING"),
        SchemaField("ano", "STRING"),
        SchemaField("mes", "STRING"),
        SchemaField("nome_mes", "STRING"),
        SchemaField("trimestre", "STRING"),
        SchemaField("ano_mes_label", "STRING"),
    ],
    "raw_tipo_manut": [
        SchemaField("tipo_manutencao_id", "STRING"),
        SchemaField("descricao", "STRING"),
        SchemaField("criticidade_padrao", "STRING"),
    ],
    "raw_turno": [
        SchemaField("turno_id", "STRING"),
        SchemaField("janela", "STRING"),
        SchemaField("coef_performance", "STRING"),
    ],
    "raw_produto": [
        SchemaField("produto_id", "STRING"),
        SchemaField("modelo", "STRING"),
        SchemaField("tensao_v", "STRING"),
        SchemaField("capacidade_ah", "STRING"),
        SchemaField("linha_segmento", "STRING"),
        SchemaField("data_lancamento", "STRING"),
        SchemaField("data_descontinuacao", "STRING"),
    ],
    "raw_maquina": [
        SchemaField("maquina_id", "STRING"),
        SchemaField("tipo", "STRING"),
        SchemaField("fabricante", "STRING"),
        SchemaField("ano", "STRING"),
        SchemaField("linha_id", "STRING"),
    ],
    "raw_fornecedor": [
        SchemaField("fornecedor_id", "STRING"),
        SchemaField("categoria", "STRING"),
        SchemaField("leadtime_dias", "STRING"),
        SchemaField("qualificacao", "STRING"),
        SchemaField("data_cadastro", "STRING"),
        SchemaField("data_ultima_avaliacao", "STRING"),
        SchemaField("descricao", "STRING"),
    ],
    "raw_defeito": [
        SchemaField("defeito_id", "STRING"),
        SchemaField("descricao", "STRING"),
        SchemaField("gravidade", "STRING"),
    ],
    "raw_materia_prima": [
        SchemaField("materia_prima_id", "STRING"),
        SchemaField("nome_material", "STRING"),
    ],
    "raw_cliente": [
        SchemaField("cliente_id", "STRING"),
        SchemaField("tipo_cliente", "STRING"),
        SchemaField("cidade", "STRING"),
        SchemaField("tipo_plano", "STRING"),
        SchemaField("data_cadastro", "STRING"),
        SchemaField("data_ultima_compra", "STRING"),
    ],
    "raw_lote": [
        SchemaField("lote_id", "STRING"),
        SchemaField("produto_id", "STRING"),
        SchemaField("linha_id", "STRING"),
        SchemaField("maquina_id", "STRING"),
        SchemaField("inicio_producao", "STRING"),
        SchemaField("fim_producao", "STRING"),
        SchemaField("duracao_horas", "STRING"),
    ],
    "raw_producao": [
        SchemaField("ordem_producao_id", "STRING"),
        SchemaField("lote_id", "STRING"),
        SchemaField("produto_id", "STRING"),
        SchemaField("linha_id", "STRING"),
        SchemaField("maquina_id", "STRING"),
        SchemaField("turno_id", "STRING"),
        SchemaField("inicio", "STRING"),
        SchemaField("ciclo_minuto_nominal", "STRING"),
        SchemaField("duracao_horas", "STRING"),
        SchemaField("temperatura_media_c", "STRING"),
        SchemaField("vibracao_media_rpm", "STRING"),
        SchemaField("pressao_media_bar", "STRING"),
        SchemaField("quantidade_planejada", "STRING"),
        SchemaField("quantidade_produzida", "STRING"),
        SchemaField("quantidade_refugada", "STRING"),
    ],
    "raw_qualidade": [
        SchemaField("teste_id", "STRING"),
        SchemaField("lote_id", "STRING"),
        SchemaField("produto_id", "STRING"),
        SchemaField("data_teste", "STRING"),
        SchemaField("tensao_medida_v", "STRING"),
        SchemaField("resistencia_interna_mohm", "STRING"),
        SchemaField("capacidade_ah_teste", "STRING"),
        SchemaField("defeito_id", "STRING"),
        SchemaField("aprovado", "STRING"),
    ],
    "raw_compras": [
        SchemaField("compra_id", "STRING"),
        SchemaField("fornecedor_id", "STRING"),
        SchemaField("materia_prima_id", "STRING"),
        SchemaField("data_compra", "STRING"),
        SchemaField("quantidade_comprada", "STRING"),
        SchemaField("custo_unitario", "STRING"),
        SchemaField("custo_total", "STRING"),
    ],
    "raw_map_lote_compras": [
        SchemaField("lote_id", "STRING"),
        SchemaField("compra_id", "STRING"),
    ],
    "raw_vendas": [
        SchemaField("venda_id", "STRING"),
        SchemaField("ano_mes_id", "STRING"),
        SchemaField("cliente_id", "STRING"),
        SchemaField("produto_id", "STRING"),
        SchemaField("ordem_producao_id", "STRING"),
        SchemaField("lote_id", "STRING"),  # pedido do PBI
        SchemaField("data_venda", "STRING"),
        SchemaField("quantidade_vendida", "STRING"),
        SchemaField("valor_total_venda", "STRING"),
    ],
    "raw_garantia": [
        SchemaField("garantia_id", "STRING"),
        SchemaField("cliente_id", "STRING"),
        SchemaField("produto_id", "STRING"),
        SchemaField("lote_id", "STRING"),
        SchemaField("data_reclamacao", "STRING"),
        SchemaField("dias_pos_venda", "STRING"),
        SchemaField("defeito_id", "STRING"),
        SchemaField("status", "STRING"),
        SchemaField("tempo_resposta_dias", "STRING"),
        SchemaField("custo_garantia", "STRING"),
    ],
    "raw_manutencao": [
        SchemaField("evento_manutencao_id", "STRING"),
        SchemaField("maquina_id", "STRING"),
        SchemaField("linha_id", "STRING"),
        SchemaField("tipo_manutencao_id", "STRING"),
        SchemaField("inicio", "STRING"),
        SchemaField("fim", "STRING"),
        SchemaField("duracao_min", "STRING"),
        SchemaField("criticidade", "STRING"),
    ],
    "monitoramento_alertas": [
        SchemaField("alerta_id", "STRING"),
        SchemaField("data_ocorrencia", "TIMESTAMP"),
        SchemaField("nivel", "STRING"),
        SchemaField("maquina_id", "STRING"),
        SchemaField("mensagem", "STRING"),
        SchemaField("valor_medido", "FLOAT64"),
    ],
}

# -------------------------
# STATIC DIMENSIONS
# -------------------------
DADOS_ESTATICOS = {
    "raw_linha": [
        {"linha_id": "L01", "descricao": "Linha de Montagem Automotiva", "turnos_operacionais": "3"},
        {"linha_id": "L02", "descricao": "Linha de Injeção Plástica", "turnos_operacionais": "3"},
        {"linha_id": "L03", "descricao": "Linha de Envase Químico", "turnos_operacionais": "3"},
        {"linha_id": "L04", "descricao": "Linha de Pesados", "turnos_operacionais": "2"},
        {"linha_id": "L05", "descricao": "Linha de Testes", "turnos_operacionais": "1"},
    ],
    "raw_turno": [
        {"turno_id": "T1", "janela": "06:00 - 14:00", "coef_performance": "1.0"},
        {"turno_id": "T2", "janela": "14:00 - 22:00", "coef_performance": "0.98"},
        {"turno_id": "T3", "janela": "22:00 - 06:00", "coef_performance": "0.95"},
    ],
    "raw_tipo_manut": [
        {"tipo_manutencao_id": "TM01", "descricao": "Preventiva", "criticidade_padrao": "Baixa"},
        {"tipo_manutencao_id": "TM02", "descricao": "Corretiva", "criticidade_padrao": "Alta"},
        {"tipo_manutencao_id": "TM03", "descricao": "Preditiva", "criticidade_padrao": "Média"},
    ],
    "raw_defeito": [
        {"defeito_id": "D00", "descricao": "Sem Defeito", "gravidade": "Nenhuma"},
        {"defeito_id": "D01", "descricao": "Vazamento de Ácido", "gravidade": "Alta"},
        {"defeito_id": "D02", "descricao": "Baixa Tensão", "gravidade": "Média"},
        {"defeito_id": "D03", "descricao": "Caixa Rachada", "gravidade": "Média"},
        {"defeito_id": "D04", "descricao": "Sobreaquecimento (Curto)", "gravidade": "Crítica"},
        {"defeito_id": "D05", "descricao": "Terminal Oxidado", "gravidade": "Baixa"},
    ],
    "raw_materia_prima": [
        {"materia_prima_id": "MP001", "nome_material": "Chumbo"},
        {"materia_prima_id": "MP002", "nome_material": "Ácido Sulfúrico"},
        {"materia_prima_id": "MP003", "nome_material": "Polipropileno"},
        {"materia_prima_id": "MP004", "nome_material": "Separadores"},
        {"materia_prima_id": "MP005", "nome_material": "Eletrólito"},
    ],
    "raw_fornecedor": [
        {"fornecedor_id": "F001", "categoria": "Chumbo/Metais", "leadtime_dias": "10", "qualificacao": "A",
         "data_cadastro": "2022-01-10", "data_ultima_avaliacao": "2025-01-01", "descricao": "Fornecedor Metal 1"},
        {"fornecedor_id": "F002", "categoria": "Químicos/Ácidos", "leadtime_dias": "7", "qualificacao": "A",
         "data_cadastro": "2022-02-15", "data_ultima_avaliacao": "2025-01-01", "descricao": "Indústria Química 2"},
        {"fornecedor_id": "F003", "categoria": "Plásticos/Polímeros", "leadtime_dias": "14", "qualificacao": "B",
         "data_cadastro": "2022-03-20", "data_ultima_avaliacao": "2025-01-01", "descricao": "PlastCorp 3"},
        {"fornecedor_id": "F004", "categoria": "Componentes Elétricos", "leadtime_dias": "20", "qualificacao": "A",
         "data_cadastro": "2022-05-05", "data_ultima_avaliacao": "2025-01-01", "descricao": "ElectroParts 4"},
    ],
    "raw_produto": [
        {"produto_id": "BAT001", "modelo": "AV-50Ah", "tensao_v": "12", "capacidade_ah": "50",
         "linha_segmento": "Reposição", "data_lancamento": "2022-02-01", "data_descontinuacao": ""},
        {"produto_id": "BAT002", "modelo": "AV-60Ah", "tensao_v": "12", "capacidade_ah": "60",
         "linha_segmento": "Reposição", "data_lancamento": "2022-06-15", "data_descontinuacao": ""},
        {"produto_id": "BAT003", "modelo": "AV-70Ah", "tensao_v": "24", "capacidade_ah": "70",
         "linha_segmento": "Montadora", "data_lancamento": "2023-01-10", "data_descontinuacao": ""},
        {"produto_id": "BAT004", "modelo": "AV-90Ah", "tensao_v": "12", "capacidade_ah": "90",
         "linha_segmento": "Reposição", "data_lancamento": "2023-08-20", "data_descontinuacao": ""},
        {"produto_id": "BAT005", "modelo": "AV-100Ah", "tensao_v": "12", "capacidade_ah": "100",
         "linha_segmento": "Montadora", "data_lancamento": "2024-05-01", "data_descontinuacao": ""},
    ],
}

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}
MESES_ABR = {1: "jan", 2: "fev", 3: "mar", 4: "abr", 5: "mai", 6: "jun", 7: "jul", 8: "ago", 9: "set", 10: "out", 11: "nov", 12: "dez"}

ESTADOS_BR = ["SP", "RJ", "MG", "RS", "PE", "BA", "PR", "SC"]

# -------------------------
# CLIENTS
# -------------------------
def bq():
    return bigquery.Client(project=PROJECT_ID)

def gcs():
    return storage.Client(project=PROJECT_ID)

# -------------------------
# BQ SETUP
# -------------------------
def setup_bq(client: bigquery.Client) -> None:
    ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(ds_ref)
    except NotFound:
        client.create_dataset(ds_ref)

    for table, schema in SCHEMAS.items():
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table}"
        try:
            client.get_table(table_ref)
        except NotFound:
            client.create_table(bigquery.Table(table_ref, schema=schema))

# -------------------------
# STATE (com migração)
# -------------------------
def default_state():
    return {
        "seed": 42,
        "static": False,
        "cnt_op": 0,
        "cnt_lote": 0,
        "cnt_venda": 0,
        "cnt_compra": 0,
        "cnt_cliente": 0,
        "cnt_garantia": 0,
        "cnt_manut": 0,
        "clientes": [],
        "clientes_dim": {},   # <-- mantém atributos do cliente para updates append-only
        "fleet": None,
    }

def load_state(sc: storage.Client) -> dict:
    bucket = sc.bucket(BUCKET_NAME)
    blob = bucket.blob(STATE_FILE)

    base = default_state()

    if not blob.exists():
        return base

    state = json.loads(blob.download_as_text())

    # MIGRAÇÃO: garante chaves novas
    for k, v in base.items():
        if k not in state:
            state[k] = v

    # saneamento
    if state.get("clientes") is None:
        state["clientes"] = []
    if state.get("clientes_dim") is None:
        state["clientes_dim"] = {}
    if state.get("fleet") == []:
        state["fleet"] = None

    # garante inteiros
    for k in ["cnt_op","cnt_lote","cnt_venda","cnt_compra","cnt_cliente","cnt_garantia","cnt_manut","seed"]:
        try:
            state[k] = int(state.get(k, 0))
        except Exception:
            state[k] = 0

    return state

def save_state(sc: storage.Client, state: dict) -> None:
    sc.bucket(BUCKET_NAME).blob(STATE_FILE).upload_from_string(
        json.dumps(state, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

# -------------------------
# HELPERS
# -------------------------
def to_str(v):
    if v is None:
        return ""
    return str(v)

def turno(dt: datetime) -> str:
    h = dt.hour
    if 6 <= h < 14:
        return "T1"
    if 14 <= h < 22:
        return "T2"
    return "T3"

def hot_layer(dt: datetime) -> bool:
    # só carrega pro BQ dados do ano atual (economia)
    return dt.year == datetime.now(TZ_BR).year

def write_gcs_jsonl(sc: storage.Client, table: str, rows: list[dict], run_id: str, dt: datetime) -> str:
    if not rows:
        return ""

    prefix = f"bronze/{table}/dt={dt.date().isoformat()}/hr={dt.hour:02d}/run={run_id}"
    filename = f"part-{uuid.uuid4().hex}.jsonl"
    blob_name = f"{prefix}/{filename}"

    buf = io.StringIO()
    ingest_ts = datetime.now(TZ_BR).isoformat()

    for r in rows:
        rr = dict(r)
        rr["_run_id"] = run_id
        rr["_ingested_at"] = ingest_ts
        buf.write(json.dumps(rr, ensure_ascii=False))
        buf.write("\n")

    bucket = sc.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(buf.getvalue(), content_type="application/json")

    return f"gs://{BUCKET_NAME}/{blob_name}"

def load_bq_from_uri(client: bigquery.Client, table: str, uri: str) -> None:
    if not uri:
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=SCHEMAS[table],
        write_disposition="WRITE_APPEND",
        ignore_unknown_values=True,  # ignora _run_id / _ingested_at
    )

    client.load_table_from_uri(
        uri,
        f"{PROJECT_ID}.{DATASET_ID}.{table}",
        job_config=job_config,
    ).result()

# -------------------------
# STATIC BUILDERS
# -------------------------
def build_dim_tempo(ano_ini=2022, ano_fim=2027):
    out = []
    for ano in range(ano_ini, ano_fim + 1):
        for mes in range(1, 13):
            out.append({
                "ano_mes_id": f"{ano}-{mes:02d}",
                "ano": str(ano),
                "mes": f"{mes:02d}",
                "nome_mes": MESES_PT[mes],
                "trimestre": str((mes - 1) // 3 + 1),
                "ano_mes_label": f"{MESES_ABR[mes]}/{str(ano)[2:]}",
            })
    return out

def build_metas_vendas(dim_tempo):
    metas = []
    for i, r in enumerate(dim_tempo, 1):
        metas.append({
            "meta_id": f"M{i:04d}",
            "ano_mes_id": r["ano_mes_id"],
            "meta_quantidade": "2000",
            "meta_valor": "500000",
        })
    return metas

# -------------------------
# FLEET / PHYSICS (ML-ready)
# -------------------------
def gen_fleet(state: dict) -> list[dict]:
    if state.get("fleet"):
        return state["fleet"]

    fleet = []
    for i in range(1, 21):
        fleet.append({
            "maquina_id": f"M{i:03d}",
            "tipo": random.choice(["Montadora", "Injetora", "Envasadora", "Robo", "Tester"]),
            "fabricante": random.choice(["Siemens", "Bosch", "ABB", "Kuka", "Engel"]),
            "ano": str(random.choice([2019, 2020, 2021, 2022, 2023, 2024])),
            "linha_id": random.choice(["L01", "L02", "L03", "L04", "L05"]),
        })

    state["fleet"] = fleet
    return fleet

def desgaste_maquina(ano_fab: str, dt: datetime):
    try:
        ano = int(ano_fab)
    except Exception:
        ano = 2021

    idade = max(0, dt.year - ano)
    fator = 1.0 + idade * 0.03

    temp = float(np.random.normal(65.0 * fator, 3.0))
    vib = float(np.random.normal(1200.0 * fator, 150.0))
    perf = max(0.85, 1.0 - idade * 0.01)

    return round(temp, 1), round(vib, 0), perf

def calc_oee(dur_h: float, ciclo_min: float, perf: float):
    minutos = dur_h * 60.0
    cap_max = int(minutos / ciclo_min)
    qtd_plan = int(cap_max * 0.95)

    eff = float(np.clip(np.random.normal(perf, 0.05), 0.0, 1.0))
    qtd_prod = int(qtd_plan * eff)

    taxa_ref = 0.01 + (1.0 - perf)
    qtd_ref = int(qtd_prod * random.uniform(0.0, taxa_ref))

    return qtd_plan, qtd_prod, qtd_ref

# -------------------------
# GENERATORS (negócio)
# -------------------------
def gen_clientes(dt: datetime, state: dict):
    rows = []

    # Cliente fundador
    if not state["clientes"]:
        state["cnt_cliente"] += 1
        cid = f"C{state['cnt_cliente']:04d}"
        state["clientes"].append(cid)

        rec = {
            "cliente_id": cid,
            "tipo_cliente": "Distribuidor",
            "cidade": "SP",
            "tipo_plano": "Standard",
            "data_cadastro": "2022-01-01",
            "data_ultima_compra": "",
        }
        state["clientes_dim"][cid] = rec
        rows.append(rec)

    # Entrada de clientes ao longo do tempo
    prob = 0.20 if dt.year <= 2023 else 0.08 if dt.year == 2024 else 0.04
    if random.random() < prob:
        state["cnt_cliente"] += 1
        cid = f"C{state['cnt_cliente']:04d}"
        state["clientes"].append(cid)

        rec = {
            "cliente_id": cid,
            "tipo_cliente": random.choice(["Distribuidor", "Autopeças", "Montadora"]),
            "cidade": random.choice(ESTADOS_BR),
            "tipo_plano": random.choice(["Básico", "Standard", "Premium"]),
            "data_cadastro": dt.date().isoformat(),
            "data_ultima_compra": "",
        }
        state["clientes_dim"][cid] = rec
        rows.append(rec)

    return rows

def gen_compras(dt: datetime, state: dict):
    rows = []

    prob = 0.55 if dt.year <= 2023 else 0.45 if dt.year == 2024 else 0.35
    if random.random() >= prob:
        return rows

    qtd = random.randint(1, 4)
    fornecedores = [f["fornecedor_id"] for f in DADOS_ESTATICOS["raw_fornecedor"]]
    materias = [m["materia_prima_id"] for m in DADOS_ESTATICOS["raw_materia_prima"]]

    for _ in range(qtd):
        state["cnt_compra"] += 1
        compra_id = f"CP{state['cnt_compra']:06d}"
        qtd_comprada = random.randint(500, 2000)
        custo_unit = round(random.uniform(20, 100), 2)
        custo_total = round(qtd_comprada * custo_unit, 2)

        rows.append({
            "compra_id": compra_id,
            "fornecedor_id": random.choice(fornecedores),
            "materia_prima_id": random.choice(materias),
            "data_compra": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "quantidade_comprada": to_str(qtd_comprada),
            "custo_unitario": to_str(custo_unit),
            "custo_total": to_str(custo_total),
        })

    return rows

def gen_producao(dt: datetime, state: dict, fleet: list[dict]):
    prod = []
    lotes = []
    qual = []
    alerts = []

    produtos = [p["produto_id"] for p in DADOS_ESTATICOS["raw_produto"]]

    for m in fleet:
        state["cnt_op"] += 1
        state["cnt_lote"] += 1

        op_id = f"OP{state['cnt_op']:07d}"
        lote_id = f"Lote{state['cnt_lote']:07d}"

        temp, vib, perf = desgaste_maquina(m["ano"], dt)

        dur = round(random.uniform(0.80, 1.00), 2)
        fim = dt + timedelta(hours=dur)

        pid = random.choice(produtos)
        ciclo_nom = 0.5  # 30s
        q_plan, q_prod, q_ref = calc_oee(dur, ciclo_nom, perf)

        lotes.append({
            "lote_id": lote_id,
            "produto_id": pid,
            "linha_id": m["linha_id"],
            "maquina_id": m["maquina_id"],
            "inicio_producao": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "fim_producao": fim.strftime("%Y-%m-%d %H:%M:%S"),
            "duracao_horas": to_str(dur),
        })

        prod.append({
            "ordem_producao_id": op_id,
            "lote_id": lote_id,
            "produto_id": pid,
            "linha_id": m["linha_id"],
            "maquina_id": m["maquina_id"],
            "turno_id": turno(dt),
            "inicio": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "ciclo_minuto_nominal": to_str(ciclo_nom),
            "duracao_horas": to_str(dur),
            "temperatura_media_c": to_str(temp),
            "vibracao_media_rpm": to_str(vib),
            "pressao_media_bar": to_str(round(random.uniform(6, 8), 1)),
            "quantidade_planejada": to_str(q_plan),
            "quantidade_produzida": to_str(q_prod),
            "quantidade_refugada": to_str(q_ref),
        })

        qual.append({
            "teste_id": f"T{state['cnt_lote']:07d}",
            "lote_id": lote_id,
            "produto_id": pid,
            "data_teste": (fim + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
            "tensao_medida_v": to_str(round(random.normalvariate(12.6, 0.2), 2)),
            "resistencia_interna_mohm": "6.0",
            "capacidade_ah_teste": "60.0",
            "defeito_id": "D00",
            "aprovado": "1" if q_prod > 0 else "0",
        })

        # alerta (ML-ready)
        if temp > 100.0 or vib > 2000.0:
            alerts.append({
                "alerta_id": f"ALT-{uuid.uuid4().hex[:10]}",
                # BQ TIMESTAMP aceita string RFC3339/ISO
                "data_ocorrencia": dt.isoformat(),
                "nivel": "CRITICO",
                "maquina_id": m["maquina_id"],
                "mensagem": "Anomalia Detectada",
                "valor_medido": float(temp),
            })

    return prod, lotes, qual, alerts

def gen_map_lote_compras(lotes: list[dict], compras: list[dict]):
    rows = []
    if not lotes or not compras:
        return rows

    compra_ids = [c["compra_id"] for c in compras]
    for l in lotes:
        k = random.randint(0, min(3, len(compra_ids)))
        if k == 0:
            continue
        escolhidas = random.sample(compra_ids, k=k)
        for cid in escolhidas:
            rows.append({"lote_id": l["lote_id"], "compra_id": cid})
    return rows

def gen_vendas(dt: datetime, state: dict, lotes: list[dict], producao: list[dict]):
    """
    Retorna:
      - vendas (raw_vendas): inclui ordem_producao_id preenchido via mapeamento lote->OP
      - cliente_updates (raw_cliente): append-only com data_ultima_compra preenchida (auditável)
    """
    vendas = []
    cliente_updates = []

    if not lotes or not state["clientes"] or not producao:
        return vendas, cliente_updates

    lote_to_op = {p["lote_id"]: p["ordem_producao_id"] for p in producao}

    # vende ~20% dos lotes, mínimo 1
    n = max(1, len(lotes) // 5)
    amostra = lotes[:n]

    updated_clients = set()

    for l in amostra:
        state["cnt_venda"] += 1
        cid = random.choice(state["clientes"])
        op_id = lote_to_op.get(l["lote_id"], "")

        vendas.append({
            "venda_id": f"V{state['cnt_venda']:07d}",
            "ano_mes_id": dt.strftime("%Y-%m"),
            "cliente_id": cid,
            "produto_id": l["produto_id"],
            "ordem_producao_id": op_id,
            "lote_id": l["lote_id"],
            "data_venda": dt.date().isoformat(),
            "quantidade_vendida": to_str(random.randint(20, 120)),
            "valor_total_venda": to_str(round(random.uniform(5000, 15000), 2)),
        })

        # append-only: cria nova linha de cliente com data_ultima_compra atualizada
        if cid in state["clientes_dim"] and cid not in updated_clients:
            state["clientes_dim"][cid]["data_ultima_compra"] = dt.date().isoformat()
            cliente_updates.append(dict(state["clientes_dim"][cid]))
            updated_clients.add(cid)

    return vendas, cliente_updates

def gen_garantia(dt: datetime, state: dict, vendas: list[dict]):
    """
    Gera garantias a partir das vendas do período.
    Observação: data_reclamacao fica dt + [1..90] dias (simula atraso real).
    """
    rows = []
    if not vendas:
        return rows

    defeitos = [d["defeito_id"] for d in DADOS_ESTATICOS["raw_defeito"]]

    for v in vendas:
        if random.random() < 0.01:
            state["cnt_garantia"] += 1
            dias = random.randint(1, 90)
            status = random.choice(["Aprovada", "Negada", "Negada - Mau Uso"])
            custo = round(random.uniform(200, 3000), 2) if "Aprovada" in status else 0.0

            rows.append({
                "garantia_id": f"W{state['cnt_garantia']:07d}",
                "cliente_id": v["cliente_id"],
                "produto_id": v["produto_id"],
                "lote_id": v["lote_id"],
                "data_reclamacao": (dt + timedelta(days=dias)).strftime("%Y-%m-%d %H:%M:%S"),
                "dias_pos_venda": to_str(dias),
                "defeito_id": random.choice(defeitos),
                "status": status,
                "tempo_resposta_dias": to_str(random.randint(1, 15)),
                "custo_garantia": to_str(custo),
            })

    return rows

def gen_manutencao(dt: datetime, state: dict, fleet: list[dict]):
    rows = []
    if random.random() < 0.05:
        state["cnt_manut"] += 1
        m = random.choice(fleet)
        fim = dt + timedelta(hours=2)

        rows.append({
            "evento_manutencao_id": f"EVM{state['cnt_manut']:07d}",
            "maquina_id": m["maquina_id"],
            "linha_id": m["linha_id"],
            "tipo_manutencao_id": random.choice(["TM01", "TM02", "TM03"]),
            "inicio": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "fim": fim.strftime("%Y-%m-%d %H:%M:%S"),
            "duracao_min": "120",
            "criticidade": random.choice(["Baixa", "Média", "Alta"]),
        })

    return rows

# -------------------------
# RUN HELPERS (persist)
# -------------------------
def persist_table(sc, bq_client, dt, run_id, table, rows):
    uri = write_gcs_jsonl(sc, table, rows, run_id, dt)
    if uri and hot_layer(dt):
        load_bq_from_uri(bq_client, table, uri)

# -------------------------
# MAIN HANDLER
# -------------------------
@functions_framework.http
def executar_simulacao(request):
    args = request.args or {}

    mode = args.get("mode", "incremental").lower()
    start = args.get("start")  # YYYY-MM-DD
    end = args.get("end")      # YYYY-MM-DD

    run_id = str(uuid.uuid4())
    sc = gcs()
    state = load_state(sc)

    # seeds determinísticas por state (reprodutível)
    random.seed(state["seed"])
    np.random.seed(state["seed"])

    bq_client = bq()
    setup_bq(bq_client)

    # sempre garantir fleet e static 1x
    fleet = gen_fleet(state)

    # static 1x (vai para GCS; no BQ só ano atual)
    if not state.get("static", False):
        dim_tempo = build_dim_tempo(2022, 2027)
        metas = build_metas_vendas(dim_tempo)

        static_payloads = {
            "raw_tempo": dim_tempo,
            "raw_metas_vendas": metas,
            "raw_linha": DADOS_ESTATICOS["raw_linha"],
            "raw_turno": DADOS_ESTATICOS["raw_turno"],
            "raw_tipo_manut": DADOS_ESTATICOS["raw_tipo_manut"],
            "raw_defeito": DADOS_ESTATICOS["raw_defeito"],
            "raw_materia_prima": DADOS_ESTATICOS["raw_materia_prima"],
            "raw_fornecedor": DADOS_ESTATICOS["raw_fornecedor"],
            "raw_produto": DADOS_ESTATICOS["raw_produto"],
            "raw_maquina": fleet,
        }

        now = datetime.now(TZ_BR)
        for t, rows in static_payloads.items():
            persist_table(sc, bq_client, now, run_id, t, rows)

        state["static"] = True
        save_state(sc, state)

    # -------------------------
    # BACKFILL (somente GCS)
    # -------------------------
    if mode == "backfill":
        if not start or not end:
            return "ERRO: backfill requer ?mode=backfill&start=YYYY-MM-DD&end=YYYY-MM-DD", 400

        try:
            dt1 = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=TZ_BR)
            dt2 = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=TZ_BR)
        except Exception:
            return "ERRO: formato de data inválido. Use YYYY-MM-DD", 400

        cur = dt1
        counts = {k: 0 for k in ["cli","cli_upd","comp","map","prod","lote","qual","vend","gar","man","alt"]}

        while cur <= dt2:
            cli = gen_clientes(cur, state)
            comp = gen_compras(cur, state)

            prod, lotes, qual, alt = gen_producao(cur, state, fleet)
            mapa = gen_map_lote_compras(lotes, comp)

            vend, cli_upd = gen_vendas(cur, state, lotes, prod)
            gar = gen_garantia(cur, state, vend)
            man = gen_manutencao(cur, state, fleet)

            # grava tudo no lake (GCS)
            for t, rows in [
                ("raw_cliente", cli + cli_upd),
                ("raw_compras", comp),
                ("raw_map_lote_compras", mapa),
                ("raw_producao", prod),
                ("raw_lote", lotes),
                ("raw_qualidade", qual),
                ("raw_vendas", vend),
                ("raw_garantia", gar),
                ("raw_manutencao", man),
                ("monitoramento_alertas", alt),
            ]:
                write_gcs_jsonl(sc, t, rows, run_id, cur)

            counts["cli"] += len(cli)
            counts["cli_upd"] += len(cli_upd)
            counts["comp"] += len(comp)
            counts["map"] += len(mapa)
            counts["prod"] += len(prod)
            counts["lote"] += len(lotes)
            counts["qual"] += len(qual)
            counts["vend"] += len(vend)
            counts["gar"] += len(gar)
            counts["man"] += len(man)
            counts["alt"] += len(alt)

            cur += timedelta(days=1)

        # muda seed pra próxima execução não repetir dados
        state["seed"] = (state["seed"] + 1) % 1_000_000_000
        save_state(sc, state)
        return f"OK backfill run_id={run_id} | {counts}", 200

    # -------------------------
    # INCREMENTAL (GCS + BQ ano atual)
    # -------------------------
    cur = datetime.now(TZ_BR)
    counts = {k: 0 for k in ["cli","cli_upd","comp","map","prod","lote","qual","vend","gar","man","alt"]}

    for _ in range(HORAS_POR_LOTE):
        cli = gen_clientes(cur, state)
        comp = gen_compras(cur, state)

        prod, lotes, qual, alt = gen_producao(cur, state, fleet)
        mapa = gen_map_lote_compras(lotes, comp)

        vend, cli_upd = gen_vendas(cur, state, lotes, prod)
        gar = gen_garantia(cur, state, vend)
        man = gen_manutencao(cur, state, fleet)

        # persiste no GCS sempre; carrega no BQ só ano atual
        persist_table(sc, bq_client, cur, run_id, "raw_cliente", cli + cli_upd)
        persist_table(sc, bq_client, cur, run_id, "raw_compras", comp)
        persist_table(sc, bq_client, cur, run_id, "raw_map_lote_compras", mapa)
        persist_table(sc, bq_client, cur, run_id, "raw_producao", prod)
        persist_table(sc, bq_client, cur, run_id, "raw_lote", lotes)
        persist_table(sc, bq_client, cur, run_id, "raw_qualidade", qual)
        persist_table(sc, bq_client, cur, run_id, "raw_vendas", vend)
        persist_table(sc, bq_client, cur, run_id, "raw_garantia", gar)
        persist_table(sc, bq_client, cur, run_id, "raw_manutencao", man)
        persist_table(sc, bq_client, cur, run_id, "monitoramento_alertas", alt)

        counts["cli"] += len(cli)
        counts["cli_upd"] += len(cli_upd)
        counts["comp"] += len(comp)
        counts["map"] += len(mapa)
        counts["prod"] += len(prod)
        counts["lote"] += len(lotes)
        counts["qual"] += len(qual)
        counts["vend"] += len(vend)
        counts["gar"] += len(gar)
        counts["man"] += len(man)
        counts["alt"] += len(alt)

        cur += timedelta(hours=1)

    # muda seed pra próxima execução não repetir dados
    state["seed"] = (state["seed"] + 1) % 1_000_000_000
    save_state(sc, state)
    return f"OK incremental run_id={run_id} | {counts}", 200
