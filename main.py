# =========================
# AUTOVOLT LAKEHOUSE (BRONZE) - Cloud Run HTTP
# - Lake-first (GCS JSONL)
# - BigQuery "hot layer" só ano atual (economia/free)
# - Histórico (backfill) grava só no GCS
# - Estado em state/state.json (migração automática)
# =========================

import functions_framework
import warnings
warnings.simplefilter("ignore", category=FutureWarning)

import io
import json
import uuid
import random
import logging
from datetime import datetime, timedelta, timezone, date
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

class CompactJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat() 
        if isinstance(obj, np.integer):
            return int(obj) 
        if isinstance(obj, np.floating):
            return float(obj) 
        return super().default(obj)
        
# -------------------------
# CONFIG
# -------------------------
PROJECT_ID = "autovolt-analytics-479417"
DATASET_ID = "autovolt_bronze"
BUCKET_NAME = "bucket_ingestao"

TZ_BR = timezone(timedelta(hours=-3))  # America/Recife
HORAS_POR_LOTE = 1

STATE_FILE = "state/state.json"

# -------------------------
# CLIENT SEEDING / BALANCE
# -------------------------
SEED_CLIENTES_QTD = 800          # ajuste (500-1000 ok)
ULTIMOS_CLIENTES_JANELA = 200    # janela anti-monopólio
PESO_INTENSIDADE = 2.2          # >2 favorece antigos; menor = mais uniforme
PESO_PISO = 0.25                # chance mínima pros mais novos

# -------------------------
# TAXAS DE FALHA (RAQUEL)
# -------------------------
PROB_FALHA_DIARIA = 0.12        # Aumentado para gerar mais instabilidades
CHANCE_GARANTIA_LOTE_RUIM = 5.0 # Multiplicador de chance de acionar garantia se temp for alta

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# -------------------------
# SCHEMAS (Bronze)
# -------------------------
SCHEMAS = {
    # NOVO SCHEMA DE ESTOQUE
    "raw_estoque_movimento": [
        SchemaField("movimento_id", "STRING"),
        SchemaField("produto_id", "STRING"),
        SchemaField("lote_id", "STRING"),
        SchemaField("tipo_movimento", "STRING"), # ENTRADA ou SAIDA
        SchemaField("quantidade", "INTEGER"),
        SchemaField("data_movimento", "TIMESTAMP"),
    ],
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
        SchemaField("lote_id", "STRING"),
        SchemaField("data_venda", "STRING"),
        SchemaField("quantidade_vendida", "STRING"),
        SchemaField("valor_total_venda", "STRING"),
    ],
    "raw_garantia": [
        SchemaField("garantia_id", "STRING"),
        SchemaField("venda_id", "STRING"), # ADICIONADO PARA SINCRONISMO
        SchemaField("cliente_id", "STRING"),
        SchemaField("produto_id", "STRING"),
        SchemaField("lote_id", "STRING"),
        SchemaField("data_reclamacao", "STRING"),
        SchemaField("data_limite_sla", "STRING"), # ADICIONADO (SLA)
        SchemaField("status_sla", "STRING"),      # ADICIONADO (SLA)
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
        {"fornecedor_id": "F001", "categoria": "Chumbo/Metais", "leadtime_dias": "10", "qualificacao": "A", "data_cadastro": "2022-01-10", "data_ultima_avaliacao": "2025-01-01", "descricao": "Fornecedor Metal 1"},
        {"fornecedor_id": "F002", "categoria": "Químicos/Ácidos", "leadtime_dias": "7", "qualificacao": "A", "data_cadastro": "2022-02-15", "data_ultima_avaliacao": "2025-01-01", "descricao": "Indústria Química 2"},
        {"fornecedor_id": "F003", "categoria": "Plásticos/Polímeros", "leadtime_dias": "14", "qualificacao": "B", "data_cadastro": "2022-03-20", "data_ultima_avaliacao": "2025-01-01", "descricao": "PlastCorp 3"},
        {"fornecedor_id": "F004", "categoria": "Componentes Elétricos", "leadtime_dias": "20", "qualificacao": "A", "data_cadastro": "2022-05-05", "data_ultima_avaliacao": "2025-01-01", "descricao": "ElectroParts 4"},
    ],
    "raw_produto": [
        {"produto_id": "BAT001", "modelo": "AV-50Ah", "tensao_v": "12", "capacidade_ah": "50", "linha_segmento": "Reposição", "data_lancamento": "2022-02-01", "data_descontinuacao": ""},
        {"produto_id": "BAT002", "modelo": "AV-60Ah", "tensao_v": "12", "capacidade_ah": "60", "linha_segmento": "Reposição", "data_lancamento": "2022-06-15", "data_descontinuacao": ""},
        {"produto_id": "BAT003", "modelo": "AV-70Ah", "tensao_v": "24", "capacidade_ah": "70", "linha_segmento": "Montadora", "data_lancamento": "2023-01-10", "data_descontinuacao": ""},
        {"produto_id": "BAT004", "modelo": "AV-90Ah", "tensao_v": "12", "capacidade_ah": "90", "linha_segmento": "Reposição", "data_lancamento": "2023-08-20", "data_descontinuacao": ""},
        {"produto_id": "BAT005", "modelo": "AV-100Ah", "tensao_v": "12", "capacidade_ah": "100", "linha_segmento": "Montadora", "data_lancamento": "2024-05-01", "data_descontinuacao": ""},
    ],
}

MESES_PT = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}
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
# STATE (com migração e buffer de sincronismo)
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
        "cnt_movimento": 0, # Novo para estoque
        "clientes": [],
        "fleet": None,
        "clientes_seeded": False,
        "ultimos_clientes": [],
        "buffer_estoque": [],      # NOVO: Garante que vendas só puxam lotes que existem
        "buffer_vendas": [],       # NOVO: Garante que garantias só puxam vendas que existem
    }

def load_state(sc: storage.Client) -> dict:
    bucket = sc.bucket(BUCKET_NAME)
    blob = bucket.blob(STATE_FILE)
    base = default_state()

    if not blob.exists():
        return base

    state = json.loads(blob.download_as_text())

    for k, v in base.items():
        if k not in state:
            state[k] = v

    if state.get("clientes") is None: state["clientes"] = []
    if state.get("fleet") == []: state["fleet"] = None
    if state.get("ultimos_clientes") is None: state["ultimos_clientes"] = []
    if not isinstance(state["ultimos_clientes"], list): state["ultimos_clientes"] = []
    
    if "buffer_estoque" not in state: state["buffer_estoque"] = []
    if "buffer_vendas" not in state: state["buffer_vendas"] = []

    for k in ["cnt_op", "cnt_lote", "cnt_venda", "cnt_compra", "cnt_cliente", "cnt_garantia", "cnt_manut", "cnt_movimento"]:
        try: state[k] = int(state.get(k, 0))
        except Exception: state[k] = 0

    return state

def save_state(sc: storage.Client, state: dict) -> None:
    # Evita que o JSON de estado fique gigantesco durante o backfill
    state["buffer_estoque"] = state["buffer_estoque"][-2000:]
    state["buffer_vendas"] = state["buffer_vendas"][-2000:]
    
    sc.bucket(BUCKET_NAME).blob(STATE_FILE).upload_from_string(
        json.dumps(state, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

# -------------------------
# HELPERS
# -------------------------
def to_str(v):
    if v is None: return ""
    return str(v)

def turno(dt: datetime) -> str:
    h = dt.hour
    if 6 <= h < 14: return "T1"
    if 14 <= h < 22: return "T2"
    return "T3"

def hot_layer(dt: datetime) -> bool:
    return dt.year == datetime.now(TZ_BR).year

def write_gcs_jsonl(sc: storage.Client, table: str, rows: list[dict], run_id: str, dt: datetime) -> str:
    if not rows: return ""
    prefix = f"bronze/{table}/dt={dt.date().isoformat()}/hr={dt.hour:02d}/run={run_id}"
    filename = f"part-{uuid.uuid4().hex}.jsonl"
    blob_name = f"{prefix}/{filename}"

    buf = io.StringIO()
    ingest_ts = datetime.now(TZ_BR).isoformat()

    for r in rows:
        rr = dict(r)
        rr["_run_id"] = run_id
        rr["_ingested_at"] = ingest_ts
        buf.write(json.dumps(rr, ensure_ascii=False, cls=CompactJSONEncoder))
        buf.write("\n")

    bucket = sc.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(buf.getvalue(), content_type="application/json")

    return f"gs://{BUCKET_NAME}/{blob_name}"

def load_bq_from_uri(client: bigquery.Client, table: str, uri: str) -> None:
    if not uri: return
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=SCHEMAS[table],
        write_disposition="WRITE_APPEND",
        ignore_unknown_values=True,
    )
    client.load_table_from_uri(uri, f"{PROJECT_ID}.{DATASET_ID}.{table}", job_config=job_config).result()

def persist_table(sc, bq_client, dt, run_id, table, rows):
    uri = write_gcs_jsonl(sc, table, rows, run_id, dt)
    if uri and hot_layer(dt):
        load_bq_from_uri(bq_client, table, uri)

# -------------------------
# CLIENT SEED + BALANCE HELPERS
# -------------------------
def random_date_between(start_date: datetime, end_date: datetime) -> datetime:
    if end_date <= start_date: return start_date
    delta = end_date - start_date
    seconds = int(delta.total_seconds())
    return start_date + timedelta(seconds=random.randint(0, max(1, seconds)))

def seed_clientes_iniciais(state: dict, dt_now: datetime) -> list[dict]:
    if state.get("clientes_seeded", False) or len(state.get("clientes", [])) >= 50:
        state["clientes_seeded"] = True
        return []

    alvo = int(SEED_CLIENTES_QTD)
    rows = []
    start = datetime(2022, 1, 1, tzinfo=TZ_BR)
    end = dt_now

    for _ in range(alvo):
        state["cnt_cliente"] += 1
        cid = f"C{state['cnt_cliente']:04d}"
        state["clientes"].append(cid)
        dt_cad = random_date_between(start, end).date().isoformat()
        rows.append({
            "cliente_id": cid,
            "tipo_cliente": random.choice(["Distribuidor", "Autopeças", "Montadora"]),
            "cidade": random.choice(ESTADOS_BR),
            "tipo_plano": random.choice(["Básico", "Standard", "Premium"]),
            "data_cadastro": dt_cad,
        })

    state["clientes_seeded"] = True
    return rows

def escolher_cliente_por_idade(state: dict) -> str:
    clientes = state.get("clientes", [])
    if not clientes: return ""
    if len(clientes) < 5: return random.choice(clientes)

    n = len(clientes)
    weights = []
    for i in range(n):
        x = 1 - (i / (n - 1))
        w = PESO_PISO + (x ** PESO_INTENSIDADE)
        weights.append(w)

    ult = state.get("ultimos_clientes", [])[-ULTIMOS_CLIENTES_JANELA:]
    if ult:
        freq = {}
        for c in ult: freq[c] = freq.get(c, 0) + 1
        limite = max(3, int(0.12 * len(ult)))

        for _ in range(6):
            c = random.choices(clientes, weights=weights, k=1)
            if freq.get(c, 0) <= limite: return c

    return random.choices(clientes, weights=weights, k=1)

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
            "meta_quantidade": "1500", 
            "meta_valor": "400000",
        })
    return metas

# -------------------------
# FLEET / PHYSICS (ML-ready)
# -------------------------
def gen_fleet(state: dict) -> list[dict]:
    if state.get("fleet"): return state["fleet"]

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

def desgaste_maquina(ano_fab: str, dt: datetime, horas_para_falha: int = None):
    # [REQUISITO DA RAQUEL]: Aumento gradual em vez de pulo repentino
    try: ano = int(ano_fab)
    except Exception: ano = 2021

    idade = max(0, dt.year - ano)
    fator_base = 1.0 + idade * 0.02 
    
    incremento_falha = 1.0
    variancia_extra = 1.0
    
    if horas_para_falha is not None:
        if horas_para_falha == 0:  
            incremento_falha = 1.45  # Quebra iminente (+45%)
            variancia_extra = 3.0    
        elif horas_para_falha == 1: 
            incremento_falha = 1.30  # Sinais fortíssimos
            variancia_extra = 2.4
        elif horas_para_falha == 2: 
            incremento_falha = 1.20  # Sinais claros
            variancia_extra = 1.8
        elif horas_para_falha == 3: 
            incremento_falha = 1.10  # Primeiros sinais (+10%)
            variancia_extra = 1.3
            
    temp = float(np.random.normal(65.0 * fator_base * incremento_falha, 3.0 * variancia_extra))
    vib = float(np.random.normal(1200.0 * fator_base * incremento_falha, 150.0 * variancia_extra))
    perf = max(0.70, (1.0 - idade * 0.01) / incremento_falha)

    return round(temp, 1), round(vib, 0), perf

def calc_oee(dur_h: float, ciclo_min: float, perf: float):
    minutos = dur_h * 60.0
    cap_max = int(minutos / ciclo_min)
    qtd_plan = int(cap_max * 0.85) 

    eff = float(np.clip(np.random.normal(perf + 0.05, 0.04), 0.7, 1.1))
    qtd_prod = int(qtd_plan * eff)

    taxa_ref = 0.01 + (1.0 - perf)
    qtd_ref = int(qtd_prod * random.uniform(0.0, taxa_ref))

    return qtd_plan, qtd_prod, qtd_ref

# -------------------------
# GENERATORS (negócio)
# -------------------------
def gen_clientes(dt: datetime, state: dict, passo_horas: int = 1):
    rows = []
    if not state["clientes"]:
        state["cnt_cliente"] += 1
        cid = f"C{state['cnt_cliente']:04d}"
        state["clientes"].append(cid)
        rows.append({"cliente_id": cid, "tipo_cliente": "Distribuidor", "cidade": "SP", "tipo_plano": "Standard", "data_cadastro": "2022-01-01"})

    lambda_por_hora = 0.25
    qtd_novos = int(np.random.poisson(lam=lambda_por_hora * passo_horas))
    qtd_novos = min(qtd_novos, 10)

    for _ in range(qtd_novos):
        state["cnt_cliente"] += 1
        cid = f"C{state['cnt_cliente']:04d}"
        state["clientes"].append(cid)
        rows.append({"cliente_id": cid, "tipo_cliente": random.choice(["Distribuidor", "Autopeças", "Montadora"]), "cidade": random.choice(ESTADOS_BR), "tipo_plano": random.choice(["Básico", "Standard", "Premium"]), "data_cadastro": dt.date().isoformat()})
    return rows

def gen_compras(dt: datetime, state: dict):
    rows = []
    prob = 0.55 if dt.year <= 2023 else 0.45 if dt.year == 2024 else 0.35
    if random.random() >= prob: return rows

    qtd = random.randint(1, 4)
    fornecedores = [f["fornecedor_id"] for f in DADOS_ESTATICOS["raw_fornecedor"]]
    materias = [m["materia_prima_id"] for m in DADOS_ESTATICOS["raw_materia_prima"]]

    for _ in range(qtd):
        state["cnt_compra"] += 1
        compra_id = f"CP{state['cnt_compra']:06d}"
        qtd_comprada = random.randint(500, 2000)
        custo_unit = round(random.uniform(20, 100), 2)
        rows.append({"compra_id": compra_id, "fornecedor_id": random.choice(fornecedores), "materia_prima_id": random.choice(materias), "data_compra": dt.strftime("%Y-%m-%d %H:%M:%S"), "quantidade_comprada": to_str(qtd_comprada), "custo_unitario": to_str(custo_unit), "custo_total": to_str(round(qtd_comprada * custo_unit, 2))})
    return rows

def gen_producao(dt: datetime, state: dict, fleet: list[dict], falhas_programadas: dict = None):
    prod, lotes, qual, alerts, estoque_mov = [], [], [], [], []
    falhas_programadas = falhas_programadas or {}
    produtos = [p["produto_id"] for p in DADOS_ESTATICOS["raw_produto"]]

    for m in fleet:
        state["cnt_op"] += 1
        state["cnt_lote"] += 1
        op_id = f"OP{state['cnt_op']:07d}"
        lote_id = f"Lote{state['cnt_lote']:07d}"

        horas_aviso = falhas_programadas.get(m["maquina_id"], None)
        temp, vib, perf = desgaste_maquina(m["ano"], dt, horas_para_falha=horas_aviso)

        dur = round(random.uniform(0.80, 1.00), 2)
        fim = dt + timedelta(hours=dur)
        pid = random.choice(produtos)
        ciclo_nom = 0.5 
        
        q_plan, q_prod, q_ref = calc_oee(dur, ciclo_nom, perf)

        lotes.append({"lote_id": lote_id, "produto_id": pid, "linha_id": m["linha_id"], "maquina_id": m["maquina_id"], "inicio_producao": dt.strftime("%Y-%m-%d %H:%M:%S"), "fim_producao": fim.strftime("%Y-%m-%d %H:%M:%S"), "duracao_horas": to_str(dur)})

        prod.append({"ordem_producao_id": op_id, "lote_id": lote_id, "produto_id": pid, "linha_id": m["linha_id"], "maquina_id": m["maquina_id"], "turno_id": turno(dt), "inicio": dt.strftime("%Y-%m-%d %H:%M:%S"), "ciclo_minuto_nominal": to_str(ciclo_nom), "duracao_horas": to_str(dur), "temperatura_media_c": to_str(temp), "vibracao_media_rpm": to_str(vib), "pressao_media_bar": to_str(round(random.uniform(6, 8), 1)), "quantidade_planejada": to_str(q_plan), "quantidade_produzida": to_str(q_prod), "quantidade_refugada": to_str(q_ref)})

        qual.append({"teste_id": f"T{state['cnt_lote']:07d}", "lote_id": lote_id, "produto_id": pid, "data_teste": (fim + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"), "tensao_medida_v": to_str(round(random.normalvariate(12.6, 0.2), 2)), "resistencia_interna_mohm": "6.0", "capacidade_ah_teste": "60.0", "defeito_id": "D00", "aprovado": "1" if q_prod > 0 else "0"})

        # [REQUISITO] ALIMENTANDO O ESTOQUE E O BUFFER PARA SINCRONISMO PERFEITO
        state["cnt_movimento"] += 1
        mov_id = f"MOV{state['cnt_movimento']:08d}"
        estoque_mov.append({"movimento_id": mov_id, "produto_id": pid, "lote_id": lote_id, "tipo_movimento": "ENTRADA", "quantidade": q_prod, "data_movimento": fim.strftime("%Y-%m-%d %H:%M:%S")})
        
        # Guarda no state para que gen_vendas ache ESTE lote específico
        state["buffer_estoque"].append({"lote_id": lote_id, "produto_id": pid, "op_id": op_id, "qtd": q_prod, "temp_max": temp})

        if temp > 100.0 or vib > 2000.0:
            alerts.append({"alerta_id": f"ALT-{uuid.uuid4().hex[:10]}", "data_ocorrencia": dt, "nivel": "CRITICO", "maquina_id": m["maquina_id"], "mensagem": "Anomalia Detectada - Sensor Crítico", "valor_medido": float(temp if temp > 100 else vib)})

    return prod, lotes, qual, alerts, estoque_mov

def gen_map_lote_compras(lotes: list[dict], compras: list[dict]):
    rows = []
    if not lotes or not compras: return rows
    compra_ids = [c["compra_id"] for c in compras]
    for l in lotes:
        k = random.randint(0, min(3, len(compra_ids)))
        if k == 0: continue
        escolhidas = random.sample(compra_ids, k=k)
        for cid in escolhidas: rows.append({"lote_id": l["lote_id"], "compra_id": cid})
    return rows

def gen_vendas(dt: datetime, state: dict):
    rows = []
    estoque_saidas = []
    
    # [REQUISITO] SINCRONISMO: Venda só ocorre se há lote no buffer
    lotes_disponiveis = state.get("buffer_estoque", [])
    if not lotes_disponiveis or not state["clientes"]:
        return rows, estoque_saidas

    # Simula a venda de 10% a 30% do pátio
    n = max(1, int(len(lotes_disponiveis) * random.uniform(0.1, 0.3)))
    
    for _ in range(n):
        if not lotes_disponiveis: break
        lote = lotes_disponiveis.pop(0) # Retira do estoque (FIFO)
        
        state["cnt_venda"] += 1
        cliente = escolher_cliente_por_idade(state)
        if cliente:
            state.setdefault("ultimos_clientes", []).append(cliente)
            state["ultimos_clientes"] = state["ultimos_clientes"][-ULTIMOS_CLIENTES_JANELA:]

        venda_id = f"V{state['cnt_venda']:07d}"
        venda_item = {
            "venda_id": venda_id,
            "ano_mes_id": dt.strftime("%Y-%m"),
            "cliente_id": cliente,
            "produto_id": lote["produto_id"],
            "ordem_producao_id": lote["op_id"],
            "lote_id": lote["lote_id"],
            "data_venda": dt.date().isoformat(),
            "quantidade_vendida": to_str(lote["qtd"]),
            "valor_total_venda": to_str(round(lote["qtd"] * random.uniform(80, 150), 2)),
        }
        rows.append(venda_item)
        
        # Gera saída de estoque
        state["cnt_movimento"] += 1
        estoque_saidas.append({"movimento_id": f"MOV{state['cnt_movimento']:08d}", "produto_id": lote["produto_id"], "lote_id": lote["lote_id"], "tipo_movimento": "SAIDA", "quantidade": lote["qtd"], "data_movimento": dt.strftime("%Y-%m-%d %H:%M:%S")})

        # Buffer para garantir que a Garantia aponte para Vendas REAIS
        state["buffer_vendas"].append({
            "venda_id": venda_id, "lote_id": lote["lote_id"], "produto_id": lote["produto_id"], 
            "cliente_id": cliente, "data_venda": dt.date(), "temp_max_prod": lote["temp_max"]
        })

    return rows, estoque_saidas

def gen_garantia(dt: datetime, state: dict):
    rows = []
    vendas = state.get("buffer_vendas", [])
    if not vendas: return rows

    GARANTIA_DIAS = 180
    defeitos_pesos = [("D00", 0.25), ("D05", 0.22), ("D02", 0.20), ("D03", 0.16), ("D01", 0.12), ("D04", 0.05)]
    custo_base = {"D05": (150, 600), "D02": (250, 1200), "D03": (200, 1000), "D01": (500, 2200), "D04": (900, 4000)}
    mau_uso_prob = {"D03": 0.55, "D05": 0.18, "D02": 0.10, "D01": 0.08, "D04": 0.06}

    defeitos_ids = [d for d, _ in defeitos_pesos]
    defeitos_w = [w for _, w in defeitos_pesos]

    # Escolhe aleatoriamente algumas vendas antigas para avaliar garantia
    amostra_vendas = random.sample(vendas, k=min(20, len(vendas)))

    for v in amostra_vendas:
        # [REQUISITO RAQUEL]: Se o lote foi produzido com instabilidade térmica, chance de dar pau aumenta!
        probabilidade_base = 0.01
        if v.get("temp_max_prod", 0) > 85.0:
            probabilidade_base *= CHANCE_GARANTIA_LOTE_RUIM

        if random.random() >= probabilidade_base:
            continue

        state["cnt_garantia"] += 1
        # Data da reclamação precisa ser DEPOIS da venda
        dias = random.randint(1, 90)
        data_reclamacao = v["data_venda"] + timedelta(days=dias)
        
        # Ignora se tentou reclamar no "futuro" relativo ao loop
        if data_reclamacao > dt.date():
            continue

        defeito_id = random.choices(defeitos_ids, weights=defeitos_w, k=1)
        dentro_garantia = dias <= GARANTIA_DIAS

        if defeito_id == "D00":
            status = "Negada"
            custo = 0.0
        else:
            p_mau = mau_uso_prob.get(defeito_id, 0.10)
            if dias > 60: p_mau = min(0.85, p_mau + 0.10)
            if random.random() < p_mau:
                status = "Negada - Mau Uso"
                custo = 0.0
            else:
                if dentro_garantia:
                    status = "Aprovada"
                    lo, hi = custo_base.get(defeito_id, (200, 1500))
                    custo = round(random.uniform(lo, hi), 2)
                else:
                    if random.random() < 0.08:
                        status = "Aprovada"
                        lo, hi = custo_base.get(defeito_id, (200, 1500))
                        custo = round(random.uniform(lo, hi), 2)
                    else:
                        status = "Negada"
                        custo = 0.0

        # [REQUISITO] SLA de Garantia (7 dias de prazo)
        data_limite_sla = data_reclamacao + timedelta(days=7)
        tempo_resposta = random.randint(1, 15)
        status_sla = "NO PRAZO" if tempo_resposta <= 7 else "ATRASADO"

        rows.append({
            "garantia_id": f"W{state['cnt_garantia']:07d}",
            "venda_id": v["venda_id"],
            "cliente_id": v["cliente_id"],
            "produto_id": v["produto_id"],
            "lote_id": v["lote_id"],
            "data_reclamacao": data_reclamacao.strftime("%Y-%m-%d 09:00:00"),
            "data_limite_sla": data_limite_sla.strftime("%Y-%m-%d 18:00:00"),
            "status_sla": status_sla,
            "dias_pos_venda": to_str(dias),
            "defeito_id": defeito_id,
            "status": status,
            "tempo_resposta_dias": to_str(tempo_resposta),
            "custo_garantia": to_str(custo),
        })

    return rows

def gen_manutencao(dt: datetime, state: dict, fleet: list[dict]):
    rows = []
    if random.random() < 0.05:
        state["cnt_manut"] += 1
        m = random.choice(fleet)
        tipo = random.choice(["TM01", "TM02", "TM03"])
        
        # [REQUISITO XAVAS]: Tempos dinâmicos baseados no tipo de manutenção
        if tipo == "TM02":
            duracao = random.randint(120, 480) # Quebra severa
            crit = "Alta"
        elif tipo == "TM01":
            duracao = random.randint(60, 150)  # Preventiva
            crit = "Baixa"
        else:
            duracao = random.randint(40, 90)   # Preditiva
            crit = "Média"

        fim = dt + timedelta(minutes=duracao)

        rows.append({
            "evento_manutencao_id": f"EVM{state['cnt_manut']:07d}",
            "maquina_id": m["maquina_id"],
            "linha_id": m["linha_id"],
            "tipo_manutencao_id": tipo,
            "inicio": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "fim": fim.strftime("%Y-%m-%d %H:%M:%S"),
            "duracao_min": to_str(duracao),
            "criticidade": crit,
        })
    return rows

# -------------------------
# MAIN HANDLER (COM BACKFILL COMPLETO)
# -------------------------
@functions_framework.http
def executar_simulacao(request):
    args = request.args or {}
    mode = args.get("mode", "incremental").lower()
    start = args.get("start") 
    end = args.get("end") 

    run_id = str(uuid.uuid4())
    sc = gcs()
    state = load_state(sc)

    state["seed"] = int(state.get("seed", 42)) + 1
    random.seed(state["seed"])
    np.random.seed(state["seed"])

    bq_client = bq()
    setup_bq(bq_client)

    now_seed = datetime.now(TZ_BR)
    seed_rows = seed_clientes_iniciais(state, now_seed)
    if seed_rows:
        persist_table(sc, bq_client, now_seed, run_id, "raw_cliente", seed_rows)
        save_state(sc, state)

    fleet = gen_fleet(state)

    if not state.get("static", False):
        dim_tempo = build_dim_tempo(2022, 2027)
        metas = build_metas_vendas(dim_tempo)
        static_payloads = {
            "raw_tempo": dim_tempo, "raw_metas_vendas": metas, "raw_linha": DADOS_ESTATICOS["raw_linha"],
            "raw_turno": DADOS_ESTATICOS["raw_turno"], "raw_tipo_manut": DADOS_ESTATICOS["raw_tipo_manut"],
            "raw_defeito": DADOS_ESTATICOS["raw_defeito"], "raw_materia_prima": DADOS_ESTATICOS["raw_materia_prima"],
            "raw_fornecedor": DADOS_ESTATICOS["raw_fornecedor"], "raw_produto": DADOS_ESTATICOS["raw_produto"],
            "raw_maquina": fleet,
        }
        now = datetime.now(TZ_BR)
        for t, rows in static_payloads.items(): persist_table(sc, bq_client, now, run_id, t, rows)
        state["static"] = True
        save_state(sc, state)

    # -------------------------
    # BACKFILL COMPLETO DE VOLTA
    # -------------------------
    if mode == "backfill":
        if not start or not end: return "ERRO: backfill requer ?mode=backfill&start=YYYY-MM-DD&end=YYYY-MM-DD", 400
        try:
            dt1 = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=TZ_BR)
            dt2 = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=TZ_BR)
        except Exception:
            return "ERRO: formato de data inválido. Use YYYY-MM-DD", 400

        cur = dt1
        counts = {k: 0 for k in ["cli", "comp", "map", "prod", "lote", "qual", "vend", "gar", "man", "alt", "estq"]}

        while cur <= dt2:
            cli = gen_clientes(cur, state, passo_horas=24)
            comp = gen_compras(cur, state)

            maquinas_em_falha = {}
            if random.random() < PROB_FALHA_DIARIA: 
                 maquina_sorteada = random.choice(fleet)["maquina_id"]
                 maquinas_em_falha[maquina_sorteada] = random.randint(0, 3)

            prod, lotes, qual, alt, estq_in = gen_producao(cur, state, fleet, falhas_programadas=maquinas_em_falha)
            mapa = gen_map_lote_compras(lotes, comp)
            vend, estq_out = gen_vendas(cur, state)
            gar = gen_garantia(cur, state)
            man = gen_manutencao(cur, state, fleet)
            
            estq_movimento = estq_in + estq_out

            for t, rows in [
                ("raw_cliente", cli), ("raw_compras", comp), ("raw_map_lote_compras", mapa),
                ("raw_producao", prod), ("raw_lote", lotes), ("raw_qualidade", qual),
                ("raw_estoque_movimento", estq_movimento), ("raw_vendas", vend), 
                ("raw_garantia", gar), ("raw_manutencao", man), ("monitoramento_alertas", alt)
            ]: write_gcs_jsonl(sc, t, rows, run_id, cur)

            counts["cli"] += len(cli); counts["comp"] += len(comp); counts["map"] += len(mapa)
            counts["prod"] += len(prod); counts["lote"] += len(lotes); counts["qual"] += len(qual)
            counts["estq"] += len(estq_movimento); counts["vend"] += len(vend); counts["gar"] += len(gar)
            counts["man"] += len(man); counts["alt"] += len(alt)

            cur += timedelta(days=1)

        save_state(sc, state)
        return f"OK backfill run_id={run_id} | {counts}", 200

    # -------------------------
    # INCREMENTAL COMPLETO DE VOLTA
    # -------------------------
    cur = datetime.now(TZ_BR)
    counts = {k: 0 for k in ["cli", "comp", "map", "prod", "lote", "qual", "vend", "gar", "man", "alt", "estq"]}

    for _ in range(HORAS_POR_LOTE):
        cli = gen_clientes(cur, state, passo_horas=1)
        comp = gen_compras(cur, state)

        maquinas_em_falha = {}
        if random.random() < PROB_FALHA_DIARIA:
             maquina_sorteada = random.choice(fleet)["maquina_id"]
             maquinas_em_falha[maquina_sorteada] = random.randint(0, 3)

        prod, lotes, qual, alt, estq_in = gen_producao(cur, state, fleet, falhas_programadas=maquinas_em_falha)
        mapa = gen_map_lote_compras(lotes, comp)
        vend, estq_out = gen_vendas(cur, state)
        gar = gen_garantia(cur, state)
        man = gen_manutencao(cur, state, fleet)
        
        estq_movimento = estq_in + estq_out

        persist_table(sc, bq_client, cur, run_id, "raw_cliente", cli)
        persist_table(sc, bq_client, cur, run_id, "raw_compras", comp)
        persist_table(sc, bq_client, cur, run_id, "raw_map_lote_compras", mapa)
        persist_table(sc, bq_client, cur, run_id, "raw_producao", prod)
        persist_table(sc, bq_client, cur, run_id, "raw_lote", lotes)
        persist_table(sc, bq_client, cur, run_id, "raw_qualidade", qual)
        persist_table(sc, bq_client, cur, run_id, "raw_estoque_movimento", estq_movimento)
        persist_table(sc, bq_client, cur, run_id, "raw_vendas", vend)
        persist_table(sc, bq_client, cur, run_id, "raw_garantia", gar)
        persist_table(sc, bq_client, cur, run_id, "raw_manutencao", man)
        persist_table(sc, bq_client, cur, run_id, "monitoramento_alertas", alt)

        counts["cli"] += len(cli); counts["comp"] += len(comp); counts["map"] += len(mapa)
        counts["prod"] += len(prod); counts["lote"] += len(lotes); counts["qual"] += len(qual)
        counts["estq"] += len(estq_movimento); counts["vend"] += len(vend); counts["gar"] += len(gar)
        counts["man"] += len(man); counts["alt"] += len(alt)

        cur += timedelta(hours=1)

    save_state(sc, state)
    return f"OK incremental run_id={run_id} | {counts}", 200