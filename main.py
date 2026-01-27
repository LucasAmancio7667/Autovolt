import functions_framework
import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.filterwarnings("ignore", module="google.cloud.bigquery")

import io
import json
import uuid
import time
import random
import hashlib
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

# =========================
# CONFIG
# =========================
PROJECT_ID = "autovolt-analytics-479417"
DATASET_ID = "autovolt_bronze"
BUCKET_NAME = "bucket_ingestao"

TZ_BR = timezone(timedelta(hours=-3))
HORAS_POR_LOTE = 1

STATE_BLOB = "state/state.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# =========================
# SCHEMAS (todas do print EXCETO raw_usuario e raw_controle_acesso)
# =========================
SCHEMAS = {
    "raw_linha": [SchemaField("linha_id","STRING"),SchemaField("descricao","STRING"),SchemaField("turnos_operacionais","STRING")],
    "raw_metas_vendas": [SchemaField("meta_id","STRING"),SchemaField("ano_mes_id","STRING"),SchemaField("meta_quantidade","STRING"),SchemaField("meta_valor","STRING")],
    "raw_tempo": [SchemaField("ano_mes_id","STRING"),SchemaField("ano","STRING"),SchemaField("mes","STRING"),SchemaField("nome_mes","STRING"),SchemaField("trimestre","STRING"),SchemaField("ano_mes_label","STRING")],
    "raw_tipo_manut": [SchemaField("tipo_manutencao_id","STRING"),SchemaField("descricao","STRING"),SchemaField("criticidade_padrao","STRING")],
    "raw_turno": [SchemaField("turno_id","STRING"),SchemaField("janela","STRING"),SchemaField("coef_performance","STRING")],

    "raw_produto": [SchemaField("produto_id","STRING"),SchemaField("modelo","STRING"),SchemaField("tensao_v","STRING"),SchemaField("capacidade_ah","STRING"),SchemaField("linha_segmento","STRING"),SchemaField("data_lancamento","STRING"),SchemaField("data_descontinuacao","STRING")],
    "raw_maquina": [SchemaField("maquina_id","STRING"),SchemaField("tipo","STRING"),SchemaField("fabricante","STRING"),SchemaField("ano","STRING"),SchemaField("linha_id","STRING")],
    "raw_fornecedor": [SchemaField("fornecedor_id","STRING"),SchemaField("categoria","STRING"),SchemaField("leadtime_dias","STRING"),SchemaField("qualificacao","STRING"),SchemaField("data_cadastro","STRING"),SchemaField("data_ultima_avaliacao","STRING"),SchemaField("descricao","STRING")],
    "raw_defeito": [SchemaField("defeito_id","STRING"),SchemaField("descricao","STRING"),SchemaField("gravidade","STRING")],
    "raw_materia_prima": [SchemaField("materia_prima_id","STRING"),SchemaField("nome_material","STRING")],

    "raw_cliente": [SchemaField("cliente_id","STRING"),SchemaField("tipo_cliente","STRING"),SchemaField("cidade","STRING"),SchemaField("tipo_plano","STRING"),SchemaField("data_cadastro","STRING"),SchemaField("data_ultima_compra","STRING")],

    "raw_lote": [SchemaField("lote_id","STRING"),SchemaField("produto_id","STRING"),SchemaField("linha_id","STRING"),SchemaField("maquina_id","STRING"),SchemaField("inicio_producao","STRING"),SchemaField("fim_producao","STRING"),SchemaField("duracao_horas","STRING")],
    "raw_producao": [SchemaField("ordem_producao_id","STRING"),SchemaField("lote_id","STRING"),SchemaField("produto_id","STRING"),SchemaField("linha_id","STRING"),SchemaField("maquina_id","STRING"),SchemaField("turno_id","STRING"),SchemaField("inicio","STRING"),SchemaField("ciclo_minuto_nominal","STRING"),SchemaField("duracao_horas","STRING"),SchemaField("temperatura_media_c","STRING"),SchemaField("vibracao_media_rpm","STRING"),SchemaField("pressao_media_bar","STRING"),SchemaField("quantidade_planejada","STRING"),SchemaField("quantidade_produzida","STRING"),SchemaField("quantidade_refugada","STRING")],
    "raw_qualidade": [SchemaField("teste_id","STRING"),SchemaField("lote_id","STRING"),SchemaField("produto_id","STRING"),SchemaField("data_teste","STRING"),SchemaField("tensao_medida_v","STRING"),SchemaField("resistencia_interna_mohm","STRING"),SchemaField("capacidade_ah_teste","STRING"),SchemaField("defeito_id","STRING"),SchemaField("aprovado","STRING")],

    "raw_compras": [SchemaField("compra_id","STRING"),SchemaField("fornecedor_id","STRING"),SchemaField("materia_prima_id","STRING"),SchemaField("data_compra","STRING"),SchemaField("quantidade_comprada","STRING"),SchemaField("custo_unitario","STRING"),SchemaField("custo_total","STRING")],
    "raw_map_lote_compras": [SchemaField("lote_id","STRING"),SchemaField("compra_id","STRING")],

    # ajuste pedido: lote_id em vendas
    "raw_vendas": [SchemaField("venda_id","STRING"),SchemaField("ano_mes_id","STRING"),SchemaField("cliente_id","STRING"),SchemaField("produto_id","STRING"),SchemaField("ordem_producao_id","STRING"),SchemaField("lote_id","STRING"),SchemaField("data_venda","STRING"),SchemaField("quantidade_vendida","STRING"),SchemaField("valor_total_venda","STRING")],

    "raw_garantia": [SchemaField("garantia_id","STRING"),SchemaField("cliente_id","STRING"),SchemaField("produto_id","STRING"),SchemaField("lote_id","STRING"),SchemaField("data_reclamacao","STRING"),SchemaField("dias_pos_venda","STRING"),SchemaField("defeito_id","STRING"),SchemaField("status","STRING"),SchemaField("tempo_resposta_dias","STRING"),SchemaField("custo_garantia","STRING")],
    "raw_manutencao": [SchemaField("evento_manutencao_id","STRING"),SchemaField("maquina_id","STRING"),SchemaField("linha_id","STRING"),SchemaField("tipo_manutencao_id","STRING"),SchemaField("inicio","STRING"),SchemaField("fim","STRING"),SchemaField("duracao_min","STRING"),SchemaField("criticidade","STRING")],

    "monitoramento_alertas": [
        SchemaField("alerta_id","STRING"),
        SchemaField("data_ocorrencia","TIMESTAMP"),
        SchemaField("nivel","STRING"),
        SchemaField("maquina_id","STRING"),
        SchemaField("mensagem","STRING"),
        SchemaField("valor_medido","FLOAT64"),
    ],
}

# =========================
# STATIC DATA
# =========================
MESES_PT = {1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"}
MESES_ABBR = {1:"jan",2:"fev",3:"mar",4:"abr",5:"mai",6:"jun",7:"jul",8:"ago",9:"set",10:"out",11:"nov",12:"dez"}

DADOS_ESTATICOS = {
    "raw_linha": [
        {"linha_id":"L01","descricao":"Linha de Montagem Automotiva","turnos_operacionais":"3"},
        {"linha_id":"L02","descricao":"Linha de Injeção Plástica","turnos_operacionais":"3"},
        {"linha_id":"L03","descricao":"Linha de Envase Químico","turnos_operacionais":"3"},
        {"linha_id":"L04","descricao":"Linha de Pesados","turnos_operacionais":"2"},
        {"linha_id":"L05","descricao":"Linha de Testes","turnos_operacionais":"1"},
    ],
    "raw_turno": [
        {"turno_id":"T1","janela":"06:00 - 14:00","coef_performance":"1.0"},
        {"turno_id":"T2","janela":"14:00 - 22:00","coef_performance":"0.98"},
        {"turno_id":"T3","janela":"22:00 - 06:00","coef_performance":"0.95"},
    ],
    "raw_tipo_manut": [
        {"tipo_manutencao_id":"TM01","descricao":"Preventiva","criticidade_padrao":"Baixa"},
        {"tipo_manutencao_id":"TM02","descricao":"Corretiva","criticidade_padrao":"Alta"},
        {"tipo_manutencao_id":"TM03","descricao":"Preditiva","criticidade_padrao":"Média"},
    ],
    "raw_defeito": [
        {"defeito_id":"D00","descricao":"Sem Defeito","gravidade":"Nenhuma"},
        {"defeito_id":"D01","descricao":"Vazamento de Ácido","gravidade":"Alta"},
        {"defeito_id":"D02","descricao":"Baixa Tensão","gravidade":"Média"},
        {"defeito_id":"D03","descricao":"Caixa Rachada","gravidade":"Média"},
        {"defeito_id":"D04","descricao":"Sobreaquecimento (Curto)","gravidade":"Crítica"},
        {"defeito_id":"D05","descricao":"Terminal Oxidado","gravidade":"Baixa"},
    ],
    "raw_materia_prima": [
        {"materia_prima_id":"MP001","nome_material":"Chumbo"},
        {"materia_prima_id":"MP002","nome_material":"Ácido Sulfúrico"},
        {"materia_prima_id":"MP003","nome_material":"Polipropileno"},
        {"materia_prima_id":"MP004","nome_material":"Separadores"},
        {"materia_prima_id":"MP005","nome_material":"Eletrólito"},
    ],
    "raw_fornecedor": [
        {"fornecedor_id":"F001","categoria":"Chumbo/Metais","leadtime_dias":"10","qualificacao":"A","data_cadastro":"2022-01-10","data_ultima_avaliacao":"2025-01-01","descricao":"Fornecedor Metal 1"},
        {"fornecedor_id":"F002","categoria":"Químicos/Ácidos","leadtime_dias":"7","qualificacao":"A","data_cadastro":"2022-02-15","data_ultima_avaliacao":"2025-01-01","descricao":"Indústria Química 2"},
        {"fornecedor_id":"F003","categoria":"Plásticos/Polímeros","leadtime_dias":"14","qualificacao":"B","data_cadastro":"2022-03-20","data_ultima_avaliacao":"2025-01-01","descricao":"PlastCorp 3"},
        {"fornecedor_id":"F004","categoria":"Componentes Elétricos","leadtime_dias":"21","qualificacao":"A","data_cadastro":"2022-05-05","data_ultima_avaliacao":"2025-01-01","descricao":"ElectroParts 4"},
    ],
    "raw_produto": [
        {"produto_id":"BAT001","modelo":"AV-50Ah","tensao_v":"12","capacidade_ah":"50","linha_segmento":"Montadora","data_lancamento":"2022-03-01","data_descontinuacao":""},
        {"produto_id":"BAT002","modelo":"AV-60Ah","tensao_v":"12","capacidade_ah":"60","linha_segmento":"Reposição","data_lancamento":"2022-07-15","data_descontinuacao":""},
        {"produto_id":"BAT003","modelo":"AV-70Ah","tensao_v":"12","capacidade_ah":"70","linha_segmento":"Montadora","data_lancamento":"2023-02-10","data_descontinuacao":""},
        {"produto_id":"BAT004","modelo":"AV-80Ah","tensao_v":"24","capacidade_ah":"80","linha_segmento":"Reposição","data_lancamento":"2023-09-20","data_descontinuacao":""},
        {"produto_id":"BAT005","modelo":"AV-100Ah","tensao_v":"12","capacidade_ah":"100","linha_segmento":"Reposição","data_lancamento":"2024-06-05","data_descontinuacao":""},
    ],
}

# =========================
# CLIENTS
# =========================
def bq_client(): return bigquery.Client(project=PROJECT_ID)
def gcs_client(): return storage.Client(project=PROJECT_ID)

# =========================
# BQ SETUP
# =========================
def ensure_dataset_and_tables(client: bigquery.Client):
    ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(ds_ref)
    except NotFound:
        client.create_dataset(ds_ref)

    for tbl, schema in SCHEMAS.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{tbl}"
        try:
            client.get_table(table_id)
        except NotFound:
            client.create_table(bigquery.Table(table_id, schema=schema))

# =========================
# STATE
# =========================
def load_state(sc: storage.Client) -> dict:
    bucket = sc.bucket(BUCKET_NAME)
    blob = bucket.blob(STATE_BLOB)
    if not blob.exists():
        return {
            "seed": 42,
            "static_done": False,
            "cnt_op": 0,
            "cnt_lote": 0,
            "cnt_venda": 0,
            "cnt_compra": 0,
            "cnt_garantia": 0,
            "cnt_manut": 0,
            "cnt_cliente": 0,
            "_clientes": [],
            "_fleet": None
        }
    return json.loads(blob.download_as_text())

def save_state(sc: storage.Client, state: dict):
    sc.bucket(BUCKET_NAME).blob(STATE_BLOB).upload_from_string(
        json.dumps(state, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

# =========================
# HELPERS
# =========================
def calc_turno(dt: datetime) -> str:
    h = dt.hour
    if 6 <= h < 14: return "T1"
    if 14 <= h < 22: return "T2"
    return "T3"

def gcs_write_jsonl(sc: storage.Client, tabela: str, rows: list[dict], run_id: str, dt: datetime) -> str:
    if not rows:
        return ""
    bucket = sc.bucket(BUCKET_NAME)
    dt_path = f"dt={dt.strftime('%Y-%m-%d')}/hr={dt.strftime('%H')}/run_id={run_id}"
    file_name = f"part-{tabela}-{dt.strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}.jsonl"
    blob_name = f"bronze/{tabela}/{dt_path}/{file_name}"

    def row_hash(r: dict) -> str:
        return hashlib.sha256(json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",",":")).encode("utf-8")).hexdigest()

    buf = io.StringIO()
    ing = datetime.now(TZ_BR).isoformat()
    for r in rows:
        rr = dict(r)
        rr["_run_id"] = run_id
        rr["_ingested_at"] = ing
        rr["_source_file"] = file_name
        rr["_row_hash"] = row_hash(r)
        buf.write(json.dumps(rr, ensure_ascii=False))
        buf.write("\n")

    bucket.blob(blob_name).upload_from_string(buf.getvalue(), content_type="application/json")
    return f"gs://{BUCKET_NAME}/{blob_name}"

def bq_load(client: bigquery.Client, tabela: str, uri: str):
    if not uri:
        return
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=SCHEMAS[tabela],
        ignore_unknown_values=True
    )
    client.load_table_from_uri(uri, f"{PROJECT_ID}.{DATASET_ID}.{tabela}", job_config=job_config).result()

def should_load_to_bq(dt: datetime) -> bool:
    return dt.year == datetime.now(TZ_BR).year

# =========================
# GERADORES
# =========================
def gerar_dim_tempo_2022_2027():
    rows = []
    for ano in range(2022, 2028):
        for mes in range(1, 13):
            rows.append({
                "ano_mes_id": f"{ano}-{mes:02d}",
                "ano": str(ano),
                "mes": f"{mes:02d}",
                "nome_mes": MESES_PT[mes],
                "trimestre": str((mes - 1)//3 + 1),
                "ano_mes_label": f"{MESES_ABBR[mes]}/{str(ano)[2:]}"
            })
    return rows

def get_or_create_fleet(state: dict):
    if state.get("_fleet"):
        return state["_fleet"]
    fleet = []
    for i in range(1, 21):
        fleet.append({
            "maquina_id": f"M{i:03d}",
            "tipo": random.choice(["Montadora Automática","Injetora de Plástico","Envasadora de Ácido","Robô de Solda","Testador de Carga"]),
            "fabricante": random.choice(["Siemens","Engel","Bosch","ABB","Kuka"]),
            "ano": str(random.choice([2021, 2022, 2023, 2024])),
            "linha_id": random.choice(["L01","L02","L03","L04","L05"])
        })
    state["_fleet"] = fleet
    return fleet

def desgaste_maquina(ano_fab: int, data_sim: datetime):
    idade = max(0, data_sim.year - ano_fab)
    fator = 1.0 + idade * 0.03
    temp = float(np.random.normal(65.0 * fator, 3.0))
    vib = float(np.random.normal(1200.0 * fator, 150.0))
    perf = float(max(0.85, 1.0 - idade * 0.01))
    return round(temp,1), round(vib,0), perf

def oee_fisico(dur_h: float, ciclo_min: float, perf: float):
    mins = dur_h * 60.0
    cap = int(mins / ciclo_min)
    plan = int(cap * 0.95)
    eff = float(np.random.normal(perf, 0.05))
    eff = min(1.0, max(0.0, eff))
    prod = int(plan * eff)
    ref = int(prod * random.uniform(0.0, 0.01 + (1.0 - perf)))
    return plan, prod, ref

def gerar_clientes(data_sim: datetime, state: dict):
    rows = []
    ano = data_sim.year
    if ano <= 2023:
        qtd = 5
    elif ano == 2024:
        qtd = 2
    else:
        qtd = 1 if random.random() < 0.2 else 0

    for _ in range(qtd):
        state["cnt_cliente"] += 1
        cid = f"C{state['cnt_cliente']:04d}"
        rows.append({
            "cliente_id": cid,
            "tipo_cliente": random.choice(["Distribuidor","Autopeças","Montadora"]),
            "cidade": random.choice(["SP","RJ","MG","RS","PE","BA","PR","SC"]),
            "tipo_plano": random.choice(["Básico","Standard","Premium"]),
            "data_cadastro": data_sim.date().isoformat(),
            "data_ultima_compra": ""
        })
        state["_clientes"].append(cid)
    return rows

def gerar_compras(data_sim: datetime, state: dict):
    rows = []
    if random.random() < 0.6:
        qtd = random.randint(1, 4)
        for _ in range(qtd):
            state["cnt_compra"] += 1
            compra_id = f"C{state['cnt_compra']}"
            fornecedor_id = random.choice(["F001","F002","F003","F004"])
            mp_id = random.choice(["MP001","MP002","MP003","MP004","MP005"])
            q = random.randint(500, 2000)
            cu = round(random.uniform(20, 100), 2)
            rows.append({
                "compra_id": compra_id,
                "fornecedor_id": fornecedor_id,
                "materia_prima_id": mp_id,
                "data_compra": data_sim.strftime("%Y-%m-%d %H:%M:%S"),
                "quantidade_comprada": str(q),
                "custo_unitario": str(cu),
                "custo_total": str(round(q*cu, 2))
            })
    return rows

def gerar_producao_lote_qual_alertas(data_sim: datetime, state: dict, fleet: list[dict]):
    prod, lote, qual, alerts = [], [], [], []
    ciclo = 0.5
    produtos = [p["produto_id"] for p in DADOS_ESTATICOS["raw_produto"]]

    for m in fleet:
        state["cnt_op"] += 1
        state["cnt_lote"] += 1
        op_id = f"OP{state['cnt_op']}"
        lote_id = f"Lote{state['cnt_lote']}"
        ano_fab = int(m["ano"])

        temp, vib, perf = desgaste_maquina(ano_fab, data_sim)
        dur = round(random.uniform(0.8, 1.0), 2)
        fim = data_sim + timedelta(hours=dur)
        pid = random.choice(produtos)

        q_plan, q_prod, q_ref = oee_fisico(dur, ciclo, perf)

        lote.append({
            "lote_id": lote_id,
            "produto_id": pid,
            "linha_id": m["linha_id"],
            "maquina_id": m["maquina_id"],
            "inicio_producao": data_sim.strftime("%Y-%m-%d %H:%M:%S"),
            "fim_producao": fim.strftime("%Y-%m-%d %H:%M:%S"),
            "duracao_horas": str(dur),
        })

        prod.append({
            "ordem_producao_id": op_id,
            "lote_id": lote_id,
            "produto_id": pid,
            "linha_id": m["linha_id"],
            "maquina_id": m["maquina_id"],
            "turno_id": calc_turno(data_sim),
            "inicio": data_sim.strftime("%Y-%m-%d %H:%M:%S"),
            "ciclo_minuto_nominal": str(ciclo),
            "duracao_horas": str(dur),
            "temperatura_media_c": str(temp),
            "vibracao_media_rpm": str(vib),
            "pressao_media_bar": str(round(random.uniform(6, 8), 1)),
            "quantidade_planejada": str(q_plan),
            "quantidade_produzida": str(q_prod),
            "quantidade_refugada": str(q_ref),
        })

        qual.append({
            "teste_id": f"T{state['cnt_lote']}",
            "lote_id": lote_id,
            "produto_id": pid,
            "data_teste": (fim + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
            "tensao_medida_v": str(round(random.normalvariate(12.6, 0.2), 2)),
            "resistencia_interna_mohm": "6.0",
            "capacidade_ah_teste": "60.0",
            "defeito_id": "D00",
            "aprovado": "1" if q_prod > 0 else "0",
        })

        if temp > 100.0 or vib > 2000:
            alerts.append({
                "alerta_id": f"ALT-{int(time.time())}-{m['maquina_id']}-{uuid.uuid4().hex[:6]}",
                "data_ocorrencia": data_sim,
                "nivel": "CRITICO",
                "maquina_id": m["maquina_id"],
                "mensagem": "Anomalia Detectada (Physics/ML Pattern)",
                "valor_medido": float(temp),
            })

    return prod, lote, qual, alerts

def gerar_map_lote_compras(lotes: list[dict], compras: list[dict]):
    if not lotes or not compras:
        return []
    rows = []
    compra_ids = [c["compra_id"] for c in compras]
    for l in lotes[:max(1, len(lotes)//4)]:
        rows.append({"lote_id": l["lote_id"], "compra_id": random.choice(compra_ids)})
    return rows

def gerar_vendas(data_sim: datetime, state: dict, lotes: list[dict]):
    rows = []
    if not state["_clientes"] or not lotes:
        return rows
    amostra = lotes[:max(1, len(lotes)//5)]
    for l in amostra:
        state["cnt_venda"] += 1
        cid = random.choice(state["_clientes"])
        rows.append({
            "venda_id": f"V{state['cnt_venda']}",
            "ano_mes_id": data_sim.strftime("%Y-%m"),
            "cliente_id": cid,
            "produto_id": l["produto_id"],
            "ordem_producao_id": "",
            "lote_id": l["lote_id"],
            "data_venda": data_sim.date().isoformat(),
            "quantidade_vendida": str(random.randint(20, 120)),
            "valor_total_venda": str(round(random.uniform(5000, 15000), 2)),
        })
    return rows

def gerar_garantia(data_sim: datetime, state: dict, vendas: list[dict]):
    rows = []
    for v in vendas:
        if random.random() < 0.01:
            state["cnt_garantia"] += 1
            dias = random.randint(1, 90)
            status = random.choice(["Aprovada","Negada - Mau Uso","Negada - Produto Funcional"])
            custo = round(random.uniform(200, 3000), 2) if "Aprovada" in status else 0.0
            rows.append({
                "garantia_id": f"W{state['cnt_garantia']}",
                "cliente_id": v["cliente_id"],
                "produto_id": v["produto_id"],
                "lote_id": v["lote_id"],
                "data_reclamacao": (data_sim + timedelta(days=dias)).strftime("%Y-%m-%d %H:%M:%S"),
                "dias_pos_venda": str(dias),
                "defeito_id": random.choice(["D00","D01","D02","D03","D04","D05"]),
                "status": status,
                "tempo_resposta_dias": str(random.randint(1, 15)),
                "custo_garantia": str(custo)
            })
    return rows

def gerar_manutencao(data_sim: datetime, state: dict, fleet: list[dict]):
    rows = []
    if random.random() < 0.05:
        state["cnt_manut"] += 1
        m = random.choice(fleet)
        ini = data_sim
        fim = ini + timedelta(hours=2)
        rows.append({
            "evento_manutencao_id": f"EVM{state['cnt_manut']}",
            "maquina_id": m["maquina_id"],
            "linha_id": m["linha_id"],
            "tipo_manutencao_id": random.choice(["TM01","TM02","TM03"]),
            "inicio": ini.strftime("%Y-%m-%d %H:%M:%S"),
            "fim": fim.strftime("%Y-%m-%d %H:%M:%S"),
            "duracao_min": "120",
            "criticidade": random.choice(["Baixa","Média","Alta"])
        })
    return rows

# =========================
# MAIN
# =========================
@functions_framework.http
def executar_simulacao(request):
    """
    Query params:
      mode=backfill&start=2022-01-01&end=2022-12-31   (diário, só GCS)
      mode=incremental                                (hora, GCS + BQ só ano atual)
    """
    args = request.args or {}
    mode = args.get("mode", "incremental")
    start = args.get("start")
    end = args.get("end")

    run_id = str(uuid.uuid4())
    sc = gcs_client()
    state = load_state(sc)

    random.seed(state.get("seed", 42))
    np.random.seed(state.get("seed", 42))

    bq = bq_client()
    ensure_dataset_and_tables(bq)

    # static boot (uma vez) — NÃO toca raw_usuario e raw_controle_acesso
    if not state.get("static_done", False):
        now = datetime.now(TZ_BR)
        tempo = gerar_dim_tempo_2022_2027()
        metas = [{"meta_id": f"M{idx:03d}", "ano_mes_id": r["ano_mes_id"], "meta_quantidade": "2000", "meta_valor": "500000"} for idx, r in enumerate(tempo, start=1)]
        fleet = get_or_create_fleet(state)

        static_payloads = dict(DADOS_ESTATICOS)
        static_payloads["raw_tempo"] = tempo
        static_payloads["raw_metas_vendas"] = metas
        static_payloads["raw_maquina"] = fleet

        for tbl, rows in static_payloads.items():
            uri = gcs_write_jsonl(sc, tbl, rows, run_id, now)
            if should_load_to_bq(now):
                bq_load(bq, tbl, uri)

        state["static_done"] = True
        save_state(sc, state)

    fleet = get_or_create_fleet(state)

    if mode == "backfill":
        if not start or not end:
            return "ERRO: backfill requer start e end (YYYY-MM-DD).", 400

        dt_start = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=TZ_BR)
        dt_end = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=TZ_BR)

        cur = dt_start
        totals = {k:0 for k in ["cli","comp","map","prod","lote","qual","vend","gar","man","alt"]}

        while cur <= dt_end:
            cli = gerar_clientes(cur, state)
            comp = gerar_compras(cur, state)
            prod, lote, qual, alt = gerar_producao_lote_qual_alertas(cur, state, fleet)
            mp = gerar_map_lote_compras(lote, comp)
            vend = gerar_vendas(cur, state, lote)
            gar = gerar_garantia(cur, state, vend)
            man = gerar_manutencao(cur, state, fleet)

            payloads = [
                ("raw_cliente", cli),
                ("raw_compras", comp),
                ("raw_map_lote_compras", mp),
                ("raw_producao", prod),
                ("raw_lote", lote),
                ("raw_qualidade", qual),
                ("raw_vendas", vend),
                ("raw_garantia", gar),
                ("raw_manutencao", man),
                ("monitoramento_alertas", alt),
            ]

            for tbl, rows in payloads:
                gcs_write_jsonl(sc, tbl, rows, run_id, cur)  # backfill: só GCS

            totals["cli"] += len(cli); totals["comp"] += len(comp); totals["map"] += len(mp)
            totals["prod"] += len(prod); totals["lote"] += len(lote); totals["qual"] += len(qual)
            totals["vend"] += len(vend); totals["gar"] += len(gar); totals["man"] += len(man); totals["alt"] += len(alt)

            cur += timedelta(days=1)

        save_state(sc, state)
        return f"OK backfill run_id={run_id} | {totals}", 200

    # incremental
    now = datetime.now(TZ_BR)
    totals = {k:0 for k in ["cli","comp","map","prod","lote","qual","vend","gar","man","alt"]}

    cur = now
    for _ in range(HORAS_POR_LOTE):
        cli = gerar_clientes(cur, state)
        comp = gerar_compras(cur, state)
        prod, lote, qual, alt = gerar_producao_lote_qual_alertas(cur, state, fleet)
        mp = gerar_map_lote_compras(lote, comp)
        vend = gerar_vendas(cur, state, lote)
        gar = gerar_garantia(cur, state, vend)
        man = gerar_manutencao(cur, state, fleet)

        payloads = [
            ("raw_cliente", cli),
            ("raw_compras", comp),
            ("raw_map_lote_compras", mp),
            ("raw_producao", prod),
            ("raw_lote", lote),
            ("raw_qualidade", qual),
            ("raw_vendas", vend),
            ("raw_garantia", gar),
            ("raw_manutencao", man),
            ("monitoramento_alertas", alt),
        ]

        for tbl, rows in payloads:
            uri = gcs_write_jsonl(sc, tbl, rows, run_id, cur)
            if should_load_to_bq(cur):
                bq_load(bq, tbl, uri)

        totals["cli"] += len(cli); totals["comp"] += len(comp); totals["map"] += len(mp)
        totals["prod"] += len(prod); totals["lote"] += len(lote); totals["qual"] += len(qual)
        totals["vend"] += len(vend); totals["gar"] += len(gar); totals["man"] += len(man); totals["alt"] += len(alt)

        cur += timedelta(hours=1)

    save_state(sc, state)
    return f"OK incremental run_id={run_id} | {totals}", 200
