# =========================
# AUTOVOLT LAKEHOUSE PIPELINE - FINAL
# =========================

import functions_framework
import warnings
warnings.simplefilter("ignore", category=FutureWarning)

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

STATE_FILE = "state/state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# =========================
# SCHEMAS (SEM raw_usuario / raw_controle_acesso)
# =========================

SCHEMAS = {

    "raw_linha": [
        SchemaField("linha_id","STRING"),
        SchemaField("descricao","STRING"),
        SchemaField("turnos_operacionais","STRING")
    ],

    "raw_metas_vendas": [
        SchemaField("meta_id","STRING"),
        SchemaField("ano_mes_id","STRING"),
        SchemaField("meta_quantidade","STRING"),
        SchemaField("meta_valor","STRING")
    ],

    "raw_tempo": [
        SchemaField("ano_mes_id","STRING"),
        SchemaField("ano","STRING"),
        SchemaField("mes","STRING"),
        SchemaField("nome_mes","STRING"),
        SchemaField("trimestre","STRING"),
        SchemaField("ano_mes_label","STRING")
    ],

    "raw_tipo_manut": [
        SchemaField("tipo_manutencao_id","STRING"),
        SchemaField("descricao","STRING"),
        SchemaField("criticidade_padrao","STRING")
    ],

    "raw_turno": [
        SchemaField("turno_id","STRING"),
        SchemaField("janela","STRING"),
        SchemaField("coef_performance","STRING")
    ],

    "raw_produto": [
        SchemaField("produto_id","STRING"),
        SchemaField("modelo","STRING"),
        SchemaField("tensao_v","STRING"),
        SchemaField("capacidade_ah","STRING"),
        SchemaField("linha_segmento","STRING"),
        SchemaField("data_lancamento","STRING"),
        SchemaField("data_descontinuacao","STRING")
    ],

    "raw_maquina": [
        SchemaField("maquina_id","STRING"),
        SchemaField("tipo","STRING"),
        SchemaField("fabricante","STRING"),
        SchemaField("ano","STRING"),
        SchemaField("linha_id","STRING")
    ],

    "raw_fornecedor": [
        SchemaField("fornecedor_id","STRING"),
        SchemaField("categoria","STRING"),
        SchemaField("leadtime_dias","STRING"),
        SchemaField("qualificacao","STRING"),
        SchemaField("data_cadastro","STRING"),
        SchemaField("data_ultima_avaliacao","STRING"),
        SchemaField("descricao","STRING")
    ],

    "raw_defeito": [
        SchemaField("defeito_id","STRING"),
        SchemaField("descricao","STRING"),
        SchemaField("gravidade","STRING")
    ],

    "raw_materia_prima": [
        SchemaField("materia_prima_id","STRING"),
        SchemaField("nome_material","STRING")
    ],

    "raw_cliente": [
        SchemaField("cliente_id","STRING"),
        SchemaField("tipo_cliente","STRING"),
        SchemaField("cidade","STRING"),
        SchemaField("tipo_plano","STRING"),
        SchemaField("data_cadastro","STRING"),
        SchemaField("data_ultima_compra","STRING")
    ],

    "raw_lote": [
        SchemaField("lote_id","STRING"),
        SchemaField("produto_id","STRING"),
        SchemaField("linha_id","STRING"),
        SchemaField("maquina_id","STRING"),
        SchemaField("inicio_producao","STRING"),
        SchemaField("fim_producao","STRING"),
        SchemaField("duracao_horas","STRING")
    ],

    "raw_producao": [
        SchemaField("ordem_producao_id","STRING"),
        SchemaField("lote_id","STRING"),
        SchemaField("produto_id","STRING"),
        SchemaField("linha_id","STRING"),
        SchemaField("maquina_id","STRING"),
        SchemaField("turno_id","STRING"),
        SchemaField("inicio","STRING"),
        SchemaField("ciclo_minuto_nominal","STRING"),
        SchemaField("duracao_horas","STRING"),
        SchemaField("temperatura_media_c","STRING"),
        SchemaField("vibracao_media_rpm","STRING"),
        SchemaField("pressao_media_bar","STRING"),
        SchemaField("quantidade_planejada","STRING"),
        SchemaField("quantidade_produzida","STRING"),
        SchemaField("quantidade_refugada","STRING")
    ],

    "raw_qualidade": [
        SchemaField("teste_id","STRING"),
        SchemaField("lote_id","STRING"),
        SchemaField("produto_id","STRING"),
        SchemaField("data_teste","STRING"),
        SchemaField("tensao_medida_v","STRING"),
        SchemaField("resistencia_interna_mohm","STRING"),
        SchemaField("capacidade_ah_teste","STRING"),
        SchemaField("defeito_id","STRING"),
        SchemaField("aprovado","STRING")
    ],

    "raw_compras": [
        SchemaField("compra_id","STRING"),
        SchemaField("fornecedor_id","STRING"),
        SchemaField("materia_prima_id","STRING"),
        SchemaField("data_compra","STRING"),
        SchemaField("quantidade_comprada","STRING"),
        SchemaField("custo_unitario","STRING"),
        SchemaField("custo_total","STRING")
    ],

    "raw_map_lote_compras": [
        SchemaField("lote_id","STRING"),
        SchemaField("compra_id","STRING")
    ],

    "raw_vendas": [
        SchemaField("venda_id","STRING"),
        SchemaField("ano_mes_id","STRING"),
        SchemaField("cliente_id","STRING"),
        SchemaField("produto_id","STRING"),
        SchemaField("ordem_producao_id","STRING"),
        SchemaField("lote_id","STRING"),
        SchemaField("data_venda","STRING"),
        SchemaField("quantidade_vendida","STRING"),
        SchemaField("valor_total_venda","STRING")
    ],

    "raw_garantia": [
        SchemaField("garantia_id","STRING"),
        SchemaField("cliente_id","STRING"),
        SchemaField("produto_id","STRING"),
        SchemaField("lote_id","STRING"),
        SchemaField("data_reclamacao","STRING"),
        SchemaField("dias_pos_venda","STRING"),
        SchemaField("defeito_id","STRING"),
        SchemaField("status","STRING"),
        SchemaField("tempo_resposta_dias","STRING"),
        SchemaField("custo_garantia","STRING")
    ],

    "raw_manutencao": [
        SchemaField("evento_manutencao_id","STRING"),
        SchemaField("maquina_id","STRING"),
        SchemaField("linha_id","STRING"),
        SchemaField("tipo_manutencao_id","STRING"),
        SchemaField("inicio","STRING"),
        SchemaField("fim","STRING"),
        SchemaField("duracao_min","STRING"),
        SchemaField("criticidade","STRING")
    ],

    "monitoramento_alertas": [
        SchemaField("alerta_id","STRING"),
        SchemaField("data_ocorrencia","TIMESTAMP"),
        SchemaField("nivel","STRING"),
        SchemaField("maquina_id","STRING"),
        SchemaField("mensagem","STRING"),
        SchemaField("valor_medido","FLOAT64")
    ],
}


# =========================
# CLIENTS
# =========================

def bq(): return bigquery.Client(project=PROJECT_ID)
def gcs(): return storage.Client(project=PROJECT_ID)


# =========================
# SETUP
# =========================

def setup_bq(client):

    ds = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")

    try:
        client.get_dataset(ds)
    except:
        client.create_dataset(ds)

    for t, s in SCHEMAS.items():

        ref = f"{PROJECT_ID}.{DATASET_ID}.{t}"

        try:
            client.get_table(ref)
        except:
            client.create_table(bigquery.Table(ref, schema=s))


# =========================
# STATE
# =========================

def load_state(sc):

    blob = sc.bucket(BUCKET_NAME).blob(STATE_FILE)

    if not blob.exists():

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
            "fleet": None
        }

    return json.loads(blob.download_as_text())


def save_state(sc, state):

    sc.bucket(BUCKET_NAME).blob(STATE_FILE).upload_from_string(
        json.dumps(state, indent=2),
        content_type="application/json"
    )


# =========================
# HELPERS
# =========================

def turno(dt):

    h = dt.hour

    if 6 <= h < 14: return "T1"
    if 14 <= h < 22: return "T2"
    return "T3"


def write_gcs(sc, table, rows, run_id, dt):

    if not rows: return ""

    path = f"bronze/{table}/dt={dt.date()}/hr={dt.hour}/run={run_id}"
    name = f"part-{uuid.uuid4().hex}.jsonl"

    blob = sc.bucket(BUCKET_NAME).blob(f"{path}/{name}")

    buf = io.StringIO()

    for r in rows:

        rr = dict(r)
        rr["_run"] = run_id
        rr["_ingested"] = datetime.now(TZ_BR).isoformat()

        buf.write(json.dumps(rr, ensure_ascii=False))
        buf.write("\n")

    blob.upload_from_string(buf.getvalue(), content_type="application/json")

    return f"gs://{BUCKET_NAME}/{path}/{name}"


def load_bq(client, table, uri):

    if not uri: return

    job = bigquery.LoadJobConfig(
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        schema=SCHEMAS[table],
        ignore_unknown_values=True
    )

    client.load_table_from_uri(
        uri,
        f"{PROJECT_ID}.{DATASET_ID}.{table}",
        job_config=job
    ).result()


def hot_layer(dt):

    return dt.year == datetime.now(TZ_BR).year


# =========================
# GENERATORS
# =========================

def gen_clients(dt, state):

    rows = []

    if not state["clientes"]:

        state["cnt_cliente"] += 1

        cid = f"C{state['cnt_cliente']:04d}"

        state["clientes"].append(cid)

        rows.append({
            "cliente_id": cid,
            "tipo_cliente": "Distribuidor",
            "cidade": "SP",
            "tipo_plano": "Standard",
            "data_cadastro": "2022-01-01",
            "data_ultima_compra": ""
        })

    if random.random() < 0.1:

        state["cnt_cliente"] += 1

        cid = f"C{state['cnt_cliente']:04d}"

        state["clientes"].append(cid)

        rows.append({
            "cliente_id": cid,
            "tipo_cliente": random.choice(["Distribuidor","Autopeças","Montadora"]),
            "cidade": random.choice(["SP","RJ","MG","RS","PE","BA"]),
            "tipo_plano": random.choice(["Básico","Standard","Premium"]),
            "data_cadastro": dt.date().isoformat(),
            "data_ultima_compra": ""
        })

    return rows


def gen_fleet(state):

    if state["fleet"]:
        return state["fleet"]

    fleet = []

    for i in range(1, 21):

        fleet.append({
            "maquina_id": f"M{i:03d}",
            "tipo": random.choice(["Montadora","Injetora","Envasadora","Robo","Tester"]),
            "fabricante": random.choice(["Siemens","Bosch","ABB","Kuka"]),
            "ano": str(random.choice([2021,2022,2023,2024])),
            "linha_id": random.choice(["L01","L02","L03","L04","L05"])
        })

    state["fleet"] = fleet

    return fleet


def desgaste(ano, dt):

    idade = max(0, dt.year - int(ano))

    f = 1 + idade*0.03

    temp = np.random.normal(65*f,3)
    vib = np.random.normal(1200*f,150)

    perf = max(0.85,1-idade*0.01)

    return round(temp,1),round(vib,0),perf


def oee(dur, ciclo, perf):

    cap = int((dur*60)/ciclo)

    plan = int(cap*0.95)

    eff = np.clip(np.random.normal(perf,0.05),0,1)

    prod = int(plan*eff)

    ref = int(prod*random.uniform(0,0.01+(1-perf)))

    return plan,prod,ref


def gen_production(dt, state, fleet):

    prod=[]; lote=[]; qual=[]; alerts=[]

    produtos=["BAT001","BAT002","BAT003","BAT004","BAT005"]

    for m in fleet:

        state["cnt_op"]+=1
        state["cnt_lote"]+=1

        op=f"OP{state['cnt_op']}"
        lid=f"Lote{state['cnt_lote']}"

        temp,vib,perf = desgaste(m["ano"],dt)

        dur=round(random.uniform(0.8,1),2)
        fim=dt+timedelta(hours=dur)

        pid=random.choice(produtos)

        plan,pr,rf=oee(dur,0.5,perf)

        lote.append({
            "lote_id":lid,
            "produto_id":pid,
            "linha_id":m["linha_id"],
            "maquina_id":m["maquina_id"],
            "inicio_producao":dt.strftime("%F %T"),
            "fim_producao":fim.strftime("%F %T"),
            "duracao_horas":str(dur)
        })

        prod.append({
            "ordem_producao_id":op,
            "lote_id":lid,
            "produto_id":pid,
            "linha_id":m["linha_id"],
            "maquina_id":m["maquina_id"],
            "turno_id":turno(dt),
            "inicio":dt.strftime("%F %T"),
            "ciclo_minuto_nominal":"0.5",
            "duracao_horas":str(dur),
            "temperatura_media_c":str(temp),
            "vibracao_media_rpm":str(vib),
            "pressao_media_bar":str(round(random.uniform(6,8),1)),
            "quantidade_planejada":str(plan),
            "quantidade_produzida":str(pr),
            "quantidade_refugada":str(rf)
        })

        qual.append({
            "teste_id":f"T{state['cnt_lote']}",
            "lote_id":lid,
            "produto_id":pid,
            "data_teste":(fim+timedelta(minutes=10)).strftime("%F %T"),
            "tensao_medida_v":str(round(random.normalvariate(12.6,0.2),2)),
            "resistencia_interna_mohm":"6",
            "capacidade_ah_teste":"60",
            "defeito_id":"D00",
            "aprovado":"1" if pr>0 else "0"
        })

        if temp>100 or vib>2000:

            alerts.append({
                "alerta_id":f"ALT-{uuid.uuid4().hex[:8]}",
                "data_ocorrencia":dt,
                "nivel":"CRITICO",
                "maquina_id":m["maquina_id"],
                "mensagem":"Anomalia Detectada",
                "valor_medido":float(temp)
            })

    return prod,lote,qual,alerts


def gen_sales(dt,state,lotes):

    rows=[]

    if not state["clientes"]: return rows

    for l in lotes[:max(1,len(lotes)//5)]:

        state["cnt_venda"]+=1

        rows.append({
            "venda_id":f"V{state['cnt_venda']}",
            "ano_mes_id":dt.strftime("%Y-%m"),
            "cliente_id":random.choice(state["clientes"]),
            "produto_id":l["produto_id"],
            "ordem_producao_id":"",
            "lote_id":l["lote_id"],
            "data_venda":dt.date().isoformat(),
            "quantidade_vendida":str(random.randint(20,120)),
            "valor_total_venda":str(round(random.uniform(5_000,15_000),2))
        })

    return rows


def gen_guarantee(dt,state,vendas):

    rows=[]

    for v in vendas:

        if random.random()<0.01:

            state["cnt_garantia"]+=1

            dias=random.randint(1,90)

            status=random.choice(["Aprovada","Negada","Negada - Mau Uso"])

            custo=round(random.uniform(200,3000),2) if "Aprovada" in status else 0

            rows.append({
                "garantia_id":f"W{state['cnt_garantia']}",
                "cliente_id":v["cliente_id"],
                "produto_id":v["produto_id"],
                "lote_id":v["lote_id"],
                "data_reclamacao":(dt+timedelta(days=dias)).strftime("%F %T"),
                "dias_pos_venda":str(dias),
                "defeito_id":"D00",
                "status":status,
                "tempo_resposta_dias":str(random.randint(1,15)),
                "custo_garantia":str(custo)
            })

    return rows


def gen_maintenance(dt,state,fleet):

    rows=[]

    if random.random()<0.05:

        state["cnt_manut"]+=1

        m=random.choice(fleet)

        fim=dt+timedelta(hours=2)

        rows.append({
            "evento_manutencao_id":f"EVM{state['cnt_manut']}",
            "maquina_id":m["maquina_id"],
            "linha_id":m["linha_id"],
            "tipo_manutencao_id":random.choice(["TM01","TM02","TM03"]),
            "inicio":dt.strftime("%F %T"),
            "fim":fim.strftime("%F %T"),
            "duracao_min":"120",
            "criticidade":random.choice(["Baixa","Média","Alta"])
        })

    return rows


# =========================
# MAIN
# =========================

@functions_framework.http
def executar_simulacao(request):

    args=request.args or {}
    mode=args.get("mode","incremental")
    start=args.get("start")
    end=args.get("end")

    run_id=str(uuid.uuid4())

    sc=gcs()
    state=load_state(sc)

    random.seed(state["seed"])
    np.random.seed(state["seed"])

    client=bq()

    setup_bq(client)

    fleet=gen_fleet(state)

    # static (uma vez)
    if not state["static"]:

        # tempo
        tempo=[]

        for a in range(2022,2028):
            for m in range(1,13):

                tempo.append({
                    "ano_mes_id":f"{a}-{m:02d}",
                    "ano":str(a),
                    "mes":f"{m:02d}",
                    "nome_mes":["","Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"][m],
                    "trimestre":str((m-1)//3+1),
                    "ano_mes_label":f"{['','jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'][m]}/{str(a)[2:]}"
                })

        static_payloads={

            "raw_tempo":tempo,

            "raw_metas_vendas":[
                {"meta_id":f"M{idx:03d}","ano_mes_id":r["ano_mes_id"],"meta_quantidade":"2000","meta_valor":"500000"}
                for idx,r in enumerate(tempo,1)
            ],

            "raw_maquina":fleet
        }

        for tbl,rows in static_payloads.items():

            uri=write_gcs(sc,tbl,rows,run_id,datetime.now(TZ_BR))

            if hot_layer(datetime.now(TZ_BR)):
                load_bq(client,tbl,uri)

        state["static"]=True
        save_state(sc,state)


    # ================= BACKFILL =================
    if mode=="backfill":

        dt1=datetime.strptime(start,"%Y-%m-%d").replace(tzinfo=TZ_BR)
        dt2=datetime.strptime(end,"%Y-%m-%d").replace(tzinfo=TZ_BR)

        cur=dt1

        while cur<=dt2:

            cli=gen_clients(cur,state)
            comp=gen_clients(cur,state)

            prod,lote,qual,alt=gen_production(cur,state,fleet)

            vend=gen_sales(cur,state,lote)
            gar=gen_guarantee(cur,state,vend)
            man=gen_maintenance(cur,state,fleet)

            packs=[
                ("raw_cliente",cli),
                ("raw_compras",comp),
                ("raw_producao",prod),
                ("raw_lote",lote),
                ("raw_qualidade",qual),
                ("raw_vendas",vend),
                ("raw_garantia",gar),
                ("raw_manutencao",man),
                ("monitoramento_alertas",alt)
            ]

            for t,r in packs:
                write_gcs(sc,t,r,run_id,cur)

            cur+=timedelta(days=1)

        save_state(sc,state)

        return f"OK backfill {run_id}"


    # ================= INCREMENTAL =================

    now=datetime.now(TZ_BR)

    cur=now

    for _ in range(HORAS_POR_LOTE):

        cli=gen_clients(cur,state)
        comp=gen_clients(cur,state)

        prod,lote,qual,alt=gen_production(cur,state,fleet)

        vend=gen_sales(cur,state,lote)
        gar=gen_guarantee(cur,state,vend)
        man=gen_maintenance(cur,state,fleet)

        packs=[
            ("raw_cliente",cli),
            ("raw_compras",comp),
            ("raw_producao",prod),
            ("raw_lote",lote),
            ("raw_qualidade",qual),
            ("raw_vendas",vend),
            ("raw_garantia",gar),
            ("raw_manutencao",man),
            ("monitoramento_alertas",alt)
        ]

        for t,r in packs:

            uri=write_gcs(sc,t,r,run_id,cur)

            if hot_layer(cur):
                load_bq(client,t,uri)

        cur+=timedelta(hours=1)

    save_state(sc,state)

    return f"OK incremental {run_id}"
