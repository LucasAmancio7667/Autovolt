import functions_framework
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", module="google.cloud.bigquery")

import time
import random
import os
import logging
import json
import io
import pandas as pd
import numpy as np # Essencial para curvas normais (ML)
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

# --- CONFIGURAÇÃO ---
PROJECT_ID = "autovolt-analytics-479417"
DATASET_ID = "autovolt_bronze"
BUCKET_NAME = "bucket_ingestao" # <--- Nome corrigido conforme solicitado
HORAS_POR_LOTE = 1 

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SCHEMAS (Tipagem Forte) ---
SCHEMAS = {
    "raw_linha": [SchemaField("linha_id", "STRING"), SchemaField("descricao", "STRING"), SchemaField("turnos_operacionais", "STRING")],
    "raw_metas_vendas": [SchemaField("meta_id", "STRING"), SchemaField("ano_mes_id", "STRING"), SchemaField("meta_quantidade", "STRING"), SchemaField("meta_valor", "STRING")],
    "raw_tempo": [SchemaField("ano_mes_id", "STRING"), SchemaField("ano", "STRING"), SchemaField("mes", "STRING"), SchemaField("nome_mes", "STRING"), SchemaField("trimestre", "STRING"), SchemaField("ano_mes_label", "STRING")],
    "raw_tipo_manut": [SchemaField("tipo_manutencao_id", "STRING"), SchemaField("descricao", "STRING"), SchemaField("criticidade_padrao", "STRING")],
    "raw_turno": [SchemaField("turno_id", "STRING"), SchemaField("janela", "STRING"), SchemaField("coef_performance", "STRING")],
    "raw_producao": [SchemaField("ordem_producao_id", "STRING"), SchemaField("lote_id", "STRING"), SchemaField("produto_id", "STRING"), SchemaField("linha_id", "STRING"), SchemaField("maquina_id", "STRING"), SchemaField("turno_id", "STRING"), SchemaField("inicio", "STRING"), SchemaField("ciclo_minuto_nominal", "STRING"), SchemaField("duracao_horas", "STRING"), SchemaField("temperatura_media_c", "STRING"), SchemaField("vibracao_media_rpm", "STRING"), SchemaField("pressao_media_bar", "STRING"), SchemaField("quantidade_planejada", "STRING"), SchemaField("quantidade_produzida", "STRING"), SchemaField("quantidade_refugada", "STRING")],
    "raw_qualidade": [SchemaField("teste_id", "STRING"), SchemaField("lote_id", "STRING"), SchemaField("produto_id", "STRING"), SchemaField("data_teste", "STRING"), SchemaField("tensao_medida_v", "STRING"), SchemaField("resistencia_interna_mohm", "STRING"), SchemaField("capacidade_ah_teste", "STRING"), SchemaField("defeito_id", "STRING"), SchemaField("aprovado", "STRING")],
    "raw_produto": [SchemaField("produto_id", "STRING"), SchemaField("modelo", "STRING"), SchemaField("tensao_v", "STRING"), SchemaField("capacidade_ah", "STRING"), SchemaField("linha_segmento", "STRING"), SchemaField("data_lancamento", "STRING"), SchemaField("data_descontinuacao", "STRING")],
    "raw_maquina": [SchemaField("maquina_id", "STRING"), SchemaField("tipo", "STRING"), SchemaField("fabricante", "STRING"), SchemaField("ano", "STRING"), SchemaField("linha_id", "STRING")],
    "raw_fornecedor": [SchemaField("fornecedor_id", "STRING"), SchemaField("categoria", "STRING"), SchemaField("leadtime_dias", "STRING"), SchemaField("qualificacao", "STRING"), SchemaField("data_cadastro", "STRING"), SchemaField("data_ultima_avaliacao", "STRING"), SchemaField("descricao", "STRING")],
    "raw_defeito": [SchemaField("defeito_id", "STRING"), SchemaField("descricao", "STRING"), SchemaField("gravidade", "STRING")],
    "raw_materia_prima": [SchemaField("materia_prima_id", "STRING"), SchemaField("nome_material", "STRING")],
    "raw_cliente": [SchemaField("cliente_id", "STRING"), SchemaField("tipo_cliente", "STRING"), SchemaField("cidade", "STRING"), SchemaField("tipo_plano", "STRING"), SchemaField("data_cadastro", "STRING"), SchemaField("data_ultima_compra", "STRING")],
    "raw_lote": [SchemaField("lote_id", "STRING"), SchemaField("produto_id", "STRING"), SchemaField("linha_id", "STRING"), SchemaField("maquina_id", "STRING"), SchemaField("inicio_producao", "STRING"), SchemaField("fim_producao", "STRING"), SchemaField("duracao_horas", "STRING")],
    "raw_map_lote_compras": [SchemaField("lote_id", "STRING"), SchemaField("compra_id", "STRING")],
    "raw_compras": [SchemaField("compra_id", "STRING"), SchemaField("fornecedor_id", "STRING"), SchemaField("materia_prima_id", "STRING"), SchemaField("data_compra", "STRING"), SchemaField("quantidade_comprada", "STRING"), SchemaField("custo_unitario", "STRING"), SchemaField("custo_total", "STRING")],
    "raw_vendas": [SchemaField("venda_id", "STRING"), SchemaField("ano_mes_id", "STRING"), SchemaField("cliente_id", "STRING"), SchemaField("produto_id", "STRING"), SchemaField("ordem_producao_id", "STRING"), SchemaField("data_venda", "STRING"), SchemaField("quantidade_vendida", "STRING"), SchemaField("valor_total_venda", "STRING")],
    "raw_garantia": [SchemaField("garantia_id", "STRING"), SchemaField("cliente_id", "STRING"), SchemaField("produto_id", "STRING"), SchemaField("lote_id", "STRING"), SchemaField("data_reclamacao", "STRING"), SchemaField("dias_pos_venda", "STRING"), SchemaField("defeito_id", "STRING"), SchemaField("status", "STRING"), SchemaField("tempo_resposta_dias", "STRING"), SchemaField("custo_garantia", "STRING")],
    "raw_manutencao": [SchemaField("evento_manutencao_id", "STRING"), SchemaField("maquina_id", "STRING"), SchemaField("linha_id", "STRING"), SchemaField("tipo_manutencao_id", "STRING"), SchemaField("inicio", "STRING"), SchemaField("fim", "STRING"), SchemaField("duracao_min", "STRING"), SchemaField("criticidade", "STRING")],
    "raw_controle_acesso": [SchemaField("email_usuario", "STRING"), SchemaField("cargo", "STRING")],
    "monitoramento_alertas": [SchemaField("alerta_id", "STRING"), SchemaField("data_ocorrencia", "TIMESTAMP"), SchemaField("nivel", "STRING"), SchemaField("maquina_id", "STRING"), SchemaField("mensagem", "STRING"), SchemaField("valor_medido", "FLOAT64")]
}

# --- DADOS ESTÁTICOS ---
DADOS_ESTATICOS = {
    "raw_linha": [
        {"linha_id": "L01", "descricao": "Linha de Montagem Automotiva", "turnos_operacionais": "3"},
        {"linha_id": "L02", "descricao": "Linha de Injeção Plástica", "turnos_operacionais": "3"},
        {"linha_id": "L03", "descricao": "Linha de Envase Químico", "turnos_operacionais": "3"},
        {"linha_id": "L04", "descricao": "Linha de Pesados", "turnos_operacionais": "2"},
        {"linha_id": "L05", "descricao": "Linha de Testes", "turnos_operacionais": "1"}
    ],
    "raw_metas_vendas": [{"meta_id": f"M{i}", "ano_mes_id": f"2025-{i:02d}", "meta_quantidade": "2000", "meta_valor": "500000"} for i in range(1, 13)],
    "raw_turno": [
        {"turno_id": "T1", "janela": "06:00 - 14:00", "coef_performance": "1.0"},
        {"turno_id": "T2", "janela": "14:00 - 22:00", "coef_performance": "0.98"},
        {"turno_id": "T3", "janela": "22:00 - 06:00", "coef_performance": "0.95"}
    ],
    "raw_tipo_manut": [
        {"tipo_manutencao_id": "TM01", "descricao": "Preventiva", "criticidade_padrao": "Baixa"},
        {"tipo_manutencao_id": "TM02", "descricao": "Corretiva", "criticidade_padrao": "Alta"},
        {"tipo_manutencao_id": "TM03", "descricao": "Preditiva", "criticidade_padrao": "Média"}
    ]
}

ESTADOS_BRASIL = ["SP", "RJ", "MG", "RS", "PE", "BA", "PR", "SC"]
CACHE = {"CLIENTES": [], "PRODUTOS": {}, "MAQUINAS": [], "LINHAS": [], "MATERIAS": {}, "DEFEITOS": []}
estoque_materia_prima = []; estoque_produtos_acabados = []; historico_vendas = []
# Contadores Globais (Respeitando sua lógica sequencial)
cnt_compra=0; cnt_op=0; cnt_lote=0; cnt_venda=0; cnt_garantia=0; cnt_manut=0; cnt_cliente=0

# --- MOTOR FÍSICO & ML (NOVO) ---
def calcular_desgaste_maquina(maquina_info, data_sim):
    """
    Calcula parâmetros físicos baseados na idade da máquina.
    Isso gera o padrão para o algoritmo de Machine Learning detectar anomalias futuras.
    """
    ano_fab = int(maquina_info.get("ano", 2020))
    idade_anos = data_sim.year - ano_fab
    
    # Fator de degradação: Máquinas velhas vibram mais e esquentam mais
    # 2023 (máquina nova) -> Fator perto de 1.0
    # 2026 (máquina velha) -> Fator perto de 1.15 ou mais
    fator_idade = 1 + (max(0, idade_anos) * 0.03) 

    # Base Physics (Normal Distribution)
    # Temperatura: Base 65ºC + Idade
    temp_base = 65.0 * fator_idade
    temp_real = np.random.normal(temp_base, 3.0) # Variação normal

    # Vibração: Base 1200 RPM + Idade
    vib_base = 1200.0 * fator_idade
    vib_real = np.random.normal(vib_base, 150.0)

    # Performance cai levemente com a idade
    performance_max = max(0.85, 1.0 - (idade_anos * 0.01))
    
    return round(temp_real, 1), round(vib_real, 0), performance_max

def calcular_oee_fisico(duracao_horas, ciclo_minutos_nominal, performance_fator):
    """
    CORREÇÃO DO OEE:
    Calcula a produção baseada na capacidade física real, não em números aleatórios.
    """
    minutos_totais = duracao_horas * 60
    
    # Capacidade Teórica (Se a máquina não parasse nunca)
    capacidade_maxima = int(minutos_totais / ciclo_minutos_nominal)
    
    # Planejado (Geralmente 90-95% da capacidade máxima)
    qtd_planejada = int(capacidade_maxima * 0.95)
    
    # Realizado (Afetado pela performance da máquina/idade)
    # Variação natural do processo (Gaussian)
    eficiencia_real = np.random.normal(performance_fator, 0.05) # Média performance_fator, desvio 5%
    eficiencia_real = min(1.0, max(0.0, eficiencia_real)) # Trava entre 0% e 100%
    
    qtd_produzida = int(qtd_planejada * eficiencia_real)
    
    # Refugo (Máquinas piores refugam mais)
    taxa_refugo = 0.01 + (1.0 - performance_fator) # Se performance cai, refugo sobe
    qtd_refugada = int(qtd_produzida * np.random.uniform(0.0, taxa_refugo))
    
    return qtd_planejada, qtd_produzida, qtd_refugada

# --- FUNÇÕES DE INFRAESTRUTURA (BQ + GCS) ---
def conectar_bq(): return bigquery.Client(project=PROJECT_ID)
def conectar_gcs(): return storage.Client(project=PROJECT_ID)

def enviar_bq(client, dados, tabela):
    if not dados: return
    try:
        # 1. JSONL em Memória
        buffer = io.StringIO()
        timestamp_agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nome_arquivo = f"{tabela}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        for row in dados:
            row_str = {k: str(v) for k, v in row.items()}
            # Metadados de Auditoria
            row_str['metadata_data_ingestao'] = timestamp_agora
            row_str['metadata_arquivo_origem'] = nome_arquivo
            json.dump(row_str, buffer, ensure_ascii=False)
            buffer.write('\n')

        # 2. Upload para Bucket (Data Lake Bronze)
        data_path = datetime.now()
        blob_name = f"bronze/{tabela}/{data_path.year}/{data_path.month:02d}/{data_path.day:02d}/{nome_arquivo}"
        
        storage_client = conectar_gcs()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(buffer.getvalue(), content_type='application/json')
        
        # 3. Load Job (Gratuito e Auditável)
        dataset_ref = client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(tabela)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            autodetect=True,
            ignore_unknown_values=True,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        uri = f"gs://{BUCKET_NAME}/{blob_name}"
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()

        logging.info(f"✅ {tabela}: Inseridos {len(dados)} registros via Lakehouse.")

    except Exception as e:
        logging.error(f"❌ Falha em {tabela}: {e}")

# --- UTILITÁRIOS ---
def obter_max_id(client, tabela, coluna, prefixo):
    # Função segura para manter seus IDs sequenciais
    try:
        q = f"SELECT MAX(CAST(REGEXP_EXTRACT({coluna}, r'{prefixo}(\\d+)') AS INT64)) FROM `{PROJECT_ID}.{DATASET_ID}.{tabela}`"
        r = list(client.query(q).result())
        if r and r[0][0]: return int(r[0][0])
    except: pass
    return 0

def calcular_turno(data_hora):
    h = data_hora.hour
    if 6 <= h < 14: return "T1"
    elif 14 <= h < 22: return "T2"
    else: return "T3"

# --- GERADORES DE NEGÓCIO ---

def gerar_producao(client, data_sim):
    global cnt_op, cnt_lote, estoque_produtos_acabados, CACHE
    
    d_fact=[]; d_dim=[]; d_map=[]; d_qual=[]
    
    # Se cache vazio, criar defaults
    if not CACHE["MAQUINAS"]: CACHE["MAQUINAS"] = [{"id": f"M{i:03d}", "ano": 2020} for i in range(1,21)]
    if not CACHE["LINHAS"]: CACHE["LINHAS"] = ["L01"]
    
    produtos_keys = list(CACHE["PRODUTOS"].keys()) or ["BAT001"]

    for maq in CACHE["MAQUINAS"]: # maq agora é um dict com 'id' e 'ano'
        mid = maq["id"]
        
        # 1. Calcular Física ML (Idade impactando vibração/temp)
        temp, vib, perf_factor = calcular_desgaste_maquina(maq, data_sim)
        
        cnt_op += 1; cnt_lote += 1
        op_id = f"OP{cnt_op}"; lid = f"Lote{cnt_lote}"
        
        # Simula produção cobrindo quase toda a hora (Alta Disponibilidade para OEE subir)
        ini = data_sim
        dur = round(random.uniform(0.8, 1.0), 2) # Produziu entre 48 e 60 min da hora
        fim = ini + timedelta(hours=dur)
        
        pid = random.choice(produtos_keys)
        linha = random.choice(CACHE["LINHAS"])

        # 2. Calcular Produção baseada na Física (Conserta os 14% de OEE)
        ciclo_nominal = 0.5 # 30 segundos por bateria (realista)
        q_plan, q_prod, q_ref = calcular_oee_fisico(dur, ciclo_nominal, perf_factor)

        # Inserção Dimensão
        d_dim.append({
            "lote_id": lid, "produto_id": pid, "linha_id": linha, "maquina_id": mid,
            "inicio_producao": ini.strftime("%Y-%m-%d %H:%M:%S"),
            "fim_producao": fim.strftime("%Y-%m-%d %H:%M:%S"), "duracao_horas": dur
        })
        
        # Inserção Fato
        d_fact.append({
            "ordem_producao_id": op_id, "lote_id": lid, "produto_id": pid, "linha_id": linha,
            "maquina_id": mid, "turno_id": calcular_turno(ini), "inicio": ini.strftime("%Y-%m-%d %H:%M:%S"),
            "temperatura_media_c": temp, "vibracao_media_rpm": vib, "pressao_media_bar": round(random.uniform(6,8),1),
            "ciclo_minuto_nominal": ciclo_nominal, "duracao_horas": dur,
            "quantidade_planejada": q_plan, "quantidade_produzida": q_prod, "quantidade_refugada": q_ref
        })

        # Alerta ML (Se fugir muito do padrão)
        if temp > 100.0 or vib > 2000:
            alerta = {"alerta_id": f"ALT-{int(time.time())}-{mid}", "data_ocorrencia": ini.strftime("%Y-%m-%d %H:%M:%S"), "nivel": "CRITICO", "maquina_id": mid, "mensagem": "Anomalia Detectada (ML Pattern)", "valor_medido": temp}
            enviar_bq(client, [alerta], "monitoramento_alertas")

        # Qualidade
        aprovado = 1 if q_prod > 0 else 0
        d_qual.append({
            "teste_id": f"T{cnt_lote}", "lote_id": lid, "produto_id": pid,
            "data_teste": (fim+timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
            "tensao_medida_v": round(random.normalvariate(12.6, 0.2), 2),
            "resistencia_interna_mohm": 6.0, "capacidade_ah_teste": 60.0,
            "defeito_id": "D00", "aprovado": aprovado
        })
        estoque_produtos_acabados.append({"lote_id": lid, "produto_id": pid, "op_id": op_id})

    return d_fact, d_dim, d_map, d_qual

def gerar_clientes_genesis(data_sim):
    """Gera clientes com datas retroativas se a base estiver vazia"""
    global cnt_cliente, CACHE
    dados = []
    # Se temos poucos clientes, criar um lote 'antigo'
    if len(CACHE["CLIENTES"]) < 50:
        for _ in range(10):
            cnt_cliente += 1; nid = f"C{cnt_cliente:04d}"
            # Data de cadastro retroativa (para suportar simulação de 2023)
            data_cad = data_sim - timedelta(days=random.randint(30, 700)) 
            cli = {"cliente_id": nid, "tipo_cliente": "Distribuidor", "cidade": random.choice(ESTADOS_BRASIL), "tipo_plano": "Standard", "data_cadastro": data_cad.strftime("%Y-%m-%d"), "data_ultima_compra": ""}
            dados.append(cli); CACHE["CLIENTES"].append(nid)
    return dados

def gerar_vendas(data_sim):
    global cnt_venda, CACHE
    dados = []
    if not estoque_produtos_acabados: return []
    
    # Vendas consistentes
    qtd_vendas = random.randint(1, len(CACHE["MAQUINAS"])) # Vende proporcional ao parque fabril
    
    for _ in range(qtd_vendas):
        if not estoque_produtos_acabados: break
        item = estoque_produtos_acabados.pop(0)
        cnt_venda += 1
        
        # Pega cliente existente (para não criar cliente novo com data futura)
        cli_id = random.choice(CACHE["CLIENTES"]) if CACHE["CLIENTES"] else "C001"
        
        dados.append({
            "venda_id": f"V{cnt_venda}", "ano_mes_id": data_sim.strftime("%Y-%m"),
            "cliente_id": cli_id, "produto_id": item["produto_id"], "ordem_producao_id": item["op_id"],
            "data_venda": data_sim.strftime("%Y-%m-%d"), "quantidade_vendida": random.randint(50, 100),
            "valor_total_venda": random.uniform(5000, 15000)
        })
    return dados

# --- SETUP E EXECUÇÃO ---
def inicializar_ambiente(client):
    global cnt_compra, cnt_op, cnt_lote, cnt_venda, cnt_garantia, cnt_manut, cnt_cliente, CACHE
    
    # Garante Dataset
    try: client.create_dataset(DATASET_ID, exists_ok=True)
    except: pass

    # Cria tabelas se não existirem
    for tbl, schema in SCHEMAS.items():
        try:
            client.create_table(bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{tbl}", schema=schema), exists_ok=True)
        except: pass

    # Popula Estáticos
    for tbl, dados in DADOS_ESTATICOS.items():
        if obter_max_id(client, tbl, "xxx", "x") == 0 and tbl != "raw_producao": # Check simplificado
             enviar_bq(client, dados, tbl)

    # Recupera Estado (IDs Sequenciais)
    cnt_op = obter_max_id(client, "raw_producao", "ordem_producao_id", "OP")
    cnt_lote = obter_max_id(client, "raw_lote", "lote_id", "Lote")
    cnt_venda = obter_max_id(client, "raw_vendas", "venda_id", "V")
    cnt_cliente = obter_max_id(client, "raw_cliente", "cliente_id", "C")
    
    # Carrega Cache Inteligente (Com ano de fabricação da máquina para ML)
    q_maq = f"SELECT maquina_id, ano, linha_id FROM `{PROJECT_ID}.{DATASET_ID}.raw_maquina`"
    try:
        res = list(client.query(q_maq).result())
        CACHE["MAQUINAS"] = [{"id": r.maquina_id, "ano": r.ano} for r in res]
        CACHE["LINHAS"] = list(set([r.linha_id for r in res]))
    except: pass
    
    q_cli = f"SELECT cliente_id FROM `{PROJECT_ID}.{DATASET_ID}.raw_cliente` LIMIT 2000"
    try: CACHE["CLIENTES"] = [r.cliente_id for r in client.query(q_cli).result()]
    except: pass
    
    q_prod = f"SELECT produto_id FROM `{PROJECT_ID}.{DATASET_ID}.raw_produto`"
    try: CACHE["PRODUTOS"] = {r.produto_id: {} for r in client.query(q_prod).result()}
    except: pass

@functions_framework.http
def executar_simulacao(request):
    print("🚀 Simulador ML-Ready (Physics + Lakehouse)...")
    try:
        client = conectar_bq()
        inicializar_ambiente(client)
        
        # Simulação roda para "AGORA" (Incremento horário)
        # Se quiser simular passado, basta mudar esta variável data_sim manualmente
        data_sim = datetime.now(timezone(timedelta(hours=-3))) 
        
        b_prod=[]; b_lote=[]; b_qual=[]; b_vend=[]; b_cli=[]

        print(f"⚙️ Processando {HORAS_POR_LOTE} hora(s) para {data_sim}...")
        for _ in range(HORAS_POR_LOTE):
            # 1. Clientes Genesis (Cria base antiga se necessário)
            b_cli.extend(gerar_clientes_genesis(data_sim))
            
            # 2. Produção Física (ML Engine)
            df, dd, _, dq = gerar_producao(client, data_sim)
            b_prod.extend(df); b_lote.extend(dd); b_qual.extend(dq)
            
            # 3. Vendas (Consistentes com clientes existentes)
            b_vend.extend(gerar_vendas(data_sim))
            
            data_sim += timedelta(hours=1)

        print(f"📦 Persistindo {len(b_prod)} registros...")
        enviar_bq(client, b_cli, "raw_cliente")
        enviar_bq(client, b_prod, "raw_producao")
        enviar_bq(client, b_lote, "raw_lote")
        enviar_bq(client, b_qual, "raw_qualidade")
        enviar_bq(client, b_vend, "raw_vendas")
        
        return f"Sucesso! Gerados {len(b_prod)} registros de produção (OEE Otimizado & ML Features)."

    except Exception as e:
        msg = f"❌ ERRO FATAL: {str(e)}"
        print(msg)
        return msg, 500