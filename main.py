import functions_framework
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", module="google.cloud.bigquery")

import time
import random
import os
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

# --- CONFIGURAÇÃO DE LOGS ---
logging.basicConfig(level=logging.INFO)

# --- CONFIGURAÇÃO DO PROJETO ---
PROJECT_ID = "autovolt-analytics-479417"
DATASET_ID = "autovolt_bronze"
HORAS_POR_LOTE = 1 

# --- SCHEMAS ---
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
        {"linha_id": "L01", "descricao": "Linha de Montagem Automotiva (Alta Velocidade)", "turnos_operacionais": "3"},
        {"linha_id": "L02", "descricao": "Linha de Injeção de Caixas Plásticas", "turnos_operacionais": "3"},
        {"linha_id": "L03", "descricao": "Linha de Envase de Ácido e Carga", "turnos_operacionais": "3"},
        {"linha_id": "L04", "descricao": "Linha de Baterias Pesadas (Caminhões/Ônibus)", "turnos_operacionais": "2"},
        {"linha_id": "L05", "descricao": "Linha de Prototipagem e Testes Especiais", "turnos_operacionais": "1"}
    ],
    "raw_metas_vendas": [
        {"meta_id": "M001", "ano_mes_id": "2025-01", "meta_quantidade": "1500", "meta_valor": "350000.00"},
        {"meta_id": "M002", "ano_mes_id": "2025-02", "meta_quantidade": "1800", "meta_valor": "420000.00"},
        {"meta_id": "M003", "ano_mes_id": "2025-03", "meta_quantidade": "2000", "meta_valor": "500000.00"},
        {"meta_id": "M004", "ano_mes_id": "2025-04", "meta_quantidade": "2200", "meta_valor": "550000.00"},
        {"meta_id": "M005", "ano_mes_id": "2025-05", "meta_quantidade": "2500", "meta_valor": "600000.00"}
    ],
    "raw_turno": [
        {"turno_id": "T1", "janela": "06:00 - 14:00", "coef_performance": "1.0"},
        {"turno_id": "T2", "janela": "14:00 - 22:00", "coef_performance": "0.95"},
        {"turno_id": "T3", "janela": "22:00 - 06:00", "coef_performance": "0.90"}
    ],
    "raw_tipo_manut": [
        {"tipo_manutencao_id": "TM01", "descricao": "Preventiva Programada", "criticidade_padrao": "Baixa"},
        {"tipo_manutencao_id": "TM02", "descricao": "Corretiva Emergencial", "criticidade_padrao": "Alta"},
        {"tipo_manutencao_id": "TM03", "descricao": "Preditiva (Análise de Vibração)", "criticidade_padrao": "Média"}
    ]
}

ESTADOS_BRASIL = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
CACHE = {"CLIENTES": [], "PRODUTOS": {}, "MAQUINAS": [], "LINHAS": [], "FORNECEDORES": {}, "MATERIAS": {}, "DEFEITOS": []}
estoque_materia_prima = []; estoque_produtos_acabados = []; historico_vendas = []
cnt_compra=0; cnt_op=0; cnt_lote=0; cnt_venda=0; cnt_garantia=0; cnt_manut=0; cnt_cliente=0

# --- FUNÇÃO NOVA: ENVIAR PARA PAINEL ANDON (TV) ---
# Movi para cima para evitar erros de referência
def registrar_alerta_bq(client, alerta):
    try:
        row = {
            "alerta_id": f"ALT-{int(time.time())}",
            "data_ocorrencia": (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"),
            "nivel": "CRITICO",
            "maquina_id": str(alerta.get("maquina", "M000")),
            "mensagem": str(alerta.get("msg", "ERRO CRITICO")),
            "valor_medido": float(alerta.get("temp", 0.0))
        }
        # Insere direto na tabela de alertas
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.monitoramento_alertas"
        errors = client.insert_rows_json(table_ref, [row])
        if errors:
            logging.error(f"Erros BQ: {errors}")
        else:
            logging.info("Alerta enviado para Painel TV BigQuery")
    except Exception as e:
        logging.error(f"Erro ao atualizar Painel TV: {e}")

def conectar_bq(): return bigquery.Client(project=PROJECT_ID)

# --- GERAR CALENDÁRIO AUTOMÁTICO ---
def gerar_dimensao_tempo():
    dados = []
    meses_pt = {1:"Janeiro", 2:"Fevereiro", 3:"Março", 4:"Abril", 5:"Maio", 6:"Junho", 7:"Julho", 8:"Agosto", 9:"Setembro", 10:"Outubro", 11:"Novembro", 12:"Dezembro"}
    meses_abbr = {1:"jan", 2:"fev", 3:"mar", 4:"abr", 5:"mai", 6:"jun", 7:"jul", 8:"ago", 9:"set", 10:"out", 11:"nov", 12:"dez"}
    
    for ano in range(2023, 2027):
        for mes in range(1, 13):
            dados.append({
                "ano_mes_id": f"{ano}-{mes:02d}",
                "ano": str(ano),
                "mes": f"{mes:02d}",
                "nome_mes": meses_pt[mes],
                "trimestre": str((mes - 1) // 3 + 1),
                "ano_mes_label": f"{meses_abbr[mes]}/{str(ano)[2:]}"
            })
    return dados

# --- AUTO-CURA (CSVs LOCAIS) ---
def verificar_e_criar_arquivos_base():
    if not os.path.exists("raw_materia_prima.csv"):
        pd.DataFrame({
            "materia_prima_id": ["MP001", "MP002", "MP003", "MP004", "MP005"],
            "nome_material": ["Chumbo", "Ácido Sulfúrico", "Polipropileno (Plástico)", "Separadores de Polietileno", "Eletrólito"]
        }).to_csv("raw_materia_prima.csv", index=False)

    if not os.path.exists("raw_fornecedor.csv"):
        fornecedores = []
        for i in range(1, 6): fornecedores.append(["F"+f"{i:03d}", "Chumbo/Metais", str(random.randint(5,15)), "A", "2023-01-10", "2024-01-01", f"Fornecedor Metal {i}"])
        for i in range(6, 11): fornecedores.append(["F"+f"{i:03d}", "Químicos/Ácidos", str(random.randint(3,10)), "A", "2023-02-15", "2024-01-01", f"Indústria Química {i}"])
        for i in range(11, 16): fornecedores.append(["F"+f"{i:03d}", "Plásticos/Polímeros", str(random.randint(7,20)), "B", "2023-03-20", "2024-01-01", f"PlastCorp {i}"])
        for i in range(16, 21): fornecedores.append(["F"+f"{i:03d}", "Componentes Elétricos", str(random.randint(10,30)), "A", "2023-05-05", "2024-01-01", f"ElectroParts {i}"])
        pd.DataFrame(fornecedores, columns=["fornecedor_id", "categoria", "leadtime_dias", "qualificacao", "data_cadastro", "data_ultima_avaliacao", "descricao"]).to_csv("raw_fornecedor.csv", index=False)

    if not os.path.exists("raw_defeito.csv"):
        pd.DataFrame({
            "defeito_id": ["D00", "D01", "D02", "D03", "D04", "D05"],
            "descricao": ["Sem Defeito", "Vazamento de Ácido", "Baixa Tensão", "Caixa Rachada", "Sobreaquecimento (Curto)", "Terminal Oxidado"],
            "gravidade": ["Nenhuma", "Alta", "Média", "Média", "Crítica", "Baixa"]
        }).to_csv("raw_defeito.csv", index=False)

    if not os.path.exists("raw_produto.csv"):
        pd.DataFrame({
            "produto_id": ["BAT001", "BAT002", "BAT003", "BAT004", "BAT005", "BAT006", "BAT007", "BAT008", "BAT009", "BAT010"],
            "modelo": ["AV-50Ah", "AV-60Ah", "AV-70Ah", "AV-80Ah", "AV-90Ah", "AV-100Ah", "AV-110Ah", "AV-120Ah", "AV-130Ah", "AV-140Ah"],
            "tensao_v": ["12", "24", "12", "24", "48", "12", "12", "12", "12", "12"],
            "capacidade_ah": ["50", "60", "70", "80", "90", "100", "110", "120", "130", "140"],
            "linha_segmento": ["Montadora", "Reposição", "Montadora", "Reposição", "Montadora", "Reposição", "Montadora", "Reposição", "Reposição", "Montadora"],
            "data_lancamento": ["2024-10-21", "2025-01-19", "2025-01-22", "2024-12-20", "2024-12-14", "2024-12-13", "2025-02-17", "2025-01-16", "2025-02-16", "2024-12-18"],
            "data_descontinuacao": ["", "", "", "", "", "", "", "", "", ""]
        }).to_csv("raw_produto.csv", index=False)

    if not os.path.exists("raw_maquina.csv"):
        maquinas = []
        for i in range(1, 21):
            tipo = random.choice(["Montadora Automática", "Injetora de Plástico", "Envasadora de Ácido", "Robô de Solda", "Testador de Carga"])
            fab = random.choice(["Siemens", "Engel", "Bosch", "ABB", "Kuka"])
            linha = f"L{random.randint(1,5):02d}"
            maquinas.append([f"M{i:03d}", tipo, fab, str(random.randint(2015, 2024)), linha])
        pd.DataFrame(maquinas, columns=["maquina_id", "tipo", "fabricante", "ano", "linha_id"]).to_csv("raw_maquina.csv", index=False)
    
    if not os.path.exists("raw_cliente.csv"):
        pd.DataFrame({"cliente_id": ["C001"], "tipo_cliente": ["Distribuidor"], "cidade": ["SP"], "tipo_plano": ["Básico"], "data_cadastro": ["2023-01-01"], "data_ultima_compra": [""]}).to_csv("raw_cliente.csv", index=False)

def obter_max_id_hibrido(client, tabela, coluna, prefixo, arquivo_csv):
    max_val = 0
    try:
        q = f"SELECT MAX(CAST(REGEXP_EXTRACT({coluna}, r'{prefixo}(\\d+)') AS INT64)) FROM `{PROJECT_ID}.{DATASET_ID}.{tabela}`"
        r = list(client.query(q).result())
        if r and r[0][0]: max_val = int(r[0][0])
    except: pass
    return max_val

def calcular_turno(data_hora):
    h = data_hora.hour
    if 6 <= h < 14: return "T1"
    elif 14 <= h < 22: return "T2"
    else: return "T3"

# --- POPULADOR INTELIGENTE (ESTÁTICO + TEMPO) ---

def criar_tabela_usuarios_simples(client):
    dados_usuarios = [
        {"email_usuario": "jam4@discente.ifpe.edu.br", "cargo": "DIRETORIA"},
        {"email_usuario": "adventuregamesbr123@gmail.com", "cargo": "DIRETORIA"},
        {"email_usuario": "juliosicesar5@gmail.com", "cargo": "DIRETORIA"},
        {"email_usuario": "pedrohenriquereisxavier@gmail.com", "cargo": "DIRETORIA"},
        {"email_usuario": "fcss1@discente.ifpe.edu.br", "cargo": "DIRETORIA"},
        {"email_usuario": "coordenador_compras@empresa.com", "cargo": "SUPRIMENTOS"},
        {"email_usuario": "gerente_planta@empresa.com", "cargo": "INDUSTRIAL"},
        {"email_usuario": "gerente_vendas@empresa.com", "cargo": "COMERCIAL"},
        {"email_usuario": "engenheiro_qualidade@empresa.com", "cargo": "QUALIDADE"}
    ]
    
    df = pd.DataFrame(dados_usuarios).astype(str).replace("None", "").replace("nan", "")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )
    
    client.load_table_from_dataframe(
        df, 
        client.dataset(DATASET_ID).table("raw_controle_acesso"), 
        job_config=job_config
    ).result()
    
    print("✅ Tabela raw_controle_acesso criada/atualizada")

def popular_tabelas_estaticas(client):
    print("🚦 Verificando Tabelas Estáticas...")
    
    for tabela, dados in DADOS_ESTATICOS.items():
        try:
            res = list(client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.{tabela}`").result())
            if res[0][0] == 0:
                print(f"   ⚠️ Populando {tabela}...")
                client.load_table_from_dataframe(pd.DataFrame(dados), client.dataset(DATASET_ID).table(tabela), job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
        except Exception as e: print(f"❌ Erro {tabela}: {e}")

    try:
        res = list(client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.raw_tempo`").result())
        if res[0][0] == 0:
            print("   ⚠️ Populando raw_tempo (Calendário)...")
            dados_tempo = gerar_dimensao_tempo()
            client.load_table_from_dataframe(pd.DataFrame(dados_tempo), client.dataset(DATASET_ID).table("raw_tempo"), job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
            print("   ✅ raw_tempo criada!")
    except Exception as e: print(f"❌ Erro raw_tempo: {e}")

def inicializar_ambiente(client):
    global cnt_compra, cnt_op, cnt_lote, cnt_venda, cnt_garantia, cnt_manut, cnt_cliente, CACHE
    verificar_e_criar_arquivos_base()

    dataset_ref = client.dataset(DATASET_ID)
    try: client.get_dataset(dataset_ref)
    except NotFound: client.create_dataset(dataset_ref)

    for tbl, schema in SCHEMAS.items():
        ref = dataset_ref.table(tbl)
        try: client.get_table(ref)
        except NotFound:
            client.create_table(bigquery.Table(ref, schema=schema))
            arquivo_padrao = f"{tbl}.csv"
            if os.path.exists(arquivo_padrao):
                try:
                    df = pd.read_csv(arquivo_padrao, dtype=str).replace("nan", "")
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    client.load_table_from_dataframe(df, ref, job_config=job_config).result()
                except: pass

    criar_tabela_usuarios_simples(client)
    popular_tabelas_estaticas(client)

    cnt_compra = obter_max_id_hibrido(client, "raw_compras", "compra_id", "C", "raw_compras.csv")
    cnt_op = obter_max_id_hibrido(client, "raw_producao", "ordem_producao_id", "OP", "raw_producao.csv")
    cnt_lote = obter_max_id_hibrido(client, "raw_lote", "lote_id", "Lote", "raw_lote.csv")
    cnt_venda = obter_max_id_hibrido(client, "raw_vendas", "venda_id", "V", "raw_vendas.csv")
    cnt_garantia = obter_max_id_hibrido(client, "raw_garantia", "garantia_id", "W", "raw_garantia.csv")
    cnt_manut = obter_max_id_hibrido(client, "raw_manutencao", "evento_manutencao_id", "EVM", "raw_manutencao.csv")
    cnt_cliente = obter_max_id_hibrido(client, "raw_cliente", "cliente_id", "C", "raw_cliente.csv")

    try:
        df_c = pd.read_csv("raw_cliente.csv"); CACHE["CLIENTES"] = df_c['cliente_id'].tolist()
        df_p = pd.read_csv("raw_produto.csv"); CACHE["PRODUTOS"] = df_p.set_index('produto_id')[['capacidade_ah']].to_dict('index')
        df_m = pd.read_csv("raw_maquina.csv"); CACHE["MAQUINAS"] = df_m['maquina_id'].tolist(); CACHE["LINHAS"] = df_m['linha_id'].unique().tolist()
        df_f = pd.read_csv("raw_fornecedor.csv"); CACHE["FORNECEDORES"] = df_f.groupby('categoria')['fornecedor_id'].apply(list).to_dict()
        df_mp = pd.read_csv("raw_materia_prima.csv"); CACHE["MATERIAS"] = df_mp.set_index('materia_prima_id')['nome_material'].to_dict()
        CACHE["DEFEITOS"] = pd.read_csv("raw_defeito.csv")['defeito_id'].tolist()
    except: CACHE["CLIENTES"] = ["C001"]

    print("🔒 Sincronizando tabela técnica de Lookup (Security)...")
    dataset_silver = DATASET_ID.replace("bronze", "silver")
    query_sync_lookup = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset_silver}.sys_acessos_lookup` AS
    SELECT DISTINCT email_usuario, cargo FROM `{PROJECT_ID}.{DATASET_ID}.raw_controle_acesso`
    WHERE email_usuario IS NOT NULL
    """
    try:
        client.query(query_sync_lookup).result()
        print("   ✅ Tabela sys_acessos_lookup atualizada com sucesso.")
    except Exception as e:
        print(f"   ❌ ERRO CRÍTICO: Falha ao atualizar Lookup de Segurança: {e}")


def enviar_bq(client, dados, tabela):
    if not dados: return
    try:
        df = pd.DataFrame(dados).astype(str).replace("None", "").replace("nan", "")
        client.load_table_from_dataframe(df, client.dataset(DATASET_ID).table(tabela), job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
    except Exception as e: print(f"Erro {tabela}: {e}")

# --- GERADORES ---
def gerar_novos_clientes(data_sim):
    global cnt_cliente, CACHE; dados = []
    if random.random() < 0.05: 
        cnt_cliente += 1; nid = f"C{cnt_cliente:04d}"
        plano = random.choices(["Básico", "Intermediário", "Estendido", "Premium"], weights=[40, 30, 20, 10], k=1)[0]
        cli = {"cliente_id": nid, "tipo_cliente": random.choice(["Montadora", "Distribuidor", "Autopeças"]), "cidade": random.choice(ESTADOS_BRASIL), "tipo_plano": plano, "data_cadastro": data_sim.strftime("%Y-%m-%d"), "data_ultima_compra": ""}
        dados.append(cli); CACHE["CLIENTES"].append(nid)
    return dados

def gerar_compras(data_sim, forcar_compra=False):
    global cnt_compra, estoque_materia_prima, CACHE; dados = []
    if not CACHE["MATERIAS"]: CACHE["MATERIAS"] = {"MP001": "Genérico"}
    if forcar_compra or random.random() < 0.4:
        qtd = random.randint(3, 8) if forcar_compra else random.randint(1, 4)
        for _ in range(qtd):
            cnt_compra += 1; cid = f"C{cnt_compra}"
            mp_id = random.choice(list(CACHE["MATERIAS"].keys()))
            fornecedores_validos = [f for sublist in CACHE["FORNECEDORES"].values() for f in sublist] or ["F001"]
            item = {"compra_id": cid, "fornecedor_id": random.choice(fornecedores_validos), "materia_prima_id": mp_id, "data_compra": data_sim.strftime("%Y-%m-%d %H:%M:%S"), "quantidade_comprada": random.randint(500, 2000), "custo_unitario": round(random.uniform(20, 100), 2), "custo_total": 0}
            item["custo_total"] = round(item["quantidade_comprada"] * item["custo_unitario"], 2)
            dados.append(item); estoque_materia_prima.append({"id": cid, "mp": mp_id})
    return dados


# --- VERSÃO CORRIGIDA: FORÇA A DATA DE HOJE (SEM PYTZ) ---
def gerar_producao(client, data_sim):
    global cnt_op, cnt_lote, estoque_produtos_acabados, CACHE
    
    # 1. Importações Nativas (Funciona em qualquer Python)
    from datetime import datetime, timedelta, timezone
    
    d_fact=[]; d_dim=[]; d_map=[]; d_qual=[]

    # 2. Forçar Data de Agora (Fuso -3h Recife/Brasília na mão)
    fuso_br = timezone(timedelta(hours=-3))
    agora = datetime.now(fuso_br)
    # ---------------------------------------------------------

    # Garante máquinas
    lista_maquinas = CACHE.get("MAQUINAS", [])
    if not lista_maquinas:
        lista_maquinas = [f"M{i:03d}" for i in range(1, 21)]

    print(f"🏭 Produzindo em: {agora} (Forçado)")

    for mid in lista_maquinas:
        cnt_op += 1; cnt_lote += 1
        op_id = f"OP{cnt_op}"; lid = f"Lote{cnt_lote}"
        
        # Define horário: Agora menos alguns minutos aleatórios
        minutos_atras = random.randint(0, 50)
        ini = agora - timedelta(minutes=minutos_atras)
        dur = round(random.uniform(3,10),1)
        fim = ini + timedelta(hours=dur)
        
        # Preenche dados básicos se cache falhar
        produtos_lista = list(CACHE.get("PRODUTOS", {}).keys()) or ["BAT001"]
        linhas_lista = CACHE.get("LINHAS", []) or ["L01"]
        pid = random.choice(produtos_lista)

        # Simulação de Falhas
        esta_em_surto = random.random() < (0.05 if mid == "M001" else 0.005)
        if esta_em_surto:
            temp = round(random.uniform(90.0, 115.0), 1); vib = round(random.uniform(1800, 2500), 0)
            pres = round(random.uniform(6.0, 8.0), 1)
        else:
            temp = round(random.uniform(55.0, 75.0), 1); vib = round(random.uniform(1000, 1400), 0)
            pres = round(random.uniform(10.0, 12.0), 1)

        # Criação dos Dicionários (Igual ao original, mas usando a data 'ini' corrigida)
        d_dim.append({
            "lote_id": lid, "produto_id": pid, "linha_id": random.choice(linhas_lista), 
            "maquina_id": mid, "inicio_producao": ini.strftime("%Y-%m-%d %H:%M:%S"), 
            "fim_producao": fim.strftime("%Y-%m-%d %H:%M:%S"), "duracao_horas": dur
        })
        
        q_plan = random.choice([100,200]); q_prod = int(q_plan*random.uniform(0.9,1.0))
        
        d_fact.append({
            "ordem_producao_id": op_id, "lote_id": lid, "produto_id": pid, 
            "linha_id": d_dim[-1]["linha_id"], "maquina_id": mid, "turno_id": calcular_turno(ini), 
            "inicio": ini.strftime("%Y-%m-%d %H:%M:%S"), "temperatura_media_c": temp, 
            "vibracao_media_rpm": vib, "pressao_media_bar": pres, "ciclo_minuto_nominal": 5.0, 
            "duracao_horas": dur, "quantidade_planejada": q_plan, 
            "quantidade_produzida": q_prod, "quantidade_refugada": q_plan-q_prod
        })

        # Alertar BQ se falha grave
        if temp > 95.0:
            alerta = {"evento": "ALERTA_MAQUINA", "tipo": "SUPERAQUECIMENTO", "maquina": mid, "temp": temp, "msg": "CRITICO: Superaquecimento!"}
            registrar_alerta_bq(client, alerta)

        d_qual.append({
            "teste_id": f"T{cnt_lote}", "lote_id": lid, "produto_id": pid, 
            "data_teste": (fim+timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S"), 
            "tensao_medida_v": 12.6, "resistencia_interna_mohm": 6.0, 
            "capacidade_ah_teste": 60.0, "defeito_id": "D00", "aprovado": 1
        })
        estoque_produtos_acabados.append({"lote_id": lid, "produto_id": pid, "op_id": op_id, "def": None})

    return d_fact, d_dim, d_map, d_qual

# --- SUBSTITUA A FUNÇÃO EXECUTAR_SIMULACAO POR ESTA ---
@functions_framework.http
def executar_simulacao(request):
    print("🚀 --- INICIANDO EXECUÇÃO V28 (DEBUG) ---")
    try:
        client = conectar_bq()
        inicializar_ambiente(client)
        
        print("🛒 Gerando compras iniciais...")
        # Força compras para garantir lista cheia (agora ignorado na produção, mas bom manter)
        enviar_bq(client, gerar_compras(datetime.now(), forcar_compra=True), "raw_compras")
        
        data_sim = datetime.now()
        b_prod=[]; b_lote=[]; b_map=[]; b_qual=[]

        print("⚙️ Entrando no loop de horas...")
        # Loop reduzido para 1 hora como combinado
        for _ in range(1): 
            data_sim += timedelta(hours=1)
            # Passando o client para evitar erros
            df, dd, dm, dq = gerar_producao(client, data_sim)
            b_prod.extend(df); b_lote.extend(dd); b_map.extend(dm); b_qual.extend(dq)
        
        print(f"📦 Enviando {len(b_prod)} registros para o BigQuery (raw_producao)...")
        enviar_bq(client, b_prod, "raw_producao")
        enviar_bq(client, b_lote, "raw_lote")
        enviar_bq(client, b_qual, "raw_qualidade")
        
        if len(b_prod) > 0:
            return f"SUCESSO: {len(b_prod)} linhas de produção geradas!"
        else:
            return "AVISO: O script rodou mas gerou 0 linhas. Verifique os logs."
            
    except Exception as e:
        print(f"❌ ERRO FATAL NO SCRIPT: {str(e)}")
        return f"ERRO: {str(e)}"

def gerar_vendas(data_sim):
    global cnt_venda, CACHE; dados=[]
    if len(estoque_produtos_acabados) < 3: return []
    if random.random() < 0.5:
        item = estoque_produtos_acabados.pop(0); cnt_venda += 1
        cap = float(CACHE["PRODUTOS"].get(item["produto_id"], {"capacidade_ah": 60}).get("capacidade_ah", 60))
        preco_final = (cap * 40) * random.uniform(0.9, 1.2)
        qtd = random.randint(20,100)
        clientes_lista = CACHE["CLIENTES"] or ["C001"]
        v = {"venda_id": f"V{cnt_venda}", "ano_mes_id": data_sim.strftime("%Y-%m"), "cliente_id": random.choice(clientes_lista), "produto_id": item["produto_id"], "ordem_producao_id": item["op_id"], "data_venda": data_sim.strftime("%Y-%m-%d"), "quantidade_vendida": qtd, "valor_total_venda": round(preco_final * qtd, 2)}
        dados.append(v); historico_vendas.append({"v":v, "l":item})
    return dados

def gerar_garantia(data_sim):
    global cnt_garantia; dados=[]
    for h in historico_vendas[:]:
        vd=datetime.strptime(h["v"]["data_venda"], "%Y-%m-%d"); dias=(data_sim-vd).days
        if dias>90: historico_vendas.remove(h); continue
        chance = 0.06 if h["l"]["def"] else 0.005
        if random.random()<chance:
            cnt_garantia+=1
            tem_def = h["l"]["def"] is not None
            status = "Aprovada" if (tem_def and random.random()<0.85) else "Negada - Mau Uso" if tem_def else "Negada - Produto Funcional" if random.random()<0.90 else "Aprovada - Cortesia"
            custo = round(random.uniform(200, 3000), 2) if "Aprovada" in status else 0.0
            dados.append({"garantia_id": f"W{cnt_garantia}", "cliente_id": h["v"]["cliente_id"], "produto_id": h["v"]["produto_id"], "lote_id": h["l"]["lote_id"], "data_reclamacao": data_sim.strftime("%Y-%m-%d %H:%M:%S"), "dias_pos_venda": dias, "defeito_id": h["l"]["def"] or "D00", "status": status, "tempo_resposta_dias": random.randint(1, 15), "custo_garantia": custo})
            historico_vendas.remove(h)
    return dados

def gerar_manutencao(data_sim):
    global cnt_manut, CACHE; dados=[]
    if random.random()<0.05:
        maquinas_lista = CACHE["MAQUINAS"] or ["M001"]; linhas_lista = CACHE["LINHAS"] or ["L01"]
        cnt_manut+=1; ini=data_sim; fim=ini+timedelta(hours=2)
        dados.append({"evento_manutencao_id": f"EVM{cnt_manut}", "maquina_id": random.choice(maquinas_lista), "linha_id": random.choice(linhas_lista), "tipo_manutencao_id": "TM01", "inicio": ini.strftime("%Y-%m-%d %H:%M:%S"), "fim": fim.strftime("%Y-%m-%d %H:%M:%S"), "duracao_min": 120, "criticidade": "Média"})
    return dados

def atualizar_clientes_pos_simulacao(client):
    print("🔨 Atualizando Data da Última Compra dos Clientes...")
    try: client.query(f"UPDATE `{PROJECT_ID}.{DATASET_ID}.raw_cliente` c SET data_ultima_compra = (SELECT CAST(MAX(data_venda) AS STRING) FROM `{PROJECT_ID}.{DATASET_ID}.raw_vendas` v WHERE v.cliente_id = c.cliente_id) WHERE TRUE").result()
    except Exception as e: print(f"⚠️ Erro ao atualizar clientes: {e}")

@functions_framework.http
def executar_simulacao(request):
    print("🚀 Simulador V29 (COM DEBUG)...")
    
    try:
        # 1. Conexão
        client = conectar_bq()
        inicializar_ambiente(client)
        
        # 2. Encher o estoque (Sua correção)
        print("🛒 Forçando compra de estoque inicial...")
        for _ in range(3):
            enviar_bq(client, gerar_compras(datetime.now(), forcar_compra=True), "raw_compras")
            
        data_sim = datetime.now()
        b_cli=[]; b_comp=[]; b_prod=[]; b_lote=[]; b_map=[]; b_qual=[]; b_vend=[]; b_gar=[]; b_man=[]

        print(f"⚙️ Iniciando loop de {HORAS_POR_LOTE} horas...")

        for i in range(HORAS_POR_LOTE):
            data_sim += timedelta(hours=1)
            b_cli.extend(gerar_novos_clientes(data_sim))
            b_comp.extend(gerar_compras(data_sim))
            
            # Gerar Produção
            df, dd, dm, dq = gerar_producao(client, data_sim)
            
            # --- DEBUG IMPORTANTE: Mostra se produziu algo nesta hora ---
            print(f"   ⏱️ Hora {i+1}: Gerados {len(df)} registros de produção.")
            
            b_prod.extend(df); b_lote.extend(dd); b_map.extend(dm); b_qual.extend(dq)
            b_vend.extend(gerar_vendas(data_sim))
            b_gar.extend(gerar_garantia(data_sim))
            b_man.extend(gerar_manutencao(data_sim))
        
        # 3. Envio para o Banco
        print(f"📦 Enviando TOTAL de {len(b_prod)} linhas de produção para o BigQuery...")
        
        enviar_bq(client, b_cli, "raw_cliente")
        enviar_bq(client, b_comp, "raw_compras")
        enviar_bq(client, b_prod, "raw_producao")
        enviar_bq(client, b_lote, "raw_lote")
        enviar_bq(client, b_map, "raw_map_lote_compras")
        enviar_bq(client, b_qual, "raw_qualidade")
        enviar_bq(client, b_vend, "raw_vendas")
        enviar_bq(client, b_gar, "raw_garantia")
        enviar_bq(client, b_man, "raw_manutencao")
        
        atualizar_clientes_pos_simulacao(client)
        
        # 4. Retorno informativo
        mensagem_final = f"Simulação concluída! Sucesso: {len(b_prod)} novas linhas de produção geradas."
        print(f"✅ {mensagem_final}")
        return mensagem_final

    except Exception as e:
        # Se der erro, ele aparece aqui
        erro_msg = f"❌ ERRO CRÍTICO NO SCRIPT: {str(e)}"
        print(erro_msg)
        return erro_msg, 500
    
