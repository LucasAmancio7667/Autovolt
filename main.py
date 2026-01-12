import functions_framework
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", module="google.cloud.bigquery")

import time
import random
import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

# --- CONFIGURAÇÃO ---
# (Na nuvem, a autenticação é automática, não precisa de json local)
PROJECT_ID = "autovolt-analytics-479417"
DATASET_ID = "autovolt_bronze"
HORAS_POR_LOTE = 24  # O Robô vai simular 24h de trabalho a cada execução

# --- SCHEMAS (COMPLETOS) ---
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
    "raw_manutencao": [SchemaField("evento_manutencao_id", "STRING"), SchemaField("maquina_id", "STRING"), SchemaField("linha_id", "STRING"), SchemaField("tipo_manutencao_id", "STRING"), SchemaField("inicio", "STRING"), SchemaField("fim", "STRING"), SchemaField("duracao_min", "STRING"), SchemaField("criticidade", "STRING")]
}

# --- LISTAS AUXILIARES ---
ESTADOS_BRASIL = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
CACHE = {"CLIENTES": [], "PRODUTOS": {}, "MAQUINAS": [], "LINHAS": [], "FORNECEDORES": {}, "MATERIAS": {}, "DEFEITOS": []}
estoque_materia_prima = []; estoque_produtos_acabados = []; historico_vendas = []
cnt_compra=0; cnt_op=0; cnt_lote=0; cnt_venda=0; cnt_garantia=0; cnt_manut=0; cnt_cliente=0

def conectar_bq(): return bigquery.Client(project=PROJECT_ID)

# --- FUNÇÃO DE AUTO-CURA (RECRIA ARQUIVOS PERDIDOS) ---
def verificar_e_criar_arquivos_base():
    """Gera CSVs locais temporários para o script funcionar na nuvem."""
    
    if not os.path.exists("raw_materia_prima.csv"):
        pd.DataFrame({
            "materia_prima_id": ["MP001", "MP002", "MP003", "MP004", "MP005"],
            "nome_material": ["Chumbo", "Ácido Sulfúrico", "Polipropileno (Plástico)", "Separadores de Polietileno", "Eletrólito"]
        }).to_csv("raw_materia_prima.csv", index=False)

    if not os.path.exists("raw_fornecedor.csv"):
        pd.DataFrame({
            "fornecedor_id": ["F001", "F002", "F003", "F004"],
            "categoria": ["Chumbo/Metais", "Químicos/Ácidos", "Plásticos/Polímeros", "Componentes Elétricos"],
            "leadtime_dias": ["5", "3", "7", "10"],
            "qualificacao": ["A", "A", "B", "A"],
            "data_cadastro": ["2023-01-01", "2023-02-15", "2023-03-10", "2023-05-20"],
            "data_ultima_avaliacao": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"],
            "descricao": ["Fornecedor Global Metais", "ChemTech Ind", "PlastCorp", "ElectroParts"]
        }).to_csv("raw_fornecedor.csv", index=False)

    if not os.path.exists("raw_defeito.csv"):
        pd.DataFrame({
            "defeito_id": ["D00", "D01", "D02", "D03", "D04", "D05"],
            "descricao": ["Sem Defeito", "Vazamento de Ácido", "Baixa Tensão", "Caixa Rachada", "Sobreaquecimento (Curto)", "Terminal Oxidado"],
            "gravidade": ["Nenhuma", "Alta", "Média", "Média", "Crítica", "Baixa"]
        }).to_csv("raw_defeito.csv", index=False)

    # --- LISTA COMPLETA E CORRIGIDA (CAPACIDADE BATE COM NOME) ---
    if not os.path.exists("raw_produto.csv"):
        pd.DataFrame({
            "produto_id": ["BAT001", "BAT002", "BAT003", "BAT004", "BAT005", "BAT006", "BAT007", "BAT008", "BAT009", "BAT010"],
            "modelo": ["AV-50Ah", "AV-60Ah", "AV-70Ah", "AV-80Ah", "AV-90Ah", "AV-100Ah", "AV-110Ah", "AV-120Ah", "AV-130Ah", "AV-140Ah"],
            "tensao_v": ["12", "24", "12", "24", "48", "12", "12", "12", "12", "12"],
            "capacidade_ah": ["50", "60", "70", "80", "90", "100", "110", "120", "130", "140"], # CORRIGIDO AQUI
            "linha_segmento": ["Montadora", "Reposição", "Montadora", "Reposição", "Montadora", "Reposição", "Montadora", "Reposição", "Reposição", "Montadora"],
            "data_lancamento": ["2024-10-21", "2025-01-19", "2025-01-22", "2024-12-20", "2024-12-14", "2024-12-13", "2025-02-17", "2025-01-16", "2025-02-16", "2024-12-18"],
            "data_descontinuacao": ["", "", "", "", "", "", "", "", "", ""]
        }).to_csv("raw_produto.csv", index=False)

    if not os.path.exists("raw_maquina.csv"):
        pd.DataFrame({
            "maquina_id": ["M001", "M002", "M003"],
            "tipo": ["Montadora Automática", "Injetora de Plástico", "Envasadora de Ácido"],
            "fabricante": ["Siemens", "Engel", "Bosch"],
            "ano": ["2018", "2020", "2019"],
            "linha_id": ["L01", "L01", "L02"]
        }).to_csv("raw_maquina.csv", index=False)
    
    if not os.path.exists("raw_cliente.csv"):
        pd.DataFrame({
            "cliente_id": ["C001"],
            "tipo_cliente": ["Distribuidor"],
            "cidade": ["SP"],
            "tipo_plano": ["Básico"],
            "data_cadastro": ["2023-01-01"],
            "data_ultima_compra": [""]
        }).to_csv("raw_cliente.csv", index=False)


def obter_max_id_hibrido(client, tabela, coluna, prefixo, arquivo_csv):
    max_val = 0
    try:
        q = f"SELECT MAX(CAST(REGEXP_EXTRACT({coluna}, r'{prefixo}(\d+)') AS INT64)) FROM `{PROJECT_ID}.{DATASET_ID}.{tabela}`"
        r = list(client.query(q).result())
        if r and r[0][0]: max_val = int(r[0][0])
    except: pass
    return max_val

def calcular_turno(data_hora):
    h = data_hora.hour
    if 6 <= h < 14: return "T1"
    elif 14 <= h < 22: return "T2"
    else: return "T3"

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
    except:
        CACHE["CLIENTES"] = ["C001"]

def enviar_bq(client, dados, tabela):
    if not dados: return
    try:
        df = pd.DataFrame(dados).astype(str).replace("None", "").replace("nan", "")
        table_ref = client.dataset(DATASET_ID).table(tabela)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    except Exception as e: print(f"Erro {tabela}: {e}")

# --- GERADORES COMPLEXOS (MANTIDOS) ---
def gerar_novos_clientes(data_sim):
    global cnt_cliente, CACHE; dados = []
    if random.random() < 0.05: 
        cnt_cliente += 1; nid = f"C{cnt_cliente:04d}"
        plano = random.choices(["Básico", "Intermediário", "Estendido", "Premium"], weights=[40, 30, 20, 10], k=1)[0]
        cli = {
            "cliente_id": nid, "tipo_cliente": random.choice(["Montadora", "Distribuidor", "Autopeças"]),
            "cidade": random.choice(ESTADOS_BRASIL), "tipo_plano": plano,
            "data_cadastro": data_sim.strftime("%Y-%m-%d"), "data_ultima_compra": ""
        }
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
            item = {
                "compra_id": cid, "fornecedor_id": random.choice(fornecedores_validos), "materia_prima_id": mp_id,
                "data_compra": data_sim.strftime("%Y-%m-%d %H:%M:%S"), "quantidade_comprada": random.randint(500, 2000),
                "custo_unitario": round(random.uniform(20, 100), 2), "custo_total": 0
            }
            item["custo_total"] = round(item["quantidade_comprada"] * item["custo_unitario"], 2)
            dados.append(item); estoque_materia_prima.append({"id": cid, "mp": mp_id})
    return dados

def gerar_producao(data_sim):
    global cnt_op, cnt_lote, estoque_produtos_acabados, CACHE
    d_fact=[]; d_dim=[]; d_map=[]; d_qual=[]
    if len(estoque_materia_prima) < 5: return [],[],[],[]
    for _ in range(random.randint(1, 4)):
        cnt_op += 1; cnt_lote += 1
        op_id = f"OP{cnt_op}"; lid = f"Lote{cnt_lote}"
        maquinas_lista = CACHE["MAQUINAS"] or ["M001"]; produtos_lista = list(CACHE["PRODUTOS"].keys()) or ["BAT001"]
        linhas_lista = CACHE["LINHAS"] or ["L01"]; defeitos_lista = CACHE["DEFEITOS"] or ["D00"]
        mid = random.choice(maquinas_lista); pid = random.choice(produtos_lista)
        dur = round(random.uniform(3,10),1); ini = data_sim; fim = ini+timedelta(hours=dur)
        esta_em_surto = random.random() < (0.20 if mid == "M001" else 0.02)
        
        if esta_em_surto:
            temp = round(random.uniform(90.0, 115.0), 1); vib = round(random.uniform(1800, 2500), 0)
            pres = round(random.uniform(6.0, 8.0), 1); fator_defeito = 0.80
        else:
            temp = round(random.uniform(55.0, 75.0), 1); vib = round(random.uniform(1000, 1400), 0)
            pres = round(random.uniform(10.0, 12.0), 1); fator_defeito = 0.01

        d_dim.append({"lote_id": lid, "produto_id": pid, "linha_id": random.choice(linhas_lista), "maquina_id": mid, "inicio_producao": ini.strftime("%Y-%m-%d %H:%M:%S"), "fim_producao": fim.strftime("%Y-%m-%d %H:%M:%S"), "duracao_horas": dur})
        q_plan = random.choice([100,200]); q_prod = int(q_plan*random.uniform(0.9,1.0))
        d_fact.append({"ordem_producao_id": op_id, "lote_id": lid, "produto_id": pid, "linha_id": d_dim[-1]["linha_id"], "maquina_id": mid, "turno_id": calcular_turno(data_sim), "inicio": ini.strftime("%Y-%m-%d %H:%M:%S"), "temperatura_media_c": temp, "vibracao_media_rpm": vib, "pressao_media_bar": pres, "ciclo_minuto_nominal": 5.0, "duracao_horas": dur, "quantidade_planejada": q_plan, "quantidade_produzida": q_prod, "quantidade_refugada": q_plan-q_prod})
        
        insumos = random.sample(estoque_materia_prima, k=min(len(estoque_materia_prima), 2))
        for ins in insumos: d_map.append({"lote_id": lid, "compra_id": ins["id"]})
        
        tem_def=False; d_real="D00"
        if random.random() < fator_defeito: 
            tem_def=True; d_real = "D04" if esta_em_surto else random.choice(defeitos_lista)
        aprovado=1; d_rep="D00"
        if tem_def and random.random()>0.1: aprovado=0; d_rep=d_real
        d_qual.append({"teste_id": f"T{cnt_lote}", "lote_id": lid, "produto_id": pid, "data_teste": (fim+timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S"), "tensao_medida_v": 10.5 if tem_def else 12.6, "resistencia_interna_mohm": 8.0 if tem_def else 6.0, "capacidade_ah_teste": 60.0, "defeito_id": d_rep, "aprovado": aprovado})
        if aprovado==1: estoque_produtos_acabados.append({"lote_id": lid, "produto_id": pid, "op_id": op_id, "def": d_real if tem_def else None})
    return d_fact, d_dim, d_map, d_qual

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

@functions_framework.http
def executar_simulacao(request):
    print("🚀 Simulador V22 (CLOUD TRIGGER)...")
    client = conectar_bq(); inicializar_ambiente(client)
    
    enviar_bq(client, gerar_compras(datetime.now() - timedelta(days=1), forcar_compra=True), "raw_compras")
    data_sim = datetime.now()
    b_cli=[]; b_comp=[]; b_prod=[]; b_lote=[]; b_map=[]; b_qual=[]; b_vend=[]; b_gar=[]; b_man=[]

    # Executa apenas 1 ciclo de HORAS_POR_LOTE para não estourar timeout
    for _ in range(HORAS_POR_LOTE):
        data_sim += timedelta(hours=1)
        b_cli.extend(gerar_novos_clientes(data_sim))
        b_comp.extend(gerar_compras(data_sim))
        df, dd, dm, dq = gerar_producao(data_sim)
        b_prod.extend(df); b_lote.extend(dd); b_map.extend(dm); b_qual.extend(dq)
        b_vend.extend(gerar_vendas(data_sim))
        b_gar.extend(gerar_garantia(data_sim))
        b_man.extend(gerar_manutencao(data_sim))
    
    enviar_bq(client, b_cli, "raw_cliente")
    enviar_bq(client, b_comp, "raw_compras")
    enviar_bq(client, b_prod, "raw_producao")
    enviar_bq(client, b_lote, "raw_lote")
    enviar_bq(client, b_map, "raw_map_lote_compras")
    enviar_bq(client, b_qual, "raw_qualidade")
    enviar_bq(client, b_vend, "raw_vendas")
    enviar_bq(client, b_gar, "raw_garantia")
    enviar_bq(client, b_man, "raw_manutencao")
    
    return f"Simulação concluída com sucesso. {HORAS_POR_LOTE} horas geradas com lógica COMPLETA."
