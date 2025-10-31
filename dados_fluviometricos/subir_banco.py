import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, INTEGER
import time

# ==========================================================
# CONFIGURAÇÃO (ADAPTADA PARA DADOS FLUVIAIS)
# ==========================================================

CONNECTION_STRING = 'mysql+mysqlconnector://inmet_user:123456@localhost:3308/inmet_db'

CAMINHO_CSV = r'C:\Users\seduc\Documents\dados_inmet\Cotas_Fluviais\cotas_fluviais_consolidadas_final.csv'
NOME_DA_TABELA = 'dados_fluviometricos'

# ==========================================================
# MAPEAMENTO BASE DE TIPOS (ADAPTADO PARA DADOS FLUVIAIS)
# Como todos os valores de cota são inteiros (em cm), usamos INTEGER.
# ==========================================================

mapeamento_de_tipos = {
    'municipio': VARCHAR(255),
    'codigo_estacao': INTEGER,
    'nivel_consistencia': INTEGER,
    'data': VARCHAR(10),
    'hora': VARCHAR(10),
    'tipo_medicao_cota': VARCHAR(50),
    'cota_media_diaria': INTEGER,
    'cota_maxima': INTEGER,
    'cota_minima': INTEGER,
    'cota_media': INTEGER,
    'dia_cota_maxima': INTEGER,
    'dia_cota_minima': INTEGER,
    'cota_maxima_status': INTEGER,
    'cota_minima_status': INTEGER,
    'cota_media_status': INTEGER,
    'cota_media_anual': INTEGER,
    'cota_media_anual_status': INTEGER,
}

# Adiciona dinamicamente as colunas de cota diária e status (01 a 31)
for i in range(1, 32):
    dia = str(i).zfill(2)
    mapeamento_de_tipos[f'cota_dia_{dia}'] = INTEGER
    mapeamento_de_tipos[f'cota_dia_{dia}_status'] = INTEGER

# ==========================================================
# EXECUÇÃO PRINCIPAL
# ==========================================================

if __name__ == "__main__":
    if not os.path.exists(CAMINHO_CSV):
        print(f"❌ ERRO: Arquivo CSV não encontrado em: {CAMINHO_CSV}")
    else:
        try:
            print(f"Lendo o arquivo CSV: {os.path.basename(CAMINHO_CSV)}...")
            df = pd.read_csv(CAMINHO_CSV, sep=';', encoding='latin1', low_memory=False)
            print(f"✅ Arquivo lido com sucesso. {len(df)} linhas carregadas.")

            print("Iniciando limpeza e conversão definitiva dos dados...")
            
            # --- LÓGICA DE LIMPEZA CORRIGIDA ---
            
            # Lista de colunas que contêm valores de cota e podem ter pontos de milhar
            colunas_com_pontos = [col for col in df.columns if 'cota' in col and 'status' not in col]
            
            for col in colunas_com_pontos:
                # ETAPA 1: Remove os pontos de milhar
                df[col] = df[col].astype(str).str.replace('.', '', regex=False)
                # ETAPA 2: Converte para número, tratando erros e vazios
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            # Lista de colunas que são inteiros puros (códigos, status, dias)
            colunas_inteiras_puras = [col for col in df.columns if 'status' in col or 'codigo' in col or 'nivel' in col or 'dia_' in col]

            for col in colunas_inteiras_puras:
                # Converte para número e depois para o tipo Int64 que remove o .0
                # Esta é a solução definitiva para o problema do ".0"
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            # Tratamento de colunas de texto para garantir que vazios fiquem como ''
            text_cols = ['data', 'hora', 'tipo_medicao_cota', 'municipio']
            for col in text_cols:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str)

            print("✅ Conversões e limpeza concluídas com sucesso.")
            
            colunas_para_enviar = [col for col in mapeamento_de_tipos.keys() if col in df.columns]
            df_final = df[colunas_para_enviar]

            # ==========================================================
            # ENVIO AO BANCO DE DADOS
            # ==========================================================
            engine = create_engine(CONNECTION_STRING)
            print(f"\nConectando ao MySQL e enviando dados para '{NOME_DA_TABELA}'...")
            start_time = time.time()

            with engine.connect() as connection:
                with connection.begin() as transaction:
                    try:
                        connection.execute(text(f"DROP TABLE IF EXISTS `{NOME_DA_TABELA}`"))

                        df_final.to_sql(
                            name=NOME_DA_TABELA, con=connection, if_exists='append',
                            index=False, chunksize=5000, method='multi', dtype=mapeamento_de_tipos
                        )

                        transaction.commit()
                        tempo = time.time() - start_time
                        print(f"\n✅ SUCESSO! {len(df_final)} registros enviados em {tempo:.2f} segundos.")
                        print(f"Tabela criada: `{NOME_DA_TABELA}` no banco de dados 'inmet_db'.")
                    except Exception as e:
                        transaction.rollback(); raise e

        except Exception as e:
            print(f"\n❌ ERRO FATAL: {e}")