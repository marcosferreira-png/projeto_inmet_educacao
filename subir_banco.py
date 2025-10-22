import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, Float, TIMESTAMP
import time

# --- CONFIGURAÇÃO FINAL PARA DOCKER COM MYSQL ---
# *** A ÚNICA MUDANÇA SIGNIFICATIVA ESTÁ AQUI ***
CONNECTION_STRING = 'mysql+mysqlconnector://inmet_user:123456@localhost:3308/inmet_db'

# Apontando para o arquivo CSV COMPLETO com nomes em minúsculo
CAMINHO_CSV = r'C:\Users\seduc\Documents\dados_inmet\INMET_AMAZONAS_PADRONIZADO_2000-2025.csv'
NOME_DA_TABELA = 'dados_inmet_amazonas' # O nome da tabela no banco será em minúsculo

# --- MAPEAMENTO DE TIPOS (Funciona tanto para PostgreSQL quanto para MySQL) ---
mapeamento_de_tipos = {
    'data': VARCHAR(10), # Definir um tamanho para VARCHAR é uma boa prática em MySQL
    'hora': VARCHAR(8),
    'precipitacao_mm': Float,
    'pressao_estacao_mb': Float,
    'pressao_max_mb': Float,
    'pressao_min_mb': Float,
    'radiacao_global_kj_m2': Float,
    'temperatura_c': Float,
    'temp_orvalho_c': Float,
    'temp_max_c': Float,
    'temp_min_c': Float,
    'temp_orvalho_max_c': Float,
    'temp_orvalho_min_c': Float,
    'umidade_max_pct': Float,
    'umidade_min_pct': Float,
    'umidade_relativa_pct': Float,
    'vento_direcao_graus': Float,
    'vento_rajada_ms': Float,
    'vento_velocidade_ms': Float,
    'arquivo_origem': VARCHAR(255),
    'data_hora': TIMESTAMP
}

if __name__ == "__main__":
    if not os.path.exists(CAMINHO_CSV):
        print(f"ERRO: Arquivo CSV final não encontrado: {CAMINHO_CSV}")
    else:
        try:
            print(f"Lendo o arquivo CSV final: {os.path.basename(CAMINHO_CSV)}...")
            df = pd.read_csv(CAMINHO_CSV, sep=';', encoding='latin1', low_memory=False)
            print(f"Arquivo lido. {len(df)} linhas carregadas.")

            print("Iniciando conversão final de tipos de dados...")
            for col, tipo in mapeamento_de_tipos.items():
                if tipo == Float and col in df.columns:
                    df[col] = pd.to_numeric(
                        df[col].astype(str).str.replace(',', '.', regex=False),
                        errors='coerce'
                    )

            if 'data' in df.columns and 'hora_utc' in df.columns:
                hora_str = df['hora_utc'].astype(str).str.zfill(4)
                datetime_str = df['data'].astype(str) + ' ' + hora_str.str.slice(0, 2) + ':' + hora_str.str.slice(2, 4)
                df['data_hora'] = pd.to_datetime(datetime_str, format='%Y/%m/%d %H:%M', errors='coerce')
            print("Conversão concluída.")
            
            colunas_para_enviar = [col for col in mapeamento_de_tipos.keys() if col in df.columns]
            df_final = df[colunas_para_enviar]
            
            engine = create_engine(CONNECTION_STRING)
            print(f"Conectando ao banco MySQL local e enviando dados para a tabela '{NOME_DA_TABELA}'...")
            print("Isso pode levar alguns minutos...")
            
            start_time = time.time()
            with engine.connect() as connection:
                with connection.begin() as transaction:
                    try:
                        # A sintaxe para apagar a tabela é a mesma
                        connection.execute(text(f'DROP TABLE IF EXISTS `{NOME_DA_TABELA}`'))
                        
                        df_final.to_sql(
                            name=NOME_DA_TABELA, con=connection, if_exists='append',
                            index=False, chunksize=10000, method='multi', dtype=mapeamento_de_tipos
                        )
                        transaction.commit()
                        upload_time = time.time() - start_time
                        print(f"\nSUCESSO! Carga concluída em {upload_time:.2f} segundos.")
                        print(f"Os dados estão na tabela '{NOME_DA_TABELA}' do seu banco MySQL local.")
                    except Exception as e:
                        transaction.rollback(); raise e
        except Exception as e:
            print(f"\nOcorreu um erro fatal durante o processo: {e}")