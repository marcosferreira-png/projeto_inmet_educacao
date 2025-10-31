import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, Float, INTEGER
import time

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================

CONNECTION_STRING = 'mysql+mysqlconnector://inmet_user:123456@localhost:3308/inmet_db'

CAMINHO_CSV = r'C:\Users\seduc\Documents\dados_inmet\Cotas_Pluviais\chuvas_consolidadas_final.csv'
NOME_DA_TABELA = 'dados_pluviometricos'

# ==========================================================
# MAPEAMENTO BASE DE TIPOS
# ==========================================================

mapeamento_de_tipos = {
    'municipio': VARCHAR(255),
    'codigo_estacao': INTEGER,
    'nivel_consistencia': INTEGER,
    'data': VARCHAR(10),
    'tipo_medicao_chuva': VARCHAR(50),
    'chuva_maxima_diaria_mm': Float,
    'chuva_total_mensal_mm': Float,
    'dia_chuva_maxima': INTEGER,
    'total_dias_com_chuva': INTEGER,
    'chuva_maxima_status': INTEGER,
    'chuva_total_status': INTEGER,
    'total_dias_com_chuva_status': INTEGER,
    'chuva_total_anual_mm': Float,
    'chuva_total_anual_status': INTEGER,
}

# Adiciona dinamicamente as colunas de chuva diária e status (01 a 31)
for i in range(1, 32):
    dia = str(i).zfill(2)
    mapeamento_de_tipos[f'chuva_dia_{dia}'] = Float
    mapeamento_de_tipos[f'chuva_dia_{dia}_status'] = INTEGER

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

            print("Iniciando limpeza e conversão dos dados...")

            # Remove espaços extras e normaliza valores nulos
            df = df.replace(['', ' ', 'nan', 'NaN', 'None'], pd.NA)

            # ----------------------------------------------------------
            # Conversão automática dos tipos numéricos
            # ----------------------------------------------------------

            for col in df.columns:
                # Colunas *_status → inteiro sem .0
                if col.endswith('_status'):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

                # Outras colunas numéricas (chuvas, dias, código etc.)
                elif (
                    'chuva' in col
                    or 'dias' in col
                    or 'codigo' in col
                    or 'nivel' in col
                ):
                    # Substitui vírgulas por pontos antes da conversão
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace(',', '.', regex=False)
                        .replace(['', 'nan', 'NaN'], pd.NA)
                    )
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # ----------------------------------------------------------
            # Tratamento da coluna 'data'
            # ----------------------------------------------------------
            if 'data' in df.columns:
                df['data'] = df['data'].fillna('').astype(str)

            print("✅ Conversões e limpeza concluídas com sucesso.")

            # Mantém apenas as colunas mapeadas que realmente existem
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
                        # Remove tabela anterior, se existir
                        connection.execute(text(f"DROP TABLE IF EXISTS `{NOME_DA_TABELA}`"))

                        # Envia DataFrame para o MySQL
                        df_final.to_sql(
                            name=NOME_DA_TABELA,
                            con=connection,
                            if_exists='append',
                            index=False,
                            chunksize=10000,
                            method='multi',
                            dtype=mapeamento_de_tipos
                        )

                        transaction.commit()
                        tempo = time.time() - start_time
                        print(f"\n✅ SUCESSO! {len(df_final)} registros enviados em {tempo:.2f} segundos.")
                        print(f"Tabela criada: `{NOME_DA_TABELA}` no banco de dados 'inmet_db'.")
                    except Exception as e:
                        transaction.rollback()
                        raise e

        except Exception as e:
            print(f"\n❌ ERRO FATAL: {e}")
