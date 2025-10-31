import os
import pandas as pd

# --- CONFIGURAÇÃO ---
PASTA_DE_ENTRADA = r'C:\Users\seduc\Documents\dados_inmet\Cotas_Pluviais\COTAS PLUVIAIS 2025-20251029T131939Z-1-001\COTAS PLUVIAIS 2025'
PASTA_DE_SAIDA = r'C:\Users\seduc\Documents\dados_inmet\Cotas_Pluviais'
ARQUIVO_SAIDA = os.path.join(PASTA_DE_SAIDA, 'chuvas_consolidadas_final.csv')

# Mapeamento dos nomes de colunas
MAPEAMENTO_DE_NOMES = {
    'EstacaoCodigo': 'codigo_estacao', 'NivelConsistencia': 'nivel_consistencia', 'Data': 'data',
    'TipoMedicaoChuvas': 'tipo_medicao_chuva', 'Maxima': 'chuva_maxima_diaria_mm',
    'Total': 'chuva_total_mensal_mm', 'DiaMaxima': 'dia_chuva_maxima', 'NumDiasDeChuva': 'total_dias_com_chuva',
    'MaximaStatus': 'chuva_maxima_status', 'TotalStatus': 'chuva_total_status',
    'NumDiasDeChuvaStatus': 'total_dias_com_chuva_status', 'TotalAnual': 'chuva_total_anual_mm',
    'TotalAnualStatus': 'chuva_total_anual_status',
    'Chuva01': 'chuva_dia_01', 'Chuva02': 'chuva_dia_02', 'Chuva03': 'chuva_dia_03',
    'Chuva04': 'chuva_dia_04', 'Chuva05': 'chuva_dia_05', 'Chuva06': 'chuva_dia_06',
    'Chuva07': 'chuva_dia_07', 'Chuva08': 'chuva_dia_08', 'Chuva09': 'chuva_dia_09',
    'Chuva10': 'chuva_dia_10', 'Chuva11': 'chuva_dia_11', 'Chuva12': 'chuva_dia_12',
    'Chuva13': 'chuva_dia_13', 'Chuva14': 'chuva_dia_14', 'Chuva15': 'chuva_dia_15',
    'Chuva16': 'chuva_dia_16', 'Chuva17': 'chuva_dia_17', 'Chuva18': 'chuva_dia_18',
    'Chuva19': 'chuva_dia_19', 'Chuva20': 'chuva_dia_20', 'Chuva21': 'chuva_dia_21',
    'Chuva22': 'chuva_dia_22', 'Chuva23': 'chuva_dia_23', 'Chuva24': 'chuva_dia_24',
    'Chuva25': 'chuva_dia_25', 'Chuva26': 'chuva_dia_26', 'Chuva27': 'chuva_dia_27',
    'Chuva28': 'chuva_dia_28', 'Chuva29': 'chuva_dia_29', 'Chuva30': 'chuva_dia_30',
    'Chuva31': 'chuva_dia_31',
    'Chuva01Status': 'chuva_dia_01_status', 'Chuva02Status': 'chuva_dia_02_status',
    'Chuva03Status': 'chuva_dia_03_status', 'Chuva04Status': 'chuva_dia_04_status',
    'Chuva05Status': 'chuva_dia_05_status', 'Chuva06Status': 'chuva_dia_06_status',
    'Chuva07Status': 'chuva_dia_07_status', 'Chuva08Status': 'chuva_dia_08_status',
    'Chuva09Status': 'chuva_dia_09_status', 'Chuva10Status': 'chuva_dia_10_status',
    'Chuva11Status': 'chuva_dia_11_status', 'Chuva12Status': 'chuva_dia_12_status',
    'Chuva13Status': 'chuva_dia_13_status', 'Chuva14Status': 'chuva_dia_14_status',
    'Chuva15Status': 'chuva_dia_15_status', 'Chuva16Status': 'chuva_dia_16_status',
    'Chuva17Status': 'chuva_dia_17_status', 'Chuva18Status': 'chuva_dia_18_status',
    'Chuva19Status': 'chuva_dia_19_status', 'Chuva20Status': 'chuva_dia_20_status',
    'Chuva21Status': 'chuva_dia_21_status', 'Chuva22Status': 'chuva_dia_22_status',
    'Chuva23Status': 'chuva_dia_23_status', 'Chuva24Status': 'chuva_dia_24_status',
    'Chuva25Status': 'chuva_dia_25_status', 'Chuva26Status': 'chuva_dia_26_status',
    'Chuva27Status': 'chuva_dia_27_status', 'Chuva28Status': 'chuva_dia_28_status',
    'Chuva29Status': 'chuva_dia_29_status', 'Chuva30Status': 'chuva_dia_30_status',
    'Chuva31Status': 'chuva_dia_31_status'
}

# Função para extrair o nome do município
def extrair_municipio_do_arquivo(nome_arquivo):
    """Extrai o nome do município a partir do nome do arquivo."""
    try:
        municipio = nome_arquivo.rsplit('_', 2)[0]
        return municipio
    except:
        return os.path.splitext(nome_arquivo)[0]

# --- EXECUÇÃO PRINCIPAL ---
print(f"Lendo arquivos da pasta: {os.path.basename(PASTA_DE_ENTRADA)}")
lista_dfs = []

arquivos_csv = [f for f in os.listdir(PASTA_DE_ENTRADA) if f.lower().endswith('.csv')]

if not arquivos_csv:
    print(f"\nAVISO: Nenhum arquivo .csv foi encontrado na pasta '{PASTA_DE_ENTRADA}'.")
else:
    for arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(PASTA_DE_ENTRADA, arquivo)
        print(f"-> Processando arquivo: {arquivo}")
        
        try:
            df_temp = pd.read_csv(caminho_arquivo, sep=None, encoding='latin1', engine='python', on_bad_lines='skip')
            
            df_temp.rename(columns=MAPEAMENTO_DE_NOMES, inplace=True)
            
            # --- MUDANÇA AQUI: Criando apenas a coluna 'municipio' ---
            df_temp['municipio'] = extrair_municipio_do_arquivo(arquivo)
            
            lista_dfs.append(df_temp)

        except Exception as e:
            print(f"  ERRO INESPERADO ao processar o arquivo {arquivo}: {e}")

# --- CONSOLIDAÇÃO E EXPORTAÇÃO ---
if not lista_dfs:
    print("\nAVISO: Nenhum arquivo foi processado com sucesso.")
else:
    print(f"\nConcatenando {len(lista_dfs)} arquivo(s)...")
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    
    print("Ajustando tipos de dados (removendo .0)...")
    colunas_inteiras = [
        'codigo_estacao', 'nivel_consistencia', 'dia_chuva_maxima', 'total_dias_com_chuva',
        'chuva_maxima_status', 'total_dias_com_chuva_status', 'chuva_total_anual_status',
        'chuva_dia_01_status', 'chuva_dia_02_status', 'chuva_dia_03_status', 'chuva_dia_04_status',
        'chuva_dia_05_status', 'chuva_dia_06_status', 'chuva_dia_07_status', 'chuva_dia_08_status',
        'chuva_dia_09_status', 'chuva_dia_10_status', 'chuva_dia_11_status', 'chuva_dia_12_status',
        'chuva_dia_13_status', 'chuva_dia_14_status', 'chuva_dia_15_status', 'chuva_dia_16_status',
        'chuva_dia_17_status', 'chuva_dia_18_status', 'chuva_dia_19_status', 'chuva_dia_20_status',
        'chuva_dia_21_status', 'chuva_dia_22_status', 'chuva_dia_23_status', 'chuva_dia_24_status',
        'chuva_dia_25_status', 'chuva_dia_26_status', 'chuva_dia_27_status', 'chuva_dia_28_status',
        'chuva_dia_29_status', 'chuva_dia_30_status', 'chuva_dia_31_status', 'TotalStatus'
    ]
    
    for col in colunas_inteiras:
        if col in df_consolidado.columns:
            try:
                df_consolidado[col] = df_consolidado[col].astype('Int64')
            except Exception as e:
                print(f"  Aviso: Não foi possível converter a coluna '{col}' para inteiro. Erro: {e}")

    # --- MUDANÇA AQUI: Organiza as colunas, colocando 'municipio' no início ---
    novas_colunas = ['municipio'] + list(MAPEAMENTO_DE_NOMES.values())
    colunas_existentes = [col for col in novas_colunas if col in df_consolidado.columns]
    df_final = df_consolidado[colunas_existentes]

    print(f"Salvando o arquivo consolidado em: {ARQUIVO_SAIDA}")
    df_final.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='latin1')
    
    print("\nSUCESSO! Processo concluído.")