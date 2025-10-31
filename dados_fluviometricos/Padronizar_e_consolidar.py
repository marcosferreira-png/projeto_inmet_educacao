import os
import pandas as pd
import zipfile
import re

# --- CONFIGURAÇÃO ---
PASTA_DE_ENTRADA = r'C:\Users\seduc\Documents\dados_inmet\Cotas_Fluviais\COTAS FLUVIAIS 2025-20251029T145053Z-1-001\COTAS FLUVIAIS 2025'
PASTA_DE_SAIDA = r'C:\Users\seduc\Documents\dados_inmet\Cotas_Fluviais'
ARQUIVO_SAIDA = os.path.join(PASTA_DE_SAIDA, 'cotas_fluviais_consolidadas_final.csv')

# --- DICIONÁRIO DE MAPEAMENTO ---
MAPEAMENTO_DE_NOMES = {
    'EstacaoCodigo': 'codigo_estacao', 'NivelConsistencia': 'nivel_consistencia', 'Data': 'data', 'hora': 'hora',
    'MediaDiaria': 'cota_media_diaria', 'TipoMedicaoCotas': 'tipo_medicao_cota', 'Maxima': 'cota_maxima',
    'Minima': 'cota_minima', 'Media': 'cota_media', 'DiaMaxima': 'dia_cota_maxima', 'DiaMinima': 'dia_cota_minima',
    'MaximaStatus': 'cota_maxima_status', 'MinimaStatus': 'cota_minima_status', 'MediaStatus': 'cota_media_status',
    'MediaAnual': 'cota_media_anual', 'MediaAnualStatus': 'cota_media_anual_status',
    'Cota01': 'cota_dia_01', 'Cota02': 'cota_dia_02', 'Cota03': 'cota_dia_03', 'Cota04': 'cota_dia_04',
    'Cota05': 'cota_dia_05', 'Cota06': 'cota_dia_06', 'Cota07': 'cota_dia_07', 'Cota08': 'cota_dia_08',
    'Cota09': 'cota_dia_09', 'Cota10': 'cota_dia_10', 'Cota11': 'cota_dia_11', 'Cota12': 'cota_dia_12',
    'Cota13': 'cota_dia_13', 'Cota14': 'cota_dia_14', 'Cota15': 'cota_dia_15', 'Cota16': 'cota_dia_16',
    'Cota17': 'cota_dia_17', 'Cota18': 'cota_dia_18', 'Cota19': 'cota_dia_19', 'Cota20': 'cota_dia_20',
    'Cota21': 'cota_dia_21', 'Cota22': 'cota_dia_22', 'Cota23': 'cota_dia_23', 'Cota24': 'cota_dia_24',
    'Cota25': 'cota_dia_25', 'Cota26': 'cota_dia_26', 'Cota27': 'cota_dia_27', 'Cota28': 'cota_dia_28',
    'Cota29': 'cota_dia_29', 'Cota30': 'cota_dia_30', 'Cota31': 'cota_dia_31',
    'Cota01Status': 'cota_dia_01_status', 'Cota02Status': 'cota_dia_02_status', 'Cota03Status': 'cota_dia_03_status',
    'Cota04Status': 'cota_dia_04_status', 'Cota05Status': 'cota_dia_05_status', 'Cota06Status': 'cota_dia_06_status',
    'Cota07Status': 'cota_dia_07_status', 'Cota08Status': 'cota_dia_08_status', 'Cota09Status': 'cota_dia_09_status',
    'Cota10Status': 'cota_dia_10_status', 'Cota11Status': 'cota_dia_11_status', 'Cota12Status': 'cota_dia_12_status',
    'Cota13Status': 'cota_dia_13_status', 'Cota14Status': 'cota_dia_14_status', 'Cota15Status': 'cota_dia_15_status',
    'Cota16Status': 'cota_dia_16_status', 'Cota17Status': 'cota_dia_17_status', 'Cota18Status': 'cota_dia_18_status',
    'Cota19Status': 'cota_dia_19_status', 'Cota20Status': 'cota_dia_20_status', 'Cota21Status': 'cota_dia_21_status',
    'Cota22Status': 'cota_dia_22_status', 'Cota23Status': 'cota_dia_23_status', 'Cota24Status': 'cota_dia_24_status',
    'Cota25Status': 'cota_dia_25_status', 'Cota26Status': 'cota_dia_26_status', 'Cota27Status': 'cota_dia_27_status',
    'Cota28Status': 'cota_dia_28_status', 'Cota29Status': 'cota_dia_29_status', 'Cota30Status': 'cota_dia_30_status',
    'Cota31Status': 'cota_dia_31_status'
}

# --- FUNÇÕES AUXILIARES ---
def encontrar_linha_do_cabecalho(caminho_arquivo):
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as f:
            for i, line in enumerate(f):
                if 'EstacaoCodigo' in line: return i
    except: return None

def extrair_municipio_do_zip(nome_zip):
    try:
        base_name = nome_zip.rsplit('.zip', 1)[0]
        match = re.search(r'T\d{2}[_:]\d{2}[_:]\d{2}\.\d{3}Z', base_name)
        if match:
            municipio = base_name[match.end():]
            if municipio.startswith('_') or municipio.startswith('-'):
                municipio = municipio[1:]
            return municipio.strip()
        return base_name.rsplit('_', 1)[-1]
    except: return os.path.splitext(nome_zip)[0]

def descompactar_tudo(caminho_pasta):
    contem_zip = True
    while contem_zip:
        contem_zip = False
        for root, dirs, files in os.walk(caminho_pasta):
            for file in files:
                if file.lower().endswith('.zip'):
                    contem_zip = True
                    caminho_zip = os.path.join(root, file)
                    pasta_destino = os.path.join(root, os.path.splitext(file)[0])
                    print(f"   -> Descompactando ZIP aninhado: {file}")
                    os.makedirs(pasta_destino, exist_ok=True)
                    try:
                        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                            zip_ref.extractall(pasta_destino)
                        os.remove(caminho_zip)
                    except zipfile.BadZipFile:
                        print(f"      AVISO: Arquivo '{file}' está corrompido. Pulando.")
                    break
            if contem_zip: break

# --- EXECUÇÃO PRINCIPAL ---
print(f"Lendo arquivos da pasta: {os.path.basename(PASTA_DE_ENTRADA)}")
lista_dfs = []
arquivos_zip_iniciais = [f for f in os.listdir(PASTA_DE_ENTRADA) if f.lower().endswith('.zip')]

if not arquivos_zip_iniciais:
    print(f"\nAVISO: Nenhum arquivo .zip foi encontrado na pasta '{PASTA_DE_ENTRADA}'.")
else:
    for arquivo_zip in arquivos_zip_iniciais:
        municipio = extrair_municipio_do_zip(arquivo_zip)
        print(f"\nProcessando município: {municipio} (do arquivo: {arquivo_zip})")
        pasta_de_trabalho = os.path.join(PASTA_DE_ENTRADA, os.path.splitext(arquivo_zip)[0])
        caminho_zip_inicial = os.path.join(PASTA_DE_ENTRADA, arquivo_zip)
        print(f"-> Descompactando para pasta de trabalho: '{os.path.basename(pasta_de_trabalho)}'")
        os.makedirs(pasta_de_trabalho, exist_ok=True)
        try:
            with zipfile.ZipFile(caminho_zip_inicial, 'r') as zip_ref:
                zip_ref.extractall(pasta_de_trabalho)
        except zipfile.BadZipFile:
            print(f"   ERRO: O arquivo ZIP inicial '{arquivo_zip}' está corrompido. Pulando município.")
            continue
        descompactar_tudo(pasta_de_trabalho)
        for root, dirs, files in os.walk(pasta_de_trabalho):
            for arquivo_csv in files:
                if not arquivo_csv.lower().endswith('_cotas.csv'):
                    continue
                caminho_csv = os.path.join(root, arquivo_csv)
                print(f"   -> Processando arquivo de Cotas: {arquivo_csv}")
                linha_cabecalho = encontrar_linha_do_cabecalho(caminho_csv)
                if linha_cabecalho is None:
                    print(f"      ERRO: Cabeçalho não encontrado. Pulando.")
                    continue
                try:
                    df_temp = pd.read_csv(caminho_csv, encoding='latin1', sep=None, engine='python', skiprows=linha_cabecalho, on_bad_lines='skip')
                    df_temp['municipio'] = municipio
                    df_temp.rename(columns=MAPEAMENTO_DE_NOMES, inplace=True)
                    lista_dfs.append(df_temp)
                except Exception as e:
                    print(f"      ERRO INESPERADO ao ler o CSV: {e}")

# --- CONSOLIDAÇÃO E EXPORTAÇÃO ---
if not lista_dfs:
    print("\nAVISO: Nenhum arquivo de Cotas foi processado com sucesso.")
else:
    print(f"\nConcatenando {len(lista_dfs)} arquivo(s) de Cotas...")
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    
    # ==============================================================================
    # /// ETAPA FINAL E DEFINITIVA: Formatação manual para remover o .0 ///
    # ==============================================================================
    print("Formatando colunas numéricas para remover '.0' e 'NaN'...")
    
    # Aplica a formatação em TODAS as colunas do DataFrame
    for col in df_consolidado.columns:
        # A função apply percorre cada valor (x) da coluna
        df_consolidado[col] = df_consolidado[col].apply(
            lambda x: '' if pd.isna(x) else (str(int(x)) if isinstance(x, float) and x == int(x) else str(x))
        )
    # ==============================================================================

    colunas_finais_ordenadas = ['municipio'] + list(MAPEAMENTO_DE_NOMES.values())
    colunas_existentes = [col for col in colunas_finais_ordenadas if col in df_consolidado.columns]
    df_final = df_consolidado[colunas_existentes]

    print(f"Salvando o arquivo consolidado em: {ARQUIVO_SAIDA}")
    df_final.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='latin1')
    
    print("\nSUCESSO! Processo concluído.")