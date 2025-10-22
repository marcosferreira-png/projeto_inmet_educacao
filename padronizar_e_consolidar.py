import os
import pandas as pd

# --- CONFIGURAÇÃO DO TESTE ---
PASTA_RAIZ = r'C:\Users\seduc\Documents\dados_inmet'
ANO_INICIO = 2000
ANO_FIM = 2025
# Sugestão de novo nome para o arquivo de saída para não sobrescrever o anterior
ARQUIVO_SAIDA = os.path.join(PASTA_RAIZ, f'INMET_AMAZONAS_PADRONIZADO_2000-2025{ANO_INICIO}-{ANO_FIM}.csv')

# --- DICIONÁRIO PARA RENOMEAR AS COLUNAS (AGORA EM MINÚSCULO) ---
# As chaves e os valores foram convertidos para minúsculo.
MAPEAMENTO_DE_NOMES = {
    'data_yyyy_mm_dd': 'data',
    'hora_utc': 'hora',
    'precipitacao_total_horario_mm': 'precipitacao_mm',
    'pressao_atmosferica_ao_nivel_da_estacao_horaria_mb': 'pressao_estacao_mb',
    'pressao_atmosferica_maxna_hora_ant_aut_mb': 'pressao_max_mb',
    'pressao_atmosferica_min_na_hora_ant_aut_mb': 'pressao_min_mb',
    'radiacao_global_kj_m2': 'radiacao_global_kj_m2',
    'temperatura_do_ar_bulbo_seco_horaria_c': 'temperatura_c',
    'temperatura_do_ponto_de_orvalho_c': 'temp_orvalho_c',
    'temperatura_maxima_na_hora_ant_aut_c': 'temp_max_c',
    'temperatura_minima_na_hora_ant_aut_c': 'temp_min_c',
    'temperatura_orvalho_max_na_hora_ant_aut_c': 'temp_orvalho_max_c',
    'temperatura_orvalho_min_na_hora_ant_aut_c': 'temp_orvalho_min_c',
    'umidade_rel_max_na_hora_ant_aut': 'umidade_max_pct',
    'umidade_rel_min_na_hora_ant_aut': 'umidade_min_pct',
    'umidade_relativa_do_ar_horaria': 'umidade_relativa_pct',
    'vento_direcao_horaria_gr_gr': 'vento_direcao_graus',
    'vento_rajada_maxima_m_s': 'vento_rajada_ms',
    'vento_velocidade_horaria_m_s': 'vento_velocidade_ms',
    'fonte': 'arquivo_origem'
}

# Função de padronização (alterada para gerar minúsculas)
def padronizar_cabecalho(nome_coluna):
    import unicodedata, re
    s = ''.join(c for c in unicodedata.normalize('NFD', nome_coluna) if unicodedata.category(c) != 'Mn')
    # MUDANÇA 1: Convertido para minúsculas
    s = s.lower()
    s = re.sub(r'[\s/,-]+', '_', s)
    # MUDANÇA 2: Regex agora permite letras minúsculas
    s = re.sub(r'[^a-z0-9_]', '', s)
    s = re.sub(r'__+', '_', s)
    s = s.strip('_')
    # Adicionado para padronizar o nome da coluna de data que às vezes é diferente
    if 'data' in s and 'yyyy' in s:
        return 'data_yyyy_mm_dd'
    return s

# --- EXECUÇÃO PRINCIPAL ---
print(f"Iniciando a padronização final (minúsculas) para os anos {ANO_INICIO}-{ANO_FIM}...")
lista_dfs_finais = []

for ano in range(ANO_INICIO, ANO_FIM + 1):
    pasta_base_do_ano = os.path.join(PASTA_RAIZ, f'dados_amazonas_{ano}')
    if not os.path.isdir(pasta_base_do_ano): continue
    
    caminho_real_dos_arquivos = pasta_base_do_ano
    possivel_subpasta = os.path.join(pasta_base_do_ano, str(ano))
    if os.path.isdir(possivel_subpasta):
        caminho_real_dos_arquivos = possivel_subpasta

    prefixo = 'INMET_N_AM_'
    arquivos = [f for f in os.listdir(caminho_real_dos_arquivos) if prefixo in f and f.upper().endswith('.CSV')]
    if not arquivos: continue
    
    print(f"-> Processando {len(arquivos)} arquivos do ano {ano}...")
    for arquivo in arquivos:
        caminho_arquivo = os.path.join(caminho_real_dos_arquivos, arquivo)
        try:
            # A lógica de leitura que trata -9999 e vazios permanece a mesma
            df_temp = pd.read_csv(
                caminho_arquivo, encoding='latin1', sep=';', skiprows=8,
                index_col=False, na_values=['-9999', -9999, '-9999.0']
            )
            
            df_temp.columns = [padronizar_cabecalho(col) for col in df_temp.columns]
            df_temp.rename(columns=MAPEAMENTO_DE_NOMES, inplace=True)
            
            # MUDANÇA 3: Verificando o nome em minúsculo
            if 'arquivo_origem' in MAPEAMENTO_DE_NOMES.values():
                df_temp['arquivo_origem'] = arquivo
            
            df_temp = df_temp.loc[:, ~df_temp.columns.str.contains('^unnamed', na=False)]
            lista_dfs_finais.append(df_temp)
        except Exception as e:
            print(f"  - Erro ao processar o arquivo {arquivo}: {e}")

# --- CONSOLIDAÇÃO E EXPORTAÇÃO ---
if not lista_dfs_finais:
    print("\nNenhum dado foi processado.")
else:
    print("\nConcatenando todos os arquivos finais...")
    df_consolidado = pd.concat(lista_dfs_finais, ignore_index=True)
    
    colunas_finais = [col for col in MAPEAMENTO_DE_NOMES.values() if col in df_consolidado.columns]
    df_consolidado = df_consolidado[colunas_finais]
    
    print(f"Salvando o novo arquivo final em: {ARQUIVO_SAIDA}")
    df_consolidado.to_csv(ARQUIVO_SAIDA, sep=';', index=False, encoding='latin1')
    
    print("\nSUCESSO! Arquivo final foi criado com colunas em minúsculo e está pronto para o upload.")
    print("\n--- AMOSTRA DOS NOMES DE COLUNAS FINAIS ---")
    print(df_consolidado.columns.tolist())