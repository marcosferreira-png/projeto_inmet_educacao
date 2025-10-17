import os
import pandas as pd

# --- Configuração ---
PASTA_RAIZ = r'C:\Users\seduc\Documents\dados_inmet'
ANO_INICIO = 2000
ANO_FIM = 2025
ARQUIVO_SAIDA = os.path.join(PASTA_RAIZ, f'INMET_AMAZONAS_CONSOLIDADO_{ANO_INICIO}-{ANO_FIM}.csv')

# --- Execução ---
print("Iniciando a consolidação de arquivos...")
lista_dfs = []

for ano in range(ANO_INICIO, ANO_FIM + 1):
    pasta_base_do_ano = os.path.join(PASTA_RAIZ, f'dados_amazonas_{ano}')
    print(f"\n--- Processando ano: {ano} ---")

    if not os.path.isdir(pasta_base_do_ano):
        print(f"AVISO: Pasta base '{pasta_base_do_ano}' não encontrada. Pulando.")
        continue

    # --- LÓGICA INTELIGENTE PARA ENCONTRAR O CAMINHO CORRETO ---
    # Por padrão, o caminho dos arquivos é a pasta base.
    caminho_real_dos_arquivos = pasta_base_do_ano
    
    # Verifica se existe uma subpasta com o nome do ano (ex: ...\2018\2018)
    possivel_subpasta = os.path.join(pasta_base_do_ano, str(ano))
    if os.path.isdir(possivel_subpasta):
        print(f"Subpasta '{ano}' encontrada. Buscando arquivos em: {possivel_subpasta}")
        # Se encontrou, este é o novo caminho para procurar os arquivos.
        caminho_real_dos_arquivos = possivel_subpasta
    else:
        print(f"Buscando arquivos diretamente em: {pasta_base_do_ano}")

    # --- FILTRO E LEITURA (Usando o 'caminho_real_dos_arquivos') ---
    prefixo = 'INMET_N_AM_'
    # A única mudança aqui é usar a variável 'caminho_real_dos_arquivos'
    arquivos = [f for f in os.listdir(caminho_real_dos_arquivos) if prefixo in f and f.upper().endswith('.CSV')]
    
    if not arquivos:
        print("Nenhum arquivo correspondente encontrado neste local.")
        continue

    for arquivo in arquivos:
        # E aqui também, para montar o caminho completo do arquivo
        caminho_arquivo = os.path.join(caminho_real_dos_arquivos, arquivo)
        try:
            df = pd.read_csv(caminho_arquivo, encoding='latin1', sep=';', skiprows=8)
            df['fonte'] = arquivo
            lista_dfs.append(df)
            print(f"Arquivo lido: {arquivo}")
        except Exception as e:
            print(f"ERRO ao ler o arquivo {caminho_arquivo}: {e}")

# --- Consolidação e Exportação Final ---
if not lista_dfs:
    print("\nNenhum dado foi carregado. O arquivo final não será gerado.")
else:
    print(f"\nConsolidando um total de {len(lista_dfs)} arquivos...")
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    print("\n--- Amostra dos últimos 5 registros ---")
    print(df_consolidado.tail())
    print(f"Total de registros no arquivo final: {len(df_consolidado)}")
    df_consolidado.to_csv(ARQUIVO_SAIDA, sep=';', index=False, encoding='latin1')
    print(f"\nArquivo consolidado exportado com sucesso para: {ARQUIVO_SAIDA}")