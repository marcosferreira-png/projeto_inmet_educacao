import requests
import zipfile
import io

# Define o intervalo de anos
ano_inicio = 2000
ano_fim = 2025

# Itera sobre cada ano no intervalo (o range é inclusivo no início, exclusivo no fim, por isso usamos ano_fim + 1)
for ano in range(ano_inicio, ano_fim + 1):
    # Constrói a URL dinamicamente usando o ano atual
    # Apenas esta linha foi modificada para aceitar a variável 'ano'
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"

    print(f"\n=======================================================")
    print(f"Iniciando download e processamento do ano: {ano} (URL: {url})")
    print(f"=======================================================")
    
    # Faz o download
    response = requests.get(url)

    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # Filtrar arquivos que começam com 'INMET_N_AM_'
            prefixo = 'INMET_N_AM_'
            arquivos_am = [f for f in zip_file.namelist() if prefixo in f]

            if not arquivos_am:
                 print(f"Nenhum arquivo encontrado com o prefixo '{prefixo}' no ZIP de {ano}.")
                 continue # Pula para o próximo ano se não houver arquivos

            print(f"Total de arquivos com prefixo '{prefixo}': {len(arquivos_am)}")

            # Extrair esses arquivos para uma pasta local (a pasta agora inclui o ano)
            pasta_destino = f"./dados_amazonas_{ano}"
            
            # Nota: O método extractall cria o diretório de destino se ele não existir
            zip_file.extractall(path=pasta_destino, members=arquivos_am)

            for arquivo in arquivos_am:
                print(f"Extraído: {arquivo} -> para {pasta_destino}")
    else:
        print(f"Erro ao baixar o arquivo para o ano {ano}. Status: {response.status_code}")
