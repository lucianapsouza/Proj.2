# ==========================================
# IMPORTANDO BIBLIOTECAS
# ==========================================
import pandas as pd
import os
import requests

# ==========================================
# 1️⃣ BAIXANDO OS ARQUIVOS
# ==========================================
links = {
    "Janeiro_2025.csv": "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource/e1868ec1-7495-46dd-a0c7-75baf162706d",
    "Fevereiro_2025.csv": "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource/ee8c0bfa-11b6-4d7a-8094-c349b19c43dd",
    "Marco_2025.csv": "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource/65706bcd-80ab-4231-a7ef-9e329420f001",
    "Abril_2025.csv": "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource/8f22bdc1-3044-46ee-9dd1-4ace84da28e4",
    "Maio_2025.csv": "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource/aa5a886f-65a8-4e4d-9de1-b5b2c37d3ebc",
    "Junho_2025.csv": "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br/resource/670173fb-8333-49c8-8a0a-461ef2f95ded"
}

# Pasta onde os arquivos serão salvos
pasta_destino = r'C:\Users\Luciana\Documents\Data_analysis\Proj_01\Proj.2'
# cria uma pasta no caminho pasta_destino
os.makedirs(pasta_destino, exist_ok=True)

for nome_arquivo, url in links.items():  # laço para percorrer o dicionário "links"
    # variável que recebe o caminho completo
    caminho_completo = os.path.join(pasta_destino, nome_arquivo)
    # response guarda o resultado, incluindo o conteúdo do arquivo
    response = requests.get(url)

    if response.status_code == 200:  # testando se o servidor respondeu com sucesso
        with open(caminho_completo, 'wb') as f:
            f.write(response.content)
        print(f" {nome_arquivo} baixado com sucesso!")
    else:
        print(f" Falha ao baixar {nome_arquivo}")

# ==========================================
# 2️⃣ UNINDO OS DADOS
# ==========================================
pasta_uniao = pasta_destino  # usando a mesma pasta onde os arquivos foram salvos
arquivos = [f for f in os.listdir(
    pasta_uniao) if f.endswith('csv')]  # lista arquivos CSV
planilhas = []  # lista para armazenar os DataFrames

for arquivo in arquivos:
    try:
        caminho_arquivo = os.path.join(pasta_uniao, arquivo)
        df_temp = pd.read_csv(caminho_arquivo, sep=';',
                              encoding='utf-8', low_memory=False)
        # pega o nome do arquivo sem extensão
        nome_mes = os.path.splitext(arquivo)[0]
        df_temp['Mês'] = nome_mes  # adiciona coluna com o mês
        planilhas.append(df_temp)
        print(f"Lido com sucesso: {arquivo}")
    except Exception as e:
        print(f"Erro ao ler {arquivo}: {e}")

if planilhas:
    df_final = pd.concat(planilhas, ignore_index=True)
    caminho_saida = os.path.join(pasta_uniao, 'Base_Completa_Jan_Jun_2025.csv')
    df_final.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8-sig')
    print(f"\nArquivo final salvo como CSV em:\n{caminho_saida}")
else:
    print("Nenhum arquivo foi carregado com sucesso.")

# ==========================================
# 3️⃣ EXCLUINDO COLUNAS DESNECESSÁRIAS
# ==========================================
caminho_base = caminho_saida
df = pd.read_csv(caminho_base, sep=';', encoding='utf-8-sig')
print("Arquivo carregado com sucesso!")
print(df.head())

colunas_para_excluir = [
    'Gestor', 'Canal de Origem', 'Região', 'Sexo', 'Faixa Etária',
    'Ano Abertura', 'Mês Abertura', 'Data Abertura', 'Data Resposta',
    'Data Análise', 'Data Recusa', 'Prazo Resposta', 'Prazo Analise Gestor'
]

df = df.drop(columns=colunas_para_excluir, errors='ignore')
caminho_limpo = os.path.join(
    pasta_uniao, 'Base_Completa_Jan_Jun_2025_Limpo.csv')
df.to_csv(caminho_limpo, sep=';', index=False, encoding='utf-8-sig')
print(f"Arquivo limpo salvo em {caminho_limpo}")

# ==========================================
# 4️⃣ RECLASSIFICANDO DADOS DA COLUNA ÁREA
# ==========================================
mapeamento = {
    'Serviços Financeiros': 'Financeiro',
    'Telecomunicações': 'Telefonia e Informática',
    'Produtos de Telefonia e Informática': 'Telefonia e Informática',
    'Transportes': 'Serviços Básicos',
    'Saúde': 'Serviços Básicos',
    'Alimentos': 'Serviços Básicos',
    'Água, Energia, Gás': 'Serviços Básicos',
    'Habitação': 'Serviços Básicos',
    'Educação': 'Serviços Básicos',
    'Produtos Eletrodomésticos e Eletrônicos': 'Eletrodomésticos e eletrônicos',
    'Demais Serviços': 'Demais Serviços e Produtos',
    'Demais Produtos': 'Demais Serviços e Produtos',
    'Turismo/Viagens': 'Demais Serviços e Produtos',
    'Loterias, Apostas e Promoções Comerciais': 'Demais Serviços e Produtos'
}

df['Área Reclassificada'] = df['Área'].map(mapeamento)
caminho_reclass = os.path.join(pasta_uniao, 'dados_reclassificados.csv')
df.to_csv(caminho_reclass, index=False, sep=';', encoding='utf-8-sig')
print("Reclassificação concluída e arquivo salvo.")

# ==========================================
# 5️⃣ RECLASSIFICANDO OS PROBLEMAS POR PALAVRAS-CHAVE
# ==========================================


def classificar_problema(texto, grupo):
    texto = str(texto).lower()
    # Abaixo segue o mapeamento para cada grupo
    if grupo == "Cobrança / Contestação":
        if any(p in texto for p in ['indevida', 'não contratado', 'não previsto', 'duplicidade', 'cobrança', 'pagamento já efetuado']):
            return "Cobrança indevida ou duplicada"
        elif any(p in texto for p in ['dificuldade', 'boleto', 'fatura', 'informações']):
            return "Dificuldade na obtenção de boletos e informações"
        elif any(p in texto for p in ['negativação indevida', 'spc', 'serasa']):
            return "Negativação Indevida"
        else:
            return 'Outros'

    elif grupo == "Atendimento / SAC":
        if any(p in texto for p in ['demanda não resolvida', 'não respondida', 'respondida após o prazo']):
            return "Demanda não resolvida ou em atraso"
        elif any(p in texto for p in ['dificuldade de contato', 'contato', 'demora no atendimento']):
            return "Dificuldade no contato ou demora no atendimento"
        elif any(p in texto for p in ['má qualidade', 'descortesia', 'constragimento']):
            return "Má qualidade no atendimento"
        elif any(p in texto for p in ['prioritário', 'acessibilidade']):
            return "Ausência em atendimento prioritário"
        elif any(p in texto for p in ['discriminação', 'racial', 'etnia', 'idoso', 'gênero']):
            return "Diferentes tipos de discriminação"
        else:
            return 'Outros'

    elif grupo == "Contrato / Oferta":
        if any(p in texto for p in ['ligações', 'telemarketing', 'ligações indesejadas']):
            return "Ligações indesejadas"
        elif any(p in texto for p in ['dificuldade', 'dificuldades', 'informações', 'contrato', 'contratar', 'contratação']):
            return "Dificuldade com contratação e informações"
        elif any(p in texto for p in ['cancelamento', 'cancelar o serviço']):
            return "Dificuldades com cancelamento"
        elif 'venda casada' in texto:
            return "Venda Casada"
        else:
            return 'Outros'

    elif grupo == "Dados Pessoais e Privacidade":
        if any(p in texto for p in ['segurança', 'compartilhamento indevido', 'não autorizado de dados', 'vazamento de dados']):
            return "Vazamento de Dados"
        elif any(p in texto for p in ['acesso a dados', 'dados pessoais ou financeiros incorretos', 'desatualização']):
            return "Dificuldade de acesso ou atualização de dados pessoais"
        else:
            return 'Outros'

    elif grupo == 'Entrega do Produto':
        if any(p in texto for p in ['não entrega', 'demora na entrega']):
            return "Problemas com entrega"
        else:
            return 'Outros'

    elif grupo == 'Informação':
        if any(p in texto for p in ['informações incompletas', 'inadequadas']):
            return 'Informações incompletas ou inadequadas'
        elif 'dificuldade' in texto:
            return 'Dificuldade na obtenção de informações'
        else:
            return 'Outros'

    elif grupo == 'Saúde e Segurança':
        if any(p in texto for p in ['risco', 'dano físico']):
            return 'Dano físico decorrente da prestação de serviço'
        elif any(p in texto for p in ['validade', "alteração de odor, sabor", 'produto sem inspeção', 'vencida']):
            return 'Problemas com validade ou condição do produto'
        elif 'informação nutricional' in texto:
            return 'Informações nutricionais falsas'
        else:
            return 'Outros'

    elif grupo == 'Vício de Qualidade':
        if any(p in texto for p in ['produto danificado', 'não funciona']):
            return 'Produto danificado'
        elif any(p in texto for p in ['funcionamento inadequado', 'instabilidade', 'queda', 'interrupção', 'suspensão']):
            return 'Funcionamento ou suspensão inadequada do serviço'
        elif 'má qualidade' in texto:
            return 'Má qualidade no serviço'
        else:
            return 'Outros'


# Aplicando a classificação
df['Subcategoria Problema'] = df.apply(lambda row: classificar_problema(
    row['Problema'], row['Grupo Problema']), axis=1)
caminho_subcat = os.path.join(
    pasta_uniao, 'Base_Completa_Jan_Jun_2025_ComSubcategoria.csv')
df.to_csv(caminho_subcat, index=False, sep=';', encoding='utf-8-sig')
print(f"Classificação concluída. Arquivo salvo em {caminho_subcat}")
