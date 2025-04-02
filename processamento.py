import os
import pandas as pd
import numpy as np
from datetime import datetime
from unidecode import unidecode
import re

# Configuração de caminhos
DADOS_ORIGINAIS = 'dados/originais'
DADOS_LIMPOS = 'dados/limpos'

# Definição das colunas
COLUNAS_RELEVANTES = [
    'COMP', 'REGIAO', 'UF', 'MUNICIPIO', 'CNES', 'DS_TIPO_UNIDADE',
    'DESC_NATUREZA_JURIDICA', 'TP_GESTAO', 'LEITOS EXISTENTES', 'LEITOS SUS',
    'UTI TOTAL - EXIST', 'UTI TOTAL - SUS', 'UTI ADULTO - EXIST', 'UTI ADULTO - SUS',
    'UTI PEDIATRICO - EXIST', 'UTI PEDIATRICO - SUS', 'UTI NEONATAL - EXIST',
    'UTI NEONATAL - SUS', 'UTI QUEIMADO - EXIST', 'UTI QUEIMADO - SUS',
    'UTI CORONARIANA - EXIST', 'UTI CORONARIANA - SUS'
]

COLUNAS_REMOVER = [
    'MOTIVO DESABILITACAO', 'RAZAO SOCIAL', 'NO_LOGRADOURO', 'NU_ENDERECO',
    'NO_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP', 'NU_TELEFONE', 'NO_EMAIL',
    'ID_ESTABELECIMENTO', 'NOME ESTABELECIMENTO', 'CO_TIPO_UNIDADE', 'NATUREZA_JURIDICA'
]

MAPEAMENTOS = {
    'TP_GESTAO': {
        'M': 'Municipal',
        'E': 'Estadual',
        'D': 'Dupla',
        'S': 'Sem Gestão'
    },
    'REGIAO': {
        'NORDESTE': 'Nordeste',
        'NORTE': 'Norte',
        'SUDESTE': 'Sudeste',
        'CENTRO-OESTE': 'Centro-Oeste',
        'SUL': 'Sul'
    }
}

def remover_colunas(df):
    """Remove colunas desnecessárias mantendo apenas as relevantes"""
    return df[[col for col in COLUNAS_RELEVANTES if col in df.columns and col != 'ANO']]

from unidecode import unidecode
import re

def normalizar_dados(df, ano_arquivo):
    """Aplica normalizações e transformações nos dados de forma otimizada"""
    # Converter colunas numéricas
    colunas_numericas = [col for col in df.columns if any(keyword in col for keyword in ['LEITOS', 'UTI'])]
    for col in colunas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Processar a coluna COMP para extrair ano e mês
    if 'COMP' in df.columns:
        df['COMP'] = df['COMP'].astype(str).str.zfill(6)  # Garante 6 dígitos
        df['ANO'] = df['COMP'].str[:4].astype(int)
        df['MES'] = df['COMP'].str[4:6].astype(int)
        
        # Validação básica dos dados
        mask = (df['MES'] < 1) | (df['MES'] > 12)
        if mask.any():
            print(f"  Aviso: {mask.sum()} registros com mês inválido. Corrigindo para 1...")
            df.loc[mask, 'MES'] = 1
    else:
        # Se não tiver COMP, usamos o ano do arquivo e mês 1
        df['ANO'] = ano_arquivo
        df['MES'] = 1
    
    # Aplicar mapeamentos de categorias
    for col, mapeamento in MAPEAMENTOS.items():
        if col in df.columns and col != 'DESC_NATUREZA_JURIDICA':  # Tratamos essa separadamente
            df[col] = df[col].replace(mapeamento)
    
    # Função para corrigir a natureza jurídica
    def corrigir_natureza_juridica(texto):
        if not isinstance(texto, str) or texto.strip() == '':
            return 'NAO INFORMADO'
        
        # Padronização básica
        texto = (
            texto.strip().upper()
            .replace('_', ' ')  # Remove underscores
            .replace('-', ' ')   # Remove hífens
        )
        
        # Remove acentos e caracteres especiais
        texto_sem_acentos = unidecode(texto)
        
        # Corrige erros comuns de digitação usando expressões regulares
        texto_corrigido = (
            re.sub(r'FILANTR[OÓ]?PICO', 'FILANTROPICO', texto_sem_acentos)
            .replace('PBLICO', 'PUBLICO')
            .replace('HOSPITAL P BLICO', 'HOSPITAL PUBLICO')
            .replace('FILANTRPICO', 'FILANTROPICO')
            .replace('HOSPITALPUBLICO', 'HOSPITAL PUBLICO')
            .replace('HOSPITALFILANTROPICO', 'HOSPITAL FILANTROPICO')
        )
        
        # Remove espaços extras e padroniza
        texto_final = ' '.join(texto_corrigido.split())
        
        # Mapeamento final para garantir consistência
        if 'HOSPITAL FILANTROPICO' in texto_final:
            return 'HOSPITAL FILANTROPICO'
        elif 'HOSPITAL PUBLICO' in texto_final:
            return 'HOSPITAL PUBLICO'
        
        return texto_final if texto_final != '' else 'NAO INFORMADO'
    
    # Aplicar correção para natureza jurídica
    if 'DESC_NATUREZA_JURIDICA' in df.columns:
        df['DESC_NATUREZA_JURIDICA'] = (
            df['DESC_NATUREZA_JURIDICA']
            .astype(str)
            .apply(corrigir_natureza_juridica)
        )
        
        # Aplica mapeamentos finais se existirem no MAPEAMENTOS
        if 'DESC_NATUREZA_JURIDICA' in MAPEAMENTOS:
            df['DESC_NATUREZA_JURIDICA'] = (
                df['DESC_NATUREZA_JURIDICA']
                .replace(MAPEAMENTOS['DESC_NATUREZA_JURIDICA'])
                .str.strip()
            )
    
    # Padronizar outras colunas de texto
    text_cols = ['REGIAO', 'UF', 'MUNICIPIO', 'DS_TIPO_UNIDADE']
    for col in text_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.upper()
                .apply(unidecode)  # Remove acentos
            )
    
    return df

def verificar_consistencia_colunas():
    """
    Verifica a consistência das colunas entre todos os arquivos CSV.
    Mostra quais colunas diferem em cada arquivo que não segue o padrão.
    """
    import glob
    
    # 1. Definir o padrão de colunas esperado (baseado no arquivo de 2018)
    COLUNAS_ESPERADAS = [
        "COMP", "REGIAO", "UF", "MUNICIPIO", "MOTIVO DESABILITACAO", "CNES",
        "NOME ESTABELECIMENTO", "RAZAO SOCIAL", "TP_GESTAO", "CO_TIPO_UNIDADE",
        "DS_TIPO_UNIDADE", "NATUREZA_JURIDICA", "DESC_NATUREZA_JURIDICA",
        "NO_LOGRADOURO", "NU_ENDERECO", "NO_COMPLEMENTO", "NO_BAIRRO", "CO_CEP",
        "NU_TELEFONE", "NO_EMAIL", "LEITOS EXISTENTES", "LEITOS SUS",
        "UTI TOTAL - EXIST", "UTI TOTAL - SUS", "UTI ADULTO - EXIST",
        "UTI ADULTO - SUS", "UTI PEDIATRICO - EXIST", "UTI PEDIATRICO - SUS",
        "UTI NEONATAL - EXIST", "UTI NEONATAL - SUS", "UTI QUEIMADO - EXIST",
        "UTI QUEIMADO - SUS", "UTI CORONARIANA - EXIST", "UTI CORONARIANA - SUS"
    ]
    
    # 2. Encontrar todos os arquivos CSV
    arquivos = glob.glob(os.path.join(DADOS_ORIGINAIS, 'Leitos_*.csv'))
    
    if not arquivos:
        print("Nenhum arquivo CSV encontrado para verificação")
        return False
    
    # 3. Verificar cada arquivo
    problemas_encontrados = False
    
    for arquivo in arquivos:
        ano = os.path.basename(arquivo).split('_')[1].split('.')[0]
        try:
            # Ler apenas o cabeçalho
            df = pd.read_csv(arquivo, nrows=0)
            colunas_arquivo = list(df.columns)
            
            # Verificar se as colunas são idênticas às esperadas
            if colunas_arquivo != COLUNAS_ESPERADAS:
                problemas_encontrados = True
                print(f"\n=== Problemas encontrados no arquivo {ano} ===")
                
                # Verificar colunas faltantes
                faltantes = set(COLUNAS_ESPERADAS) - set(colunas_arquivo)
                if faltantes:
                    print("\nColunas ESPERADAS que estão FALTANDO:")
                    for col in sorted(faltantes):
                        print(f"- {col}")
                
                # Verificar colunas extras
                extras = set(colunas_arquivo) - set(COLUNAS_ESPERADAS)
                if extras:
                    print("\nColunas PRESENTES que NÃO ERAM ESPERADAS:")
                    for col in sorted(extras):
                        print(f"- {col}")
                
                # Verificar diferenças de nomenclatura
                print("\nDiferenças de nomenclatura (esperado -> encontrado):")
                for esperado, encontrado in zip(COLUNAS_ESPERADAS, colunas_arquivo):
                    if esperado != encontrado:
                        print(f"- Esperado: '{esperado}' | Encontrado: '{encontrado}'")
                        
        except Exception as e:
            print(f"\nErro ao verificar o arquivo {arquivo}: {str(e)}")
            problemas_encontrados = True
    
    # 4. Resumo final
    if not problemas_encontrados:
        print("\nTodos os arquivos estão com as colunas no padrão esperado!")
        return True
    else:
        print("\nATENÇÃO: Foram encontrados problemas de consistência nas colunas.")
        return False


def processar_arquivo(arquivo_path, ano):
    """Processa um único arquivo CSV com tratamento de encoding"""
    print(f"Processando arquivo: {os.path.basename(arquivo_path)}...")
    
    # Tenta ler com utf-8, se falhar tenta latin-1
    try:
        df = pd.read_csv(arquivo_path, encoding='utf-8', sep=',')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(arquivo_path, encoding='latin-1', sep=',')
            print("   Arquivo lido com encoding latin-1")
        except Exception as e:
            print(f"   Erro ao ler arquivo: {e}")
            return None
    
    # Processar dados
    df = remover_colunas(df)
    return normalizar_dados(df, ano)

def processar_todos_arquivos():
    """Processa todos os arquivos na pasta originais"""
    # Verificar se a pasta existe
    if not os.path.exists(DADOS_ORIGINAIS):
        print(f"Erro: Pasta não encontrada - {DADOS_ORIGINAIS}")
        return None
    
    # Criar pasta de saída se não existir
    os.makedirs(DADOS_LIMPOS, exist_ok=True)
    
    dados_consolidados = []
    arquivos_processados = 0
    
    for arquivo in os.listdir(DADOS_ORIGINAIS):
        if arquivo.startswith('Leitos_') and arquivo.endswith('.csv'):
            try:
                ano = int(arquivo.split('_')[1].split('.')[0])
                caminho_arquivo = os.path.join(DADOS_ORIGINAIS, arquivo)
                df = processar_arquivo(caminho_arquivo, ano)
                
                if df is not None:
                    dados_consolidados.append(df)
                    arquivos_processados += 1
            except Exception as e:
                print(f"Erro ao processar {arquivo}: {str(e)}")
    
    if arquivos_processados == 0:
        print("Nenhum arquivo foi processado com sucesso.")
        return None
    
    # Consolidar dados
    df_final = pd.concat(dados_consolidados, ignore_index=True)
    df_final.sort_values(['ANO', 'MES', 'UF'], inplace=True)
    
    # Salvar arquivo consolidado
    data_atual = datetime.now().strftime('%Y%m%d_%H%M%S')
    caminho_saida = os.path.join(DADOS_LIMPOS, f'leitos_consolidados_{data_atual}.csv')
    df_final.to_csv(caminho_saida, index=False, encoding='utf-8', sep=',')
    
    print(f"\nProcessamento concluído com sucesso!")
    print(f"Arquivos processados: {arquivos_processados}")
    print(f"Arquivo gerado: {caminho_saida}")
    print(f"Total de registros: {len(df_final):,}")
    print(f"Período coberto: {df_final['ANO'].min()} a {df_final['ANO'].max()}")
    
    return df_final

if __name__ == '__main__':
    print("Iniciando verificação de consistência das colunas...")
    
    if verificar_consistencia_colunas():
        print("\nIniciando processamento dos dados...")
        df_final = processar_todos_arquivos()
    else:
        print("\nProcessamento interrompido. Corrija as inconsistências nas colunas antes de continuar.")