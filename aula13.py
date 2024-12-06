import os
os.system('cls')
import numpy as np 
from matplotlib import pyplot as plt
import polars as pl 
from datetime import datetime
import gc

ENDERECO_DADOS = r'./Dados/'

try:
    print('\nIniciando leitura do arquivo parquet...')
    inicio = datetime.now()

    df_bolsa_familia = pl.read_parquet (ENDERECO_DADOS + 'bolsa_familia.parquet')

    print(df_bolsa_familia)

    fim = datetime.now()

    print(f'Tempo de execução para leitura do parquet: {fim - inicio}')
    print('\nArquivo parquet lido com sucesso')

except ImportError as e:
    print("Erro ao obter dados: ", e)

try:
    print('Visualizando a distribuição dos valores das parcela em um boxplot...')

    hora_inicio = datetime.now()

    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])
    plt.boxplot(array_valor_parcela, vert=False)
    plt.title ('Distribuição dos valores das parcelas')

    hora_fim = datetime.now()


except ImportError as e:
    print("Erro ao obter dados: ", e)