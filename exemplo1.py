import os
os.system('cls')
#import pandas as pd
import polars as pl 
from datetime import datetime
import gc

ENDERECO_DADOS = r'./Dados/'

try:
    print('Obtendo Dados')
   
    incio = datetime.now()
    lista_arquivos = []    
    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    for arquivo in lista_dir_arquivos:            
        
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)

    for arquivo in lista_arquivos:
        print(f'Processndo Arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding= 'iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df
    print(df.head())

    print('\nDados dos DataFrames conectados om sucesso!')
    print('\nInciando a gravação do arquivo Parquet...')

    # df_bolsa_familia.write_parquet(ENDERECO_DADOS +'bolsa_familia.parquet')
    df_bolsa_familia_plan = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    df_bolsa_familia = df_bolsa_familia_plan.collect()


    del df_bolsa_familia
    gc.collect()

    fim = datetime.now()

    print(f'Tempo de execução: {fim - incio}')
    print('Gravação do arquivo Parquet realizada com sucesso!')

except ImportError as e:
    print("Erro ao obter dados: ", e)



