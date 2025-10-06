import pandas as pd 
import requests
from datetime import date
import io
import zipfile
import psycopg2
import numpy as np
from psycopg2 import OperationalError
from psycopg2.extras import execute_values

conn_params = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",   
        "password": "12345",  
        "dbname": "postgres"       
    }


df_agg = pd.DataFrame() 
anos = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025]
for ano in anos:
    print(ano)
    if ano == 2025:
        url_zip = f"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/Leitos_csv_{ano}.zip"
        df = pd.read_csv(url_zip, compression='zip', sep=';', encoding="latin-1", dtype=str)

    else:
        url_zip = f"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/Leitos_{ano}.csv"
        df = pd.read_csv(url_zip, sep=',', encoding="latin-1", dtype=str)
        df.columns = ['COMP', 'REGIAO', 'UF', 'MUNICIPIO', 'MOTIVO_DESABILITACAO',
               'CNES', 'NOME_ESTABELECIMENTO', 'RAZAO_SOCIAL', 'TP_GESTAO',
               'CO_TIPO_UNIDADE', 'DS_TIPO_UNIDADE', 'NATUREZA_JURIDICA',
               'DESC_NATUREZA_JURIDICA', 'NO_LOGRADOURO', 'NU_ENDERECO',
               'NO_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP', 'NU_TELEFONE', 'NO_EMAIL',
               'LEITOS_EXISTENTES', 'LEITOS_SUS', 'UTI_TOTAL_EXIST', 'UTI_TOTAL_SUS',
               'UTI_ADULTO_EXIST', 'UTI_ADULTO_SUS', 'UTI_PEDIATRICO_EXIST',
               'UTI_PEDIATRICO_SUS', 'UTI_NEONATAL_EXIST', 'UTI_NEONATAL_SUS',
               'UTI_QUEIMADO_EXIST', 'UTI_QUEIMADO_SUS', 'UTI_CORONARIANA_EXIST',
               'UTI_CORONARIANA_SUS']
        df['CO_IBGE'] = 'nan'

        df = df[['COMP', 'REGIAO', 'UF', 'CO_IBGE','MUNICIPIO', 'MOTIVO_DESABILITACAO',
               'CNES', 'NOME_ESTABELECIMENTO', 'RAZAO_SOCIAL', 'TP_GESTAO',
               'CO_TIPO_UNIDADE', 'DS_TIPO_UNIDADE', 'NATUREZA_JURIDICA',
               'DESC_NATUREZA_JURIDICA', 'NO_LOGRADOURO', 'NU_ENDERECO',
               'NO_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP', 'NU_TELEFONE', 'NO_EMAIL',
               'LEITOS_EXISTENTES', 'LEITOS_SUS', 'UTI_TOTAL_EXIST', 'UTI_TOTAL_SUS',
               'UTI_ADULTO_EXIST', 'UTI_ADULTO_SUS', 'UTI_PEDIATRICO_EXIST',
               'UTI_PEDIATRICO_SUS', 'UTI_NEONATAL_EXIST', 'UTI_NEONATAL_SUS',
               'UTI_QUEIMADO_EXIST', 'UTI_QUEIMADO_SUS', 'UTI_CORONARIANA_EXIST',
               'UTI_CORONARIANA_SUS']]
            
    df_agg = pd.concat([df_agg, df], ignore_index=True)
    print(f"{len(df)} dados resgatados de {ano}\n")

lista_inteiros = ['COMP','LEITOS_EXISTENTES', 'LEITOS_SUS','UTI_TOTAL_EXIST', 'UTI_TOTAL_SUS',
'UTI_ADULTO_EXIST', 'UTI_ADULTO_SUS', 'UTI_PEDIATRICO_EXIST',
'UTI_PEDIATRICO_SUS', 'UTI_NEONATAL_EXIST', 'UTI_NEONATAL_SUS',
'UTI_QUEIMADO_EXIST', 'UTI_QUEIMADO_SUS', 'UTI_CORONARIANA_EXIST',
'UTI_CORONARIANA_SUS']

for col in lista_inteiros:
    df_agg[col] = df_agg[col].astype("Int64")
    
df_agg.columns = ['ANOMES', 'REGIAO', 'UF', 'CO_IBGE','MUNICIPIO', 'MOTIVO_DESABILITACAO',
       'COD_CNES', 'NM_ESTABELECIMENTO', 'RAZAO_SOCIAL', 'TP_GESTAO',
       'CO_TIPO_UNIDADE', 'DSCR_TIPO_UNIDADE', 'NATUREZA_JURIDICA',
       'DESC_NATUREZA_JURIDICA', 'NO_LOGRADOURO', 'NU_ENDERECO',
       'NO_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP', 'NU_TELEFONE', 'NO_EMAIL',
       'QTD_LEITOS_EXISTENTES', 'QTD_LEITOS_SUS', 'QTD_UTI_TOTAL_EXIST', 'QTD_UTI_TOTAL_SUS',
       'QTD_UTI_ADULTO_EXIST', 'QTD_UTI_ADULTO_SUS', 'QTD_UTI_PEDIATRICO_EXIST',
       'QTD_UTI_PEDIATRICO_SUS', 'QTD_UTI_NEONATAL_EXIST', 'QTD_UTI_NEONATAL_SUS',
       'QTD_UTI_QUEIMADO_EXIST', 'QTD_UTI_QUEIMADO_SUS', 'QTD_UTI_CORONARIANA_EXIST',
       'QTD_UTI_CORONARIANA_SUS']


df_fato_leitos = df_agg[['ANOMES', 'COD_CNES', 'NM_ESTABELECIMENTO', 'DSCR_TIPO_UNIDADE',
                        'QTD_LEITOS_EXISTENTES', 'QTD_LEITOS_SUS', 'QTD_UTI_TOTAL_EXIST', 'QTD_UTI_TOTAL_SUS',
                        'QTD_UTI_ADULTO_EXIST', 'QTD_UTI_ADULTO_SUS', 'QTD_UTI_PEDIATRICO_EXIST',
                        'QTD_UTI_PEDIATRICO_SUS', 'QTD_UTI_NEONATAL_EXIST', 'QTD_UTI_NEONATAL_SUS',
                        'QTD_UTI_QUEIMADO_EXIST', 'QTD_UTI_QUEIMADO_SUS', 'QTD_UTI_CORONARIANA_EXIST',
                        'QTD_UTI_CORONARIANA_SUS']]

df_fato_leitos.replace({pd.NA: 0}, inplace = True)
df_fato_leitos.replace({np.nan: 0}, inplace = True)

df_fato_leitos.columns= df_fato_leitos.columns.str.lower()

def insert_data(cursor, table, columns, df_sep, conflict_column, update_on_conflict=False):

        if df.empty:
            print(f"[!] DataFrame vazio para {table}, nada inserido.")
            return
        
        if isinstance(conflict_column, (list, tuple)):
            conflict_target = ", ".join(conflict_column)
        else:
            conflict_target = conflict_column

        if update_on_conflict:
            update_cols = [col for col in columns if col != conflict_column]
            set_clause = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
            conflict_clause = f"ON CONFLICT ({conflict_target}) DO UPDATE SET {set_clause}"
        else:
            conflict_clause = f"ON CONFLICT ({conflict_target}) DO NOTHING"

        sql = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES %s
            {conflict_clause};
        """
        values = [tuple(row) for row in df_sep.to_numpy()]
        execute_values(cursor, sql, values, page_size=1000)
        action = "UPSERT (insert/update)" if update_on_conflict else "INSERT (ignorar duplicatas)"
        print(action)

table = {"fato_leito": (
            ['anomes', 'cod_cnes', 'nm_estabelecimento', 'dscr_tipo_unidade',
           'qtd_leitos_existentes', 'qtd_leitos_sus', 'qtd_uti_total_exist',
           'qtd_uti_total_sus', 'qtd_uti_adulto_exist', 'qtd_uti_adulto_sus',
           'qtd_uti_pediatrico_exist', 'qtd_uti_pediatrico_sus',
           'qtd_uti_neonatal_exist', 'qtd_uti_neonatal_sus',
           'qtd_uti_queimado_exist', 'qtd_uti_queimado_sus',
           'qtd_uti_coronariana_exist', 'qtd_uti_coronariana_sus'],
            df_fato_leitos,
            ("anomes", "cod_cnes"),
            True
    )}

try:
    conn = None
    cursor = None
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            for tables, (columns, df_sep, conflict_column, update_on_conflict) in table.items():
                    print(f"Processando tabela: {table}")
                    insert_data(cursor, tables, columns, df_sep, conflict_column, update_on_conflict)
        conn.commit()
        print("Dados inseridos com sucesso.")
except OperationalError as e:
    print(f"Erro de conexão: {e}")
except psycopg2.Error as e:
    print(f"Ocorreu um erro ao inserir dados: {e}")


        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        