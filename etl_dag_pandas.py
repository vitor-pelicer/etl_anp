from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
import os
import pandas as pd
from datetime import datetime
import glob
from sqlalchemy import create_engine

PG_IP = os.environ["PG_IP"]
PG_USR = os.environ["PG_USR"]
PG_PWD = os.environ["PG_PWD"]
PG_DB = os.environ["PG_DB"]
PATH = os.environ["ANP_PATH"]

def data_cleaning():
    # Realiza a busca de arquivos CSV no diretório PATH
    csv_files = glob.glob(os.path.join(PATH, "*.csv"))
    # Realiza a leitura e o append do dataframe de todos os arquivos CSV listados
    dfs=[]
    for file in csv_files:
        df_csv = pd.read_csv(file, sep=';', decimal=',')
        dfs.append(df_csv)
    if dfs:
        df = pd.concat(dfs)
    # Realiza as transformações de dados requeridas
    if dfs:
        df = df[~df.duplicated()]
        df = df[["Regiao - Sigla","Estado - Sigla","Municipio","Cep","Produto","Valor de Venda","Unidade de Medida"]]

        mapeamento = {"Regiao - Sigla":"regiao",
                      "Estado - Sigla":"estado",
                      "Municipio":"municipio",
                      "Cep":"cep",
                      "Produto":"produto",
                      "Valor de Venda":"valordevenda",
                      "Unidade de Medida":"unidadedemedida"}
        df=df.rename(columns=mapeamento)
        
        # Cria a conexão com o postgresql
        engine = create_engine(f'postgresql://{PG_USR}:{PG_PWD}@{PG_IP}/{PG_DB}')
        # Envia os dados para o banco
        df.to_sql("anp", engine, if_exists='replace', index=False)
        # Fecha a conexão com o postgresql
        engine.dispose()
        

default_args = {
    'start_date': datetime(2016, 1, 1),
    'owner': 'airflow'
}
    
with DAG("anp_etl", schedule_interval=None, catchup=False, default_args=default_args):
    etl = PythonOperator(task_id="etl", python_callable=data_cleaning)
    move_files = BashOperator(task_id="move_files", bash_command="mv ${ANP_PATH}/*.csv ${ANP_READ_PATH}")
    etl>>move_files
    