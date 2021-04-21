import os
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from airflow.models import Variable


def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    dt = df.to_json()
    context['task_instance'].xcom_push(key='download_titanic_dataset', value=dt)


def pivot_dataset(**context):
    titanic_df = context['task_instance'].xcom_pull(key='download_titanic_dataset', task_ids='download_titanic_dataset')
    dt = pd.read_json(titanic_df)
    df = dt.pivot_table(index=['Sex'],
                        columns=['Pclass'],
                        values='Name',
                        aggfunc='count').reset_index()
    push_to_postgresql('pivot_titanic', df)

def mean_fare_per_class(**context):
    titanic_df = context['task_instance'].xcom_pull(key='download_titanic_dataset', task_ids='download_titanic_dataset')
    df = pd.read_json(titanic_df)
    dt = df.groupby('Pclass')['Fare'].mean()
    push_to_postgresql('mean_fare_titanic', dt)


#@python_operator()
def push_to_postgresql(db_name, dataframe):
   # create sql engine for sqlalchemy
   alchemyEngine = create_engine('postgresql://admin:memide05@localhost:5432/gb_airflow')
   postgreSQLConnection = alchemyEngine.connect()
   postgreSQLTable = db_name
   dataframe.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='replace')
   postgreSQLConnection.close()
   pivot = Variable.get('pivot')
   mean_fare = Variable.get('mean_fares')

