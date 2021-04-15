import os
import pandas as pd
from airflow.models import DAG, Variable

def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    #df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    dt = df.to_json()
    context['task_instance'].xcom_push(key='download_titanic_dataset', value=dt)


def pivot_dataset(**context):
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    #df.to_csv(get_path('titanic_pivot.csv'))
    dt = df.to_json()
    context['task_instance'].xcom_push(key='pivot_dataset', value=dt)


def mean_fare_per_class(**context):
    tt_df = pd.read_csv(get_path('titanic.csv'))
    df = tt_df.groupby('Pclass')['Fare'].mean()
    #df.to_csv(get_path('titanic_mean_fares.csv'))
    dt = df.to_json()
    context['task_instance'].xcom_push(key='mean_fare_per_class', value=dt)



