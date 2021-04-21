from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from util.settings import default_settings
from util.tasks import download_titanic_dataset, pivot_dataset, mean_fare_per_class

# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(
        **default_settings(),  # Базовые аргументы
) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
    )
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
    )
    #Cчитывает файл titanic.csv и расчитывает среднюю
    #арифметическую цену билета (Fare) для каждого класса (Pclass)
    # и сохраняет результирующий датафрейм в файл titanic_mean_fares.csv
    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fare_per_class',
        python_callable=mean_fare_per_class,
    )

    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is: {{ ds }}"',
    )

    # Порядок выполнения тасок
first_task >> create_titanic_dataset >> mean_fares_titanic_dataset >> pivot_titanic_dataset >> last_task
