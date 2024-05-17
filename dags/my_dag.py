import os

import pendulum
from airflow.decorators import dag, task


@dag(
    schedule='*/1   *   *   *   * ',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)

def test_task():
    import pandas as pd

    @task()
    def extract_data(**kwargs):

        ti = kwargs['ti']
        # Получаем путь к директории DAG файла
        dag_folder = os.path.dirname(__file__)

        # Формируем полный путь к CSV файлу
        csv_path = os.path.join(dag_folder, "data/test.csv")

        # Ебашим датафрейм
        df = pd.read_csv(csv_path)

        with open(csv_path, 'r') as f:
            header = f.readline().strip().split(',')
        # logger.debug(header)
        # print(header)
        # пушим данные в xcom
        ti.xcom_push(key='load_csv_posgres', value=df)

    @task
    def print_data(**kwargs):
        ti = kwargs['ti']

        value = ti.xcom_pull(key='load_csv_posgres')

        print(value)

    (
        extract_data()
        >> print_data()
    )


test_task()
