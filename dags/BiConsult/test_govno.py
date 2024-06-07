import io
import os
import logging
import time
from datetime import datetime, timedelta

import pendulum
import pandas as pd
from suds.client import Client
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

client = Client('https://sales-ws.farfetch.com/pub/apistock.asmx?wsdl', timeout=30)
key = '1vSu3k1DvCE='

log = logging.getLogger('suds.client')
log.setLevel(logging.WARNING)
handler = logging.FileHandler('detail.log', 'a', 'utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s-%(levelname)s-%(message)s'))
log.addHandler(handler)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5)
}


@dag(
    dag_id='GetAllItemsWithStock',
    default_args=default_args,
    schedule='@once',
    catchup=False,
    concurrency=4,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['FarFetch'],
)
def FarFetchGetAllItemsWithStock():
    def Request(key):
        response = client.service.GetAllItemsWithStock(key)
        data = response.GetAllItemsWithStockResult.diffgram[0].DocumentElement[0].Table
        header = [j[0] for j in data[0]]
        body = [[str(j[1][0] if type(j[1]) is list else str(j[1])) for j in row] for row in data]
        df = pd.DataFrame(body, columns=header)
        return df

    @task
    def GetAllItemsWithStock(**kwargs):
        try:
            df = Request(key)
            task_instance = get_current_context()
            task_instance.xcom_push(key='db_load', value=df)
        except Exception as e:
            logging.error(f'Ошибка при загрузке справочника позиций: {e}')

        logging.info(f'DataFrame информация: {df.info()}')


    @task
    def return_data(**kwargs):
        task_instance = get_current_context()
        df = task_instance.xcom_pull(key='db_load')

        logging.info(f'DataFrame информация: {df.info()}')

    (
        GetAllItemsWithStock() >>
        return_data()
    )
FarFetchGetAllItemsWithStock()