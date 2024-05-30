import os
import pendulum
from airflow.decorators import dag, task
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
from suds.client import Client

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
    schedule='0 * * * SUN-FRI',
    catchup=False,
    concurrency=4,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['FarFetch'],
)
def FarFetchGetAllItemsWithStock():

    @task
    def GetAllItemsWithStock():

        def Request(key):
            df = pd.DataFrame()
            response = client.service.GetAllItemsWithStock(key)

            header = []
            for j in response.GetAllItemsWithStockResult.diffgram[0].DocumentElement[0].Table[0]:
                header.append(j[0])

            body = []
            for s in range(len(response.GetAllItemsWithStockResult.diffgram[0].DocumentElement[0].Table)):
                print(s)
                row = []
                for j in response.GetAllItemsWithStockResult.diffgram[0].DocumentElement[0].Table[s]:
                    if type(j[1]) is list:
                        row.append(str(j[1][0]))
                    else:
                        row.append(str(j[1]))
                body.append(row)
                df = pd.DataFrame(body, columns=header)

            directory_path = f'{CUR_DIR}/resources/Dictionary'

            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
            else:
                print("Директория '%s' уже существует." % directory_path)

            file_path = f'{CUR_DIR}/resources/Dictionary/items_{datetime.now().date()}.csv'
            df.drop_duplicates().to_csv(file_path, encoding='utf-8', index=False)

        errorcount = 1
        while errorcount <= 6:
            try:
                time.sleep(0.5)
                Request(key)
                time.sleep(0.5)
                break
            except Exception as e:
                if errorcount == 6:
                    print(
                        f'============Error============ \n Ошибка произошла в период загрузки справочника позиций  \n {e} \n'
                        f'=============================')
                else:
                    print(f'Попытка {errorcount} для справочника позиций ')
                errorcount = errorcount + 1

    @task
    def latest_file(**kwargs):
        ti = kwargs['ti']
        directory = f'{CUR_DIR}/resources/Dictionary'
        latest_file = None
        latest_timestamp = 0
        print('__' * 30)
        print(directory)
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                timestamp = os.path.getmtime(filepath)
                if timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                    latest_file = filepath
        df = pd.read_csv(latest_file)
        print(df.info())
        ti.xcom_push(key='load_csv_posgres', value=latest_file)

    @task
    def transform(**kwargs):
        ti = kwargs['ti']
        latest_file = ti.xcom_pull(task_ids='latest_file',
                                   key='load_csv_posgres')
        df = pd.read_csv(latest_file)
        df = df.drop(['_id', '_rowOrder'], axis=1)
        bd_columns = ['item_id', 'brand', 'department', 'group_stocks',
                      'name_stocks', 'full_price', 'discount_price', '"size"',
                      'quantity', 'size_range_type', '"style"', 'gender_id',
                      'gender', 'season_id', 'season', 'barcode', 'sku',
                      'store_barcode', 'created', 'updated']
        columns_names = df.columns.tolist()
        renamed_df = df.rename(columns=dict(zip(columns_names, bd_columns)))
        file_path = f'{CUR_DIR}/resources/Dictionary/transform_items_{datetime.now().date()}.csv'
        renamed_df.drop_duplicates().to_csv(file_path, encoding='utf-8', index=False)
        ti.xcom_push(key='db_load', value=file_path)

    @task
    def db_load(**kwargs):
        ti = kwargs['ti']
        db_load = ti.xcom_pull(task_ids='transform', key='db_load')
        df = pd.read_csv(db_load)
        print(df.info())

    (
        GetAllItemsWithStock() >>
        latest_file() >>
        transform() >>
        db_load()
    )


FarFetchGetAllItemsWithStock()
