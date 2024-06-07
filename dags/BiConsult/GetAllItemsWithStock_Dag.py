import io
import os
import logging
import time
from datetime import datetime, timedelta

import pendulum
import pandas as pd
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.models import Variable
from suds.client import Client
from airflow.decorators import dag, task


CUR_DIR = os.path.abspath(os.path.dirname(__file__))
KEY = '1vSu3k1DvCE='
client = Client('https://sales-ws.farfetch.com/pub/apistock.asmx?wsdl',
                timeout=30)
log = logging.getLogger('suds.client')
log.setLevel(logging.WARNING)
handler = logging.FileHandler('detail.log', 'a', 'utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s-%(levelname)s-%(message)s')) # noqa
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
# FarFetchGetAllItemsWithStock
def farfetch_gettAll_withStock():
    # GetAllItemsWithStock
    @task
    def get_all_items_wStock(**kwargs):
        response = client.service.GetAllItemsWithStock(KEY)
        data = response.GetAllItemsWithStockResult.diffgram[0].DocumentElement[0].Table # noqa
        header = [x[0] for x in data[0]]
        body = [[str(j[1][0] if isinstance(j[1], list)
                 else str(j[1])) for j in row]for row in data]
        df = pd.DataFrame(body, columns=header)

        logging.info(f'Дата фрейм: {df.head()}')
        kwargs['ti'].xcom_push(key='db_load', value=df)


    @task
    def transform(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='get_all_items_wStock',
                                   key='db_load')
        logging.info('Дата фрейм ' +
                     f'Пришёл в слеующем виде: {df}')
        df = df.drop(['_id', '_rowOrder'], axis=1)
        bd_columns = ['item_id', 'brand', 'department', 'group_stocks',
                      'name_stocks', 'full_price', 'discount_price', '"size"',
                      'quantity', 'size_range_type', '"style"', 'gender_id',
                      'gender', 'season_id', 'season', 'barcode', 'sku',
                      'store_barcode', 'created', 'updated']
        columns_names = df.columns.tolist()
        df = df.rename(columns=dict(zip(columns_names, bd_columns)))
        print(df.info())
        df['department'] = df['department'].astype(str)
        df['group_stocks'] = df['group_stocks'].astype(str)
        df['full_price'] = df['full_price'].astype(str)
        df['discount_price'] = df['discount_price'].astype(str)
        df['"style"'] = df['"style"'].astype(str)
        df['gender'] = df['gender'].astype(str)
        df['barcode'] = df['barcode'].astype(str)
        df['sku'] = df['sku'].astype(str)
        df['store_barcode'] = df['store_barcode'].astype(str)
        df[['created', 'updated']] = None
        df['created'] = pd.to_datetime(df['created'])  # Преобразование в datetime
        df['updated'] = pd.to_datetime(df['updated'])  # Преобразование в datetime
        df['updates'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "000"
        ti.xcom_push(key='db_load', value=df)

    @task
    def db_load(**kwargs):

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = kwargs['ti'].xcom_pull(task_ids='transform', key='db_load')
        hook = PostgresHook('airflowdb')
        conn = hook.get_conn()
        cursor = conn.cursor()
        logging.info(f'Созданный датафрейм: {df.head()}')
        cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        logging.info('Execute выполнился')
        db_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        logging.info(f'БД датафрейм  {db_df.head()}')
        cursor.close()
        conn.close()

    (
        get_all_items_wStock() >>
        transform() >>
        db_load()
    )


farfetch_gettAll_withStock()
