import io
import os
import logging
import time
from datetime import datetime, timedelta
<<<<<<< HEAD

import pendulum
=======
>>>>>>> 9204747347721db918b307f60a8d998249241a99
import pandas as pd
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.models import Variable
from suds.client import Client
<<<<<<< HEAD
from airflow.decorators import dag, task


=======
import pendulum
>>>>>>> 9204747347721db918b307f60a8d998249241a99
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
<<<<<<< HEAD
    def GetAllItemsWithStock(**kwargs):

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
            print(df.info())
            return df

        errorcount = 1
        while errorcount <= 6:
            try:
                time.sleep(0.5)
                df = Request(key)
                time.sleep(0.5)
                ti = kwargs['ti']
                ti.xcom_push(key='db_load', value=df)

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
    def transform(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='GetAllItemsWithStock',
                                   key='db_load')
        print(df)
=======
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
>>>>>>> 9204747347721db918b307f60a8d998249241a99
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
<<<<<<< HEAD
        ti.xcom_push(key='db_load', value=df)
=======
        df['updates'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "000"
        ti.xcom_push(key='db_load', value=df)

>>>>>>> 9204747347721db918b307f60a8d998249241a99

    @task
    def db_load(**kwargs):

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = kwargs['ti'].xcom_pull(task_ids='transform', key='db_load')
        hook = PostgresHook('airflowdb')
        conn = hook.get_conn()
        cursor = conn.cursor()
<<<<<<< HEAD

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, header=False, index=False, sep='\t')
            csv_buffer.seek(0)

            copy_query = """
                COPY farfetch.items_temp (
                    item_id, brand, department, group_stocks, name_stocks,
                    full_price, discount_price, "size", quantity, size_range_type,
                    "style", gender_id, gender, season_id, season,
                    barcode, sku, store_barcode, updated
                ) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '', HEADER);
            """
            cursor.copy_expert(sql=copy_query, file=csv_buffer)
            conn.commit()

    (
        GetAllItemsWithStock() >>
=======
        logging.info(f'Созданный датафрейм: {df.head()}')
        cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        logging.info('Execute выполнился')
        db_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        logging.info(f'БД датафрейм  {db_df.head()}')
        cursor.close()
        conn.close()

    (
        get_all_items_wStock() >>
>>>>>>> 9204747347721db918b307f60a8d998249241a99
        transform() >>
        db_load()
    )

farfetch_gettAll_withStock()
