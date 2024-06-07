import io
import os
import logging
import time
from datetime import datetime, timedelta

import pendulum
import pandas as pd
from suds.client import Client
from airflow.decorators import dag, task
# [END import_module]

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

client = Client('https://sales-ws.farfetch.com/pub/apistock.asmx?wsdl', timeout=30)
key = '1vSu3k1DvCE='

log = logging.getLogger('suds.client')
log.setLevel(logging.WARNING)
handler = logging.FileHandler('detail.log', 'a', 'utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s-%(levelname)s-%(message)s'))
log.addHandler(handler)


# [START instantiate_dag]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["farfetch"],
)
def taskflow_api():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
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
        return df

    # [END extract]

    # [START transform]
    @task(multiple_outputs=True)
    def transform(data):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        df = data
        print(df)
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
        return df

    @task()
    def load(data):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = data
        hook = PostgresHook('airflowdb')
        conn = hook.get_conn()
        cursor = conn.cursor()

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

    # [END load]

    # [START main_flow]
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary)
    # [END main_flow]


# [START dag_invocation]
taskflow_api()
