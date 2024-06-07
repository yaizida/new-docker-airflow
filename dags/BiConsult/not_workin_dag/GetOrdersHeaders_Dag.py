import io
import os
import logging
import time
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from suds.client import Client
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
CUR_DIR = os.path.abspath(os.path.dirname(__file__))
KEY = Variable.get("farfetch_key")
client = Client('https://sales-ws.farfetch.com/pub/apistock.asmx?wsdl', timeout=30)
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
    dag_id='GetOrdersHeaders',
    default_args=default_args,
    schedule='0 * * * SUN-FRI',
    catchup=False,
    concurrency=4,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['FarFetch'],
)
def FarFetchGetOrdersHeaders():
    @task
    def GetOrdersHeaders():
        def Request(Step, key):
            df = pd.DataFrame()
            response = client.service.GetOrdersHeaders(Step, key)
            '''with open(f"./Dictionary/{Step}_OrdersHeaders.xml", "wb") as f:
                f.write(response)'''
            for s in range(len(response.Data.OrderWorkflowStepCommonStoreHeaderDTO)):
                header = []
                header.append('Step')
                body = []
                row = []
                row.append(Step)
                for j in response.Data.OrderWorkflowStepCommonStoreHeaderDTO[s]:
                    header.append(j[0])
                for j in response.Data.OrderWorkflowStepCommonStoreHeaderDTO[s]:
                    if type(j[1]) is list:
                        row.append(str(j[1][0]))
                    else:
                        row.append(str(j[1]))
                body.append(row)
                df1 = pd.DataFrame(body, columns=header)
                result = df.append(df1)
                df = result
            csv_file = f'{CUR_DIR}/resources/Dictionary/{Step}_OrdersHeaders_{datetime.now().date()}.csv'
            df.drop_duplicates().to_csv(csv_file, encoding='utf-8', index=False)
            return csv_file
        for Step in ['Step1', 'Step2', 'Step3']:
            errorcount = 1
            while errorcount <= 6:
                try:
                    time.sleep(0.5)
                    Request(Step, KEY)
                    time.sleep(0.5)
                    break
                except Exception as e:
                    if errorcount == 6:
                        print(
                            f'============Error============ \n Ошибка произошла в период загрузки справочника позиций  \n {e} \n'
                            f'=============================')
                    else:
                        print(f'Попытка {errorcount} для справочника заголовков заказов ')
                    errorcount = errorcount + 1

    GetOrdersHeaders()


FarFetchGetOrdersHeaders()
