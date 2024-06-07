import io
import os
import logging
import time
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from suds.client import Client
from airflow.decorators import dag, task
from airflow.models import Variable
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
    dag_id='GetOrdersRows',
    default_args=default_args,
    schedule='0 * * * SUN-FRI',
    catchup=False,
    concurrency=4,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['FarFetch'],
)
def FarFetchGetOrdersRows():
    @task
    def GetOrdersRows():
        def OrderInfo(orderid, key):
            dforder = pd.DataFrame()
            body = []
            header = []
            header.append('OrderId')
            response = client.service.GetOrdersRows(orderid, key)
            for j in response.Data.OrderWorkflowStepCommonStoreRowDTO[0]:
                header.append(j[0])
            for s in range(len(response.Data.OrderWorkflowStepCommonStoreRowDTO)):
                row = []
                row.append(orderid)
                for j in response.Data.OrderWorkflowStepCommonStoreRowDTO[s]:
                    if type(j[1]) is list:
                        row.append(str(j[1][0]))
                    else:
                        row.append(str(j[1]))
                body.append(row)
                df1 = pd.DataFrame(body, columns=header)
                result = dforder.append(df1)
                dforder = result
            return dforder

        def Request(key, datestart, dataend, filename):
            global e
            df = pd.DataFrame()
            listtest = []
            response = client.service.GetOrdersByDate(key, datestart, dataend)
            for s in range(len(response.GetOrdersByDateResult.diffgram[0].DocumentElement[0].dtGOBD)):
                listtest.append(response.GetOrdersByDateResult.diffgram[0].DocumentElement[0].dtGOBD[s][2][0])
            listtest = list(set(listtest))
            for x in listtest:
                errorcount = 1
                while errorcount <= 6:
                    try:
                        print(x)
                        res = OrderInfo(x, key)
                        df = df.append(res)
                        break
                    except Exception as e:
                        if errorcount == 6:
                            print(
                                f'============Error============\n Ошибка произошла в период загрузки id {x}  \n {e}\n '
                                f'=============================')
                        else:
                            print(f'Попытка {errorcount} для {x}')
                        errorcount = errorcount + 1
            csv_path = f'{CUR_DIR}/resources/GetOrdersRows/{filename}.csv'
            df.drop_duplicates().to_csv(csv_path, encoding='utf-8', index=False)
            return csv_path
        datelist = pd.date_range(start=str(datetime.now().date() - timedelta(days=14)), end=str(datetime.now().date()))
        for i, date in enumerate(datelist):
            date_start = datetime.combine(date, datetime.min.time()) - timedelta(hours=2)
            date_end = (date_start + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
            filename = date_start.strftime("%Y%m%d")
            date_start = date_start.strftime('%Y-%m-%dT%H:%M:%S')
            errorcount = 1
            while errorcount <= 6:
                try:
                    print(f'{date_start}-{date_end}')
                    data = Request(KEY, date_start, date_end, filename)
                    break
                except Exception as e:
                    if errorcount == 6:
                        print(
                            f'============Error============ \n Ошибка произошла в период загрузки {date_start}-{date_end}  \n {e} \n'
                            f'=============================')
                    else:
                        print(f'Попытка {errorcount} для {date_start}-{date_end}')
                    errorcount = errorcount + 1

    GetOrdersRows()


FarFetchGetOrdersRows()
