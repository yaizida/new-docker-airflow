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
    dag_id='GetReturns',
    default_args=default_args,
    schedule='0 * * * SUN-FRI',
    catchup=False,
    concurrency=4,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['FarFetch'],
)
def FarFetchGetReturns():
    @task
    def GetReturns():
        def Request(key):
            dfall = pd.DataFrame()
            response = client.service.GetReturns(key)
            for s in range(len(response.ReturnOrderDto)):
                df = pd.DataFrame()
                header = []
                body = []
                row = []
                for j in response.ReturnOrderDto[s]:
                    header.append(j[0])
                for j in range(len(response.ReturnOrderDto[s])):
                    if header[j] != 'OrderLines':
                        row.append(getattr(response.ReturnOrderDto[s], header[j]))
                    else:
                        dflist = pd.DataFrame()
                        for i in range(len(getattr(response.ReturnOrderDto[s],
                                                   header[j]).ReturnOrderLineDto)):
                            rowappend = []
                            bodyappend = []
                            headerappend = []
                            for x in getattr(response.ReturnOrderDto[s],
                                             header[j]).ReturnOrderLineDto[i]:
                                headerappend.append(x[0])
                            for y in getattr(response.ReturnOrderDto[s],
                                             header[j]).ReturnOrderLineDto[i]:
                                if isinstance(y[1], list):
                                    rowappend.append(str(y[1][0]))
                                else:
                                    rowappend.append(str(y[1]))
                            bodyappend.append(rowappend)
                            dfstr = pd.DataFrame(bodyappend,
                                                 columns=headerappend)
                            result = dflist.append(dfstr)
                            dflist = result
                header.remove('OrderLines')
                body.append(row)
                df1 = pd.DataFrame(body, columns=header)
                result = df.append(df1)
                df = result
                resultjoin = df.join(dflist)
                result = dfall.append(resultjoin)
                dfall = result
            csv_path = f'{CUR_DIR}/resources/GetReturns/Returns.csv'
            if os.path.exists(csv_path):
                dfprev = pd.read_csv(csv_path)
                dfall = dfall.append(dfprev)
            dfall.drop_duplicates().to_csv(csv_path, encoding='utf-8', index=False)
            return csv_path
        errorcount = 1
        while errorcount <= 6:
            try:
                time.sleep(0.5)
                Request(KEY)
                time.sleep(0.5)
                break
            except Exception as e:
                if errorcount == 6:
                    print(
                        f'============Error============ \n Ошибка произошла в период загрузки возвратов  \n {e} \n'
                        f'=============================')
                else:
                    print(f'Попытка {errorcount} для  возвратов ')
                errorcount += 1

    GetReturns()


FarFetchGetReturns()
