import io
import os
import logging
import time
from datetime import datetime, timedelta

import pendulum
import pandas as pd
import numpy as np
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.decorators import dag, task
from airflow.models import Variable
from suds.client import Client
from airflow.decorators import dag, task


POSTGRES_CONN_ID = 'airflowdb'
TABLE_NAME = 'ods.items'
KEY = '1vSu3k1DvCE='
PG_HOOK = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
LAYER, NAME_DAG = os.path.realpath(__file__).split('\\')[-2:]

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
    schedule='0 0 21  *   * ',
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
    def get_all_items_wStock() -> pd.DataFrame:
        response = client.service.GetAllItemsWithStock(KEY)
        data = response.GetAllItemsWithStockResult.diffgram[0].DocumentElement[0].Table # noqa
        header = [x[0] for x in data[0]]
        body = [[str(j[1][0] if isinstance(j[1], list)
                 else str(j[1])) for j in row]for row in data]
        api_df = pd.DataFrame(body, columns=header)
        api_df = api_df.drop(['_id', '_rowOrder'], axis=1)
        logging.info(f'Дата фрейм get_all_items_wStock: {api_df.head()}')
        logging.info(f'Длина датафрейма в get_all_items_wStock: {len(api_df)}')
        
        
        api_df['inserted_dttm'] = ''
        api_df['updated_dttm'] = ''
        db_df = PG_HOOK.get_pandas_df(sql=f"SELECT * FROM {TABLE_NAME}")
        api_df = api_df.rename(columns=dict(zip(api_df.columns.to_list(), db_df.columns.to_list())))

        logging.info(api_df)
        return api_df 

    @task
    def update(api_df: pd.DataFrame):
        import inspect
        from notification_dt1 import EmailAlertMsg

        db_df = PG_HOOK.get_pandas_df(sql=f"SELECT * FROM {TABLE_NAME}")
        api_df['combined_key'] = api_df['id'].astype(str) + api_df['Size']
        db_df['combined_key'] = db_df['id'].astype(str) + db_df['Size']
        
        today_date = pendulum.now().in_timezone('Europe/Moscow').format('YYYY-MM-DD HH:mm:ss')
        api_df['inserted_dttm'] = today_date
        for i in range(len(api_df)):
            match_index = db_df[db_df['combined_key'] == api_df['combined_key'][i]].index
            if len(match_index) > 0:
                for idx in match_index:
                    for col in api_df.columns:
                        if col != 'inserted_dttm':  
                            if api_df.iloc[i][col] != db_df.iloc[idx][col]:
                                db_df.loc[idx, col] = api_df.iloc[i][col]
                                if col == 'updated_dttm':
                                    db_df.loc[idx, 'updated_dttm'] = today_date
        
        s_buf = io.StringIO()
        try:
            db_df = db_df.drop(columns='index')
        except Exception:
            logging.info('Index does not exist')

        db_df.drop('combined_key', axis=1, inplace=True)
        db_df = db_df.replace(np.nan, None)
        db_df = db_df.replace([pd.NaT], [None])
        db_df = db_df.replace('None', None)
        db_df = db_df.rename(columns={'Group': '"Group"',
                                      'Name': '"Name"', 
                                      'Style': '"Style"',
                                      'Size': '"Size"'
                                      })
        db_df.to_csv(s_buf, index=False)
        
        try:
            with PG_HOOK.get_conn() as conn:
                cur = conn.cursor()
                cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
                s_buf.seek(0)  # Сбрасываем указатель в буфере
                logging.info("Inserting data to DWH")
                query = f"""COPY {TABLE_NAME} ({', '.join(db_df.columns.to_list())}) FROM STDIN DELIMITER ',' CSV HEADER"""
                logging.info("Copy query: %s", query)
                cur.copy_expert(query, s_buf)
                conn.commit()
                logging.info('Updated Values is Done GetAllItemsWithStock')
        except Exception as error:
            conn.rollback()
            error = error
            function_name = inspect.stack()[1].function 
            logging.error(f"{LAYER}.{NAME_DAG}.{function_name} Ошибка при копировании данных: {error}")
          
        EmailAlertMsg(LAYER, NAME_DAG, function_name,  error).send_email()



    @task
    def get_new(api_df: pd.DataFrame):
        import inspect
        from notification_dt1 import EmailAlertMsg

        function_name = inspect.stack()[1].function 

        db_df = PG_HOOK.get_pandas_df(sql=f"SELECT * FROM {TABLE_NAME}")
        api_df['combined_key'] = api_df['id'].astype(str) + api_df['Size']
        db_df['combined_key'] = db_df['id'].astype(str) + db_df['Size']
        missing_values = api_df[~api_df['combined_key'].isin(db_df['combined_key'])]
        missing_values.drop('combined_key', axis=1, inplace=True)
        missing_values = missing_values.replace(np.nan, None)
        missing_values = missing_values.replace([pd.NaT], [None])
        missing_values = missing_values.rename(columns={'Group': '"Group"',
                                                        'Name': '"Name"', 
                                                        'Style': '"Style"',
                                                        'Size': '"Size"'
                                                        })  
        missing_values = missing_values.drop(['inserted_dttm', 'updated_dttm'], axis=1)
        
        if len(missing_values) > 0:
            logging.info(f'Значений которых нет в БД: {missing_values}')
            logging.info(f'Типы даных датафрейма: {db_df.dtypes}')
            logging.info(f'Столбцы  DataFrame {missing_values.columns}')
            s_buf = io.StringIO()
            # missing_values = db_df.drop(columns='index')
            missing_values.to_csv(s_buf, index=False)
            print(db_df.dtypes)
            s_buf.seek(0)
            try:
                with PG_HOOK.get_conn() as conn:
                    cur = conn.cursor()
                    logging.info("Inserting data to DWH")
                    query = f"""COPY {TABLE_NAME} ({', '.join(missing_values.columns.to_list())}) FROM STDIN DELIMITER ',' CSV HEADER"""
                    logging.info("Copy query: %s", query)
                    cur.copy_expert(query, s_buf)
                    conn.commit()
            except Exception as error:
                error = error
                conn.rollback()
                logging.error(f"{LAYER}.{NAME_DAG}.{function_name} Ошибка при копировании данных: {error}")
        else:
            logging.info('No new data to upload')

        EmailAlertMsg(LAYER, NAME_DAG, function_name, error).send_email()
        


    data = get_all_items_wStock()
    update = update(data)
    get_new = get_new(data)

    (
        data >> update >> get_new
    )

farfetch_gettAll_withStock()
