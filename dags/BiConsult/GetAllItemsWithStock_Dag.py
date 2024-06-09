import io
import os
import logging
import time
from datetime import datetime, timedelta

import pendulum
import pandas as pd
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.models import Variable
from suds.client import Client
from airflow.decorators import dag, task


POSTGRES_CONN_ID = ''
TABLE_NAME = ''
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
        return df

    @task
    def fetch_db_data():
        """Получает данные из PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        df = pg_hook.get_pandas_df(sql=f"SELECT * FROM {TABLE_NAME}")
        return df


    @task
    def compare_and_update_data(api_df: pd.DataFrame, db_df: pd.DataFrame):
        """Сравнивает DataFrame и обновляет базу данных."""

        common_cols = ["ItemID"]
        # Совместим df для сравнения
        merged_df = api_df.merge(
            db_df,
            on=common_cols,
            how="left",
            suffixes=("_api", "_db"),
        )

        # Новые записи: записи, которые есть в API, но отсутствуют в базе
        new_records = merged_df[merged_df["ItemID_db"].isnull()]
        if not new_records.empty:
            logging.info(f"Найдены {len(new_records)} новых записей.")
            # Добавьте столбцы для created и updated
            new_records["created"] = datetime.now()
            new_records["updated"] = datetime.now()
            # Преобразование данных
            new_records["item_id"] = pd.to_numeric(new_records["ItemID"])
            new_records["quantity"] = pd.to_numeric(new_records["Quantity"])
            new_records["gender_id"] = pd.to_numeric(new_records["GenderID"])
            new_records["season_id"] = pd.to_numeric(new_records["SeasonID"])
            # Выберите нужные столбцы для вставки
            insert_cols = [
                "item_id",
                "brand",
                "department",
                "group_stocks",
                "name_stocks",
                "full_price",
                "discount_price",
                "size",
                "quantity",
                "size_range_type",
                "style",
                "gender_id",
                "gender",
                "season_id",
                "season",
                "barcode",
                "sku",
                "store_barcode",
                "created",
                "updated",
            ]
            # Вставьте новые записи в базу данных
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            pg_hook.insert_rows(
                table=TABLE_NAME, rows=new_records[insert_cols].values.tolist()
            )

        # Измененные записи: записи, которые есть в API и в базе, но значения отличаются
        changed_records = merged_df[
            (merged_df["ItemID_db"].notnull())
            & (
                (merged_df["Brand_api"] != merged_df["Brand_db"])
                | (merged_df["Department_api"] != merged_df["Department_db"])
                | (merged_df["Group_api"] != merged_df["Group_db"])
                | (merged_df["Name_api"] != merged_df["Name_db"])
                | (merged_df["FullPrice_api"] != merged_df["FullPrice_db"])
                | (merged_df["DiscountPrice_api"] != merged_df["DiscountPrice_db"])
                | (merged_df["Size_api"] != merged_df["Size_db"])
                | (merged_df["Quantity_api"] != merged_df["Quantity_db"])
                | (merged_df["SizeRangeType_api"] != merged_df["SizeRangeType_db"])
                | (merged_df["Style_api"] != merged_df["Style_db"])
                | (merged_df["GenderID_api"] != merged_df["GenderID_db"])
                | (merged_df["Gender_api"] != merged_df["Gender_db"])
                | (merged_df["SeasonID_api"] != merged_df["SeasonID_db"])
                | (merged_df["Season_api"] != merged_df["Season_db"])
                | (merged_df["Barcode_api"] != merged_df["Barcode_db"])
                | (merged_df["SKU_api"] != merged_df["SKU_db"])
                | (merged_df["StoreBarcode_api"] != merged_df["StoreBarcode_db"])
            )
        ]
        if not changed_records.empty:
            logging.info(f"Найдены {len(changed_records)} измененных записей.")
            # Обновите записи в базе данных
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            for index, row in changed_records.iterrows():
                update_query = f"""
                    UPDATE {TABLE_NAME}
                    SET brand = %s, department = %s, group_stocks = %s, name_stocks = %s,
                        full_price = %s, discount_price = %s, "size" = %s, quantity = %s,
                        size_range_type = %s, "style" = %s, gender_id = %s, gender = %s,
                        season_id = %s, season = %s, barcode = %s, sku = %s, store_barcode = %s,
                        updated = %s
                    WHERE item_id = %s;
                """
                pg_hook.run(
                    update_query,
                    (
                        row["Brand_api"],
                        row["Department_api"],
                        row["Group_api"],
                        row["Name_api"],
                        row["FullPrice_api"],
                        row["DiscountPrice_api"],
                        row["Size_api"],
                        row["Quantity_api"],
                        row["SizeRangeType_api"],
                        row["Style_api"],
                        row["GenderID_api"],
                        row["Gender_api"],
                        row["SeasonID_api"],
                        row["Season_api"],
                        row["Barcode_api"],
                        row["SKU_api"],
                        row["StoreBarcode_api"],
                        datetime.now(),
                        row["ItemID_api"],
                    ),
                )

    api_data = get_all_items_wStock()
    db_data = fetch_db_data()
    compare_and_update_data(api_data, db_data)


farfetch_gettAll_withStock()
