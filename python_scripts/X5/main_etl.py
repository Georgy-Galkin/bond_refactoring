# ========================== IMPORTS ==============================
import requests
import urllib3
import logging
import sys
import os
import time
from datetime import datetime, timedelta, date
from utils import unzip_all_flat,convert_xlsx_to_csv,list_files_with_extension
from connect_and_load_to_db import connect_to_sql_server, load_csv_to_sql
from connect_to_api_and_load_to_ingest import *
# ========================== SETUP ==============================
urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO)

# ========================== CONSTANTS ==========================
MSSQL_SERVER  = 'WIPRD267'
DB_NAME       = 'Cash_sales'
PRODUCT_TABLE = 'X5_API_PRODUCTS__DEV'
CLIENT_TABLE  = 'X5_API_SHOPS__DEV'
TRENDS_TABLE  = 'X5_API_TRENDS__DEV'

LOGIN_CREDS = {
    "client_id": "bdsp-api-public",
    "username": "ivan.petrov@bonduelle.com",
    "password": "Bonduelle@2025",
    "grant_type": "password"
}
TOKEN_URL                   = 'https://dialog-sso.x5.ru/auth/realms/dialog/protocol/openid-connect/token'
JWT_URL                     = 'https://supplierportal.x5.ru/api/v1/public/auth/token'
SAVE_DATA_ROOT_PATH         = 'N:\py_scripts\API_X5'

PRODUCT_TREE_URL            = 'https://supplierportal.x5.ru/api/v1/public/tree/products?reportTypeId=8ddb5b9f-2193-453c-96ba-a0a3c14e517c'
PRODUCTS_OUTPUT_DATA_URL    = 'https://supplierportal.x5.ru/api/v1/public/tree/products/download'
PRODUCTS_OUTPUT_FOLDER      = 'products'
PRODUCTS_OUTPUT_FILE_NAME   = "Voca_Product.xlsx"
DIM_PRODUCT_COLUMNS         = [
        "УИ1 ID",
        "Название УИ1",
        "УИ2 ID",
        "Название УИ2",
        "УИ3 ID",
        "Название УИ3",
        "УИ4 ID",
        "Название УИ4",
        "PLU ID",
        "Производитель",
        "Бренд",
        "Название",
        "Нетто",
        "Архивный",
        "ТС Пятерочка",
        "ТС Перекресток",
        "ТС Карусель",
        "Перекресток Впрок",
        "ТС Чижик",
        "Актуальность",
        "Дата старта продаж",
        "Штрихкод (EAN)",
    ]

SHOPS_TREE_URL              = 'https://supplierportal.x5.ru/api/v1/public/tree/stores?reportTypeId=8ddb5b9f-2193-453c-96ba-a0a3c14e517c'
SHOPS_OUTPUT_DATA_URL       = 'https://supplierportal.x5.ru/api/v1/public/tree/stores/download'
SHOPS_OUTPUT_FOLDER         = 'shops'
SHOPS_OUTPUT_FILE_NAME      = "Voca_Client.xlsx"
DIM_CLIENT_COLUMNS          = [
        "store_id",
        "Наименование магазина",
        "ID Сети",
        "Наименование сети",
        "Федеральный округ",
        "Регион",
        "Город/Населенный пункт",
        "Адрес",
        "Макрорегион",
        "Уровень ассортимента",
        "Статус",
        "Дата открытия",
        "Код региона (субъекта РФ)",
        "Регион (Субъект РФ)",
        "Код территории X5",
        "Территория X5",
]
TRENDS_REPORT_ROOT_URL      = 'https://supplierportal.x5.ru/api/v2/public/reports/'
TRENDS_REPORT_URL           = 'https://supplierportal.x5.ru/api/v1/public/reports/trends'
EXPORT_TRENDS_ROOT_URL      = 'https://supplierportal.x5.ru/api/v1/public/export/'
TRENDS_REPORT_METRICS       = ['SALES', 'PRICE', 'SHOPS_PLU']
REPORTS_OUTPUT_FOLDER       = 'trends_report'
TRENDS_REPORT_COLUMNS       = [
        "Период",
        "Начало периода",
        "Конец периода",
        "Группа магазинов",
        "Группа товаров",
        "PLU",
        "TOTAL Продажи, руб.",
        "TOTAL Продажи, шт.",
        "TOTAL Продажи, кг/л",
        "TOTAL Количество магазинов",
        "TOTAL Количество товаров",
        "TOTAL Средняя цена, руб. за шт.",
        "TOTAL Средняя цена, руб. за кг/л",
        "TOTAL Доля в продажах категории, %",
        "TOTAL % магазинов среди продающих категорию",
        "TOTAL % товаров от всех товаров категории",
        "PROMO Продажи, руб.",
        "PROMO Доля в TOTAL продажах в руб., %",
        "PROMO Продажи, шт.",
        "PROMO Продажи, кг/л",
        "PROMO Доля в TOTAL продажах в шт., %",
        "PROMO Доля в TOTAL продажах в кг/л, %",
        "PROMO Средняя цена, руб. за шт.",
        "PROMO Средняя цена, руб. за кг/л",
        "PROMO Количество магазинов",
        "TOTAL Продажи в день на магазин, руб.",
        "TOTAL Продажи в день на магазин, шт.",
        "TOTAL Продажи в день на магазин, кг/л",
        "TOTAL Взвешенная дистрибуция, %",
        "TOTAL Индекс цены за шт",
        "TOTAL Индекс цены за кг/л",
        "LOYAL Продажи, руб",
        "LOYAL Продажи, шт.",
        "LOYAL Продажи, кг/л",
        "LOYAL Средняя цена, руб. за шт.",
        "LOYAL Средняя цена, руб. за кг/л",

]

# ========================== VARIABLES ==========================
script_run_date                  = date(2023,1,1) #date.today()
script_run_date_formatted        = script_run_date.strftime("%Y%m%d")
previous_monday                  = str(script_run_date - timedelta(days=(script_run_date.isoweekday() % 7  - 1 + 7)))
previous_sunday                  = str(script_run_date - timedelta(days=(script_run_date.isoweekday() % 7)))
script_run_starttime             = datetime.now().strftime("%H%M%S")
ingest_folder_path               = datetime.now().strftime("%Y%m%d")

# ========================== MAIN ================================
headers, cookies, access_token, refresh_token, x5_key = get_access_and_refresh_tokens(
    token_url   = TOKEN_URL, 
    jwt_url     = JWT_URL, 
    login_creds = LOGIN_CREDS)

dict_product, id_pr = get_product_level_4(
    product_tree_url = PRODUCT_TREE_URL, 
    headers          = headers, 
    cookies          = cookies)

download_product_or_client_reference_excel(
    report_type      = "product",
    json             = dict_product, 
    url              = PRODUCTS_OUTPUT_DATA_URL, 
    headers          = headers, 
    cookies          = cookies, 
    save_root_path   = SAVE_DATA_ROOT_PATH,
    save_folder      = PRODUCTS_OUTPUT_FOLDER,
    data_ingest_date = ingest_folder_path,
    filename         = PRODUCTS_OUTPUT_FILE_NAME) 


dict_shops = get_and_prepare_shops_tree(
    shops_tree_url = SHOPS_TREE_URL,
    headers        = headers,
    cookies        = cookies)

download_product_or_client_reference_excel(
    report_type      = "client",
    json             = dict_shops, 
    url              = SHOPS_OUTPUT_DATA_URL, 
    headers          = headers, 
    cookies          = cookies, 
    save_root_path   = SAVE_DATA_ROOT_PATH,
    save_folder      = SHOPS_OUTPUT_FOLDER,
    data_ingest_date = ingest_folder_path,
    filename         = SHOPS_OUTPUT_FILE_NAME)

json_trends_report = {
    "name": f"trends_report_{script_run_date_formatted}_{script_run_starttime}",
    "type": "8ddb5b9f-2193-453c-96ba-a0a3c14e517c",
    "sectionIds": ["c6941662-29fc-4273-888f-ec2e348782c7"],
    "export": True,
    "parameters": {
        "periods": {
            "periodGranularityId": "22249589-230e-4501-a971-677f5bac976b",
            "period": {
                "start": previous_monday,
                "stop": previous_sunday
            }
        },
        "metricGroups": TRENDS_REPORT_METRICS,
        "products": {
            "selection": id_pr,
            "isCategoryPluDetailing": True
        },
        "selectedShops": {
            "groupingAttributes": ["TRADE_NETWORK", "CITY"],
            "growthMeasure": "TOTAL",
            "networkElementList": dict_shops,
            "delivery": {
                "deliveryMode": "INCLUDE_ALL",
                "type": []
            }
        },
        "customers": {
            "customerType": "TOTAL"
        }
    }
}

trends_report_id = get_trends_report_id(
    trends_report_url = TRENDS_REPORT_URL,
    json_body         = json_trends_report, 
    cookies           = cookies, 
    headers           = headers
)

trends_report_response = wait_for_trends_report(
    trends_report_root_url = TRENDS_REPORT_ROOT_URL,
    report_id              = trends_report_id, 
    cookies                = cookies,
    headers                = headers, 
    poll_interval=5)

download_trends_report_zip(
    export_trends_root_url = EXPORT_TRENDS_ROOT_URL,
    export_id              = trends_report_response.json()["result"]["exportFileId"], 
    report_name            = trends_report_response.json()["result"]["name"], 
    cookies                = cookies,
    headers                = headers,
    save_root_path         = SAVE_DATA_ROOT_PATH,
    save_folder            = REPORTS_OUTPUT_FOLDER,
    data_ingest_date       = ingest_folder_path)

# unzip trends report
unzip_all_flat(f"{SAVE_DATA_ROOT_PATH}/{REPORTS_OUTPUT_FOLDER}/{ingest_folder_path}/",['.csv'])

# connect to db
conn, cursor = connect_to_sql_server(server = MSSQL_SERVER, database = DB_NAME)

# convert products & clients report to csv
convert_xlsx_to_csv(list_files_with_extension(f"{SAVE_DATA_ROOT_PATH}/{PRODUCTS_OUTPUT_FOLDER}/{ingest_folder_path}/",'.xlsx')[0])
convert_xlsx_to_csv(list_files_with_extension(f"{SAVE_DATA_ROOT_PATH}/{SHOPS_OUTPUT_FOLDER}/{ingest_folder_path}/",'.xlsx')[0])

# load products
load_csv_to_sql(
    conn = conn,
    cursor = cursor,
    table_name = PRODUCT_TABLE,
    table_schema = 'dbo',
    csv_path = list_files_with_extension(f"{SAVE_DATA_ROOT_PATH}/{PRODUCTS_OUTPUT_FOLDER}/{ingest_folder_path}/",'.csv')[0],
    field_separator = ',',
    columns = DIM_PRODUCT_COLUMNS,
    truncate_before_load = True,
    rows_per_batch= 1000000,
    row_separator= '0x0a',
    codepage= '65001'
)

# load clients
load_csv_to_sql(
    conn = conn,
    cursor = cursor,
    table_name = CLIENT_TABLE,
    table_schema = 'dbo',
    csv_path = list_files_with_extension(f"{SAVE_DATA_ROOT_PATH}/{SHOPS_OUTPUT_FOLDER}/{ingest_folder_path}/",'.csv')[0],
    field_separator = ',',
    columns = DIM_CLIENT_COLUMNS,
    truncate_before_load = True,
    rows_per_batch= 1000000,
    row_separator= '0x0a',
    codepage= '65001'
)


# load trends
load_csv_to_sql(
    conn = conn,
    cursor = cursor,
    table_name = TRENDS_TABLE,
    table_schema = 'dbo',
    csv_path = list_files_with_extension(f"{SAVE_DATA_ROOT_PATH}/{REPORTS_OUTPUT_FOLDER}/{ingest_folder_path}/",'.csv')[0],
    field_separator = ';',
    columns = TRENDS_REPORT_COLUMNS,
    truncate_before_load = True,
    rows_per_batch= 1000000,
    row_separator= '0x0a',
    codepage= '65001'
)