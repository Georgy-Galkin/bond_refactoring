import os
import sys
import logging
import zipfile
import requests
import urllib3
import time
from datetime import datetime,timedelta,date
from datetime import datetime
from os.path import isfile, join
from connect_and_extract_data_from_api import get_token,get_report_ids,download_reports
from utils import generate_date_ranges_by_weekday, list_files_with_extension
from connect_and_load_data_to_db import connect_to_sql_server,load_csv_to_sql
# ============================ Setup Logging ============================ #
# Generate dynamic log file name
log_filename = f"x5_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Create logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create file handler
file_handler = logging.FileHandler(log_filename, encoding='utf=8')
file_handler.setLevel(logging.DEBUG)

# Create stream (console) handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Formatter
formatter = logging.Formatter('%(asctime)s %(filename)s:%(message)s', datefmt='%Y=%m=%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add both handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)
urllib3.disable_warnings()
# ============================ Constants ============================ #
MSSQL_SERVER  = 'WIPRD267'
DB_NAME       = 'Cash_sales'

SALES_TABLE                       = 'stg_X5_sales__DEV'
SALES_COLUMNS                     = [
        "DAY",
        "PLANT",
        "PLU",
        "TURNOVER",
        "IS_PROMO"
]

PRODUCT_DIRECTORY_V2_TABLE        = 'stg_X5_products__DEV'
PRODUCT_DIRECTORY_V2_COLUMNS     = [
        'BARCODE',
        'BRANDCODE',
        'BRANDNAME',
        'BRUTTO',
        'CATEGORY1',
        'CATEGORY1NAME',
        'CATEGORY2',
        'CATEGORY2NAME',
        'CATEGORY3',
        'CATEGORY3NAME',
        'CATEGORY4',
        'CATEGORY4NAME',
        'CATEGORYMANAGEMENTCODE',
        'CATEGORYMANAGEMENTNAME',
        'COUNTRY',
        'DEPTH',
        'FINCODE',
        'FINCODENAME',
        'HEIGHT',
        'NETTO',
        'PLUCODE',
        'PLUNAME',
        'SHELFLIFE',
        'TARACODE',
        'TARANAME',
        'UNIT',
        'UNITBRUTTO',
        'VAT',
        'VENDOR',
        'VENDORNAME',
        'WIDTH'
]
SHOP_DIRECTORY_V3_TABLE     = 'stg_X5_stores_v3__DEV'
SHOP_DIRECTORY_V3_COLUMNS   = [
        'ADDRESSTEXT',
        'ALCOLICENSE',
        'ASSORTLEVEL',
        'CFO',
        'CFONAME',
        'CITYGENERATED',
        'CITYGUID',
        'CLASTERID',
        'CLASTERNAME',
        'CLOSEDATE',
        'CLOSEHOUR',
        'COORDINATES',
        'DIVISIONID',
        'DIVISIONNAME',
        'FIASDISTRICT',
        'FIASCITY',
        'FIASREGION',
        'FIASSTREET',
        'FIASSTREETGUID',
        'FORMATBYAREA',
        'FORMATID',
        'FORMATNAME',
        'HOUSE',
        'LEVEL1',
        'LEVEL1NAME',
        'LEVEL2',
        'LEVEL2NAME',
        'LEVEL3',
        'LEVEL3NAME',
        'MACROID',
        'MACRONAME',
        'NAMESHOP',
        'NOTOBACCO',
        'OPENDATE',
        'OPENHOUR',
        'POSTCODE',
        'REGION',
        'REGIONGUID',
        'REGIONNAME',
        'SHOPNUMBER',
        'STATUS',
        'STATUSSALES',
        'TERRITORYID',
        'TERRITORYNAME',
        'TYPESHOP',
        'GLN',
        'CLUSTERING',
        'SALESCHANNEL',
        'ASSORTCLUST',
        'NAMEASSORTCLUST',
        'CASHBOXTOTAL',
        'PLANCLOSEDATE',
        'FACTRECONSTRSTARTDATE',
        'FACTRECONSTRENDDATE',
        'BALANCENAME',
        'INN',
        'KPP'
]

INVENTORY_TABLE         = 'stg_X5_inventory__DEV'
INVENTORY_COLUMNS       = [
        'COUNT', 
        'DAY', 
        'PLANT', 
        'PLU', 
        'STOCKTYPE'
]
REPORT_TYPES            = ["SALES", "PRODUCT_DIRECTORY_V2", "SHOP_DIRECTORY_V3", "INVENTORY"]
API_HOST                = 'https://lp-api.x5.ru/v2/logistics/report'
LOGIN_CREDS             = {"email": "ivan.petrov@bonduelle.com","password": "Bonduelle@2025"}
BASE_PATH               = r"C:\Users\georg\Desktop\Offtakes\X5"

# ============================ Variables ============================ #
ingest_folder_path          = datetime.now().strftime("%Y%m%d")
complete_ingest_folder_path = f"{BASE_PATH}/{ingest_folder_path}"

today_minus_seven_days = str(date.today()-timedelta(days=7))
yesterday              = str(date.today()-timedelta(days=1))
# ============================ Main Run ============================ #

reports_date_ranges = generate_date_ranges_by_weekday(
    start_date      = today_minus_seven_days,
    end_date        = yesterday)

authentification = get_token(
    host_url        = API_HOST, 
    creds_json      = LOGIN_CREDS)


report_ids_tuple = get_report_ids(
    auth            = authentification,
    host_url        = API_HOST,
    start_date      = reports_date_ranges[0][0],
    end_date        = reports_date_ranges[0][1], 
    reports_list    = REPORT_TYPES
)

download_reports(
    auth              = authentification,
    host_url          = API_HOST,
    reports_with_ids  = report_ids_tuple,
    report_names_list = REPORT_TYPES, 
    start_date        = reports_date_ranges[0][0],
    end_date          = reports_date_ranges[0][1], 
    save_root_path    = complete_ingest_folder_path
)


conn,cursor = connect_to_sql_server(server = MSSQL_SERVER, database = DB_NAME)

# Load SALES
load_csv_to_sql(
    conn            = conn,
    cursor          = cursor,
    table_name      = SALES_TABLE,
    table_schema    = "dbo",
    csv_paths_list  = list_files_with_extension(f"{complete_ingest_folder_path}/{REPORT_TYPES[0]}"),
    field_separator = '|',
    columns         = SALES_COLUMNS,
    truncate_before_load = True,
    rows_per_batch  = 1000000,
    row_separator   = '0x0a',
    codepage        = '65001'
)


# Load PRODUCT_DIRECTORY_V2
load_csv_to_sql(
    conn            = conn,
    cursor          = cursor,
    table_name      = PRODUCT_DIRECTORY_V2_TABLE,
    table_schema    = "dbo",
    csv_paths_list  = list_files_with_extension(f"{complete_ingest_folder_path}/{REPORT_TYPES[1]}"),
    field_separator = '|',
    columns         = PRODUCT_DIRECTORY_V2_COLUMNS,
    truncate_before_load = True,
    rows_per_batch  = 1000000,
    row_separator   = '0x0a',
    codepage        = '65001'
)

# Load SHOP_DIRECTORY_V3
load_csv_to_sql(
    conn            = conn,
    cursor          = cursor,
    table_name      = SHOP_DIRECTORY_V3_TABLE,
    table_schema    = "dbo",
    csv_paths_list  = list_files_with_extension(f"{complete_ingest_folder_path}/{REPORT_TYPES[2]}"),
    field_separator = '|',
    columns         = SHOP_DIRECTORY_V3_COLUMNS,
    truncate_before_load = True,
    rows_per_batch  = 1000000,
    row_separator   = '0x0a',
    codepage        = '65001'
)

# Load INVENTORY
load_csv_to_sql(
    conn            = conn,
    cursor          = cursor,
    table_name      = INVENTORY_TABLE,
    table_schema    = "dbo",
    csv_paths_list  = list_files_with_extension(f"{complete_ingest_folder_path}/{REPORT_TYPES[3]}"),
    field_separator = '|',
    columns         = INVENTORY_COLUMNS,
    truncate_before_load = True,
    rows_per_batch  = 1000000,
    row_separator   = '0x0a',
    codepage        = '65001'
)