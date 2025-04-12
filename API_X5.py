import bondetl as be  # Main package
import sys
import logging
import warnings
import requests
import urllib3
import os
import datetime
import zipfile as zf
import re
import pandas as pd
import json
import csv

logging.basicConfig(filename='stg_X5.log', encoding='utf-8', level=logging.DEBUG,
                    format='%(asctime)s %(filename)s:%(message)s', datefmt='%Y-%m-%d %I:%M:%S')
urllib3.disable_warnings()

# для подключения к API Insides нужно получать два токена Keycloak: access_token и refresh_token
# startDate = str(datetime.date.today()-datetime.timedelta(days=11))
#date_load = datetime.date(2024, 1, 25)
date_load = datetime.date(2023, 8, 21)
startDate = str(date_load - datetime.timedelta(days=(date_load.isoweekday() % 7  - 1 + 7)))
endDate = str(date_load - datetime.timedelta(days=(date_load.isoweekday() % 7)))
#startDate = str(date_load - datetime.timedelta(days=11))
#endDate = str(date_load - datetime.timedelta(days=5))

startTime = datetime.datetime.now().strftime("%H%M%S")
# endDate = str(datetime.date.today()-datetime.timedelta(days=5))
# print(endDate)
# exit()
metric = ['SALES', 'PRICE', 'SHOPS_PLU']
file_open = f'Voca_Product.xlsx'
TableIn = 'stg_Voca_Product_X5_test'
db = 'Cash_sales'
pathXLSX = r'N:/py_scripts/API_X5/Voca_Product.xlsx'
pathCSV = r'N:/py_scripts/API_X5/Voca_Product.csv'
TableIn_product = 'stg_Voca_Product_X5_test'
db_product = 'Cash_sales'
excelData = be.readXLSX(pathXLSX)
be.toCSV(excelData, pathCSV)
headers = excelData[0]

# для подключения необходимо получение двух токенов Keycloak: access_token и refresh_token, а также внутренний JWT-токен

try:
    # Получаем access_token и refresh_token
    token = requests.post('https://dialog-sso.x5.ru/auth/realms/dialog/protocol/openid-connect/token', login_Creds,
                          verify=False).json()
    access_token = token['access_token']
    # print(access_token)
    refresh_token = token['refresh_token']
    # print(refresh_token)
    cookies = {'kc-access': access_token, 'kc-state': refresh_token}
    # # получаем JWT-токен
    JWT_token = requests.get('https://supplierportal.x5.ru/api/v1/public/auth/token', cookies=cookies).json()
    x5_key = JWT_token['result']['token']
except Exception as e:
    logging.info(e)
    sys.exit(1)

# вытаскиваем дерево продуктов, для создание части json, который передается в в json для скачивания отчета
logging.info("product get start")
reports_product = requests.get(
    'https://supplierportal.x5.ru/api/v1/public/tree/products?reportTypeId=8ddb5b9f-2193-453c-96ba-a0a3c14e517c',
    cookies=cookies, headers={'x5-api-key': x5_key})
logging.info("product get end")
products = reports_product.json()['result']['nodes']
product_level_4 = []
dict_product = []
id_pr = []
for i in range(len(products)):
    pr = products[i - 1]['children']
    for e in range(len(pr)):
        pr_1 = pr[e - 1]['children']
        for k in range(len(pr_1)):
            pr_2 = pr_1[k - 1]['children']
            product_level_4.append(pr_2)
for d in range(len(product_level_4)):
    for i in range(len(product_level_4[d])):
        value = product_level_4[d][i - 1]['id']
        level = product_level_4[d][i - 1]['level']
        id_pr.append({'id': {'code': value, 'level': level}})
        dict_product.append({'id': value, 'level': level})

# вытаскиваем дерево клиентов, для создание части json, который передается в json для скачивания отчета
reports_shops = requests.get(
    'https://supplierportal.x5.ru/api/v1/public/tree/stores?reportTypeId=8ddb5b9f-2193-453c-96ba-a0a3c14e517c',
    cookies=cookies, headers={'x5-api-key': x5_key})
shops = reports_shops.json()['result']['tradeNetworks']
tradeNetworks = []
key, value = 'selectedFully', True
key_2, value_2 = 'selectedFully', False

for i in range(len(shops)):
    del shops[i]['name']
    del shops[i]['storesCount']
    federalDistricts_range = shops[i]['federalDistricts']
    shops[i]['tradeNetworkId'] = shops[i].pop('id')
    shops[i]['federalDistricts'] = shops[i].pop('federalDistricts')
    for k in range(len(federalDistricts_range)):
        del shops[i]['federalDistricts'][k]['name']
        del shops[i]['federalDistricts'][k]['storesCount']
        shops[i]['federalDistricts'][k]['districtId'] = shops[i]['federalDistricts'][k].pop('id')
        shops[i]['federalDistricts'][k]['regions'] = shops[i]['federalDistricts'][k].pop('regions')
        region_range = federalDistricts_range[k]['regions']
        for t in range(len(region_range)):
            del shops[i]['federalDistricts'][k]['regions'][t]['name']
            del shops[i]['federalDistricts'][k]['regions'][t]['storesCount']
            shops[i]['federalDistricts'][k]['regions'][t]['regionId'] = shops[i]['federalDistricts'][k]['regions'][
                t].pop('id')
            shops[i]['federalDistricts'][k]['regions'][t]['citiesId'] = shops[i]['federalDistricts'][k]['regions'][
                t].pop('cities')
            cities_range = region_range[t]['citiesId']
            shops[i]['federalDistricts'][k]['regions'][t]['citiesId'].clear()
            shops[i]['federalDistricts'][k]['regions'][t][key] = value
            shops[i]['federalDistricts'][k][key_2] = value_2
            shops[i][key_2] = value_2

# загрузка справочников по продуктам в базу
# print(headers)
dict_product_json = {'nodes': dict_product}
# print(dict_product_json)
reports_product_voca = requests.post('https://supplierportal.x5.ru/api/v1/public/tree/products/download',
                                     json=dict_product_json, cookies=cookies,
                                     headers={'x5-api-key': x5_key, 'Content-Type': 'application/json'})
# print(reports_product_voca.text)
with open(file_open, 'wb') as excel:
    excel.write(reports_product_voca.content)

# cookies = GetToken()
# headers = Get_JWT()
# headers['Content-Type'] = 'application/json'
# dict_product = GetProduct()
# dict_product_json = {'nodes': dict_product[0]}
# reports_product_voca = requests.post('https://supplierportal.x5.ru/api/v1/public/tree/products/download', json=dict_product_json, cookies=cookies, headers=headers)
# print(reports_product_voca.content)
# exit()
# def main(listXls,pathCSV,TableIn,pathXLSX=pathXLSX_1):
sqlTableIn = be.SQL(db, TableIn)
excelData = be.readXLSX(pathXLSX)
be.toCSV(excelData, pathCSV)
headers = excelData[0]
for i, j in enumerate(headers):
    headers[i] = f'[{j}] [nvarchar](255) NULL'
headers = str.join(', ', headers)
sql_query_create = f'''
if  object_id('{TableIn}') is not null  drop table {TableIn}
; 
CREATE TABLE {TableIn} ({headers})'''
sql_bulk = f"""
        BULK INSERT {TableIn}
        FROM '{pathCSV}'

        WITH (FIRSTROW = 2,
        CODEPAGE = '65001',
        FIELDTERMINATOR = ';',
        --,
        ROWTERMINATOR='\n',
        KEEPNULLS,
        FORMAT ='CSV',
        MAXERRORS = 9999999 )
    """
sqlTableIn.Query(sql_query=sql_query_create)
sqlTableIn.Query(sql_query=sql_bulk)
os.remove(pathCSV)

""" эта часть для скачивания отчета по клиентам (в данный момент она не нужна, но оставлю, вдруг пригодится.)
json_voca = {'classifierStores': shops}
reports_shops_ex = requests.post('https://supplierportal.x5.ru/api/v1/public/tree/stores/download', json=json_voca, cookies={'kc-access': access_token, 'kc-state': refresh_token}, headers={'x5-api-key': x5_key, 'Content-Type': 'application/json'})
with open(f'Voca_Client.xlsx', 'wb') as excel:
    excel.write(reports_shops_ex.content)
TableIn_Client = 'stg_Voca_Client_X5_test'
pathXLSX_Client = r'N:/py_scripts/API_X5/Voca_Client.xlsx'
pathCSV_Client = r'N:/py_scripts/API_X5/Voca_Client.csv'
# def main(listXls,pathCSV,TableIn,pathXLSX=pathXLSX_1):
sqlTableIn_Client = be.SQL(db,TableIn_Client)
excelData_Client = be.readXLSX(pathXLSX_Client)
be.toCSV(excelData_Client,pathCSV_Client)
headers_client = excelData_Client[0]
for i,j in enumerate(headers_client):
    headers_client[i]= f'[{j}] [nvarchar](255) NULL'
headers_client = str.join(', ', headers_client)
sql_query_create_Client = f'''
if  object_id('{TableIn_Client}') is not null  drop table {TableIn_Client}
; 
CREATE TABLE {TableIn_Client} ({headers_client})'''
sql_bulk = f
            BULK INSERT {TableIn_Client}
            FROM '{pathCSV_Client}'

            WITH (FIRSTROW = 2,
            CODEPAGE = '65001',
            FIELDTERMINATOR = ';',
            --,
            ROWTERMINATOR='\n',
            KEEPNULLS,
            FORMAT ='CSV',
            MAXERRORS = 9999999 
sqlTableIn.Query(sql_query=sql_query_create_Client)
sqlTableIn.Query(sql_query=sql_bulk)
os.remove(pathCSV_Client) """

json_date = {
    "name": f"Report_1_{startDate}_{startTime}",
    "type": "8ddb5b9f-2193-453c-96ba-a0a3c14e517c",
    "sectionIds": ["c6941662-29fc-4273-888f-ec2e348782c7"],
    "export": True,
    "parameters": {
        "periods": {
            "periodGranularityId": "22249589-230e-4501-a971-677f5bac976b",
            "period": {
                "start": startDate,
                "stop": endDate
            }
        },
        "metricGroups": metric,
        "products": {
            "selection": id_pr,
            "isCategoryPluDetailing": True
        },
        "selectedShops": {
            "groupingAttributes": ["TRADE_NETWORK", "CITY"],
            "growthMeasure": "TOTAL",
            "networkElementList": shops,
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

# json_date = json.dumps(json_date, ensure_ascii=False)
# logging.info(json_date)
# reports_test = requests.post('https://supplierportal.x5.ru/api/v1/public/reports/trends', json=json_date, cookies=cookies, headers={'x5-api-key': x5_key, 'Content-Type': 'application/json'})
reports_test = requests.post('https://supplierportal.x5.ru/api/v1/public/reports/trends', json=json_date,
                             cookies=cookies, headers={'x5-api-key': x5_key, 'Content-Type': 'application/json'})
logging.info(reports_test)
logging.info(f"Result:{reports_test} Error: {reports_test.text}")
repor_id = reports_test.json()["result"]["id"]
print(repor_id, reports_test.json())
# repor_id = GetReport()
# проверка статуса готовности отчета
while True:
    flag = ""
    report_status = requests.get(f'https://supplierportal.x5.ru/api/v2/public/reports/{repor_id}', cookies=cookies,
                                 headers={'x5-api-key': x5_key})
    try:
        flag = report_status.json()["result"]["status"]
    except Exception as e:
        logging.info(e)
        sys.exit(1)
    if flag == "FAILED":
        print("Report BUILDING FAILED")
        break
    if flag == "SUCCEEDED":
        break
export = report_status.json()["result"]["exportFileId"]
report_export = requests.get(f'https://supplierportal.x5.ru/api/v1/public/export/{export}',
                             cookies={'kc-access': access_token, 'kc-state': refresh_token},
                             headers={'x5-api-key': x5_key})
report_name = report_status.json()["result"]["name"]
# сохранение архива
with open(f'N:/py_scripts/API_X5/{report_name}.zip', 'wb') as save_zip:
    save_zip.write(report_export.content)

# разархивация полученного файла
path = r'N:/py_scripts/API_X5'
extracted_path = path + '/extracted_data'
try:
    os.mkdir(path)
except:
    pass

zip_file_list = sorted(os.listdir(path),reverse=True)
zip_file_list = [file[0:file.find('.')] for file in zip_file_list if file.find('.zip') > 0]
with zf.ZipFile(f'{path}/{zip_file_list[0]}' + '.zip', 'r') as f:
    name_list = f.namelist()
name_l = name_list[0]

with zf.ZipFile(f'{path}/{zip_file_list[0]}' + '.zip', 'r') as f:
    f.extractall(extracted_path)
    # os.remove(extracted_path+'/'+zip_file_list[0]+'.zip')

csv_find = os.listdir(extracted_path + '/' + name_l)
# print(csv_find)
for i in range(len(csv_find)):
    if 'csv' in csv_find[i]:
        csv_name = csv_find[i]
# print(csv_name)
csv_1 = extracted_path + '/' + name_l + csv_name
# print(csv_1)
df = pd.read_csv(csv_1, on_bad_lines='skip')
headers = str(df.columns.tolist())
new_headers = ''
for i in range(len(headers)):
    if headers[i] != "[" and headers[i] != "'" and headers[i] != '"' and headers[i] != ']':
        new_headers += headers[i]
new_Header_list = new_headers.split(';')

# Added by AE to try to load facts
TableIn = 'stg_X5_API_test'
sqlHeader = []
sqlTableIn = be.SQL(db, TableIn)
for i, j in enumerate(new_Header_list):
    sqlHeader.append(f'[{j}] [nvarchar](255) NULL')
headers = str.join(', ', sqlHeader)
# print(headers)
sql_query_create = f'''
if  object_id('{TableIn}') is not null  drop table {TableIn}
; 
CREATE TABLE {TableIn} ({headers})'''
# print(sql_query_create)
sql_bulk = f"""
            BULK INSERT {TableIn}
            FROM '{csv_1}'

            WITH (FIRSTROW = 2,
            CODEPAGE = '65001',
            FIELDTERMINATOR = ';',
            --,
            ROWTERMINATOR='\n',
            KEEPNULLS,
            FORMAT ='CSV',
            MAXERRORS = 9999999 )
        """
sqlTableIn.Query(sql_query=sql_query_create)
sqlTableIn.Query(sql_query=sql_bulk)
#os.remove(pathCSV)
