# ========================== IMPORTS ==============================
import requests
import urllib3
import logging
import sys
import os
import time
from datetime import datetime, timedelta, date

# ========================== SETUP ==============================
urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO)

# ========================== CONSTANTS ==========================
LOGIN_CREDS = {
    "client_id": "bdsp-api-public",
    "username": "ivan.petrov@bonduelle.com",
    "password": "Bonduelle@2025",
    "grant_type": "password"
}
TOKEN_URL                   = 'https://dialog-sso.x5.ru/auth/realms/dialog/protocol/openid-connect/token'
JWT_URL                     = 'https://supplierportal.x5.ru/api/v1/public/auth/token'
SAVE_DATA_ROOT_PATH         = 'C:\\Users\\georg\\Desktop\\GG\\Projects\\BOND'

PRODUCT_TREE_URL            = 'https://supplierportal.x5.ru/api/v1/public/tree/products?reportTypeId=8ddb5b9f-2193-453c-96ba-a0a3c14e517c'
PRODUCTS_OUTPUT_DATA_URL    = 'https://supplierportal.x5.ru/api/v1/public/tree/products/download'
PRODUCTS_OUTPUT_FOLDER      = 'products'
PRODUCTS_OUTPUT_FILE_NAME   = "Voca_Product.xlsx"

SHOPS_TREE_URL              = 'https://supplierportal.x5.ru/api/v1/public/tree/stores?reportTypeId=8ddb5b9f-2193-453c-96ba-a0a3c14e517c'
SHOPS_OUTPUT_DATA_URL       = 'https://supplierportal.x5.ru/api/v1/public/tree/stores/download'
SHOPS_OUTPUT_FOLDER         = 'shops'
SHOPS_OUTPUT_FILE_NAME      = "Voca_Client.xlsx"

TRENDS_REPORT_ROOT_URL      = 'https://supplierportal.x5.ru/api/v2/public/reports/'
TRENDS_REPORT_URL           = 'https://supplierportal.x5.ru/api/v1/public/reports/trends'
EXPORT_TRENDS_ROOT_URL      = 'https://supplierportal.x5.ru/api/v1/public/export/'
TRENDS_REPORT_METRICS       = ['SALES', 'PRICE', 'SHOPS_PLU']
REPORTS_OUTPUT_FOLDER       = 'trends_report'


# ========================== VARIABLES ==========================
script_run_date                  = date(2023,1,1)
previous_monday                  = str(script_run_date - timedelta(days=(script_run_date.isoweekday() % 7  - 1 + 7)))
previous_sunday                  = str(script_run_date - timedelta(days=(script_run_date.isoweekday() % 7)))
script_run_starttime             = datetime.now().strftime("%H%M%S")
ingest_folder_path               = datetime.now().strftime("%Y%m%d")


# ========================== FUNCTIONS ==========================
# Getting access and refresh tokens
def get_access_and_refresh_tokens(token_url, jwt_url, login_creds):
    """
    **Function:** get_access_and_refresh_tokens

    **Purpose:**  
    Authenticate via Keycloak and fetch both access/refresh tokens and JWT token required for further X5 API interactions.

    **Returns:**  
    - headers (dict): Headers with x5-api-key and bearer auth  
    - cookies (dict): Auth cookies for X5 API  
    - access_token (str)  
    - refresh_token (str)  
    - x5_key (str): JWT token

    **Raises:**  
    Logs and exits on any auth error.
    """
    try:
        # Step 1: Get access and refresh tokens
        token_response = requests.post(token_url, data=login_creds, verify=False)
        token_response.raise_for_status()
        token = token_response.json()

        access_token = token.get("access_token")
        refresh_token = token.get("refresh_token")

        if not access_token or not refresh_token:
            raise Exception("Access or refresh token is missing")

        logging.info("✅ Access & refresh tokens received")
        print("🔐 Access token (short):", access_token[:40], "...")
        print("🔐 Refresh token (short):", refresh_token[:40], "...")

        # Step 2: Get JWT token
        cookies = {
            'kc-access': access_token,
            'kc-state': refresh_token
        }

        jwt_headers = {
            "Authorization": f"Bearer {access_token}"
        }

        logging.info("📡 Requesting JWT token...")
        jwt_response = requests.get(jwt_url, headers=jwt_headers, cookies=cookies, verify=False)
        print("🔁 JWT response status:", jwt_response.status_code)
        print("🔁 JWT response body (trimmed):", jwt_response.text[:500])
        jwt_response.raise_for_status()

        jwt_data = jwt_response.json()
        x5_key = jwt_data["result"]["token"]
        logging.info("✅ JWT token received")
        print("🔑 JWT (short):", x5_key[:40], "...")

        # Step 3: Build headers for future calls
        headers = {
            "x5-api-key": x5_key,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        return headers, cookies, access_token, refresh_token, x5_key

    except Exception as e:
        logging.error(f"❌ Error during API call: {e}")
        sys.exit(1)

# Getting dim product data
def get_product_level_4(product_tree_url, headers, cookies):
    """
    **Function:** get_product_level_4

    **Purpose:**  
    Fetch and parse the full X5 product hierarchy tree, extracting only level 4 products (detailed level, leaf nodes).

    **Returns:**  
    - dict_product (list): Flat list of product dicts with `id` and `level`  
    - id_pr (list): Nested list with key `id: {code, level}` used for trends request

    **Raises:**  
    Logs and returns empty lists on failure.
    """
    try:
        logging.info("🌲 Getting product tree...")
        response = requests.get(product_tree_url, headers=headers, cookies=cookies, verify=False)
        response.raise_for_status()
        products = response.json().get("result", {}).get("nodes", [])
        logging.info("✅ Product tree received")

        dict_product = []
        id_pr = []

        # Recursively find all level 4 products (no children or Ui4 level)
        def collect_level_4_products(nodes):
            for node in nodes:
                children = node.get("children", [])
                if not children or node.get("level") == "Ui4":
                    dict_product.append({
                        "id": node["id"],
                        "level": node["level"]
                    })
                    id_pr.append({
                        "id": {
                            "code": node["id"],
                            "level": node["level"]
                        }
                    })
                else:
                    collect_level_4_products(children)

        collect_level_4_products(products)
        logging.info(f"🧾 Total level 4 products found: {len(dict_product)}")

        return dict_product, id_pr

    except Exception as e:
        logging.error(f"❌ Failed to get product tree: {e}")
        return [], []
    
# Gettings shops tree data
def get_and_prepare_shops_tree(shops_tree_url, headers, cookies):
    """
    **Function:** get_and_prepare_shops_tree

    **Purpose:**  
    Fetch the X5 shops/stores hierarchy and preprocess it to fit expected trends API format — including deep cleanup of names and IDs.

    **Returns:**  
    - shops (list): Cleaned, nested structure ready to be passed to trends request

    **Raises:**  
    Logs and exits if API fails or response is invalid.
    """
    try:
        logging.info("🌐 Fetching shops tree...")
        response = requests.get(
            shops_tree_url,
            cookies=cookies,
            headers=headers  
        )
        response.raise_for_status()
        shops = response.json()['result']['tradeNetworks']
        logging.info("✅ Shops tree fetched")

        key_selected, val_true = 'selectedFully', True
        key_partial, val_false = 'selectedFully', False

        for shop in shops:
            shop.pop("name", None)
            shop.pop("storesCount", None)
            shop["tradeNetworkId"] = shop.pop("id")
            districts = shop.pop("federalDistricts", [])
            shop["federalDistricts"] = districts

            for district in districts:
                district.pop("name", None)
                district.pop("storesCount", None)
                district["districtId"] = district.pop("id")
                regions = district.pop("regions", [])
                district["regions"] = regions

                for region in regions:
                    region.pop("name", None)
                    region.pop("storesCount", None)
                    region["regionId"] = region.pop("id")
                    cities = region.pop("cities", [])
                    region["citiesId"] = []
                    region[key_selected] = val_true
                    district[key_partial] = val_false
                    shop[key_partial] = val_false

        return shops

    except Exception as e:
        logging.error(f"❌ Error while fetching or processing shops tree: {e}")
        sys.exit(1)

# Download shops and dim product xlsx
def download_product_or_client_reference_excel(report_type,json, url, headers, cookies, save_root_path,save_folder,data_ingest_date,filename):
    """
    **Function:** download_product_or_client_reference_excel

    **Purpose:**  
    Download product-level metadata as an Excel (.xlsx) reference from X5 API using selected nodes.

    **Args:**  
    - json (list): nodes for reference  
    - url (str): Endpoint to request Excel file  
    - headers (dict), cookies (dict): Auth context  
    - save_path (str): File destination

    **Returns:**  
    - True on success  
    - False on failure (logged)
    """
    save_path            = f"{save_root_path}/{save_folder}/{data_ingest_date}"
    save_path_w_filename = f"{save_path}/{filename}"
    os.makedirs(save_path, exist_ok=True)
    try:
        logging.info(f"📥 Downloading {report_type} reference to {save_path}...")
        if report_type == "product":
            payload = {"nodes": json}
        else:
            payload = {'classifierStores': json}

        response = requests.post(url, json=payload, headers=headers, cookies=cookies, verify=False)
        response.raise_for_status()

        with open(save_path_w_filename, "wb") as f:
            f.write(response.content)

        logging.info(f"✅ {report_type} reference saved to {save_path}")
        return True

    except Exception as e:
        logging.error(f"❌ Failed to download {report_type} reference: {e}")
        return False

# Getting fact trends data
def get_trends_report_id(trends_report_url,json_body, cookies, headers):
    """
    **Function:** get_trends_report_id

    **Purpose:**  
    Submit a report request to the X5 trends API, initiating report generation.

    **Args:**  
    - trends_report_url (str): Endpoint  
    - json_body (dict): Full request payload  
    - cookies, headers: Auth context

    **Returns:**  
    - report_id (str)

    **Raises:**  
    Logs and exits if request fails.
    """
    logging.info("📤 Sending trends report generation request...")
    try:
        response = requests.post(
            trends_report_url,
            json=json_body,
            cookies=cookies,
            headers=headers
        )
        response.raise_for_status()
        report_id = response.json()["result"]["id"]
        logging.info(f"✅ Trends report requested. ID: {report_id}")
        return report_id
    except Exception as e:
        logging.error(f"❌ Failed to request trends report: {e}")
        sys.exit(1)

def wait_for_trends_report(trends_report_root_url,report_id, cookies, headers, poll_interval=5):
    """
    **Function:** wait_for_trends_report

    **Purpose:**  
    Poll report status endpoint until the trends report is ready or failed.

    **Args:**  
    - trends_report_root_url (str): Report status endpoint  
    - report_id (str)  
    - cookies, headers: Auth context  
    - poll_interval (int): Delay between polls (seconds)

    **Returns:**  
    - Full response object when report status is SUCCEEDED

    **Raises:**  
    Logs and exits on failure or unexpected errors.
    """
    logging.info("⏳ Waiting for trends report to be ready...")
    while True:
        try:
            response = requests.get(
                f'{trends_report_root_url}{report_id}',
                cookies=cookies,
                headers=headers
            )
            response.raise_for_status()
            status = response.json()["result"]["status"]
            logging.info(f"🔁 Report status: {status}")

            if status == "FAILED":
                logging.error("❌ Report generation failed.")
                sys.exit(1)
            elif status == "SUCCEEDED":
                logging.info("✅ Report is ready.")
                return response
        except Exception as e:
            logging.error(f"❌ Error while polling report status: {e}")
            sys.exit(1)

        time.sleep(poll_interval)

def download_trends_report_zip(export_trends_root_url,export_id, report_name, cookies, headers, save_root_path,save_folder, data_ingest_date):
    """
    **Function:** download_trends_report_zip

    **Purpose:**  
    Download the final trends report archive as a ZIP file and store it locally.

    **Args:**  
    - export_trends_root_url (str)  
    - export_id (str): exportFileId from trends response  
    - report_name (str): Used for naming the downloaded zip  
    - cookies, headers: Auth context  
    - save_dir (str): Path to store the file  
    - data_ingest_date (str): Subfolder to organize by date

    **Raises:**  
    Logs and exits on any download or file I/O failure.
    """
    logging.info("📦 Downloading report zip...")
    try:
        response = requests.get(
            f'{export_trends_root_url}{export_id}',
            cookies=cookies,
            headers=headers
        )
        response.raise_for_status()

        save_folder_path = f"{save_root_path}\{save_folder}\{data_ingest_date}"
        os.makedirs(save_folder_path, exist_ok=True)
        file_path = os.path.join(save_folder_path, f"{report_name}.zip")

        with open(file_path, 'wb') as f:
            f.write(response.content)

        logging.info(f"✅ Report saved to: {file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to download or save report: {e}")
        sys.exit(1) 


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
    "name": f"trends_report_{ingest_folder_path}_{script_run_starttime}",
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