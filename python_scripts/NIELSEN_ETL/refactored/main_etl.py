# ========================== IMPORTS ==============================
from pathlib import Path
import json
from datetime import datetime, timedelta, date
from extract_zip_files import extract_zip_files,copy_files
import logging
# ========================== CONSTANTS =============================
JSON_CONFIG = Path("NIELSEN_ETL_CONFIG.json")

# Read the JSON file
with open(JSON_CONFIG, "r", encoding="utf-8") as file:
    CONFIG = json.load(file)

RAW_FILES_FOLDER_PATH             = CONFIG["raw_root_path"]
INGEST_FOLDER_PATH                = CONFIG["ingest_root_path"]
INGEST_FOLDER_FOR_EXTRACTED_FILES = CONFIG["ingest_subfolder_name_for_extracted_zips"]
CSV_FILES_SEPARATOR               = CONFIG["csv_files_separator"]

# ========================== VARIABLES =============================
ingest_date              = datetime.now().strftime("%Y%m%d")
full_ingest_folder_path  = f"{INGEST_FOLDER_PATH}/{ingest_date}"

# ========================== MAIN SESSION ==========================
logging.info(f"Starting NIELSEN ETL at {datetime.now()}")

files_copied = copy_files(
    src_folder  = RAW_FILES_FOLDER_PATH,
    dest_folder = INGEST_FOLDER_PATH,
    pattern     = '*.zip')

if files_copied>0:
    logging.info("Loaded files to ingest folder. Starting etl process.")