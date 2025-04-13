import pandas as pd
from pathlib import Path
import os
import logging
import pyodbc

def detect_file_type(file_path: Path) -> str:
    """
    Detect the type of file based on its name.

    Args:
        file_path (Path): Path to the file.

    Returns:
        str: One of ['MKT', 'FCT', 'PROD', 'PER'] if matched.
             None if not recognized or is a fact_data file.
    """
    name = file_path.name.upper()

    # Exclude any _fact_data files, regardless of location
    if "_FACT_DAT" in name:
        return None

    for tag in ["MKT", "FCT", "PROD", "PER"]:
        if f"_{tag}" in name:
            return tag

    return None

def load_csv_to_mssql(file_path, table_name, columns, conn_str, rows_per_batch=10000):
    """
    Truncate and load CSV data into an MSSQL table.

    Args:
        file_path (Path): Path to the CSV file.
        table_name (str): Target MSSQL table name.
        columns (list): List of column names (strings).
        conn_str (str): ODBC connection string.
        rows_per_batch (int): Number of rows per insert batch.
    """
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()

    logging.info(f"🚮 Truncating table {table_name}")
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL TRUNCATE TABLE {table_name}")

    df_iter = pd.read_csv(file_path, usecols=columns, chunksize=rows_per_batch, sep='|', dtype=str, low_memory=False)

    for chunk in df_iter:
        values = [tuple(x) for x in chunk.values]
        placeholders = ', '.join(['?'] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(insert_sql, values)
        logging.info(f"✅ Inserted {len(values)} rows into {table_name}")

    conn.close()

def process_folder_tree_and_load_to_mssql(ingest_folder, conn_str, columns_dictionary, rows_per_batch=10000):
    """
    Walk through all subfolders in the root_path, find CSVs or splited folders,
    and load them into MSSQL using predefined schema.

    Args:
        root_path (str or Path): Root directory.
        conn_str (str): ODBC connection string.
        columns (list): List of required columns (schema).
        rows_per_batch (int): Number of rows per batch insert.
    """
    root_path = Path(ingest_folder)
    for folder in [f for f in root_path.iterdir() if f.is_dir()]:
        folder_name = folder.name
        splited_dir = folder / "splited"

        if splited_dir.exists() and splited_dir.is_dir():
            for csv_file in splited_dir.glob("*.csv"):
                table_name = f"{folder_name}_fact_data"
                logging.info(f"📂 Loading split CSV {csv_file.name} into {table_name}")
                load_csv_to_mssql(csv_file, table_name, columns_dictionary['FACT'].split('|'), conn_str, rows_per_batch)
        else:
            for csv_file in folder.glob("*.csv"):
                file_type = detect_file_type(csv_file)
                if file_type:
                    table_name = f"{folder_name}_{file_type}"
                    logging.info(f"📂 Loading CSV {csv_file.name} into {table_name}")
                    load_csv_to_mssql(csv_file, table_name, columns_dictionary[table_name].split('|'), conn_str, rows_per_batch)
