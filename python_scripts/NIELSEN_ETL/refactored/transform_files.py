import os
import pandas as pd
import bondetl as be
import logging


def get_vocas(nameslist, extracted_path):
    files = []
    for name in nameslist:
        folder_path = os.path.join(extracted_path, name)
        files.extend([os.path.join(folder_path, f) for f in os.listdir(folder_path)])
    return files


def get_csvs_from_folder(nameslist, extracted_path):
    files = []
    for name in nameslist:
        folder_path = os.path.join(extracted_path, name, "splited")
        files.extend([os.path.join(folder_path, f) for f in os.listdir(folder_path)])
    return files


def split_csv(nameslist, extracted_path, headers):
    for name in nameslist:
        base_folder = os.path.join(extracted_path, name)
        split_folder = os.path.join(base_folder, "splited")
        os.makedirs(split_folder, exist_ok=True)

        for file in os.listdir(base_folder):
            if "_fact_dat" in file:
                file_path = os.path.join(base_folder, file)
                for i, chunk in enumerate(pd.read_csv(file_path, chunksize=1000000, sep="|")):
                    chunk.to_csv(
                        os.path.join(split_folder, f"{name}_fact_data{i}.csv"),
                        sep="|", columns=headers, index=False
                    )
                break


def bulk_insert(table_name, files, headers, row_separator='0x0a'):
    sql_table = be.SQL('Nielsen', table_name)
    formatted_headers = ", ".join([f"[{h}] [nvarchar](255) NULL" for h in headers])
    
    create_query = f"""
    IF OBJECT_ID('{table_name}') IS NOT NULL DROP TABLE {table_name};
    CREATE TABLE {table_name} ({formatted_headers})
    """
    sql_table.Query(sql_query=create_query)

    for i, file_path in enumerate(files):
        logging.info(f"Loading file {i+1}/{len(files)}: {file_path}")
        sql_table.bulk(file_path, start_from_row='2', delimiter="|", row_separator=row_separator)


def process_etl(headers, base_name, nameslist, extracted_path, skip_split=False, skip_bulk=False):
    if not skip_split:
        split_csv(nameslist, extracted_path, headers)

    if not skip_bulk:
        csv_files = get_csvs_from_folder(nameslist, extracted_path)
        bulk_insert(f"fact_{base_name}", csv_files, headers)

    for f in get_vocas(nameslist, extracted_path):
        try:
            name = f[-8:][:4].replace('_','')
            voca_base = f[len(extracted_path)+19:][:4]
            full_name = f"voca_{base_name}_{voca_base}_{name}"
            if name in ['FCT', 'MKT', 'PER', 'PROD']:
                headers = list(pd.read_csv(f, sep="|", low_memory=False).columns)
                bulk_insert(full_name, [f], headers)
        except:
            try:
                name = f[-17:][:4].replace('_','')
                voca_base = f[len(extracted_path)+19:][:4]
                full_name = f"voca_{base_name}_{voca_base}_{name}"
                if name in ['FCT', 'MKT', 'PER', 'PROD']:
                    headers = list(pd.read_csv(f, sep="|", low_memory=False).columns)
                    bulk_insert(full_name, [f], headers)
            except:
                pass
