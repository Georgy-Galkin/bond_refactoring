import os
import pandas as pd
import logging
import glob
import os

def list_all_extracted_files(nameslist, extracted_path):
    """
    Returns a flat list of all files inside folders listed in `nameslist`, located within `extracted_path`.
    """
    all_files = []
    for name in nameslist:
        folder_path = os.path.join(extracted_path, name)
        all_files.extend(glob.glob(os.path.join(folder_path, '*')))
    return all_files
def split_large_csvs(nameslist, extracted_path,separator, headers, chunksize=1_000_000):
    """
    Splits large '_fact_dat' CSV files into smaller chunks (default 1M rows) 
    and saves them in 'splited' subfolders inside each folder in nameslist.

    Args:
        nameslist (list): List of base folder names (e.g., ['202401_PROD']).
        extracted_path (str): Root path where extracted zip folders are located.
        headers (list): List of column names to keep in output files.
        chunksize (int): Number of rows per output file chunk.
    """
    for name in nameslist:
        base_folder = os.path.join(extracted_path, name)
        split_folder = os.path.join(base_folder, "splited")
        os.makedirs(split_folder, exist_ok=True)

        # Find the fact data file to split
        for file in os.listdir(base_folder):
            if "_fact_dat" in file:
                file_path = os.path.join(base_folder, file)
                
                # Split into chunks and write to separate files
                for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize, sep=separator)):
                    output_file = os.path.join(split_folder, f"{name}_fact_data{i}.csv")
                    chunk.to_csv(output_file, sep=separator, columns=headers, index=False)

                break  # Stop after first matching fact file

def list_csvs_from_split_folder(nameslist, extracted_path):
    """
    Collects all CSV files located inside 'splited' subfolders for each folder in nameslist.

    Args:
        nameslist (list): List of folder names (e.g., ['202401_FCT', '202401_PROD']).
        extracted_path (str): Base path where extracted data is stored.

    Returns:
        list: Full paths to all found CSV files inside the 'splited' subfolders.
    """
    all_csv_files = []
    for name in nameslist:
        split_folder = os.path.join(extracted_path, name, "splited")
        csv_files = glob.glob(os.path.join(split_folder, "*.csv"))
        all_csv_files.extend(csv_files)
    return all_csv_files


