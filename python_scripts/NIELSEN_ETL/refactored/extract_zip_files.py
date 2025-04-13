# ========================== IMPORTS ==============================
import zipfile
import os
import logging
import re
from pathlib import Path
import shutil

# ========================== LOGGER SETUP ======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ========================== UTILS ============================
def copy_files(src_folder, dest_folder, pattern="*"):
    """
    Copy all files from `src_folder` to `dest_folder` matching the given pattern.

    Args:
        src_folder (str or Path): Source directory.
        dest_folder (str or Path): Target directory.
        pattern (str): File match pattern (e.g., '*.csv', '*.txt', '*').

    Returns:
        int: Number of files copied.
    """
    src_path  = Path(src_folder)
    dest_path = Path(dest_folder)
    dest_path.mkdir(parents=True, exist_ok=True)

    files_copied = 0
    for file in src_path.glob(pattern):
        if file.is_file():
            logging.info(f"Copying {file}")
            shutil.copy2(file, dest_path / file.name)
            files_copied += 1

    logging.info(f"Successfully copied {files_copied} files")
    return files_copied

def make_dir_if_not_exists(path: Path):
    """
    Creates directory if it does not exist
    """
    if not path.exists():
        path.mkdir(parents=True)
        logging.info(f"Created directory: {path}")

def get_list_of_zip_files(path: Path):
    """
    Returns a list of zip files
    """
    return [f.stem for f in path.glob("*.zip")]

def get_nielsen_database_name_from_first_two_words(name: str):
    '''
    Splits zip folder name by _ symbol.
    Concatenates only first two parts.
    '''
    parts = re.split(r"_", name)
    return f"{parts[0]}_{parts[1]}" if len(parts) > 1 else name

# ==== MAIN FUNCTION ====
def extract_zip_files(root_path: Path, extract_subfolder: Path):
    """
    Extracts data from zip files in root_path to extracted path
    """
    make_dir_if_not_exists(root_path / extract_subfolder)

    zip_filenames_list = get_list_of_zip_files(root_path)
    logging.info(f"Found {len(zip_filenames_list)} ZIP files")

    for zip_number, zip_file in enumerate(zip_filenames_list, 1):
        zip_file_path = root_path / f"{zip_file}.zip"
        grouped_name = get_nielsen_database_name_from_first_two_words(zip_file).replace("BON_", "")
        target_folder = root_path / extract_subfolder / grouped_name

        make_dir_if_not_exists(target_folder)

        try:
            with zipfile.ZipFile(zip_file_path, 'r') as archive:
                csv_files = [f for f in archive.namelist() if f.endswith(".csv")]

                for file in csv_files:
                    archive.extract(file, path=target_folder)
                    original_path = target_folder / file
                    renamed_path = target_folder / file.replace("BON_", "")
                    os.rename(original_path, renamed_path)

            logging.info(f"[{zip_number}/{len(zip_file)}] Extracted: {zip_file_path.name}")

        except zipfile.BadZipFile:
            logging.error(f"❌ Corrupted ZIP: {zip_file_path.name}")
        except Exception as e:
            logging.error(f"❌ Failed to extract {zip_file_path.name}: {e}")

    logging.info("✅ ZIP extraction completed")