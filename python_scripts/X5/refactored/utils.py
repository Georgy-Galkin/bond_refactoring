import zipfile
from pathlib import Path
import shutil
from typing import List, Union
import pandas as pd

def unzip_all_flat(folder_path, allowed_exts=None):
    """
    Unzips all .zip files in the given folder and flattens the content
    into the same folder (ignoring subdirectories inside zips).

    Args:
        folder_path (str or Path): Path to the folder containing ZIP files.
        allowed_exts (list[str] or None): List of extensions to extract (e.g., ['.csv', '.xlsx']).
                                          If None, extracts all file types.

    Returns:
        List[Path]: List of extracted file paths.
    """
    folder_path = Path(folder_path)
    extracted_files = []

    for zip_file in folder_path.glob("*.zip"):
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for member in zip_ref.infolist():
                # Skip directories
                if member.is_dir():
                    continue

                # Optional: filter by file extension
                ext = Path(member.filename).suffix.lower()
                if allowed_exts and ext not in allowed_exts:
                    continue

                # Build output path and extract
                out_path = folder_path / Path(member.filename).name
                with zip_ref.open(member) as source, open(out_path, 'wb') as target:
                    shutil.copyfileobj(source, target)
                extracted_files.append(out_path)

    return extracted_files


def list_files_with_extension(folder: Union[str, Path], extension: str) -> List[str]:
    """
    Lists all files with the specified extension in the given folder.

    Args:
        folder (str or Path): The folder to search in.
        extension (str): File extension to match (e.g., '.csv', '.xlsx').

    Returns:
        List[str]: List of full file paths (as strings) matching the extension.
    """
    folder = Path(folder)
    return [str(p) for p in folder.glob(f"*{extension}")]

def convert_xlsx_to_csv(xlsx_path: str) -> str:
    """
    Converts a single .xlsx file to .csv and saves it in the same folder.

    Args:
        xlsx_path (str): Full path to the .xlsx file.

    Returns:
        str: Full path to the generated .csv file.
    """
    xlsx_file = Path(xlsx_path)
    if not xlsx_file.exists() or xlsx_file.suffix.lower() != '.xlsx':
        raise ValueError(f"Invalid .xlsx file: {xlsx_path}")

    df = pd.read_excel(xlsx_file)
    csv_path = xlsx_file.with_suffix('.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8')

    return str(csv_path)
