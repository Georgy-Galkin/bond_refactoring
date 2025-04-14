import zipfile
from pathlib import Path
import pandas as pd

def unzip_and_convert_xlsx_to_csv(zip_path):
    """
    Unpacks a .zip file, reads all .xlsx files inside, and saves them as .csv in the same directory.

    Args:
        zip_path (str or Path): Path to the zip archive.

    Returns:
        List[Path]: Paths to all saved CSV files.
    """
    zip_path = Path(zip_path)
    output_folder = zip_path.parent
    saved_csv_paths = []

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_folder)
        for member in zip_ref.namelist():
            extracted_file = output_folder / member
            if extracted_file.suffix.lower() == ".xlsx":
                try:
                    df = pd.read_excel(extracted_file)
                    csv_path = extracted_file.with_suffix('.csv')
                    df.to_csv(csv_path, index=False)
                    saved_csv_paths.append(csv_path)
                except Exception as e:
                    print(f"Failed to convert {extracted_file.name}: {e}")

    return saved_csv_paths
