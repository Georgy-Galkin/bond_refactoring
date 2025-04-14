import zipfile
from pathlib import Path
import shutil

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
