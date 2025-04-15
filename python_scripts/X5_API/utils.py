from datetime import datetime,date,timedelta
import logging
from typing import List, Tuple
from pathlib import Path
from typing import List, Union

def generate_date_ranges_by_weekday(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    """
    Generates a list of date ranges (start, end) between start_date and end_date.
    The step is 14 days if today is Monday, otherwise 7 days.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        List[Tuple[str, str]]: List of (start_date, end_date) ranges as strings.
    """

    today = date.today()
    step_days = 14 if today.weekday() == 0 else 7  # Monday = 0
    logging.info(f"Generating date ranges with step {step_days} days")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    ranges = []

    while start < end:
        range_start = start.strftime("%Y-%m-%d")
        start += timedelta(days=step_days)
        range_end = min(start, end).strftime("%Y-%m-%d")
        ranges.append((range_start, range_end))

    logging.info(f"Generated {len(ranges)} date ranges: {ranges}")
    return ranges

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
