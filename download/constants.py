from pathlib import Path

DATA_DIRECTORY = Path('data/')
DATA_DIRECTORY.mkdir(exist_ok=True)

ZIP_DIRECTORY = DATA_DIRECTORY / 'zip'
ZIP_DIRECTORY.mkdir(exist_ok=True)

CSV_DIRECTORY = DATA_DIRECTORY / 'csv'
CSV_DIRECTORY.mkdir(exist_ok=True)
