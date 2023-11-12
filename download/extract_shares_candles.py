import zipfile
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from .constants import ZIP_DIRECTORY, CSV_DIRECTORY


def read_year_zip(year_zip: Path):
    """
    Read all intraday files from the yearly zip-archive
    """
    columns = ['time_open', 'open', 'close', 'high', 'low', 'volume_lot']
    with zipfile.ZipFile(year_zip, 'r') as zip_ref:
        files = zip_ref.namelist()
        dfs = []
        for file in files:
            with zip_ref.open(file) as csv_file:
                df = pd.read_csv(
                    csv_file,
                    header=None,
                    index_col=False,
                    names=['figi'] + columns,
                    sep=';'
                ).drop(columns=['figi'])
                dfs.append(df)
    # Concatenate all days
    if dfs:
        return pd.concat(dfs, axis=0)
    return pd.DataFrame(columns=columns)


def read_ticker_zips(ticker_directory: Path) -> pd.DataFrame:
    """
    Read zip files for each year for the given ticker
    """
    try:
        dfs = []
        # Read all zip files in the directory
        for year_zip in ticker_directory.iterdir():
            dfs.append(read_year_zip(year_zip))
    except zipfile.BadZipFile as e:
        print(f'\n{ticker_directory}: Zip Exception {e}')
    except Exception as e:
        print(f'\n{ticker_directory}: Exception {e}')
    # Concatenate years
    return pd.concat(dfs, axis=0).sort_values(by='time_open')


def extract_shares_candles(force_compute: bool):
    """
    Extract data from data/zip/ to data/csv/
    """
    for ticker_directory in tqdm(list(ZIP_DIRECTORY.iterdir())):
        save_path = CSV_DIRECTORY / f'{ticker_directory.name}.csv'
        if save_path.exists() and not force_compute:
            continue
        df = read_ticker_zips(ticker_directory)
        df.to_csv(save_path, index=False)
