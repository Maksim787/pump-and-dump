from glob import glob
import json
import pandas as pd
from pathlib import Path
from shutil import rmtree
from tqdm import tqdm
from typing import Sequence
from zipfile import ZipFile
import os

from load_data import main as load_data


def extract_archive(ticker: str, figi: str, year: str, input_path: str, output_path: str, path_to_dataframe: str):
    files = glob(f"{input_path}/*")
    files = sorted(filter(lambda x: (figi in x) and (year in x), files))
    if not len(files):
        print(f"Not find tickets for name={ticker} in year {year}")
        return pd.DataFrame()

    zip_file = files[0]
    dataset_folder = f"{output_path}/{ticker}/{year}/"
    os.makedirs(os.path.dirname(dataset_folder), exist_ok=True)
    try:
        with ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(dataset_folder)
    except ValueError:
        rmtree(dataset_folder)
        print(f"Bad file {zip_file} for ticker {ticker} and year {year}")
        return pd.DataFrame()

    year_history = [pd.read_csv(dataset_path, header=None,
                                names=['id', 'time', 'open', 'close', 'high', 'low', 'volume'],
                                sep=';',
                                index_col=False).drop(columns=['id'])
                    for dataset_path in glob(f"{dataset_folder}/*")]
    rmtree(dataset_folder)
    year_df = pd.concat(year_history)
    year_df['ticker'] = ticker
    year_df.sort_values(['ticker', 'time'], inplace=True)
    year_df.to_csv(path_to_dataframe)
    return year_df


def get_ticker_to_figi(path: str):
    print(path)
    if not os.path.exists(path):
        print("Don't find anything data, loading...")
        load_data()
    with open(path, "r") as f:
        ticker_to_figi = json.load(f)
    return ticker_to_figi


def main(tickers: Sequence[str], path_to_dict: str = None,
         min_year: int = 2022, max_year: int = 2023):
    print(os.getcwd())
    if path_to_dict is None:
        path_to_dict = os.path.join(os.getcwd(), Path("../../data/figi/ticker_to_figi.txt").parent)
    path_to_datasets = os.path.join(os.getcwd(), Path("../../data/datasets").parent)
    # os.makedirs(os.path.dirname(path_to_datasets), exist_ok=True)
    print(os.path.dirname(path_to_datasets))
    ticker_to_figi = get_ticker_to_figi(path_to_dict)

    ticker_to_figi = {key: value for key, value in ticker_to_figi.items() if key in tickers}

    datasets = []
    for ticker, figi in tqdm(ticker_to_figi.items()):
        for year in range(min_year, max_year + 1):
            path_to_df = f"{path_to_datasets}/{ticker}/{year}.csv"
            if os.path.exists(path_to_df):
                data = pd.read_csv(path_to_df, index_col=0)
            else:
                path_to_zip_data = "../../data/zip_data"
                data = extract_archive(ticker, figi, str(year), path_to_zip_data, path_to_datasets, path_to_df)
            datasets.append(data)
    return pd.concat(datasets)


if __name__ == "__main__":
    path_to_ticker = "../../data/figi/ticker_to_figi.txt"
    all_tickers = list(get_ticker_to_figi(path_to_ticker).keys())
    all_tickers.remove("PIKK")
    main(all_tickers)
