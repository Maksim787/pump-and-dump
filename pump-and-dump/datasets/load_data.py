from datetime import datetime
import json
from time import sleep
from tqdm import tqdm
import os
import subprocess

from tinkoff.invest import Client

TOKEN = os.environ["INVEST_READ_TOKEN"]


def load_figi(rel_path: str, update_figi: bool = False):
    """
    Скачивание списка акций и их сохранение в удобном формате
    :param rel_path:
    :param update_figi: нужно ли обновить список акций
    :return:
    """
    if not os.path.exists(f"{rel_path}/figi.txt") or update_figi:
        with Client(TOKEN) as client:
            shares = client.instruments.shares().instruments
            shares = [share for share in shares if share.currency == 'rub' and not share.for_qual_investor_flag]
        figi_to_ticker: dict[str, str] = {share.figi: share.ticker for share in shares}
        ticker_to_figi: dict[str, str] = {share.ticker: share.figi for share in shares}
        with open(f"{rel_path}/figi_to_ticker.txt", 'w') as f:
            json.dump(figi_to_ticker, f)
        with open(f"{rel_path}/ticker_to_figi.txt", 'w') as f:
            json.dump(ticker_to_figi, f)
        with open(f"{rel_path}/figi.txt", 'w') as f:
            for share in shares:
                print(share.figi, file=f)
    with open(f"{rel_path}/ticker_to_figi.txt") as f:
        figi_to_ticker = json.load(f)
    with open(f"{rel_path}/ticker_to_figi.txt") as f:
        ticker_to_figi = json.load(f)
    with open(f"{rel_path}/figi.txt") as f:
        figi = [line[:-1] for line in f]
    return figi, figi_to_ticker, ticker_to_figi


def download_zip_archive(figi: str, year: int, url: str, rel_path: str, minimum_year: int = 2017):
    """
    Скачиваем по figi все доступные годы
    :param url: ссылка откуда будет происходить скачивание данных
    :param figi: figi акции
    :param year: год за который качаются акции
    :param rel_path: путь для сохранения zip-архивов
    :param minimum_year: с какого пути не читать акции
    :return:
    """
    if year < minimum_year:
        return 0
    file_name = f"{rel_path}/{figi}_{year}.zip"
    curl_command = f'curl -s --location "{url}?figi={figi}&year={year}" -H "Authorization:' \
                   f' Bearer {TOKEN}" -o "{file_name}" -w "%{{http_code}}\\n"'
    response = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
    response_code = int(response.stdout.strip())
    if response_code == 429:
        print("Rate limit exceed. Sleep 5 seconds")
        sleep(5)
        download_zip_archive(figi, year, url, rel_path, minimum_year)
        return 0
    elif response_code in (429, 500):
        print("Invalid token'")
        return 1
    elif response_code == 404:
        print(f"Data not found for figi={figi}, year={year}, removing empty file")
        os.remove(file_name)
        return 0
    elif response_code != 200:
        print(f"Unspecified error with code: {response_code}")
        return 1
    download_zip_archive(figi, year - 1, url, rel_path, minimum_year)


def main():
    rel_path = "../../data/figi"
    figi, figi_to_ticker, ticker_to_figi = load_figi(rel_path, update_figi=True)
    current_year = datetime.now().year
    url = "https://invest-public-api.tinkoff.ru/history-data"
    rel_path = "../../data/zip_data"
    for current_figi in tqdm(figi):
        download_zip_archive(current_figi, current_year, url, rel_path)


if __name__ == "__main__":
    main()
