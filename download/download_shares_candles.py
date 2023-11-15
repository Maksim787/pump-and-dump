"""
Download shares data from Tinkoff API in data/zip/ directory
"""


import aiohttp
import asyncio
import datetime
import os
from pathlib import Path

from .constants import ZIP_DIRECTORY
from .download_shares_info import download_clean_shares
from .utils import load_config, RateLimiter, limited_gather


MAX_YEARS = 5
N_REQUESTS_PER_MINUTE = 29


async def fetch_data(session: aiohttp.ClientSession, token: str, save_path: Path, figi: str, year: int) -> bool:
    """
    Download the file if possible
    Return True on success
    """
    url = 'https://invest-public-api.tinkoff.ru/history-data'
    params = {'figi': figi, 'year': year}
    headers = {"Authorization": f"Bearer {token}"}

    async with session.get(url, params=params, headers=headers) as response:
        if response.status == 404:
            print(f'{save_path}: end of history')
            return False
        response.raise_for_status()
        # Load data
        data = await response.read()
    # Save data
    try:
        with open(save_path, 'wb') as f:
            f.write(data)
    except Exception as e:
        print(f'Failed to save cache due to the exception: {e}. Remove {save_path}')
        os.remove(save_path)
        raise
    print(f'{save_path}: success')
    return True


def download_shares_candles(verbose: bool, force_compute: bool):
    shares = download_clean_shares(verbose=verbose, force_compute=force_compute)
    config = load_config()
    current_year = datetime.date.today().year

    async def _get_history_data():
        nonlocal shares
        print('Load shares')
        rate_limiter = RateLimiter(n_tasks_per_minute=N_REQUESTS_PER_MINUTE)
        async with aiohttp.ClientSession() as session:
            # Iterate over years
            for year in range(current_year, current_year - MAX_YEARS - 1, -1):
                tasks = []
                if not shares:
                    print(f'The last year is {year + 1}')
                    break
                # Iterate over shares
                download_shares = []
                existing_shares = []
                for share in shares:
                    # Create ticker directory
                    ticker_directory = ZIP_DIRECTORY / share.ticker
                    ticker_directory.mkdir(exist_ok=True)
                    file = Path(ticker_directory / f'{year}.zip')
                    if not file.exists() or force_compute:
                        # Add download task
                        tasks.append(fetch_data(session, token=config['token'], save_path=file, figi=share.figi, year=year))
                        download_shares.append(share)
                    else:
                        print(f'{file}: exists')
                        existing_shares.append(share)
                # Filter shares with longer history
                longer_history_mask = await limited_gather(*tasks, rate_limiter=rate_limiter)
                download_shares = [share for share, mask in zip(download_shares, longer_history_mask) if mask]
                shares = existing_shares + download_shares
                print(f'{year}: {len(shares)} shares: {[share.ticker for share in shares]}')
    asyncio.run(_get_history_data())
