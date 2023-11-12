"""
Download shares info from Tinkoff API
"""

import asyncio

import tinkoff.invest as inv
from .utils import load_config
from .cache import compute_or_load


def download_raw_shares(force_compute: bool) -> list[inv.Share]:
    """
    Get all shares from Tinkoff API
    """
    async def _get_shares() -> list[inv.Share]:
        config = load_config()
        async with inv.AsyncClient(token=config['token']) as client:
            return (await client.instruments.shares()).instruments
    return asyncio.run(compute_or_load(_get_shares, 'shares', force_compute=force_compute))


def _filter_share(share: inv.Share) -> bool:
    """
    Return True if the share is good
    """
    if share.currency != 'rub':
        return False  # Only rub stocks
    if share.for_qual_investor_flag:
        return False  # Not only for qualified investor
    if not share.buy_available_flag or not share.sell_available_flag:
        return False  # You can buy and sell share
    if not share.for_iis_flag:
        return False  # You can use IIS with this share
    if share.otc_flag:
        return False  # Drop OTC shares
    return True


def download_clean_shares(verbose: bool, force_compute: bool) -> list[inv.Share]:
    """
    Filter shares from Tinkoff API based on sanity conditions
    """
    raw_shares = download_raw_shares(force_compute=force_compute)
    if verbose:
        print(f'{len(raw_shares)} raw shares')

    clean_shares = list(filter(_filter_share, raw_shares))
    if verbose:
        print(f'{len(clean_shares)} clean shares')

    return clean_shares
