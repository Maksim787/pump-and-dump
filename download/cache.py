"""
Save function return value to cache/
Load the value from cache if it is already saved
"""


import pickle
import typing as tp
import os
from pathlib import Path

CACHE_DIRECTORY = Path('cache/')
CACHE_DIRECTORY.mkdir(exist_ok=True)

T = tp.TypeVar('T')


async def compute_or_load(func: tp.Callable[[], tp.Awaitable[T]], file: str, force_compute: bool) -> T:
    file = CACHE_DIRECTORY / f'{file}.pickle'
    if file.exists() and not force_compute:
        with open(file, 'rb') as f:
            return pickle.load(f)
    print('Compute function')
    result = await func()
    try:
        with open(file, 'wb') as f:
            pickle.dump(result, f)
    except Exception as e:
        print(f'Failed to save cache due to the exception: {e}. Remove {file}')
        os.remove(file)
        raise
    return result
