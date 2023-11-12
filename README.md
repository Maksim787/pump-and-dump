# pump-and-dump

## Download and extract data

Download and extract from Tinkoff API:
1. Shares info: `tinkoff.inv.Share` for each share
1. Shares candles: `time_open,open,close,high,low,volume_lot` for each year since 2018

Directories:
1. `cache/` — directory with Tinkoff API requests responses
1. `data/zip/` — directory with `.zip` archives for each ticker and year
1. `data/csv/` — directory with `.csv` for each ticker

```
python download_all.py
```

## Load dataset

TODO: