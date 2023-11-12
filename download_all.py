from download import download_clean_shares, download_shares_candles, extract_shares_candles


def main():
    # Download shares info (~1 min)
    download_clean_shares(verbose=True, force_compute=False)
    # Download candles (~30 min)
    download_shares_candles(verbose=True, force_compute=False)
    # Extract candles (~10 min)
    extract_shares_candles(force_compute=True)


if __name__ == '__main__':
    main()
