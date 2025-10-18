import os
import datetime as dt
import zipfile
import pandas as pd
import paramiko


def get_raw_dataframe():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_path = os.path.join(script_dir, 'data', 'raw-file.xlsx')
    return pd.read_excel(raw_path)


def normalize_ticker_series(series: pd.Series) -> pd.Series:
    """Normalize tickers: trim, take first token, uppercase."""
    return series.astype(str).str.strip().str.split().str[0].str.upper()


def build_shares_map(tickers: pd.Series, shares: pd.Series) -> pd.Series:
    """Return a Series indexed by normalized ticker with numeric shares values."""
    return (
        pd.DataFrame({
            'ticker': normalize_ticker_series(pd.Series(tickers)),
            'shares': pd.to_numeric(pd.Series(shares), errors='coerce')
        })
        .dropna()
        .drop_duplicates(subset='ticker')
        .set_index('ticker')['shares']
    )

if __name__ == "__main__":
    df = get_raw_dataframe()

    tickers = df['Ticker Short']
    shares = df['Shares']
    sector = df['Sector']


    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'data')

    start_date = pd.to_datetime("2025-06-29").date()
    end_date = dt.date.today()

    host = "sftp.datashop.livevol.com"
    port = 22
    user = "nan2_lehigh_edu"
    pwd = "PAIndex2023!"
    remote_folder = "/subscriptions/order_000046197/item_000053507"

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, port, user, pwd)
    with ssh.open_sftp() as sftp:
        sftp.chdir(remote_folder)
        remote_files = sftp.listdir()
        for fname in sorted(remote_files):
            if not (fname.startswith('UnderlyingEOD_') and fname.endswith('.zip') and 'Summaries' not in fname):
                continue
            try:
                d = pd.to_datetime(fname.split('_')[-1].split('.')[0]).date()
            except Exception:
                continue
            if d < start_date or d > end_date:
                continue
            local_csv = os.path.join(data_dir, f"UnderlyingEOD_{d}.csv")
            if os.path.exists(local_csv):
                continue
            local_zip = os.path.join(data_dir, fname)
            sftp.get(os.path.join(remote_folder, fname).replace('\\', '/'), local_zip.replace('\\', '/'))
            with zipfile.ZipFile(local_zip, 'r') as zf:
                zf.extractall(data_dir)
            os.remove(local_zip)
    ssh.close()

    csv_paths = []
    for fn in os.listdir(data_dir):
        if fn.startswith('UnderlyingEOD_') and fn.endswith('.csv'):
            try:
                d = pd.to_datetime(fn.split('_')[-1].split('.')[0]).date()
            except Exception:
                continue
            if start_date <= d <= end_date:
                csv_paths.append((d, os.path.join(data_dir, fn)))
    csv_paths.sort()

    frames = []
    for _, p in csv_paths:
        frames.append(pd.read_csv(p, usecols=['quote_date', 'underlying_symbol', 'close']))
    eod = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=['quote_date', 'underlying_symbol', 'close'])

    tickers_clean = normalize_ticker_series(pd.Series(tickers))
    prices = eod[eod['underlying_symbol'].isin(tickers_clean)][['quote_date', 'underlying_symbol', 'close']].reset_index(drop=True)

    # Pivot in-memory: index = date, columns = tickers, values = close
    prices['quote_date'] = pd.to_datetime(prices['quote_date'])
    prices_pivot = prices.pivot_table(index='quote_date', columns='underlying_symbol', values='close', aggfunc='last').sort_index()
    # Drop tickers with no data across the window
    prices_pivot = prices_pivot.dropna(axis=1, how='all')

    # Build market caps DataFrame: prices_pivot (Date x Ticker) * shares per ticker
    shares_map = build_shares_map(tickers, shares)

    market_caps = prices_pivot.multiply(shares_map, axis=1)
    # Drop any tickers with no market cap data
    market_caps = market_caps.dropna(axis=1, how='all')
    # Add total market cap per day
    market_caps['Total Market Cap'] = market_caps.sum(axis=1, skipna=True)

    # Cleanup: remove all extracted UnderlyingEOD_*.csv files
    for fn in os.listdir(data_dir):
        if fn.startswith('UnderlyingEOD_') and fn.endswith('.csv'):
            try:
                os.remove(os.path.join(data_dir, fn))
            except Exception as _:
                pass

# Optional: persist market caps to Excel (comment out if not needed)
market_caps.to_excel(os.path.join(data_dir, 'market_caps.xlsx'), index=True)

# Base index setup
base_index_value = 120.8389  # index level on base date
base_date = pd.Timestamp('2025-06-29')

# Use the first available trading day on/after base_date
base_ts = market_caps[market_caps.index >= base_date].index[0]

# Compute divisor from base date's total market cap
divisor = market_caps.loc[base_ts, 'Total Market Cap'] / base_index_value

# Compute index values over time
market_caps['Index Value'] = market_caps['Total Market Cap'] / divisor
market_caps.to_excel(os.path.join(data_dir, 'market_caps.xlsx'), index=True)

# Sector weights for the most recent date
sector_map = pd.DataFrame({
    'ticker': normalize_ticker_series(pd.Series(tickers)),
    'sector': pd.Series(sector).astype(str).str.strip()
}).dropna().drop_duplicates(subset='ticker').set_index('ticker')['sector']

latest_ts = market_caps.index.max()
latest_caps = market_caps.loc[latest_ts].drop(labels=[c for c in ['Total Market Cap', 'Index Value'] if c in market_caps.columns])
latest_caps = latest_caps.dropna()

_latest_df = latest_caps.rename_axis('ticker').reset_index(name='market_cap')
_latest_df['sector'] = _latest_df['ticker'].map(sector_map)
_latest_df = _latest_df.dropna(subset=['sector'])

sector_weights = (
    _latest_df.groupby('sector', as_index=False)['market_cap']
    .sum()
    .rename(columns={'market_cap': 'sector_mkt_cap'})
)
total_latest_mkt_cap = sector_weights['sector_mkt_cap'].sum()
sector_weights['Market Weight'] = (sector_weights['sector_mkt_cap'] / total_latest_mkt_cap) * 100.0
sector_weights = sector_weights.sort_values('Market Weight', ascending=False).reset_index(drop=True)
sector_weights = sector_weights[['sector', 'Market Weight']].rename(columns={'sector': 'Sector'})

# Save sector weights to Excel
sector_weights.to_csv(os.path.join(data_dir, 'sector_weights.csv'), index=False)

# Write the full index value time series to CSV (Date, Index Value)
_index_series = market_caps['Index Value']
_index_df = pd.DataFrame({
    'Date': _index_series.index.date,
    'Index Value': _index_series.values
})
_index_df.to_csv(os.path.join(data_dir, 'index_value.csv'), index=False)