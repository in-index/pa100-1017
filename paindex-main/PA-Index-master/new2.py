import os
import datetime as dt
import pandas as pd
import yfinance as yf


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
    data_dir = os.path.join(script_dir, 'data2')
    os.makedirs(data_dir, exist_ok=True)

    start_date = pd.to_datetime("2025-06-29").date()
    end_date = dt.date.today()

    # Normalize tickers for consistent joining with shares/sector maps
    tickers_clean = normalize_ticker_series(pd.Series(tickers)).dropna()
    unique_tickers = sorted(pd.unique(tickers_clean))

    # Download historical daily close prices from yfinance
    # Use end_date + 1 day because yf `end` is exclusive
    yf_end = pd.to_datetime(end_date) + pd.Timedelta(days=1)
    try:
        raw_prices = yf.download(
            tickers=unique_tickers,
            start=start_date,
            end=yf_end,
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=True,
        )
    except Exception:
        raw_prices = pd.DataFrame()

    # Extract a Date x Ticker frame of close prices, handling both single and multi-ticker outputs
    if raw_prices.empty:
        prices_pivot = pd.DataFrame(columns=unique_tickers)
    else:
        if isinstance(raw_prices.columns, pd.MultiIndex):
            # Prefer 'Close', fallback to 'Adj Close'
            close_cols = None
            if 'Close' in raw_prices.columns.get_level_values(0):
                close_cols = raw_prices['Close']
            elif 'Adj Close' in raw_prices.columns.get_level_values(0):
                close_cols = raw_prices['Adj Close']
            else:
                # last resort: try to pick the last level assuming OHLCV ordering
                level0 = raw_prices.columns.get_level_values(0)
                candidate = next((lvl for lvl in ['Close', 'Adj Close', 'Last'] if lvl in level0), None)
                close_cols = raw_prices[candidate] if candidate else pd.DataFrame(index=raw_prices.index)

            prices_pivot = close_cols.copy()
        else:
            # Single-ticker case returns single-level columns
            col_to_use = 'Close' if 'Close' in raw_prices.columns else ('Adj Close' if 'Adj Close' in raw_prices.columns else None)
            if col_to_use is None:
                prices_pivot = pd.DataFrame(columns=unique_tickers)
            else:
                # Determine the single ticker name from requested set (yfinance preserves order)
                single_ticker = unique_tickers[0] if len(unique_tickers) == 1 else None
                if single_ticker is None:
                    prices_pivot = pd.DataFrame(columns=unique_tickers)
                else:
                    prices_pivot = raw_prices[[col_to_use]].rename(columns={col_to_use: single_ticker})

        # Normalize column labels to our normalized ticker format
        if not prices_pivot.empty:
            normalized_cols = normalize_ticker_series(pd.Series(prices_pivot.columns)).tolist()
            prices_pivot.columns = normalized_cols

            # Keep only requested tickers; drop columns that are all NaN
            prices_pivot = (
                prices_pivot
                .reindex(columns=unique_tickers)
                .dropna(axis=1, how='all')
            )

        # Ensure datetime index and sorted
        prices_pivot.index = pd.to_datetime(prices_pivot.index)
        prices_pivot = prices_pivot.sort_index()

    # Build market caps DataFrame: prices_pivot (Date x Ticker) * shares per ticker
    shares_map = build_shares_map(tickers, shares)
    market_caps = prices_pivot.multiply(shares_map, axis=1)
    market_caps = market_caps.dropna(axis=1, how='all')
    market_caps['Total Market Cap'] = market_caps.sum(axis=1, skipna=True)

# Base index setup
base_index_value = 120.8389  # index level on base date
base_date = pd.Timestamp('2025-06-29')

# Use the first available trading day on/after base_date
base_ts = market_caps[market_caps.index >= base_date].index[0]

# Compute divisor from base date's total market cap
divisor = market_caps.loc[base_ts, 'Total Market Cap'] / base_index_value

# Compute index values over time
market_caps['Index Value'] = market_caps['Total Market Cap'] / divisor

# Persist market caps (with Index Value) to Excel
market_caps.to_excel(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data2', 'market_caps.xlsx'), index=True)

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

# Save sector weights to CSV
sector_weights.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data2', 'sector_weights2.csv'), index=False)

# Write the full index value time series to CSV (Date, Index Value)
_index_series = market_caps['Index Value']
_index_df = pd.DataFrame({
    'Date': _index_series.index.date,
    'Index Value': _index_series.values
})
_index_df.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data2', 'index_value2.csv'), index=False)


