import pandas as pd
import yfinance as yf

def extract_data(start_date, end_date):
    tickers_ibovespa = pd.read_parquet("data/tickers_ibovespa.parquet")
    tickers = tickers_ibovespa["sigla"].tolist()
    tickers = [t+".SA" for t in tickers]
    tickers = tickers[:5]

    download_prices(start_date, end_date, tickers)
    download_dividends(tickers)
    download_splits(tickers)

    print("Done!!!")


def download_prices(start_date, end_date, tickers):
    y_tickers = yf.Tickers(tickers)
    df = y_tickers.download(start=start_date, end=end_date, group_by="tickers", progress=True)

    df = df.stack(level=0).reset_index()

    df["dt_date"] = df["Date"].astype(str)

    df.to_parquet(
        path="/home/fernando/fernando/projects/data/financial_data_pipeline/features/prices/",
        index=False,
        partition_cols=["dt_date"],
        existing_data_behavior="delete_matching"
    )

    print("Download prices with success")


def download_dividends(tickers):
    divs = []
    for t in tickers:
        ticker = yf.Ticker(t)
        div = ticker.dividends
        div = div.reset_index()
        div["Ticker"] = t
        divs.append(div)

    divs = pd.concat(divs)
    divs = divs.reset_index(drop=True)

    divs.to_parquet(
        path="/home/fernando/fernando/projects/data/financial_data_pipeline/features/dividends/",
        index=False,
        partition_cols=["Ticker"],
        existing_data_behavior="delete_matching"
    )

    print("Download dividends with success")


def download_splits(tickers):
    splits = []
    for t in tickers:
        ticker = yf.Ticker(t)
        split = ticker.splits
        split = split.reset_index()
        split["Ticker"] = t
        splits.append(split)

    splits = pd.concat(splits)
    splits = splits.reset_index(drop=True)

    splits.to_parquet(
        path="/home/fernando/fernando/projects/data/financial_data_pipeline/features/splits/",
        index=False,
        partition_cols=["Ticker"],
        existing_data_behavior="delete_matching"
    )

    print("Download splits with success")


if __name__ == "__main__":
    extract_data("2026-04-01", "2026-04-30")