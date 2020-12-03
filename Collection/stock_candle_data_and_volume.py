import datetime as dt
import os
import pandas_datareader.data as web
import pandas as pd

start = dt.datetime(2000, 1, 1)
end = dt.datetime(2020, 12, 31)

df_NSE_tickers = pd.read_csv("../Storage1/NSE_tickers.csv")
df_BSE_tickers = pd.read_csv("../Storage1/BSE_tickers.csv")

df_combined_tickers = pd.concat([df_BSE_tickers, df_NSE_tickers])

count = 1

for ticker in df_combined_tickers["SYMBOLS"]:
    print()
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print()

    print(f"Ticker is {ticker}")

    try:
        df_NSE = web.DataReader(f"{ticker}.NS", "yahoo", start, end)

        print(f"Collected data for {ticker} from NSE")

        os.mkdir(f'../Storage1/Companies/{ticker}')

        print(f"Created directory {ticker}")

        df_NSE.to_csv(f'../Storage1/Companies/{ticker}/{ticker}NSE.csv')

        print(f"Created file {ticker}NSE.csv")

    except:
        print(f"NO DATA AVAILABLE FOR {ticker} FROM NSE")

    try:
        df_BSE = web.DataReader(f"{ticker}.BO", "yahoo", start, end)

        print(f"Collected data for {ticker} from BSE")

        if os.path.isdir(f'../Storage1/Companies/{ticker}'):
            print(f"{ticker} directory present")

        else:
            os.mkdir(f'../Storage1/Companies/{ticker}')
            print(f"Created directory {ticker}")

        df_BSE.to_csv(f'../Storage1/Companies/{ticker}/{ticker}BSE.csv')

        print(f"Created file {ticker}BSE.csv")

    except:
        print(f"NO DATA AVAILABLE FOR {ticker} FROM BSE")

    print()
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print()

    print(f"{count} of 5838 Companies completed")

    count += 1
