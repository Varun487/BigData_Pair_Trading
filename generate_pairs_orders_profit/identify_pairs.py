import re
import os
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint
from pyspark import SparkContext, SparkConf
from pyspark.sql import *

START_DATE         = '2017-01-01'
END_DATE           = '2020-01-01'
EVAL_PERIOD        = 90 # days
CORR_THRESHOLD     = 0.85
COINT_THRESHOLD    = 0.05
SPREAD_RANGE       = 500.0
SPREAD_RATIO_HIGH  = 5
SPREAD_RATIO_LOW   = 0.2

SECURITY_ID = 2
SECURITY_NAME = 3
STATUS = 4
INDUSTRY = 8
INSTRUMENT = 9

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def is_active_equity(equity: str):

    '''Ensure the equities chosen are'''

    cols = COMMA_DELIMITER.split(equity)
    return cols[STATUS].strip() == 'Active' and cols[INDUSTRY].strip() != '' and cols[INSTRUMENT].strip() == 'Equity'

def symbols_by_sector(equity: str):
    cols = COMMA_DELIMITER.split(equity)
    return '{},{},{}'.format(cols[SECURITY_ID].strip(),cols[SECURITY_NAME].strip(),cols[INDUSTRY].strip())

def by_sector(sector_ticker: str, sector: str):
    cols = COMMA_DELIMITER.split(sector_ticker)
    return cols[2] == sector

def check_if_NSE_data_exists(sector_ticker: str):
    cols = COMMA_DELIMITER.split(sector_ticker)
    symbol = cols[0]
    if not os.path.isfile('../Storage/Companies_in_range/' + symbol + 'NSE.csv'):
        return False
    return True

def zscore(data):
    return (data - data.mean())/np.std(data)

# SECTORS = ['2/3 Wheelers', 'Commercial Vehicles']
SECTORS = ['2/3 Wheelers', 'Commercial Vehicles']

if __name__ == "__main__":

    conf = SparkConf().setAppName("allpairs").setMaster("local[2]")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    equities = sc.textFile("../Storage/Equity.csv")
    sector_tickers = equities.filter(is_active_equity) \
                             .map(symbols_by_sector) \
                             .coalesce(1)

    for sector in SECTORS:
        st = sector_tickers.filter(lambda ticker: by_sector(ticker, sector)) \
                           .filter(check_if_NSE_data_exists)

        companies = st.map(lambda line: COMMA_DELIMITER.split(line)[0]).collect()

        for i in range(len(companies)):
            symbol1 = companies[i]

            for j in range(i+1, len(companies)):
                symbol2 = companies[j]

                symbol1_df = sqlContext.read.csv('../Storage/Companies_in_range/' + symbol1 + 'NSE.csv', inferSchema=True, header=True)

                symbol2_df = sqlContext.read.csv('../Storage/Companies_in_range/' + symbol2 + 'NSE.csv', inferSchema=True, header=True)

                # eliminate all data before START_DATE
                symbol1_df = symbol1_df.filter(symbol1_df.Date >= START_DATE)
                symbol2_df = symbol2_df.filter(symbol2_df.Date >= START_DATE)

                # eliminate all data after END_DATE
                symbol1_df = symbol1_df.filter(symbol1_df.Date <= END_DATE)
                symbol2_df = symbol2_df.filter(symbol2_df.Date <= END_DATE)

                # drop columns 'High', 'Low', 'Open', 'Adj Close', 'Volume'
                symbol1_df = symbol1_df.drop('High', 'Low', 'Open', 'Adj Close', 'Volume')
                symbol2_df = symbol2_df.drop('High', 'Low', 'Open', 'Adj Close', 'Volume')

                # rename 'Close' column
                symbol1_df = symbol1_df.withColumnRenamed('Close', symbol1 + '_Close')
                symbol2_df = symbol2_df.withColumnRenamed('Close', symbol2 + '_Close')

                symbol1_df = symbol1_df.sort('Date')
                symbol2_df = symbol2_df.sort('Date')

                symbol1_df = symbol1_df.toPandas()
                symbol2_df = symbol2_df.toPandas()

                # set_index to Date
                symbol1_df = symbol1_df.set_index(keys=['Date'])
                symbol2_df = symbol2_df.set_index(keys=['Date'])

                if len(symbol1_df) != len(symbol2_df):
                    print(f'*** IGNORING PAIR {symbol1} - {symbol2}, Num data points do not match: {len(symbol1_df)}, {len(symbol2_df)}\n')
                    continue

                print(symbol1_df)
                print(symbol2_df)

                # join both symbols into a single pair_df

                master_pair_df = symbol1_df.join(symbol2_df)
                spread = master_pair_df[symbol1 + '_Close'].mean() / master_pair_df[symbol2 + '_Close'].mean()

                if spread >= SPREAD_RATIO_HIGH or spread <= SPREAD_RATIO_LOW:
                    continue

                # reset_index
                master_pair_df = master_pair_df.reset_index()

                del symbol1_df
                del symbol2_df

                start_dt = START_DATE
                eval_dt = str(pd.to_datetime(start_dt) + pd.DateOffset(days=90)).split()[0]
                count = 0

                while eval_dt < END_DATE:
                    pair_df = master_pair_df[master_pair_df['Date'] >= start_dt]
                    if len(pair_df) < 5:
                        break
                    pair_df = pair_df[pair_df['Date'] <= eval_dt]
                    pair_df = pair_df.dropna()
                    print(f'Evaluating: {symbol1} - {symbol2}, {start_dt} to {eval_dt}, Num data points - {len(pair_df)}\n')
                    corr_df = pair_df.corr()
                    score, pvalue, _ = coint(pair_df[symbol1 + '_Close'], pair_df[symbol2 + '_Close'])
                    corr_value = corr_df.loc[symbol1 + '_Close', symbol2 + '_Close']
                    if (corr_value > CORR_THRESHOLD) & (pvalue < COINT_THRESHOLD):
                        pair_df["Spread"] = pair_df[symbol1 + "_Close"] - pair_df[symbol2 + "_Close"]
                        spread_mean = abs(pair_df['Spread'].mean())
                        if spread_mean < SPREAD_RANGE:
                            print(f'MATCH FOUND: CORR - {corr_value}, COINT - {pvalue}\n')
                            pair_df["Spread"] = pair_df[symbol1 + "_Close"] - pair_df[symbol2 + "_Close"]
                            pair_df["zscore"] = zscore(pair_df["Spread"])
                            pair_df.to_csv('../Storage/pairs_data/' + symbol1 + '-' + symbol2 + '-' + str(count) + '.csv', index=False)
                            count += 1
                        else:
                            print(f'*** IGNORING PAIR {symbol1} - {symbol2}, Spread TOO WIDE {spread_mean}\n')
                    else:
                        print('NO MATCH\n')

                    del pair_df
                    del corr_df

                    start_dt = str(pd.to_datetime(start_dt) + pd.DateOffset(days=15)).split()[0]
                    eval_dt = str(pd.to_datetime(start_dt) + pd.DateOffset(days=90)).split()[0]
