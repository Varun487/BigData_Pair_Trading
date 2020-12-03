import os
from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("CalculateProfit") \
    .config(conf=SparkConf()) \
    .getOrCreate()

pairs_orders_csvs = os.listdir("../Storage/pairs_orders/")
pairs_orders_csvs_num = len(pairs_orders_csvs)
count = 0

if not os.path.isdir("../Storage/pairs_profits"):
    os.mkdir("../Storage/pairs_profits")

CAPITAL = 1000000
RISK = 20000


def flip_orders(order):
    if order == "LONG":
        return "SHORT"
    elif order == "SHORT":
        return "LONG"
    return order


def calculate_shares(prices):
    shares = []

    for price in prices:
        shares.append(20000 // float(price))

    return shares


def calculate_profit(prices, orders, shares):
    profits = []

    num_orders = len(orders)

    for i in range(num_orders):

        if i == num_orders - 1:
            profits.append(0)
            break

        position = orders[i]

        # print()

        # print("ORDER: ", i, close, position)

        if position == 'LONG' or position == 'SHORT':

            for j in range(i + 1, num_orders):

                if (j == num_orders - 1) or orders[j] == 'GET_OUT_OF_POSITION':

                    profit = float(prices[j]) - float(prices[i])
                    profit *= shares[i]

                    if position == 'SHORT':
                        profit *= -1

                    # print('profit: ', profit)
                    profits.append(profit)

                    break
        else:
            profits.append(0)

        # print()

    return profits


for pair_order_csv in pairs_orders_csvs:
    count += 1
    print(f"In pair {count} of {pairs_orders_csvs_num}")

    df = spark.read.option("header", True).csv(f"../Storage/pairs_orders/{pair_order_csv}")
    df = df.filter(df.Orders != "FLAT")

    flipped_orders = udf(flip_orders, StringType())
    df = df.withColumn("Flipped_Orders", flipped_orders("Orders"))

    symbol1 = df.columns[1][:-6]
    symbol2 = df.columns[2][:-6]

    orders_df = df.toPandas()

    orders_df[symbol1 + '_shares'] = calculate_shares(orders_df[symbol1 + '_Close'])
    orders_df[symbol2 + '_shares'] = calculate_shares(orders_df[symbol2 + '_Close'])

    orders_df[symbol1 + '_profits'] = calculate_profit(orders_df[symbol1 + '_Close'], orders_df['Orders'], orders_df[symbol1 + '_shares'])
    orders_df[symbol2 + '_profits'] = calculate_profit(orders_df[symbol2 + '_Close'], orders_df['Flipped_Orders'], orders_df[symbol2 + '_shares'])

    orders_df.to_csv(f"../Storage/pairs_profits/{pair_order_csv}", index=False)

    orders_spark_df = spark.createDataFrame(orders_df)
    # orders_spark_df.show()
