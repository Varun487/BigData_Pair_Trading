import os
from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
        .master("local[2]") \
        .appName("GenerateOrders") \
        .config(conf=SparkConf()) \
        .getOrCreate()

pairs_csvs = os.listdir("../Storage/pairs_data/")
pairs_csvs_num = len(pairs_csvs)
count = 0

POSITION = "FLAT"

def generate_orders(price):

    global POSITION

    # Get out of a LONG POSITION
    if POSITION == "LONG" and (float(price) == 0 or float(price) > 0):
        POSITION = "FLAT"
        return "GET_OUT_OF_POSITION"

    # Get out of a SHORT POSITION
    elif POSITION == "SHORT" and (float(price) == 0 or float(price) < 0):
        POSITION = "FLAT"
        return "GET_OUT_OF_POSITION"

    # Get into a long POSITION
    elif float(price) < -1.5:
        POSITION = "LONG"
        return "LONG"

    # Get into a long POSITION
    elif float(price) > 1.5:
        POSITION = "SHORT"
        return "SHORT"

    # Default if no other order is placed
    return "FLAT"

if not os.path.isdir("../Storage/pairs_orders"):
    os.mkdir("../Storage/pairs_orders")

for pair_csv in pairs_csvs:
    count += 1
    print(f"In pair {count} of {pairs_csvs_num}")

    df = spark.read.option("header", True).csv(f"../Storage/pairs_data/{pair_csv}")

    orders = udf(generate_orders, StringType())
    df = df.withColumn("Orders", orders("zscore"))

    # print(df.select('zscore').rdd.map(generate_orders).collect())
    # df.show()

    df.toPandas().to_csv(f"../Storage/pairs_orders/{pair_csv}", index=False)

    POSITION = "FLAT"
