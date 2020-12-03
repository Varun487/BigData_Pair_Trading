import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import *

spark = SparkSession.builder \
        .master("local[2]") \
        .appName("HandleMissingData") \
        .config(conf=SparkConf()) \
        .getOrCreate()

companies_folders = os.listdir("../Storage/Companies_csvs")

print("----------  HANDLING MISSING DATA  ----------")

companies_csvs = os.listdir("../Storage/Companies_csvs")

print("Number of companies csv files:", len(companies_csvs))

if not os.path.exists("../Storage/Companies_drop_rows/"):

    # Making a folder with cleaned csvs
    os.mkdir("../Storage/Companies_drop_rows/")

    # Dropping rows of all csvs with NaN values
    for company_csv in companies_csvs:
        df = spark.read.option("header",True).csv(f"../Storage/Companies_csvs/{company_csv}")
        df = df.dropna(how='any')
        df.show()
        # df.write.option("header",True).csv(f"../Storage/{company_csv}")
        df.toPandas().to_csv(f"../Storage/Companies_drop_rows/{company_csv}", index=False)

    # For debuging purposes only
    # os.rmdir("../Storage/Companies_drop_rows/")

companies_drop_rows = os.listdir("../Storage/Companies_drop_rows")

print("After processing ...")

print("Number of companies csv files cleaned:", len(companies_drop_rows))
