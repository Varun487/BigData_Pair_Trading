import os
from pyspark import SparkConf
from pyspark.sql import *

spark = SparkSession.builder \
        .master("local[2]") \
        .appName("CropDataInRange") \
        .config(conf=SparkConf()) \
        .getOrCreate()

START_DATE         = '2017-01-01'
END_DATE           = '2020-01-01'

companies_folders = os.listdir("../Storage/Companies_drop_rows/")

print("----------  CROP DATA IN RANGE FROM 2017 - 2019 (BOTH INCLUDED)  ----------")

companies_drop_rows = os.listdir("../Storage/Companies_drop_rows")

print("Number of companies csv files:", len(companies_drop_rows))

count = 0
companies_csvs_num = len(companies_drop_rows)

if not os.path.exists("../Storage/Companies_in_range"):

    # Making a folder with cleaned csvs
    os.mkdir("../Storage/Companies_in_range/")

    # Dropping rows of all csvs with NaN values
    for company_csv in companies_drop_rows:
        df = spark.read.option("header",True).csv(f"../Storage/Companies_drop_rows/{company_csv}")
        count += 1
        print(f"{count} of {companies_csvs_num} - {company_csv} has {df.count()} rows")
        if df.count() > 1000:

            # eliminate all data before START_DATE
            df = df.filter(df.Date >= START_DATE)

            # eliminate all data after END_DATE
            df = df.filter(df.Date <= END_DATE)

            df.toPandas().to_csv(f"../Storage/Companies_in_range1/{company_csv}", index=False)

            df.show()

    # For debuging purposes only
    # os.rmdir("../Storage/Companies_in_range/")

companies_in_range = os.listdir("../Storage/Companies_in_range")

print("After processing ...")

print("Number of companies csv files cleaned:", len(companies_in_range))
