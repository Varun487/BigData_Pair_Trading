# Big Data Project Pair Trading Analysis

## The repositorty contains
1. Project Code
2. Project Final Report

## Project members
###### All members are from Semester 5 Section E Pes University EC Campus
1. Varun Seshu - PES2201800074
2. Hritik Shanbhag - PES2201800082
3. Shashwath S Kumar - PES2201800623

## To replicate this repo on your computer 

1. Download the storage folder shared to you in google drive.
2. The Storage2 folder is a representative sample of the complete data present.
3. Create a BD Project folder
3. Copy the storage folder as is to the newly created project folder
3. cd to that folder and run `git init`, this initializes the git repository
4. run `git remote add origin "https://github.com/Varun487/BigData_Pair_Trading"`
5. run `git pull origin master`
6. Create a virtual environment called `venv` with `python3 -m venv venv`
7. Activate the virtual environment with `source venv/bin/activate`
8. In case the virtual environment isn't working, have a look at the documentation here https://docs.python.org/3/library/venv.html
9. run `pip3 install -r requirements.txt`

## Collection

#### STATUS - Completed

###### Describes the data collected and the scripts used to collect it.

Contains 2 scripts
1. `list_of_nse_companies.py` - To get names and tickers all stocks floating in the stock market as of 3rd September 2020
2. `stock_candle_data_and_volume.py` - To get historical candle stick data of the stock tickers collected from years 2000 - 2020

## Preprocessing

###### Clean data + Find stock pairs of the stock market to trade and perform correlation and co-integration testing.


1. Extract csvs - The csvs present within the folders are brought out.

###### Pyspark is used to handle missing data and cropping the dataset
2. Handling Missing Data - Dropping the rows of the datasets which are missing data we can afford to do this due to a large amount of data and interpolation may lead to inaccurate data due to the volatility of some stocks.
3. Deleting datasets which have < 3 years worth of data.
4. Deleting the parts of the datasets with > 3 years of data (taking only data in range of years 2017-2019) - as a correlation needs to be within a fixed time period and we cannot let a strong correlation in the past affect the predictions made by the model when there is no significant correlation currently.


## Generate Orders and Calculate Profits

###### Identify pairs + Generate orders + Calculate profits using Spark

1. Identify pairs - Pairs are identified based on their sectors and cointegration, correlation thresholds.
2. Generate orders - Orders (long, short, flat, get out of position) are assigned based on price and zscore.
3. Calculate profits - Capital and Risk are decided for opening and closing a trade. Orders are placed on the shares. Profits are then calculated based on the orders.

### Graphs are plotted for visualization of spread, orders, profits.
