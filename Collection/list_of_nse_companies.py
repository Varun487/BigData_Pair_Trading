from selenium import webdriver
import pandas as pd

driver = webdriver.Chrome()
driver.get("https://indiancompanies.in/listed-companies-in-nse-with-symbol/")
rows = driver.find_elements_by_tag_name("tr")
symbols = []

for row in rows:
    symbols.append(row.text.split()[-1])

for symbol in symbols:
    print(symbol)

df = pd.DataFrame(data={'SYMBOLS': symbols[1:]})
df.to_csv("../Storage1/NSE_tickers.csv", index=False)

driver.quit()
