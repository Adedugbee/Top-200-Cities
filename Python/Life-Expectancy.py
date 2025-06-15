#install necessary packages if not already installed
pip install pandas numpy beautifulsoup4

#import packages
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

url = "https://worldpopulationreview.com/country-rankings/life-expectancy-by-country"

# Send HTTP request and parse the page
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

# Locate the table rows
rows = soup.select("table tbody tr")[0:]  # Select all rows

# Extract country, MF_Life_Expectancy, F_Life_Expectancy, M_Life_Expectancy
data = []
for row in rows:
    cols = row.find_all("td")
    if len(cols) >= 4:
        country = cols[1].text.strip()
        MF_Life_Expectancy = cols[2].text.strip()
        F_Life_Expectancy = cols[3].text.strip()
        M_Life_Expectancy = cols[4].text.strip()
        data.append([country, MF_Life_Expectancy, F_Life_Expectancy, M_Life_Expectancy])

        # Create DataFrame
df = pd.DataFrame(data, columns=["Country", "Average Life_Expectancy, UN 2024 (Age)", 
                                 "Female Life_Expectancy, UN 2024 (Age)", "Male Life_Expectancy, UN 2024 (Age)"])

print(df.head())

# Save to CSV
#df.to_csv("UN_Life_Expectancy.csv", index=False)