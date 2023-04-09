################################################################################
"""
https://data.humdata.org/dataset/novel-coronavirus-2019-ncov-cases
Not reporting after 2023.03.09.
"""
################################################################################

import datetime as dt
import numpy as np
import pandas as pd

from urllib.request import urlopen

################################################################################


"""
"""
def download(url, filename):
    response = urlopen(url).read().decode("utf-8")
    with open(filename, "w") as f:
        f.write(response)
        
"""
"""
def rename_country(csv, old_name, new_name):
    index = csv.index[csv["Country/Region"] == old_name]
    csv.loc[index, "Country/Region"] = new_name        
 
"""
"""
def update_data(url, filename_in, filename_out):
    
    print("Downloading data ...")
    download(url, filename_in)

    csv = pd.read_csv(filename_in)
    csv.drop(["Lat", "Long"], axis=1, inplace=True)
    
    rename_country(csv, "Faroe Islands", "Faeroe Islands")
    rename_country(csv, "Korea, North",  "North Korea")
    
    aggregated = csv.groupby("Country/Region").sum(numeric_only=True)
    aggregated.drop(["Diamond Princess"], axis=0, inplace=True)
    aggregated.drop(["MS Zaandam"], axis=0, inplace=True)
    aggregated.drop(["Summer Olympics 2020"], axis=0, inplace=True)
    aggregated.drop(["Winter Olympics 2022"], axis=0, inplace=True)
    aggregated.drop(["Antarctica"], axis=0, inplace=True)
    
    daily_new = aggregated.diff(axis=1).fillna(aggregated)
    rolling_average = daily_new.rolling(axis=1, window=7, min_periods=1).mean()
    
    population = pd.read_csv("../datasets/population_by_country_2020.csv")
    population.drop(["#", "Yearly Change", "Net Change", "Density (P/Km2)", "Land Area (Km2)", "Migrants (net)", "Fert. Rate", "Med. Age", "Urban Pop %", "World Share"], axis=1, inplace=True)
    population.set_index("Country (or dependency)",inplace=True)
    
    cut_off = 100000.0

    countries = list(rolling_average.index)
    for c in countries:
        if population.loc[c][0] >= cut_off:
            factor = cut_off / population.loc[c][0]
            rolling_average.loc[c] = rolling_average.loc[c].mul(factor).round(2)
        else:
            rolling_average.drop([c], axis=0, inplace=True)
            print("Dropping " + c, population.loc[c][0])
            
    columns = rolling_average.columns.values
    new_columns = np.array([dt.datetime.strptime(date, "%m/%d/%y").date().strftime("%d.%m.%Y") for date in columns])
    rolling_average.columns = new_columns
    
    rolling_average.insert(0, "region", "")
    rolling_average.insert(1, "Image URL", "")
    
    flags = pd.read_csv("../datasets/flags.csv", encoding="utf-8")
    flags.set_index("Country", inplace=True)

    for c in countries:
        rolling_average.at[c, "Image URL"] = flags.at[c, "Image URL"] 
    
    rolling_average.rename(index={"Korea, South":"South Korea"}, inplace=True)
    rolling_average.rename(index={"Taiwan*":"Taiwan"}, inplace=True)
    
    rolling_average.replace(0, "", inplace=True)
    
    print("Saving processed data ...")
    rolling_average.to_csv(filename_out)

################################################################################

print("Updating cases:")
url          = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
filename_in  = "../datasets/time_series_covid19_confirmed_global.csv"
filename_out = "../out/rolling_average_cases.csv"
update_data(url, filename_in, filename_out)
print("Done.")

print("Updating deaths:")
url          = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
filename_in  = "../datasets/time_series_covid19_deaths_global.csv"
filename_out = "../out/rolling_average_deaths.csv"
update_data(url, filename_in, filename_out)
print("Done.")