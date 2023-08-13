### Import Libraries ###
import gspread
import urllib.request
import bs4 as bs
import pandas as pd
import threading as td
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import json
from google.oauth2 import service_account
import os
from datetime import datetime

from secrets2.ss_key import sskey
#######################################################################################################################################################################################

###  Authorisation ###
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS = os.path.join(ROOT_DIR, r"secrets2/creds.json")

# Load Google API Credentials
with open(CREDS) as source:
    info = json.load(source)
credentials = service_account.Credentials.from_service_account_info(
    info
)  # Check GDrive Creds

# Open Google Sheet and set pages as variables
gc = gspread.service_account(filename=CREDS)  # Check GSheets Creds
sh = gc.open_by_key(sskey)  # Get SpreadSheet
predictions = sh.get_worksheet(0)


# Constants

# Name conversions
NAME_CONVERSION = {
    "VER": "Verstappen",
    "PER": "Perez",
    "LEC": "Leclerc",
    "SAI": "Sainz",
    "ALO": "Alonso",
    "HAM": "Hamilton",
    "RUS": "Russell",
    "OCO": "Ocon",
    "STR": "Stroll",
    "NOR": "Norris",
    "GAS": "Gasly",
    "HUL": "Hulkenburg",
    "ZHO": "Zhou",
    "MAG": "Magnussen",
    "BOT": "Bottas",
    "ALB": "Albon",
    "PIA": "Piastri",
    "TSU": "Tsunoda",
    "DEV": "De Vries",
    "SAR": "Sargeant",
    "RIC": "Ricciardo",
    "Red Bull Racing Honda RBPT": "Red Bull",
    "Aston Martin Aramco Mercedes": "Aston Martin",
    "Mercedes": "Mercedes",
    "Ferrari": "Ferrari",
    "Alfa Romeo Ferrari": "Alfa Romeo",
    "Alpine Renault": "Alpine",
    "Williams Mercedes": "Williams",
    "AlphaTauri Honda RBPT": "Alpha Tauri",
    "Haas Ferrari": "Haas",
    "McLaren Mercedes": "Mclaren"
}

def get_race_urls(year):
    race_urls = []
    source = urllib.request.urlopen(f"https://www.formula1.com/en/results.html/{year}/"
                                    f"races.html").read()
    soup = bs.BeautifulSoup(source,'lxml')
    
    for url in soup.find_all('a'):
        if str(year) in str(url.get('href')) and 'race-result' in str(url.get('href')) and url.get('href') not in race_urls:
            race_urls.append(url.get('href'))
    return race_urls


def update_driver_standings():
    print("updating driver standings...")

    # webscrape
    source = urllib.request.urlopen(
        "https://www.formula1.com/en/results.html/2023/drivers.html"
    ).read()
    soup = bs.BeautifulSoup(source, "lxml")
    table = soup.find_all("table")[0]

    # pull into df
    df = pd.read_html(str(table), flavor="bs4", header=[0])[0]
    df.drop(["Unnamed: 0", "Unnamed: 6", "Nationality", "Car", "Pos"], axis=1, inplace=True)

    # set onto gs
    set_with_dataframe(predictions, df, row=2, col=14)
    print("driver standings updated\n")


def update_team_standings():
    print("updating team standings...")

    # webscrape
    source = urllib.request.urlopen(
        "https://www.formula1.com/en/results.html/2023/team.html"
    ).read()
    soup = bs.BeautifulSoup(source, "lxml")
    table = soup.find_all("table")[0]

    # pull into df
    df = pd.read_html(str(table), flavor="bs4", header=[0])[0]
    df.drop(["Unnamed: 0", "Unnamed: 4", "Pos"], axis=1, inplace=True)

    # set onto gs
    set_with_dataframe(predictions, df, row=24, col=14)
    print("team standings updated\n")


def update_prediction_scores():
    print("updating prediction scores...")

    # grab current standings from sheet into dfs
    driver_standings = get_as_dataframe(
        predictions, header=1, usecols=[13, 14], nrows=20
    )
    team_standings = get_as_dataframe(
        predictions, header=23, usecols=[13, 14], nrows=10
    )

    # get current standings
    driver_positions = {
        NAME_CONVERSION.get((str(row["Driver"]))[-3:]): index
        for index, row in driver_standings.iterrows()
    }
    team_positions = {
        NAME_CONVERSION.get(str(row["Team"])): index
        for index, row in team_standings.iterrows()
    }

    # get predictions
    scores = get_as_dataframe(
        predictions, header=1, usecols=[1, 2, 3, 4, 5, 6], nrows=30
    )

    # update scores logic
    for index, row in scores.iterrows():
        tim_pred = row["Tim"]
        frey_pred = row["Freya"]
        tom_pred = row["Tom"]

        # update driver prediction scores first
        if index < 20:
            scores.iat[index, 1] = abs(index - driver_positions[tim_pred])
            scores.iat[index, 3] = abs(index - driver_positions[frey_pred])
            scores.iat[index, 5] = abs(index - driver_positions[tom_pred])

        # team standing scores require correction to indexes
        if index > 19:
            team_index = index - 20
            scores.iat[index, 1] = abs(team_index - team_positions[tim_pred])
            scores.iat[index, 3] = abs(team_index - team_positions[frey_pred])
            scores.iat[index, 5] = abs(team_index - team_positions[tom_pred])

    # update gs
    print(scores)
    set_with_dataframe(predictions, scores, row=2, col=2)
    print("prediction scores updated\n")
    

def update_wildcards():
    print("Updating WildCards")
    
    wildcard_scores = get_as_dataframe(
            predictions, header=32, usecols=[0, 1, 2, 3, 4, 5, 6], nrows=5
        )
    
    def update_wildcard_row(row_index: int, updated_scores_df):
        tim_pred = wildcard_scores.iat[row_index, 1]
        frey_pred = wildcard_scores.iat[row_index, 3]
        tom_pred = wildcard_scores.iat[row_index, 5]
       
        wildcard_scores.iat[row_index, 2] = updated_scores_df["Rank"].get(tim_pred)
        wildcard_scores.iat[row_index, 4] = updated_scores_df["Rank"].get(frey_pred)
        wildcard_scores.iat[row_index, 6] = updated_scores_df["Rank"].get(tom_pred)
            
    
    def update_fastest_laps():
        
        print("Updating Fastest laps...")
       
        # Scrape names
        source = urllib.request.urlopen(
            "https://www.formula1.com/en/results.html/2023/fastest-laps.html"
        ).read()
        soup = bs.BeautifulSoup(source, "lxml")
        table = soup.find_all("table")[0]
        
        # pull into df
        df = pd.read_html(str(table), flavor="bs4", header=[0])[0]
        df.drop(["Unnamed: 0", "Unnamed: 5", "Time", "Car"], axis=1, inplace=True)
        
        # Build FL count table
        driver_fl_count = {driver: count for driver, count in zip(list(NAME_CONVERSION.values())[:21], ([0]*21))}
        
        # Update FL count
        for name in df["Driver"]:
            driver_fl_count[NAME_CONVERSION.get(str(name[-3:]))] +=1# type: ignore

            
        # Insert FL count into table and add order by count, then add position
        df = pd.DataFrame.from_dict(driver_fl_count, orient="index", columns=["FL Count"])
        df.sort_values(by="FL Count", ascending=False, inplace=True)
        df["Rank"] = df["FL Count"].rank(method="dense", ascending=False) - 1 ##### TODO Method to be discussed
        
        set_with_dataframe(predictions, df, row=43, col=5, include_column_header=True, include_index=True)
        
        print("Fastest Laps:\n", df)
        # Update scores logic
        FL_index = 3
        update_wildcard_row(FL_index, df)
        
                       
    def update_DNFs_and_podiums():
        
        print("Updating podiums and DNFs...")
        
        driver_DNF_count = {driver: count for driver, count in zip(list(NAME_CONVERSION.values())[:21], ([0]*21))}
        driver_podium_count  = {driver: count for driver, count in zip(list(NAME_CONVERSION.values())[:21], ([0]*21))}
        
        def update_podium_and_dnf_counters(link, driver_DNF_count, driver_podium_count):
                try:
                    source = urllib.request.urlopen(
                        "https://www.formula1.com" + link
                    ).read()
                    soup = bs.BeautifulSoup(source, "lxml")
                    table = soup.find_all("table")[0]

                    # pull into df
                    df = pd.read_html(str(table), flavor="bs4", header=[0])[0]
                    df.drop(["Unnamed: 0", "Unnamed: 8", "No", "Car", "PTS", "Laps"], axis=1, inplace=True)

                    DNF_df = df.drop(df[df['Time/Retired'] != "DNF"].index)
                    podium_df = df.iloc[:3]


                    for name in DNF_df["Driver"]:
                        driver_DNF_count[NAME_CONVERSION.get(str(name[-3:]))] +=1 # type: ignore

                    for name in podium_df["Driver"]:
                        driver_podium_count[NAME_CONVERSION.get(str(name[-3:]))] +=1 # type: ignore

                except IndexError: pass
                
                
        threaded_links = [
            td.Thread(target=update_podium_and_dnf_counters, args=(link, driver_DNF_count, driver_podium_count))
            for link in get_race_urls(2023)
        ]
        
        for thread in threaded_links:
            thread.start()

        for thread in threaded_links:
            thread.join()
            
        
        DNF_table = pd.DataFrame.from_dict(driver_DNF_count, orient="index", columns=["DNF Count"])
        DNF_table.sort_values(by="DNF Count", ascending=False, inplace=True)
        DNF_table["Rank"] = DNF_table["DNF Count"].rank(method="dense", ascending=False) - 1
        
        set_with_dataframe(predictions, DNF_table, row=43, col=13, include_column_header=True, include_index=True)
                
        podium_table = pd.DataFrame.from_dict(driver_podium_count, orient="index", columns=["Podium Count"])
        podium_table.sort_values(by="Podium Count", ascending=False, inplace=True)
        podium_table["Rank"] = podium_table["Podium Count"].rank(method="dense", ascending=False) - 1
        
        set_with_dataframe(predictions, podium_table, row=43, col=9, include_column_header=True, include_index=True)
        
        print("DNFs:\n", DNF_table, "Podiums:\n", podium_table)

        
        # Update scores logic
        DNF_index = 1
        update_wildcard_row(DNF_index, DNF_table)
        
        podium_index = 4
        update_wildcard_row(podium_index, podium_table)


    def update_pole_positions():
        
        print("Updating poles...")

        driver_pole_count  = {driver: count for driver, count in zip(list(NAME_CONVERSION.values())[:21], ([0]*21))}
        
        
        def update_pole_counter(link, driver_pole_count):
                
           link = link[:-16]
           link = link + "qualifying.html"
           try:
               source = urllib.request.urlopen(
                   "https://www.formula1.com" + link
               ).read()
               soup = bs.BeautifulSoup(source, "lxml")
               table = soup.find_all("table")[0]
               
               # pull into df
               df = pd.read_html(str(table), flavor="bs4", header=[0])[0]
               df.drop(["Unnamed: 0", "Unnamed: 9", "No", "Car", "Laps"], axis=1, inplace=True)
               pole_df = df.iloc[:1]

               for name in pole_df["Driver"]:
                   driver_pole_count[NAME_CONVERSION.get(str(name[-3:]))] +=1 # type: ignore
           except IndexError: pass
        
        
        threaded_links = [
            td.Thread(target=update_pole_counter, args=(link, driver_pole_count))
            for link in get_race_urls(2023)
        ]
        
        for thread in threaded_links:
            thread.start()

        for thread in threaded_links:
            thread.join()

            
        # Insert Pole count into table and add order by count, then add position
        df = pd.DataFrame.from_dict(driver_pole_count, orient="index", columns=["Pole Count"])
        df.sort_values(by="Pole Count", ascending=False, inplace=True)
        df["Rank"] = df["Pole Count"].rank(method="dense", ascending=False) - 1 ##### TODO Method to be discussed
        
        set_with_dataframe(predictions, df, row=43, col=1, include_column_header=True, include_index=True)
        
        print("poles:\n", df)
        
        # Update scores logic
        pole_index = 2
        update_wildcard_row(pole_index, df)
        
    
    # Call scores df updater functions
    wildcards = [
        td.Thread(target=update_fastest_laps),
        td.Thread(target=update_pole_positions),
        td.Thread(target=update_DNFs_and_podiums)
    ]

    for w in wildcards:
        w.start()
        
    for w in wildcards:
        w.join()
    

    # Update Gsheet
    set_with_dataframe(predictions, wildcard_scores, row=33, col=1)
    print(wildcard_scores)
    print("Wildcards updated")
    current_date = datetime.now()
    current_date_string = current_date.strftime("%Y-%m-%d")
    predictions.update('B1', current_date_string)
    

if __name__ == "__main__":
    updaters = [
        td.Thread(target=update_driver_standings),
        td.Thread(target=update_team_standings),
        td.Thread(target=update_wildcards)
    ]

    for u in updaters:
        u.start()
        
    for u in updaters:
        u.join()
    
    update_prediction_scores()
    current_date = datetime.now()
    current_date_string = current_date.strftime("%Y-%m-%d")
    predictions.update('B1', current_date_string)
    
    


# TODO:
# Update overtakes


# Weekly plot of prediction scores (competition evolution)
# separate .py file
# want to process and store scores through each race week.
# need to use the predictions spreadsheet then add a column to the df each iteration
# each iteration would be calculating the scores at the end of each week (tough part)
# Could use a race week counter:
#   - each week counter is a stopping point to calculate scores per iteration.
#   - stops, drivers, constructors, and wildcards calculations.



# Future WILDCARD IDEAS
#   - Slowest FL
#   - Most Q2 Eliminations
#   - Most time spent in pits
#   - Most P11 starts
#######################################################################################################################################################################################
