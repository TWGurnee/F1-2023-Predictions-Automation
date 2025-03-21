#!/usr/bin/env python3
"""
Google Sheets F1 Utilities

functions for getting and setting from the predictions Google Sheet.
Also contains helpers for accessing the JSON race schedule
"""

## IMPORTS ##
from typing import List, Optional
import json
from datetime import datetime

from config import *
from tables import *

## Getting & setting calculated tables on sheet ## 


def get_last_calculated_raceweek() -> int:
    """Return the current raceweek from the Google Sheet (cell B2)."""
    try:
        sh = authorize_google_sheet(CREDS, sskey)
        predictions_sheet = sh.get_worksheet(CURRENT_SHEET)
        raceweek = (predictions_sheet.acell("B2").numeric_value)
        return raceweek #type: ignore
    except Exception as e:
        logger.error("Error reading current raceweek: %s", e)
        return 0
      
        
## JSON Schedule functions ##

def load_schedule():
    """Load the race schedule from the JSON file."""
    try:
        with open(JSON_FILE, "r") as f:
            schedule = json.load(f)
        return schedule
    except Exception as e:
        logger.error("Error loading race schedule: %s", e)
        return []
    
def get_race(raceweek: int) -> dict:
    """ Returns a race by the raceweek provided"""
    schedule = load_schedule()
    return schedule[raceweek]
    

def get_next_race_to_calculate(raceweek: int) -> Optional[dict]:
    """
    Load the race schedule JSON and return the argument raceweek + 1.
    """
    races = load_schedule()
    upcoming = [race for race in races if race.get("raceweek", 0) == raceweek + 1]
    if not upcoming:
        logger.info("No upcoming race found for raceweek %d.", raceweek + 1)
        return None
    return upcoming[0]

def get_next_race() -> Optional[dict]:
    """
    Load the race schedule JSON and return the next race by checking the first returned future time.
    """
    schedule = load_schedule()

    for i in range(TOT_RACES+1):
        now = datetime.now()
        race=schedule[i]
        try:
            time = datetime.fromisoformat(race["time"])
            if now < time:
                return race
            else: pass
        except Exception as e:
            logger.warning(f"Error parsing {race['name']}'s date-time. {e}")
            continue
        else:
            logger.warning("No next race in schedule")
            return {}
        

def check_missed_races(last_raceweek: int) -> Optional[int]:
    next_race = get_next_race()
    if next_race:
        races_missed = next_race["raceweek"] - last_raceweek
        if races_missed == 1:
            return False
        if races_missed > 1:
            logger.info(f"{races_missed-1} missed races. Updating accordingly.")
            return races_missed-1

        
def get_missed_races(last_raceweek: int, races_missed: int) -> List[dict]:
    """
    Returns all ready to update races, that may have been skipped.
    """
    schedule = load_schedule()
    races_to_update = []    
    for i in range(last_raceweek, races_missed + last_raceweek):
        races_to_update.append(schedule[i])
    return races_to_update


def get_race_result_urls(year: int) -> Optional[List[str]]:
    """
    Load the race schedule JSON and return a list of the race URLs.
    """
    races = load_schedule()    
    return [race["results"] for race in races]

## 

def log_update(sheet) -> None:
    next_race = get_next_race()
    sheet.update_acell("B1", datetime.now().strftime("%Y-%m-%d"))
    if next_race:
        sheet.update_acell("B3", next_race["name"])
        sheet.update_acell("B4", next_race["time"][:10])