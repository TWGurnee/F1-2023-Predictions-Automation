#!/usr/bin/env python3
"""
Scheduler for F1 Predictions Updater

This script reads "race_schedule.json" to determine the next race that has not yet
been completed
.
It then computes an update window. 
If the current UTC time falls within that window;
    it will check every few mins whether the race results have been posted.
If so, it runs the updater, making sure only to move on if the raceweek has been updated on the sheet.
"""

import time
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from config import *
from gSheet_utils import get_last_calculated_raceweek, get_race, get_next_race, get_next_race_to_calculate
from scraping_utils import scrape_table_from_url


## Schedule builders ##
def run_update() -> None:
    """Run the F1 Predictions Updater script."""
    logger.info("Running F1 Predictions Updater...")
    try:
        subprocess.run(["python3", "googleSheet_resources.py"], check=True)
        logger.info("F1 Predictions Updater Run\nChecking if succesful.")
    except subprocess.CalledProcessError as e:
        logger.error("Error running F1 Predictions Updater: %s", e)


def calculate_window(next_race: dict) -> Optional[Tuple[datetime, datetime]]:
    """
    Given the next race, compute the update window:
      - The window starts at race time + OFFSET_HOURS.
      - The window ends after UPDATE_WINDOW_HOURS.
    """
    try:
        race_time = datetime.fromisoformat(next_race["time"])
        if race_time.tzinfo is None:
            race_time = race_time.replace(tzinfo=timezone.utc)
        start_window = race_time + timedelta(hours=OFFSET_HOURS)
        end_window = start_window + timedelta(hours=UPDATE_WINDOW_HOURS)
        return start_window, end_window
    except Exception as e:
        logger.error("Error calculating window for race %s: %s", next_race.get("name"), e)
        return None


def should_run_update(next_race: dict) -> bool:
    """Return True if the current time is within the update window for the next race."""
    window = calculate_window(next_race)
    if not window:
        return False
    start_window, end_window = window
    now = datetime.now(timezone.utc)
    if start_window <= now < end_window:
        logger.info("Current time %s is between %s and %s for race %s.",
                    now, start_window, end_window, next_race.get("name"))
        return True
    return False


## Scheduler Scraping/check functions ## 

def result_is_ready(raceweek: int) -> bool:
    """ Checks a result link as to whether it is ready to be scraped"""
    race_details = get_race(raceweek=raceweek)
    check = scrape_table_from_url(race_details["results"])
    if check.empty: 
        logger.info(f"{race_details['name']} not ready to be scraped")
        logger.debug("Check results table:")        
        logger.debug(check)
        return False
    else: 
        return True


    

### Main ###

def main() -> None:
    logger.info("Scheduler started.")
    while True:
        current_week = get_last_calculated_raceweek()
        next_race = get_next_race_to_calculate(current_week)
        
        if not next_race:
            logger.info("No upcoming race found. Sleeping for 1 hour.")
            time.sleep(3600)
            continue

        window = calculate_window(next_race)
        if not window:
            logger.error("Could not calculate window for next race. Sleeping for 1 hour.")
            time.sleep(3600)
            continue

        start_window, end_window = window
        now = datetime.now(timezone.utc)
        # If current time is before window start, sleep until the window begins.
        if now < start_window:
            sleep_secs = (start_window - now).total_seconds()
            logger.info("Next race '%s' update window starts at %s. Sleeping for %.0f seconds.",
                        next_race.get("name"), start_window, sleep_secs)
            time.sleep(sleep_secs)
            continue

        # We're inside the window. Poll every 20 minutes until the sheet updates.
        logger.info("Within update window for raceweek %d; polling every few minutes.", next_race["raceweek"])
        while should_run_update(next_race) and get_last_calculated_raceweek() == current_week:
            if result_is_ready(current_week+1):
                run_update()
            else:
                logger.info("Result not uploaded yet; checking again shortly...")
                time.sleep(UPDATE_INTERVAL)

        logger.info("Raceweek updated (current raceweek: %d). Returning to scheduling for next race.",
                    get_last_calculated_raceweek())

        # Then recalc sleep time until next window.
        now = datetime.now(timezone.utc)
        window = calculate_window(next_race)
        if window:
            start_window, _ = window
            sleep_secs = max((start_window - now).total_seconds(), 0)
            logger.info("Sleeping for %.0f seconds until next update window.", sleep_secs)
            time.sleep(sleep_secs)
        else:
            logger.info("Sleeping for 1 hour.")
            time.sleep(3600)

if __name__ == "__main__":
    main()
