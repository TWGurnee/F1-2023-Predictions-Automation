#!/usr/bin/env python3
"""
This .py file contains various functions related to scraping F1 Website, responsibilities include:

Scraping the f1 website for results links.

Producing the race schedule JSON.
"""

import urllib.request, urllib.error
import bs4 as bs
import pandas as pd
from typing import List, Optional, Union
from io import StringIO
from datetime import datetime
import re
import json

from config import *
from gSheet_utils import get_race_result_urls

## Helper fucntions ## 

def extract_time_from_spans(span_texts: List[str]) -> Optional[str]:
    """ 
    Used in scraping each race schedule webpage to get the time of the race.
    This works because the race is first on the page, and is the only one with one time (07:00)
        rather than a timeslot (07:00 - 08:00)
    """
    for text in span_texts:
        t = text.strip()
        if re.fullmatch(r"\d{2}:\d{2}", t):
            return t
    return None

def extract_date_from_spans(span_texts: List[str]) -> Optional[str]:
    """ 
    Used in scraping each race schedule webpage to get the date.
    """
    # This regex matches strings like "14 - 16 Mar" or "30 May  - 01 Jun"
    pattern = re.compile(r"^\d{1,2}(?:\s+[A-Za-z]{3})?\s*-\s*\d{1,2}\s+[A-Za-z]{3}$")
    for text in span_texts:
        t = text.strip()
        if pattern.fullmatch(t):
            return t
    return None

def qualifying_scrape_url(url: str) -> str:
    """
    Modify a race result URL to get the qualifying URL.
    (Assumes that trimming the last 11 characters and appending 'qualifying' works.)
    """
    return url[:-11] + "qualifying"

def extract_dnf_counts(df: pd.DataFrame) -> dict:
    """
    Extract DNF counts from a race results DataFrame.
    Count drivers for which the "Time/retired" column exactly equals "DNF".
    """
    counts = {}
    # Filter for rows with DNF status.
    dnf_df = df[df["Time/retired"] == "DNF"]
    for name in dnf_df["Driver"]:
        key = NAME_CONVERSION.get(str(name[-3:]))
        if key:
            counts[key] = counts.get(key, 0) + 1
    return counts

def extract_podium_counts(df: pd.DataFrame) -> dict:
    """
    Extract podium counts from a race results DataFrame.
    Count drivers finishing in positions 1, 2, or 3.
    """
    counts = {}
    podium_df = df[df["Pos"].isin(["1", "2", "3"])]
    for name in podium_df["Driver"]:
        key = NAME_CONVERSION.get(str(name[-3:]))
        if key:
            counts[key] = counts.get(key, 0) + 1
    return counts

def extract_pole_counts(df: pd.DataFrame) -> dict:
    """
    Extract pole position counts from a qualifying results DataFrame.
    Only the first row (pole position) is counted.
    """
    counts = {}
    if not df.empty:
        name = df.iloc[0]["Driver"]
        key = NAME_CONVERSION.get(str(name[-3:]))
        if key:
            counts[key] = 1
    return counts


### F1 Web Scraping Functions ###

def scrape_race_result_urls(year: int) -> List[str]:
    """Scrape and return a list of race-result URLS for the season."""
    url = f"https://www.formula1.com/en/results/{year}/races"
    link = "https://www.formula1.com"
    try:
        soup = bs.BeautifulSoup(urllib.request.urlopen(url).read(), "lxml")
        return [link+a.get("href") for a in soup.find_all("a")
                if a.get("href") and str(year) in a.get("href") and "race-result" in a.get("href")]
    except Exception as e:
        logger.error("Error scraping race URLs for %s: %s", year, e)
        return []


def scrape_table_from_url(url: str) -> pd.DataFrame:
    """Scrape the first HTML table from a URL into a DataFrame."""
    try:
        soup = bs.BeautifulSoup(urllib.request.urlopen(url).read(), "lxml")
        tables = soup.find_all("table")
        if not tables:
            logger.warning("No table found at %s", url)
            return pd.DataFrame()
        # Wrap the literal HTML string in StringIO to avoid FutureWarning.
        return pd.read_html(StringIO(str(tables[0])), flavor="bs4", header=[0])[0]
    except (urllib.error.HTTPError, Exception) as e:
        logger.error("Error scraping %s: %s", url, e)
        return pd.DataFrame()


def scrape_f1_website(year: int, 
                      site: Optional[str] = None,
                      link: Optional[str] = None,
                      drop_cols: Optional[List[str]] = None,
                      all_race_URLs: bool = False
                      ) -> Union[pd.DataFrame, List[str]]:
    """
    Delegate scraping based on parameters:
      - all_race_URLs: returns a list of race-result URLs.
      - link: scrapes the table from the full URL.
      - site: scrapes the table from the site-specific URL.
    """
    if all_race_URLs:
        return get_race_result_urls(year) # type: ignore this is type checked prior to getting here.
    if link:
        return scrape_table_from_url(link)
    if site:
        df = scrape_table_from_url(f"https://www.formula1.com/en/results/{year}/{site}")
        df.drop(drop_cols, axis=1, inplace=True)
        return df
    logger.error("No valid parameter provided to scrape_f1_website")
    return pd.DataFrame()


def fetch_race_links(year: int = YEAR) -> list:
    url = f"https://www.formula1.com/en/racing/{year}"
    logger.debug("Fetching URL: %s", url)
    try:
        html = urllib.request.urlopen(url).read()
        soup = bs.BeautifulSoup(html, "lxml")
        links = [
            a.get("href") for a in soup.find_all("a")
            if a.get("href") 
               and a.get("href").startswith("/en/racing/")
               and a.get("href") != "/en/racing/"
        ]
        logger.debug("Found %d links starting with '/en/racing/' (excluding '/en/racing/')", len(links))
        # Optionally, drop the first link if it's not needed:
        links = links[1:] if links else []
        logger.debug("After dropping the first link, %d remain", len(links))
        return links
    except Exception as e:
        logger.error("Error fetching race links: %s", e)
        return []


def scrape_race_details(link: str) -> dict:
    full_url = "https://www.formula1.com" + link
    logger.info("Scraping race details from: %s", full_url)
    try:
        race_html = urllib.request.urlopen(full_url).read()
        soup = bs.BeautifulSoup(race_html, "lxml")
        all_spans = [span.get_text(strip=True) for span in soup.find_all("span")]
        
        time_str = extract_time_from_spans(all_spans)
        if time_str:
            extracted_time = datetime.strptime(time_str, "%H:%M")
        else:
            logger.warning("No time element found on %s", full_url)
            extracted_time = None

        raw_date = extract_date_from_spans(all_spans) or ""
        if not raw_date:
            logger.warning("No date element found on %s. All spans: %s", full_url, all_spans)
        
        if extracted_time and raw_date:
            m = re.match(r"(\d{1,2})(?:\s+([A-Za-z]{3}))?\s*-\s*(\d{1,2})\s+([A-Za-z]{3})", raw_date)
            if m:
                end_day = int(m.group(3))
                month_abbr = m.group(4)
                month = datetime.strptime(month_abbr, "%b").month
                race_datetime = datetime(YEAR, month, end_day,
                                         extracted_time.hour,
                                         extracted_time.minute)
            else:
                logger.warning("Date format did not match expected pattern: '%s'", raw_date)
                race_datetime = extracted_time
        else:
            race_datetime = extracted_time

        race_name = link.split(f"/{YEAR}/")[-1].replace(".html", "")
        return {"name": race_name, "date": raw_date, "time": race_datetime}
    except Exception as e:
        logger.error("Error scraping race details from %s: %s", full_url, e)
        return {}

## Main ##

def populate_race_schedule():
    race_links = fetch_race_links(YEAR)
    if not race_links:
        logger.error("No race links to process.")
        return
    
    result_links = scrape_race_result_urls(YEAR)

    schedule = []
    raceweek = 1
    for race, result in zip(race_links,result_links):
        details = scrape_race_details(race)
        if details.get("time") is not None and details.get("name"):
            details["raceweek"] = raceweek
            details["results"] = result
            logger.info("Extracted race details: %s", details)
            schedule.append(details)
            raceweek += 1
        else:
            details["time"] = "N/A"
            details["date"] = "N/A"
            details["raceweek"] = raceweek
            details["results"] = result
            schedule.append(details)
            raceweek += 1
            logger.info("Race added without full details: %s", race)
            logger.warning("Fix required: Completed races currently cannot have time/date scraped after completion.")
    
    if schedule:
        with open("race_schedule.json", "w") as f:
            json.dump(schedule, f, default=str, indent=2)
        logger.info("Race schedule saved with %d races.", len(schedule))
    else:
        logger.error("No valid races found to save.")

if __name__ == "__main__":
    populate_race_schedule()