#!/usr/bin/env python3
"""
Google Sheets F1 Predictions Utilities

Handles Google Sheets authorization and updates to the F1 prediction standings by scraping
data from the Formula 1 website using gspread and pandas.
"""

## IMPORTS ##
import time
from typing import List, Tuple
import pandas as pd
import numpy as np
import threading as td


from config import *
from tables import *
from scraping_utils import scrape_f1_website
from gSheet_utils import log_update

## Score calculations ##

# Helpers #
def initialise_driver_wildcards() -> dict:
    return {d: 0 for d in list(NAME_CONVERSION.values())[:TOT_DRIVERS]}

def inistialise_team_wildcards() -> dict:
    return {t: 0 for t in list(NAME_CONVERSION.values())[:-TOT_TEAMS]}

# Score calcs #
def calulate_prediction_scores(scoreboard: pd.DataFrame, driver_standings: dict, team_standings: dict) -> pd.DataFrame:
    
    score_cols = [i * 2 + 1 for i in range(len(PLAYERS))]
    try:
        # Precompute the row indices as a NumPy array.
        indices = np.arange(len(scoreboard))
        for player, col in zip(PLAYERS, score_cols):
            # Map player names to positions (for drivers and teams)
            driver_map = scoreboard[player].map(driver_standings)
            team_map = scoreboard[player].map(team_standings)
            # Provide a fallback using the row index if mapping returns NaN.
            driver_map = driver_map.fillna(pd.Series(indices, index=scoreboard.index))
            team_map = team_map.fillna(pd.Series(indices - PREDICTION_DRIVERS, index=scoreboard.index))
            # Compute the scores using np.where for vectorized condition handling.
            scores = np.where(
                indices < PREDICTION_DRIVERS,
                np.abs(indices - driver_map),
                np.abs((indices - PREDICTION_DRIVERS) - team_map)
            )
            scoreboard.iloc[:, col] = scores  # or use .loc if you have column names

    except Exception as e:
        logger.info("Error calculating new prediction scores, scores unchanged: %s", e)
    return scoreboard


def update_wildcard_row(wc_df: pd.DataFrame, row: int, updated: pd.DataFrame, src: str = "") -> pd.DataFrame:
    """
    Update a given row in the wildcard scoreboard with rank data from updated.
    
    Args:
        wc_df (pd.DataFrame): Original wildcard scoreboard.
        row (int): Row index to update.
        updated (pd.DataFrame): DataFrame containing updated rank info.
        src (str): Source identifier for logging.
    """
    if "Rank" not in updated.columns:
        logger.warning("Source %s missing 'Rank'; skipping row %s.", src, row)
        return wc_df
    for i, player in enumerate(PLAYERS):
        pred = wc_df.iat[row, i * 2 + 1]
        rank = updated["Rank"].get(pred, None)
        if rank is None:
            logger.warning("No rank for '%s' from %s at row %s.", pred, src, row)
        wc_df.iat[row, i * 2 + 2] = rank
    return wc_df


### Updating the Sheet ###

def update_standings(sheet, site: str, drop_cols: List[str], standings: TableCoords) -> None:
    """Generic update for standings."""
    try:
        df = scrape_f1_website(YEAR, site=site, drop_cols=drop_cols)

        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.warning(f"{site} data empty; skipping update.")
            return

        standings.set_table(sheet=sheet, updated_table=df)
    except Exception as e:
        logger.error(f"Error updating {site} standings: {e}")


def update_prediction_points(sheet) -> None:
    """Update the prediction scoreboard by comparing user predictions against standings."""
    try:
        updated_scoreboard = calulate_prediction_scores(scoreboard=SCOREBOARD.get_table(sheet),
                                                        driver_standings=WDC.get_dict(sheet),
                                                        team_standings=WCC.get_dict(sheet))
        SCOREBOARD.set_table(sheet=sheet, updated_table=updated_scoreboard)
    except Exception as e:
        logger.error("Error updating prediction points: %s", e)


def update_fastest_laps_scores(sheet) -> Tuple[int, pd.DataFrame]:
    """Process fastest laps data and return its wildcard row update."""
    logger.info("Updating Fastest laps...")
    counts = initialise_driver_wildcards()
    df = scrape_f1_website(YEAR, site="fastest-laps", drop_cols=FLS.drop_cols)
    if not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning("Fastest laps data unavailable.")
        return FLS.wildcard_index, pd.DataFrame() #type: ignore Constant ensures type safe
    
    for name in df["Driver"]:
        key = NAME_CONVERSION.get(str(name[-3:]))
        if key:
            counts[key] += 1
            
    #below only required during initialisation.
    fl_df = pd.DataFrame.from_dict(counts, orient="index", columns=["FL Count"])
    
    #still required
    fl_df.sort_values("FL Count", ascending=False, inplace=True)
    #recalculation below would still be  required - may need to rethink once it is incremental
    fl_df["Rank"] = fl_df["FL Count"].rank(method="dense", ascending=False) - 1

    #sets table back onto sheet, then also sends it to 
    FLS.set_table(sheet=sheet, updated_table=fl_df, WC=True)
    logger.info("Fastest laps updated.")
    return FLS.wildcard_index, fl_df #type: ignore Constant ensures type safe


def update_DNFs_and_podiums_scores(sheet) -> List[Tuple[int, pd.DataFrame]]:
    """Process DNFs and podium data and return their wildcard row updates."""
    logger.info("Updating Podiums and DNFs...")
    dnf_counts = {d: 0 for d in list(NAME_CONVERSION.values())[:TOT_DRIVERS]}
    podium_counts = {d: 0 for d in list(NAME_CONVERSION.values())[:TOT_DRIVERS]}
    for link in scrape_f1_website(YEAR, all_race_URLs=True) or []:
        try:
            df = scrape_f1_website(YEAR, link=link, drop_cols=PODIUMS.drop_cols) #type:ignore
            if not isinstance(df, pd.DataFrame) or df.empty:
                break

            for name in df.drop(df[df['Time/retired'] != "DNF"].index)["Driver"]:
                key = NAME_CONVERSION.get(str(name[-3:]))
                if key:
                    dnf_counts[key] += 1
                    
        
            for name in df[df["Pos"].isin(["1", "2", "3"])]["Driver"]:
                key = NAME_CONVERSION.get(str(name[-3:]))
                if key:
                    podium_counts[key] += 1
                    
        except Exception as e:
            logger.error("Error on link %s: %s", link, e)
            break
    dnf_df = pd.DataFrame.from_dict(dnf_counts, orient="index", columns=["DNF Count"])
    dnf_df.sort_values("DNF Count", ascending=False, inplace=True)
    dnf_df["Rank"] = dnf_df["DNF Count"].rank(method="dense", ascending=False) - 1
    DNFS.set_table(sheet,dnf_df,WC=True)
    podium_df = pd.DataFrame.from_dict(podium_counts, orient="index", columns=["Podium Count"])
    podium_df.sort_values("Podium Count", ascending=False, inplace=True)
    podium_df["Rank"] = podium_df["Podium Count"].rank(method="dense", ascending=False) - 1
    PODIUMS.set_table(sheet,podium_df,WC=True)
    logger.info("Podiums & DNFs updated.")
    return [(DNFS.wildcard_index, dnf_df), (PODIUMS.wildcard_index, podium_df)] #type: ignore Constant ensures type safe



def update_pole_positions_scores(sheet) -> Tuple[int, pd.DataFrame]:
    """Process pole positions data and return its wildcard row update."""
    logger.info("Updating Poles...")
    pole_counts = {d: 0 for d in list(NAME_CONVERSION.values())[:TOT_DRIVERS]}
    for link in scrape_f1_website(YEAR, all_race_URLs=True) or []:
        try:
            q_link = link[:-11] + "qualifying" #type: ignore
            df = scrape_f1_website(YEAR, link=q_link,drop_cols=POLES.drop_cols)
            
            if not isinstance(df, pd.DataFrame) or df.empty:
                break

            for name in df.iloc[:1]["Driver"]:
                key = NAME_CONVERSION.get(str(name[-3:]))
                if key:
                    pole_counts[key] += 1
        except Exception as e:
            logger.error("Error on link %s: %s", link, e)
            break
    pole_df = pd.DataFrame.from_dict(pole_counts, orient="index", columns=["Pole Count"])
    pole_df.sort_values("Pole Count", ascending=False, inplace=True)
    pole_df["Rank"] = pole_df["Pole Count"].rank(method="dense", ascending=False) - 1
    POLES.set_table(sheet, pole_df, WC=True)
    logger.info("Poles updated.")
    return POLES.wildcard_index, pole_df ##type: ignore Constant ensures type safe


def update_wildcard_scores(sheet) -> None:
    """Launch wildcard updates in parallel and apply the rank updates."""
    logger.info("Updating WildCards")
    wc_df = WILDCARD_POINTS.get_table(sheet)
    updates: List[Tuple[int, pd.DataFrame]] = []
    threads = []

    def run_and_collect(func, args):
        res = func(*args)
        if res:
            updates.append(res)

    threads.append(td.Thread(target=run_and_collect, args=(update_fastest_laps_scores, (sheet,))))
    threads.append(td.Thread(target=lambda: updates.extend(update_DNFs_and_podiums_scores(sheet,))))
    threads.append(td.Thread(target=run_and_collect, args=(update_pole_positions_scores, (sheet,))))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for row_index, upd_df in updates:
        wc_df = update_wildcard_row(wc_df, row_index, upd_df, src="Wildcard Update")
    WILDCARD_POINTS.set_table(sheet=sheet, updated_table=wc_df)
    logger.info("Wildcards updated.")

#TODO Move to OOP?
def update_points_tracker(sheet, current_raceweek: int) -> None:
    if current_raceweek:
        for i, cell in enumerate(PLAYERS_POINTS):
            target = POINTS_TRACKER.offset_cell(row_offset=current_raceweek, col_offset=i)
            new_val = int(sheet.cell(cell.row, cell.column).value)
            sheet.update_cell(*target.to_tuple(), new_val)
        logger.info(f"Raceweek {current_raceweek} score tracker updated.")
    else: logger.info("Unable to update points tracker for raceweek %s", current_raceweek)


def calculate_raceweek_points(sh, sheet: int) -> None:
    """Update all standings and points for the current raceweek in the Google Sheet."""
    ps = sh.get_worksheet(sheet)
    threads = [
        td.Thread(target=update_standings, args=(ps, "drivers", ["Nationality", "Car", "Pos"], WDC)),
        td.Thread(target=update_standings, args=(ps, "team", ["Pos"], WCC)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        
    update_wildcard_scores(ps)
    update_prediction_points(ps)
    
        
    current_raceweek = int(ps.acell("B2").value)
    update_points_tracker(sheet=ps, current_raceweek=current_raceweek)
    
    log_update(ps)

def update():
    start = time.time()
    
    sh = authorize_google_sheet(CREDS, sskey)
    
    for i in list(SHEETS.keys()):
    
        logger.info("\nUpdating sheet %s\n%s", SHEETS.get(i), "~" * 75)
        calculate_raceweek_points(sh, i)
        logger.info("Updated sheet %s\n%s", SHEETS.get(i), "#" * 76)
    
    logger.info("Total update time: %ss", time.time() - start)


if __name__ == "__main__":
    """Calulates and updates the predictions scores for the year."""
    update()
