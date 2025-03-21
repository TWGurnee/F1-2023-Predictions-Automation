#!/usr/bin/env python3
import urllib.request, urllib.error
import bs4 as bs
import json
from datetime import datetime, timedelta, timezone
import logging
import re
import time
import pandas as pd
import numpy as np
from typing import List, Optional, Union
from gspread_dataframe import get_as_dataframe
import random
import string

from src.config import *
from src.gSheet_utils import  *
from src.scraping_utils import *


sh = authorize_google_sheet(CREDS, sskey)
predictions_sheet = sh.get_worksheet(CURRENT_SHEET)

scoreboard=get_table(predictions_sheet,SCOREBOARD)
driver_standings=get_table(predictions_sheet,WDC)
team_standings=get_table(predictions_sheet,WCC)

driver_positions = {NAME_CONVERSION.get(str(r["Driver"])[-3:]): i for i, r in driver_standings.iterrows()}
team_positions = {NAME_CONVERSION.get(str(r["Team"])): i for i, r in team_standings.iterrows()}



start = time.time()
# Precompute the row indices as a NumPy array.
indices = np.arange(len(scoreboard))

# For each player, calculate the score column in a vectorized manner.
for player, col in zip(PLAYERS, [i * 2 + 1 for i in range(len(PLAYERS))]):
    # Map player names to positions (for drivers and teams)
    driver_map = scoreboard[player].map(driver_positions)
    team_map = scoreboard[player].map(team_positions)
    
    
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
    

logger.info("new method: %ss", time.time() - start)

logger.info("starting old method")
start = time.time()

score_cols = [i * 2 + 1 for i in range(len(PLAYERS))]

for index, row in scoreboard.iterrows():
            for player, col in zip(PLAYERS, score_cols):
                if index < PREDICTION_DRIVERS: #type: ignore only now is this being highlighted as an issue.. hashables??
                    scoreboard.iat[index, col] = abs(index - driver_positions.get(row[player], index)) #type: ignore
                else:
                    scoreboard.iat[index, col] = abs((index - PREDICTION_DRIVERS) - team_positions.get(row[player], index - PREDICTION_DRIVERS)) #type: ignore

logger.info("old method: %ss", time.time() - start)

##############################################################
