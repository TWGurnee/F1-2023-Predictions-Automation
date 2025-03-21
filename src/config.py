
import os

import logging
import gspread


## Google Sheets secret key import.
from secrets2.ss_key import sskey
sskey=sskey

## LOGGER ##

logging.basicConfig(
    level=logging.INFO, #NOTE INFO / DEBUG to change functionality #
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


## Local Files ## 

# Authorization
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS = os.path.join(ROOT_DIR, r"secrets2/creds.json")


def authorize_google_sheet(creds: str, sheet_key: str) -> gspread.Spreadsheet:
    """Authorize access to a Google Sheet using a service account."""
    return gspread.service_account(filename=creds).open_by_key(sheet_key)

JSON_FILE = "race_schedule.json"

## Scheduler perameters ##

OFFSET_HOURS = 1           # Window starts 1 hour after race start.
UPDATE_WINDOW_HOURS = 3   # The update window lasts 3 hours.
UPDATE_INTERVAL = 5 * 60  # When in window, run updater every 5 minutes.


### Game Details ###
YEAR = 2025
PLAYERS = ["Tim", "Freya", "Tom", "Shaun"]
SHEETS = {0: "25 Season Post-Testing"}


CURRENT_SHEET = list(SHEETS.keys())[0] # DOn't like this but works for now


### Championship details ###

TOT_TEAMS = 10
PREDICTION_DRIVERS = 20
TOT_RACES = 24

# Add new drivers into conversion below
# Used for converting the predictions into scores
#(currently predictions must follow format of dict values below.)

NAME_CONVERSION = {
    "VER": "Verstappen",
    "LAW": "Lawson",
    "NOR": "Norris",
    "PIA": "Piastri",
    "HAM": "Hamilton",
    "LEC": "Leclerc",
    "SAI": "Sainz",
    "ALB": "Albon",    
    "RUS": "Russell",
    "ANT": "Antonelli",    
    "OCO": "Ocon",
    "BEA": "Bearman",
    "ALO": "Alonso",
    "STR": "Stroll",
    "GAS": "Gasly",
    "DOO": "Doohan",
    "TSU": "Tsunoda",
    "HAD": "Hadjar",
    "HUL": "Hulkenberg",
    "BOR": "Bortoleto",
    "Red Bull Racing Honda RBPT": "Red Bull",
    "Aston Martin Aramco Mercedes": "Aston Martin",
    "Mercedes": "Mercedes",
    "Ferrari": "Ferrari",
    "Kick Sauber Ferrari": "Stake",
    "Alpine Renault": "Alpine",
    "Williams Mercedes": "Williams",
    "Racing Bulls Honda RBPT": "Racing Bulls",
    "Haas Ferrari": "Haas",
    "McLaren Mercedes": "Mclaren"
}

TOT_DRIVERS = len(NAME_CONVERSION) - TOT_TEAMS


    


# ### Coordinate Classes ###
# @dataclass(frozen=True) #TODO repr method? so can print logged errors?
# class TableCoords:
#     """
#     Represents spreadsheet table coordinate data.

#     Attributes:
#         header (int): The header row of the table.
#         row (int): The first data row.
#         column (int): The leftmost column index.
#         length (int): The number of rows covered.
#         width (List[int]): The list of column indices.
#     """
#     header: int
#     row: int
#     column: int
#     length: int
#     width: List[int]
#     drop_cols: Optional[List[str]] = field(default=None)
#     # wildcard fields
#     include_index: Optional[bool] = field(default=None)
#     wildcard_index: Optional[int] = field(default=None)
    

#     def start_cell(self) -> "CellCoords":
#         """Return the top-left cell as a CellCoords object."""
#         return CellCoords(self.row, self.column)

#     def offset_cell(self, row_offset: int = 0, col_offset: int = 0) -> "CellCoords":
#         """
#         Return a new CellCoords offset from the starting cell.
        
#         Args:
#             row_offset (int): Rows to offset.
#             col_offset (int): Columns to offset.
        
#         Returns:
#             CellCoords: The new cell coordinates.
#         """
#         return CellCoords(self.row + row_offset, self.column + col_offset)


# @dataclass(frozen=False)
# class CellCoords:
#     """
#     Represents a single cell coordinate.
    
#     Attributes:
#         row (int): The row number.
#         column (int): The column number.
#     """
#     row: int
#     column: int

#     def to_tuple(self) -> Tuple[int, int]:
#         """Return the cell coordinates as a tuple (row, column)."""
#         return (self.row, self.column)


# ### Sheet Coordinate Definitions ###
# SCOREBOARD = TableCoords(
#     header=12,
#     row=13,
#     column=2,
#     width=list(range(1, len(PLAYERS) * 2 + 1)),
#     length=30
# )
# POINTS_TRACKER = TableCoords(
#     header=27,
#     row=28,
#     column=16,
#     length=24,
#     width=[16, 17, 18, 19]
# )
# WILDCARD_POINTS = TableCoords(
#     header=SCOREBOARD.row + SCOREBOARD.length,
#     row=SCOREBOARD.row + SCOREBOARD.length + 1,
#     column=SCOREBOARD.column - 1,
#     width=list(range(len(PLAYERS) * 2 + 1)),
#     length=5
# )
# PLAYERS_POINTS = [
#     CellCoords(WILDCARD_POINTS.row + WILDCARD_POINTS.length + 1, SCOREBOARD.column + i * 2 + 1)
#     for i in range(len(PLAYERS))
# ]
# WDC = TableCoords(
#     header=SCOREBOARD.header,
#     row=SCOREBOARD.row,
#     column=SCOREBOARD.width[-1] + 4,
#     width=list(range(SCOREBOARD.width[-1] + 3, SCOREBOARD.width[-1] + 4)),
#     length=TOT_DRIVERS,
#     drop_cols=["Nationality", "Car", "Pos"]
# )
# WCC = TableCoords(
#     header=WDC.row + TOT_DRIVERS,
#     row=WDC.row + TOT_DRIVERS + 1,
#     column=WDC.column,
#     width=WDC.width,
#     length=TOT_TEAMS,
#     drop_cols=["Pos"]
# )

# ## Wildcards ##

# WILDCARDS = [
#     {"name": "Pole positions", "column":1,"cols_dropped":["No", "Car", "Laps"],"index":0},
#     {"name": "Fastest Laps", "column":5,"cols_dropped":["Time", "Car"], "index":1},
#     {"name": "Podiums", "column":9,"cols_dropped":["No", "Car", "Laps"], "index":2},
#     {"name": "DNFs", "column":13, "cols_dropped":["No", "Car", "Laps"], "index":3}  
# ]

# rows_below_wildcard_predictions = 10

# wild_coord_table = {
#     wildcard["name"]: TableCoords(
#         header=WILDCARD_POINTS.header + rows_below_wildcard_predictions,
#         row=WILDCARD_POINTS.row + rows_below_wildcard_predictions,
#         column=wildcard["column"],
#         length=TOT_DRIVERS,
#         width=[wildcard["column"] - 1, wildcard["column"], wildcard["column"] + 1],
#         drop_cols=wildcard["cols_dropped"],
#         include_index=True,
#         wildcard_index=wildcard["index"]     
#     )
#     for wildcard in WILDCARDS
# }

# POLES, PODIUMS, FLS, DNFS = (wild_coord_table[k] for k in ("Pole positions", "Podiums", "Fastest Laps", "DNFs"))

