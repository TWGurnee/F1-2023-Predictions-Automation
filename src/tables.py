import pandas as pd
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Callable, Any
from gspread_dataframe import get_as_dataframe, set_with_dataframe


from config import *
### Coordinate Classes ###
@dataclass(frozen=True) #TODO repr method? so can print logged errors?
class TableCoords:
    """
    Represents spreadsheet table coordinate data.

    Attributes:
        header (int): The header row of the table.
        row (int): The first data row.
        column (int): The leftmost column index.
        length (int): The number of rows covered.
        width (List[int]): The list of column indices.
    """
    
    header: int
    row: int
    column: int
    length: int
    width: List[int]
    #Optional fields
    name: Optional[str] = field(default=None)
    key_column: Optional[str] = field(default=None)
    #Field for driver standings string slicing [-3]
    key_transform: Optional[Callable[[Any], Any]] = field(default=None)
    drop_cols: Optional[List[str]] = field(default=None)
    # wildcard fields
    include_index: Optional[bool] = field(default=None)
    wildcard_index: Optional[int] = field(default=None)
    
    def __repr__(self) -> str:
        if self.name:
            return (f"TableCoords(name={self.name!r}, header={self.header}, row={self.row}, "
                    f"col={self.column}, length={self.length}, width={self.width})")
        else:
            return (f"TableCoords(header={self.header}, row={self.row}, "
                    f"col={self.column}, length={self.length}, width={self.width})")


    def start_cell(self) -> "CellCoords":
        """Return the top-left cell as a CellCoords object."""
        return CellCoords(self.row, self.column)

    def offset_cell(self, row_offset: int = 0, col_offset: int = 0) -> "CellCoords":
        """
        Return a new CellCoords offset from the starting cell.
        
        Args:
            row_offset (int): Rows to offset.
            col_offset (int): Columns to offset.
        
        Returns:
            CellCoords: The new cell coordinates.
        """
        return CellCoords(self.row + row_offset, self.column + col_offset)
    
    ## Getting and Setting
    
    def get_table(self, sheet) -> pd.DataFrame:
        """
        Returns a table from the provided sheet as a DataFrame based on this instance's coordinates.

        Args:
            sheet: The spreadsheet object to retrieve the data from.
        
        Returns:
            pd.DataFrame: The extracted table. If an error occurs, an empty DataFrame is returned.
        """
        try:
            table = get_as_dataframe(sheet, header=self.header, usecols=self.width, nrows=self.length)
            return table  # type: ignore
        except Exception as e:
            logging.info("unable to retrieve table: %s", e)
            return pd.DataFrame()
        
    def get_dict(self, sheet) -> dict:
        """
        Retrieves the table from the given sheet and returns a dictionary mapping keys 
        (from the column defined by self.key_column, optionally transformed) to row indices.

        Args:
            sheet: The spreadsheet object from which to retrieve the table.

        Returns:
            dict: A mapping of (optionally transformed) keys to row indices.

        Raises:
            ValueError: If key_column is not set.
        """
        if not self.key_column:
            raise ValueError("No key_column specified for this TableCoords instance.")

        try:
            # Retrieve the table from the sheet.
            df = get_as_dataframe(sheet, header=self.header, usecols=self.width, nrows=self.length)
        except Exception as e:
            logging.info("Unable to retrieve table: %s", e)
            return {}

        if df.empty:
            logging.info("The retrieved table is empty.")
            return {}

        # Vectorized transformation of the key column.
        keys = df[self.key_column].apply(self.key_transform) if self.key_transform else df[self.key_column]
        return dict(zip(keys, df.index))
    
    def set_table(self, sheet, updated_table: pd.DataFrame, WC: bool = False) -> None:
        """
        Places an updated table onto the provided sheet using this instance's coordinates.
        
        Args:
            sheet: The spreadsheet object where the table will be placed.
            updated_table (pd.DataFrame): The updated table to place on the sheet.
            WC (bool): Flag to include index or any other parameter (default is False).
        """
        try:
            set_with_dataframe(
                worksheet=sheet,
                dataframe=updated_table,
                row=self.row,
                col=self.column,
                include_index=WC
            )
        except Exception as e:
            logging.info("Unable to place updated table: %s", e)


@dataclass(frozen=False)
class CellCoords:
    """
    Represents a single cell coordinate.
    
    Attributes:
        row (int): The row number.
        column (int): The column number.
    """
    row: int
    column: int

    def to_tuple(self) -> Tuple[int, int]:
        """Return the cell coordinates as a tuple (row, column)."""
        return (self.row, self.column)


### Sheet Coordinate Definitions ###
SCOREBOARD = TableCoords(
    name="Predictions Scoreboard",
    header=12,
    row=13,
    column=2,
    width=list(range(1, len(PLAYERS) * 2 + 1)),
    length=30
)

POINTS_TRACKER = TableCoords(
    header=27,
    row=28,
    column=16,
    length=24,
    width=[16, 17, 18, 19]
)

WILDCARD_POINTS = TableCoords(
    header=SCOREBOARD.row + SCOREBOARD.length,
    row=SCOREBOARD.row + SCOREBOARD.length + 1,
    column=SCOREBOARD.column - 1,
    width=list(range(len(PLAYERS) * 2 + 1)),
    length=5
)

PLAYERS_POINTS = [
    CellCoords(WILDCARD_POINTS.row + WILDCARD_POINTS.length + 1, SCOREBOARD.column + i * 2 + 1)
    for i in range(len(PLAYERS))
]

WDC = TableCoords(
    name="WDC Standings",
    header=SCOREBOARD.header,
    row=SCOREBOARD.row,
    column=SCOREBOARD.width[-1] + 4,
    width=list(range(SCOREBOARD.width[-1] + 3, SCOREBOARD.width[-1] + 4)),
    length=TOT_DRIVERS,
    drop_cols=["Nationality", "Car", "Pos"],
    key_column="Driver",
    key_transform= lambda name: NAME_CONVERSION.get(str(name)[-3:])
)

WCC = TableCoords(
    name="WCC Standings",
    header=WDC.row + TOT_DRIVERS,
    row=WDC.row + TOT_DRIVERS + 1,
    column=WDC.column,
    width=WDC.width,
    length=TOT_TEAMS,
    drop_cols=["Pos"],
    key_column="Team",
    key_transform= lambda name: NAME_CONVERSION.get(str(name))
    
)

## Wildcards ##

WILDCARDS = [
    {"name": "Pole positions", "column":1,"cols_dropped":["No", "Car", "Laps"],"index":0},
    {"name": "Fastest Laps", "column":5,"cols_dropped":["Time", "Car"], "index":1},
    {"name": "Podiums", "column":9,"cols_dropped":["No", "Car", "Laps"], "index":2},
    {"name": "DNFs", "column":13, "cols_dropped":["No", "Car", "Laps"], "index":3}  
]

rows_below_wildcard_predictions = 10

wild_coord_table = {
    wildcard["name"]: TableCoords(
        header=WILDCARD_POINTS.header + rows_below_wildcard_predictions,
        row=WILDCARD_POINTS.row + rows_below_wildcard_predictions,
        column=wildcard["column"],
        length=TOT_DRIVERS,
        width=[wildcard["column"] - 1, wildcard["column"], wildcard["column"] + 1],
        drop_cols=wildcard["cols_dropped"],
        include_index=True,
        wildcard_index=wildcard["index"]     
    )
    for wildcard in WILDCARDS
}

POLES, PODIUMS, FLS, DNFS = (wild_coord_table[k] for k in ("Pole positions", "Podiums", "Fastest Laps", "DNFs"))

