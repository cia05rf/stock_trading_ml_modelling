"""Takes the data from the database and saves it to an hdf file."""

import os
from config import HIST_PRICES_D
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df

### READ DATA ###
SQL = """SELECT * FROM daily_prices"""
daily_prices_df = sqlaq_to_df(SQL)

### WRITE DATA ###
daily_prices_df.to_hdf(
    HIST_PRICES_D
    )