"""Takes the data from the database and saves it to an hdf file."""

import pandas as pd
import numpy as np
from tqdm import tqdm
import os

from stock_trading_ml_modelling.config import STORE_PATH
from stock_trading_ml_modelling.database.models import engine

PERIOD = 180
NORM_DATA = False
KEY = "change_per"
ROUND_DP = 3
OUT_FN = "change_per.np"
fp = os.path.join(STORE_PATH, OUT_FN)


### READ DATA ###
SQL = """SELECT * FROM daily_price"""
daily_prices_df = pd.read_sql(SQL, engine)

### FORMAT TO GROUPS ###
daily_prices_df["change_per"] = daily_prices_df.change / daily_prices_df.close
daily_prices_df = daily_prices_df[["ticker_id", "date", KEY]]
# Group by ticker
daily_prices_grouped = daily_prices_df.groupby('ticker_id')
# Start file
if os.path.exists(fp):
    os.remove(fp)
data = []
for i, g in tqdm(enumerate(daily_prices_grouped.groups),
                total=len(daily_prices_grouped.groups),
                desc="Building data"):
    # Get the group
    group = daily_prices_grouped.get_group(g)
    # Skip if too small
    if group.shape[0] < PERIOD + 1:
        continue
    # Sort by date
    group = group.sort_values('date')
    # Set the index
    group = group.set_index('date')
    # Model as normalised close over period
    new_data = np.array([
        group.iloc[st:st + PERIOD + 1][KEY].to_numpy()
        for st in range(0, group.shape[0] - PERIOD - 1)
        ])
    # Append to data
    data.append(new_data)
# Concatenate data
data = np.concatenate(data, axis=0)
# Normalise over data axis=1
if NORM_DATA:
    v = data[:, :-1]   # foo[:, -1] for the last column
    min_ = np.expand_dims(np.min(v, axis=1), axis=1)
    max_ = np.expand_dims(np.max(v, axis=1), axis=1)
    data = ((data - min_) / (max_ - min_))
data = np.round(data, ROUND_DP)
# Save data
# Data saved as 180 cols of features and final col is target
with open(fp, 'wb') as f:
    np.save(f, data)