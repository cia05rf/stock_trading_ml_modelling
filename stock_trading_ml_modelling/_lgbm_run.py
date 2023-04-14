"""Running the Light Gradient Boost model
Finding what the latest predicitions are
"""

# Import modules
import numpy as np
import pandas as pd
import math
import lightgbm as lgb
from sklearn.externals import joblib as jl
import os

from stock_trading_ml_modelling.config import LGBM_BUY_SIGNAL, \
    LGBM_SELL_SIGNAL, FT_ENG_W, LGB_MODEL, LGB_MODEL_FEATURE_LIST, \
    HIST_PRICES_W, SIGNALS
from stock_trading_ml_modelling.utils.ft_eng import calc_ema_macd, calc_ema
from stock_trading_ml_modelling.utils.file import replace_file

# variables
buy_signal = LGBM_BUY_SIGNAL
sell_signal = LGBM_SELL_SIGNAL

# Import and combine prices files
df_ft = pd.read_hdf(FT_ENG_W)
print('SHAPE: {}'.format(df_ft.shape))
print('df_ft.columns -> {}'.format(df_ft.columns))
print('df_ft.head() -> {}'.format(df_ft.head()))

# Import to lr model
lgb_mod = jl.load(LGB_MODEL)
print('lgb_mod -> {}'.format(lgb_mod))

print('SHAPE BEFORE: {}'.format(df_ft.shape))
# data_df = df_ft.replace([np.inf,-np.inf],np.nan).dropna(axis=0)
data_df = df_ft
print('SHAPE AFTER: {}'.format(data_df.shape))
print('data_df.head() -> {}'.format(data_df.head()))

# Import feature_list
f = open(LGB_MODEL_FEATURE_LIST, 'r')
feature_li = f.read().split(',')
print('feature_li -> {}'.format(feature_li))

# Run the rf_mod to get signals
data_df['signal'] = lgb_mod.predict(data_df[feature_li])
data_df['signal_prob'] = [x.max()
                          for x in lgb_mod.predict_proba(data_df[feature_li])]

# Show current buy ratings
print('BUY COUNT: {:,}'.format(data_df.loc[(data_df['date'] == data_df['date'].max()) & (
    data_df['signal'] == buy_signal)].shape[0]))
print(data_df.loc[(data_df['date'] == data_df['date'].max()) & (data_df['signal'] == buy_signal), [
      'ticker', 'signal', 'signal_prob']].sort_values(['signal', 'signal_prob'], ascending=[True, False]))

# Show current sell ratings
print('SELL COUNT: {:,}'.format(data_df.loc[(data_df['date'] == data_df['date'].max()) & (
    data_df['signal'] == sell_signal)].shape[0]))
print(data_df.loc[(data_df['date'] == data_df['date'].max()) & (data_df['signal'] == sell_signal), [
      'ticker', 'signal', 'signal_prob']].sort_values(['signal', 'signal_prob'], ascending=[True, False]))


#################################################
### COMBINE WITH PRICE DATA AND CREATE LEDGER ###
#################################################

# Import and combine prices files
df_prices = pd.read_hdf(HIST_PRICES_W)

# Sort by ticker and date then add the open_shift_neg1 field
# These allow the buying and selling to be done at a realistic price
df_prices.sort_values(['ticker', 'date'], ascending=[True, True], inplace=True)
df_prices['open_shift_neg1'] = df_prices['open'].shift(-1)
df_prices['date'] = df_prices['date'].astype('datetime64')
print('SHAPE: {}'.format(df_prices.shape))

# Join on the buy and sell signals
df_prices = pd.merge(df_prices, data_df[['ticker', 'date', 'signal', 'signal_prob']], left_on=[
                     'ticker', 'date'], right_on=['ticker', 'date'], how='inner')
print('SHAPE: {}'.format(df_prices.shape))
print(df_prices.columns)
print(df_prices.signal.value_counts())
df_prices.head()

# Limit to a test period
df_prices = df_prices[df_prices['date'] >= '2014-01-01']
print('SHAPE: {}'.format(df_prices.shape))

# Create a dictionary of max character lengths of fields for use later in h5 file appending


def get_col_lens(_df_in):
    _col_lens = {}
    for c in _df_in:
        _tmp_s = pd.Series([len(str(x)) for x in _df_in[c]])
        _col_lens[c] = _tmp_s.max()
    return _col_lens


col_lens = get_col_lens(df_prices)
print('col_lens -> {}'.format(col_lens))


# Write df_prices to a .h5 file
path_split = SIGNALS.split(".")
hf_store_name = path_split + '_tmp.' + path_split[1]
hf = pd.HDFStore(hf_store_name)
df_prices.to_hdf(hf, key='data', append=True, min_itemsize=col_lens)
hf.close()

replace_file(SIGNALS, hf_store_name)
