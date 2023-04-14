"""Feature engineering for stock prices"""

# Import libraries
import pandas as pd
import numpy as np
import re
import os
import datetime as dt

from stock_trading_ml_modelling.config import TICK_FTSE, HIST_PRICES_W, \
    HIST_PRICES_D, TARGET_PRICE_PERIOD, MIN_GAIN, MAX_DROP, PERIOD_LI, \
    LOOK_BACK_PRICE_PERIOD, PERIOD_HIGH_VOLATILITY, PERIOD_LOW_VOLATILITY, \
    NORM_WINDOW, GAP_HIGH_VOLATILITY, GAP_LOW_VOLATILITY, FT_ENG_W, \
    HF_STORE, FT_ENG_COL_LIST
from stock_trading_ml_modelling.utils.ft_eng import calc_ema, calc_ema_macd, \
    get_buys, get_sells, calc_changes, get_col_len_df, norm_time_s, flag_mins, \
    flag_maxs, mk_prev_move_float, avg_in_range, gradient
from stock_trading_ml_modelling.utils.str_formatting import clean_col_name
from stock_trading_ml_modelling.utils.file import replace_file
from stock_trading_ml_modelling.utils.timing import ProcessTime

# Programming note
# df.shift(1) looks 1 period into the past
# df.shift(-1) looks 1 period into the future

######################
### IMPORTING DATA ###
######################

# Import the ftse list
tick_ftse = pd.read_csv(
    TICK_FTSE)
tick_ftse = tick_ftse.iloc[:, 1:]
for col in tick_ftse:
    tick_ftse.rename(columns={col: clean_col_name(col)}, inplace=True)
tick_ftse.head()
tick_ftse['ticker'] = [re.sub(
    '(?=[0-9A-Z])*\.(?=[0-9A-Z]+)', '-', tick) for tick in tick_ftse['ticker']]
tick_ftse['ticker'] = [re.sub('[^0-9A-Z\-]', '', tick)
                       for tick in tick_ftse['ticker']]

# Import and combine prices files
df_prices_w = pd.read_hdf(
    HIST_PRICES_W)
# Rename columns
for col in df_prices_w:
    df_prices_w.rename(columns={col: clean_col_name(col)}, inplace=True)
# Drop unwanted columns
try:
    df_prices_w.drop(columns=["unnamed_0", "index"], inplace=True)
except Exception as e:
    print(e)
# Reformat columns where neccessary
df_prices_w["date"] = df_prices_w["date"].astype("datetime64")
print(df_prices_w.shape)
print(df_prices_w.dtypes)
print(df_prices_w.head())


#############################
### BUY HOLD SELL SIGNALS ###
#############################

# Set records into the correct order
df_prices_w = df_prices_w.sort_values(
    ['ticker', 'date'], ascending=[True, True])
df_prices_w.reset_index(inplace=True, drop=True)

# Calc a column for will the price increase next week
df_prices_w['signal'] = (df_prices_w.close.shift(-TARGET_PRICE_PERIOD) - df_prices_w.close) > 0
print('df_prices_w.signal.value_counts() -> \n{}'.format(df_prices_w.signal.value_counts()))

# Get buy signals
df_prices_w['buy'] = get_buys(df_prices_w['close'], TARGET_PRICE_PERIOD,
                              MIN_GAIN, MAX_DROP)

# Get sell signals
df_prices_w['sell'] = get_sells(df_prices_w['close'], TARGET_PRICE_PERIOD,
                                MIN_GAIN, MAX_DROP)

# Turn into a single signal
df_prices_w['signal'] = 'hold'
df_prices_w.loc[df_prices_w.sell, 'signal'] = 'sell'
df_prices_w.loc[df_prices_w.buy, 'signal'] = 'buy'
print('df_prices_w.signal.value_counts() -> \n{}'.format(df_prices_w.signal.value_counts()))

print('BUY PERCENTAGE -> {:.2f}%'.format(
    df_prices_w[df_prices_w.signal == 'buy'].shape[0]*100/df_prices_w.shape[0]))
print('SELL PERCENTAGE -> {:.2f}%'.format(
    df_prices_w[df_prices_w.signal == 'sell'].shape[0]*100/df_prices_w.shape[0]))
print('HOLD PERCENTAGE -> {:.2f}%'.format(
    df_prices_w[df_prices_w.signal == 'hold'].shape[0]*100/df_prices_w.shape[0]))

# Convert to bool
if len(np.unique(df_prices_w.signal)) == 2 and 'buy' in np.unique(df_prices_w.signal):
    df_prices_w['signal'] = df_prices_w.signal == 'buy'
    df_prices_w['signal'] = df_prices_w.signal.astype('int')
    LGBM_BUY_SIGNAL = 1
    LGBM_SELL_SIGNAL = 0
print('{} UNIQUE SIGNALS -> {}'.format(len(np.unique(df_prices_w.signal)),
      np.unique(df_prices_w.signal)))

#######################################
### FILTER OUT SHORT HISTORY SHARES ###
#######################################

# Remove tickers with fewer than 34 entries as this is where the MACD can be calculated
print('START ROW COUNT -> {}'.format(df_prices_w.shape[0]))
print('START TICK COUNT -> {}'.format(tick_ftse.shape[0]))
for tick in tick_ftse.ticker:
    print(tick, ' ->', df_prices_w[df_prices_w.ticker == tick].shape[0])
    if df_prices_w[df_prices_w.ticker == tick].shape[0] < 34:
        # Remove from dataframe
        print('\tTOO FEW RECORDS FOR {}'.format(tick))
        df_prices_w = df_prices_w.loc[df_prices_w.ticker != tick]
        print('\tNEW ROW COUNT -> {}'.format(df_prices_w.shape[0]))
        # Remove from tick_ftse
        tick_ftse = tick_ftse.loc[tick_ftse.ticker != tick]
        print('\tNEW TICK COUNT -> {}'.format(tick_ftse.shape[0]))
print('\nEND ROW COUNT -> {}'.format(df_prices_w.shape[0]))
print('END TICK COUNT -> {}'.format(tick_ftse.shape[0]))


#####################################
### CALCULATING EMA & MACD VALUES ###
#####################################

# Normalize the prices by ticker and time then create emas and macds for each ticker
print('CALCULATING EMA & MACD VALUES')
error_li = []
run_time = ProcessTime()
run_time.lap()
new_df = pd.DataFrame([])
for tick in tick_ftse.ticker:
    print('\nRUN FOR {} - {}'.format(tick, len(run_time.lap_li)))
    try:
        this_tick_df = df_prices_w[df_prices_w.ticker == tick]
        # Calculate the ema and macd
        this_tick_df = calc_ema_macd(this_tick_df)
        # Append back on to the dataframe
        new_df = new_df.append(this_tick_df)
        print('\tSUCCESS')
        run_time.lap()
        run_time.show_latest_lap_time()
    except Exception as e:
        print('\tERROR -> {}'.format(e))
        error_li.append(e)
df_prices_w = new_df
run_time.end()
print('\n\nCOMPLETED - ERRORS ENCOUNTERED -> {}'.format(len(error_li)))
if len(error_li) > 0:
    print(error_li)

# Relabel col names
for col in df_prices_w:
    df_prices_w.rename(columns={col: col.lower()}, inplace=True)

df_prices_w["change_close"], df_prices_w["per_change_close"] = calc_changes(
    df_prices_w.close.copy(), df_prices_w.open.copy())
print(df_prices_w.head())

# Sort values by ticker and date to ensure sequential records
df_prices_w = df_prices_w.sort_values(
    ['ticker', 'date'], ascending=[True, True])
df_prices_w.reset_index(inplace=True, drop=True)
print(df_prices_w.ticker.unique())
print(df_prices_w.head())

# Get column lengths
col_lens = get_col_len_df(df_prices_w)
print('col_lens -> {}'.format(col_lens))


###############################################
### IMPORT DAILY PRICES AND SUMMARISE WEEKS ###
###############################################

# Import
df_prices_d = pd.read_hdf(
    HIST_PRICES_D)
# Summarise the ratio of pos to neg change day there were in each week
df_prices_d['day_count'] = True
df_prices_d['pos_count'] = df_prices_d.change > 0
df_prices_d = df_prices_d[['ticker', 'week_start_date', 'day_count', 'pos_count']].groupby(
    ['ticker', 'week_start_date']).sum().reset_index()
df_prices_d['pos_day_ratio'] = df_prices_d.pos_count / df_prices_d.day_count


#######################
### CREATE FEATURES ###
#######################

# Create a single function to run each stock through feature creation
def create_features(_df_in):
    """A single function to run each stock through feature creation

    args:
    -----
    _df_in - pandas dataframe

    returns:
    ------
    pandas dataframe
    """

    _df_out = _df_in.copy()

    ### NORMALISING ###

    # Between 0 and 1
    for _col in [
        'close', 'volume'
        # ,'change_close'
        # ,'per_change_close'
        # ,'ema26'
    ]:
        _df_out[_col] = [norm_time_s(
            _x, _df_out[_col], NORM_WINDOW, mode='std') for _x in _df_out.index]
    #Between -1 and 1
    # for _col in [
    #     'macd'
    #     ,'signal_line'
    # ]:
    #     _df_out[_col] = [norm_time_s(_x,_df_out[_col],NORM_WINDOW,_neg_vals=True,_mode='std') for _x in _df_out.index]

    ### PRICES ###

    # Close change vs avg over set period
    # Divide by absolute to see if this is an extreme move and which direction it is in
    _df_out['abs_per_change_close'] = [
        np.abs(x) for x in _df_out.per_change_close]
    for period in PERIOD_LI:
        _df_out['change_vs_{}_wk_avg'.format(period)] = _df_out['per_change_close'] / avg_in_range(
            _df_out['abs_per_change_close'], period, inc_val=False)
    _df_out = _df_out.drop(columns=['abs_per_change_close'])

    # Previous close benchmarked against this close as 1.0 for last n records
    for n in range(1, LOOK_BACK_PRICE_PERIOD+1):
        _df_out['close_vs_shift_{}_bench'.format(
            n)] = _df_out['close'].shift(n) / _df_out['close']

    # Get gradient from previous 2 real min/max
    # Find turn points
    _df_out['real_close_min'] = flag_mins(
        _df_out['close'], period=PERIOD_HIGH_VOLATILITY, cur=False)
    _df_out['real_close_max'] = flag_maxs(
        _df_out['close'], period=PERIOD_HIGH_VOLATILITY, cur=False)
    for max_min in ['max', 'min']:
        # Find the last 2 mins
        _df_out["prev_{}_close".format(max_min)], _df_out["prev_{}_close_date".format(max_min)], _df_out["prev_{}_close_index".format(max_min)] = prev_max_min(
            _df_out[["date", 'close', "real_close_{}".format(max_min)]].copy(), 'close', "real_close_{}".format(max_min), GAP_HIGH_VOLATILITY)
        _df_out["prev_{}_close_change".format(max_min)] = mk_prev_move_float(
            _df_out['prev_{}_close'.format(max_min)])
        _df_out["prev_{}_close_index_change".format(max_min)] = mk_prev_move_float(
            _df_out['prev_{}_close_index'.format(max_min)])
        # Calc the gradient
        _df_out['prev_{}_close_grad'.format(max_min)] = _df_out["prev_{}_close_change".format(
            max_min)] / _df_out["prev_{}_close_index_change".format(max_min)]
        # Count the periods since the changeF
        _df_out['prev_{}_close_period_count'.format(
            max_min)] = _df_out.index - _df_out["prev_{}_close_index".format(max_min)]
        # Calc the projected value and diff to the actual value
        _df_out['prev_{}_projected_close'.format(max_min)] = _df_out["prev_{}_close".format(
            max_min)] + (_df_out['prev_{}_close_period_count'.format(max_min)] * _df_out['prev_{}_close_grad'.format(max_min)])
        _df_out['prev_{}_projected_close_diff'.format(max_min)] = (
            _df_out["close"] - _df_out['prev_{}_projected_close'.format(max_min)]) / _df_out["close"]
        # Keep only the wanted columns - keep grad, period_count, and project_close_diff
        _df_out = _df_out.drop(columns=[
            "prev_{}_close".format(max_min), "prev_{}_close_date".format(max_min), "prev_{}_close_index".format(
                max_min), "prev_{}_close_change".format(max_min), "prev_{}_close_index_change".format(max_min), 'prev_{}_projected_close'.format(max_min)
        ])

    # Get the ratio of positive to negative days in this week
    _df_out = pd.merge(_df_out, df_prices_d[['ticker', 'week_start_date', 'pos_day_ratio']], left_on=[
                       'ticker', 'date'], right_on=['ticker', 'week_start_date'], how='left').drop(columns=['week_start_date'])

    # Get prices ratios
    # Close vs day prices
    _df_out['close_open_ratio'] = _df_out.close / _df_out.open
    _df_out['close_high_ratio'] = _df_out.close / _df_out.high
    _df_out['close_low_ratio'] = _df_out.close / _df_out.low
    # Close vs emas
    for period in PERIOD_LI:
        _df_out['close_ema{}_ratio'.format(
            period)] = _df_out.close / calc_ema(_df_out['close'], period)

    ### VOLUME ###

    # Volume vs avg over set period
    for period in PERIOD_LI:
        _df_out['vol_vs_{}_wk_avg'.format(
            period)] = _df_out['volume'] / avg_in_range(_df_out['volume'], period, inc_val=False)

    ### MACD ###

    # MACD vs avg over set period
    # Divide by absolute to see if this is an extreme move and which direction it is in
    for period in PERIOD_LI:
        _df_out['macd_vs_{}_wk_avg'.format(period)] = _df_out['macd'] / np.abs(
            avg_in_range(_df_out['macd'], period, inc_val=False))

    # Previous macd benchmarked against this macd as 1.0 for last n records
    for n in range(1, LOOK_BACK_PRICE_PERIOD+1):
        _df_out['macd_vs_shift_{}_bench'.format(
            n)] = _df_out['macd'].shift(n) / _df_out['macd']

    # Gradient
    _df_out['macd_grad'] = gradient(_df_out['macd'], period=1)

    # Get gradient from previous 2 real min/max
    # Find turn points
    _df_out['real_macd_min'] = flag_mins(
        _df_out['macd'], period=PERIOD_LOW_VOLATILITY, cur=False)
    _df_out['real_macd_max'] = flag_maxs(
        _df_out['macd'], period=PERIOD_LOW_VOLATILITY, cur=False)
    for max_min in ['max', 'min']:
        # Find the last 2 mins
        _df_out["prev_{}_macd".format(max_min)], _df_out["prev_{}_macd_date".format(max_min)], _df_out["prev_{}_macd_index".format(max_min)] = prev_max_min(
            _df_out[["date", 'macd', "real_macd_{}".format(max_min)]].copy(), 'macd', "real_macd_{}".format(max_min), GAP_LOW_VOLATILITY)
        _df_out["prev_{}_macd_change".format(max_min)] = mk_prev_move_float(
            _df_out['prev_{}_macd'.format(max_min)])
        _df_out["prev_{}_macd_index_change".format(max_min)] = mk_prev_move_float(
            _df_out['prev_{}_macd_index'.format(max_min)])
        # Calc the gradient
        _df_out['prev_{}_macd_grad'.format(max_min)] = _df_out["prev_{}_macd_change".format(
            max_min)] / _df_out["prev_{}_macd_index_change".format(max_min)]
        # Count the periods since the change
        _df_out['prev_{}_macd_period_count'.format(
            max_min)] = _df_out.index - _df_out["prev_{}_macd_index".format(max_min)]
        # Keep only the wanted columns - keep grad, and period_count
        _df_out = _df_out.drop(columns=[
            "prev_{}_macd".format(max_min), "prev_{}_macd_date".format(max_min), "prev_{}_macd_index".format(
                max_min), "prev_{}_macd_change".format(max_min), "prev_{}_macd_index_change".format(max_min)
        ])

    ### SIGNAL LINE ###

    # Signal line vs avg over set period
    # Divide by absolute to see if this is an extreme move and which direction it is in
    for period in PERIOD_LI:
        _df_out['signal_line_vs_{}_wk_avg'.format(period)] = _df_out['signal_line'] / np.abs(
            avg_in_range(_df_out['signal_line'], period, _inc_val=False))

    # Gradient
    _df_out['signal_line_grad'] = gradient(_df_out['signal_line'], _period=1)

    # Get gradient from previous 2 real min/max
    # Find turn points
    _df_out['real_signal_line_min'] = flag_mins(
        _df_out['signal_line'], _period=PERIOD_LOW_VOLATILITY, _cur=False)
    _df_out['real_signal_line_max'] = flag_maxs(
        _df_out['signal_line'], _period=PERIOD_LOW_VOLATILITY, _cur=False)
    for max_min in ['max', 'min']:
        # Find the last 2 mins
        _df_out["prev_{}_signal_line".format(max_min)], _df_out["prev_{}_signal_line_date".format(max_min)], _df_out["prev_{}_signal_line_index".format(max_min)] = prev_max_min(
            _df_out[["date", 'signal_line', "real_signal_line_{}".format(max_min)]].copy(), 'signal_line', "real_signal_line_{}".format(max_min), GAP_LOW_VOLATILITY)
        _df_out["prev_{}_signal_line_change".format(max_min)] = mk_prev_move_float(
            _df_out['prev_{}_signal_line'.format(max_min)])
        _df_out["prev_{}_signal_line_index_change".format(max_min)] = mk_prev_move_float(
            _df_out['prev_{}_signal_line_index'.format(max_min)])
        # Calc the gradient
        _df_out['prev_{}_signal_line_grad'.format(max_min)] = _df_out["prev_{}_signal_line_change".format(
            max_min)] / _df_out["prev_{}_signal_line_index_change".format(max_min)]
        # Count the periods since the change
        _df_out['prev_{}_signal_line_period_count'.format(
            max_min)] = _df_out.index - _df_out["prev_{}_signal_line_index".format(max_min)]
        # Keep only the wanted columns - keep grad, and period_count
        _df_out = _df_out.drop(columns=[
            "prev_{}_signal_line".format(max_min), "prev_{}_signal_line_date".format(max_min), "prev_{}_signal_line_index".format(
                max_min), "prev_{}_signal_line_change".format(max_min), "prev_{}_signal_line_index_change".format(max_min)
        ])

    return _df_out


# Define the columns for the output
out_cols = [
    # NON-NORMALISED COLS
    "ticker", "date"    # STANDARD FEATURES
    # ENGINEERED FEATURES
    , "close", "volume", "change_close", "per_change_close", "ema26", "macd", "signal_line", 'pos_day_ratio', 'close_open_ratio', 'close_high_ratio', 'close_low_ratio'
]
# Add prices columns
for period in PERIOD_LI:
    out_cols.append('change_vs_{}_wk_avg'.format(period))
for n in range(1, LOOK_BACK_PRICE_PERIOD+1):
    out_cols.append('close_vs_shift_{}_bench'.format(n))
for max_min in ['max', 'min']:
    out_cols.append('prev_{}_close_grad'.format(max_min))
    out_cols.append('prev_{}_close_period_count'.format(max_min))
    out_cols.append('prev_{}_projected_close_diff'.format(max_min))
for period in PERIOD_LI:
    out_cols.append('close_ema{}_ratio'.format(period))
# Add volume columns
for period in PERIOD_LI:
    out_cols.append('vol_vs_{}_wk_avg'.format(period))
# Add macd columns
for period in PERIOD_LI:
    out_cols.append('macd_vs_{}_wk_avg'.format(period))
for n in range(1, LOOK_BACK_PRICE_PERIOD+1):
    out_cols.append('macd_vs_shift_{}_bench'.format(n))
out_cols.append('macd_grad')
for max_min in ['max', 'min']:
    out_cols.append('prev_{}_macd_grad'.format(max_min))
    out_cols.append('prev_{}_macd_period_count'.format(max_min))
# Add signal_line columns
for period in PERIOD_LI:
    out_cols.append('signal_line_vs_{}_wk_avg'.format(period))
out_cols.append('signal_line_grad')
for max_min in ['max', 'min']:
    out_cols.append('prev_{}_signal_line_grad'.format(max_min))
    out_cols.append('prev_{}_signal_line_period_count'.format(max_min))
# Append signal
out_cols.append("signal")

# A conversion for all variables ot the correct dtype
conv_di = {
    # NON-NORMALISED COLS
    "ticker": 'object', "date": 'datetime64'    # STANDARD FEATURES
    , "close": 'float64', "volume": 'float64', "change_close": 'float64', "per_change_close": 'float64', "ema26": 'float64', "macd": 'float64', "signal_line": 'float64'    # ENGINEERED FEATURES
    , 'pos_day_ratio': 'float64', 'close_open_ratio': 'float64', 'close_high_ratio': 'float64', 'close_low_ratio': 'float64'
}
# Append additional columns for key areas
# Prices
for period in PERIOD_LI:
    conv_di['change_vs_{}_wk_avg'.format(period)] = 'float64'
for n in range(1, LOOK_BACK_PRICE_PERIOD+1):
    conv_di['close_vs_shift_{}_bench'.format(n)] = 'float64'
for max_min in ['max', 'min']:
    conv_di['prev_{}_close_grad'.format(max_min)] = 'float64'
    conv_di['prev_{}_close_period_count'.format(max_min)] = 'float64'
    conv_di['prev_{}_projected_close_diff'.format(max_min)] = 'float64'
for period in PERIOD_LI:
    conv_di['close_ema{}_ratio'.format(period)] = 'float64'
# Volume
for period in PERIOD_LI:
    conv_di['vol_vs_{}_wk_avg'.format(period)] = 'float64'
# Macd
for period in PERIOD_LI:
    conv_di['macd_vs_{}_wk_avg'.format(period)] = 'float64'
for n in range(1, LOOK_BACK_PRICE_PERIOD+1):
    conv_di['macd_vs_shift_{}_bench'.format(n)] = 'float64'
conv_di['macd_grad'] = 'float64'
for max_min in ['max', 'min']:
    conv_di['prev_{}_macd_grad'.format(max_min)] = 'float64'
    conv_di['prev_{}_macd_period_count'.format(max_min)] = 'float64'
# Signal_line
for period in PERIOD_LI:
    conv_di['signal_line_vs_{}_wk_avg'.format(period)] = 'float64'
conv_di['signal_line_grad'] = 'float64'
for max_min in ['max', 'min']:
    conv_di['prev_{}_signal_line_grad'.format(max_min)] = 'float64'
    conv_di['prev_{}_signal_line_period_count'.format(max_min)] = 'float64'
# Append signal
conv_di["signal"] = 'int'

# Then loop the tickers and combine these into one large dataset
h_store = pd.HDFStore(HF_STORE)
errors = []
run_time = ProcessTime()
# Get the min col lens
min_itemsize_di = {}
for col in out_cols:
    if col in col_lens:
        min_itemsize_di[col] = col_lens[col]
for tick in tick_ftse["ticker"]:
    # for tick in ['VVO','SBRY']: #TEMP
    try:
        print("\n{}".format(len(run_time.lap_li)))
        print("RUN FOR {}".format(tick))
        # Isolate this ticker
        this_tick_df = df_prices_w[df_prices_w["ticker"]
                                   == re.sub('[^a-zA-Z0-9\-]', '', tick)].copy()
        print("shape before -> {}".format(this_tick_df.shape))
        # Create the features
        this_tick_df = create_features(this_tick_df)
        # Clarify col_lens with cur cols in data
        this_col_lens = get_col_len_df(this_tick_df)
        for col in out_cols:
            if col in min_itemsize_di:
                if this_col_lens[col] > min_itemsize_di[col]:
                    min_itemsize_di[col] = this_col_lens[col]
            else:
                min_itemsize_di[col] = this_col_lens[col]
        print("shape after -> {}".format(this_tick_df.shape))
        # Create function for appending to hdf file

        def append_to_hdf(df_in):
            df_in[out_cols].to_hdf(
                HF_STORE, key='data', append=True, min_itemsize=min_itemsize_di)
        # Append this data to the group
        try:
            append_to_hdf(this_tick_df)
            print('ADDED TO {}'.format(HF_STORE))
        except ValueError:
            print('WARNING -> Attempting to change dtypes')
            # Try changing the dtypes
            try:
                for col in out_cols:
                    # print(r'CONVERT {} FROM {} TO {}'.format(col,this_tick_df[col].dtype,conv_di[col]))
                    this_tick_df[col] = this_tick_df[col].astype(conv_di[col])
                append_to_hdf(this_tick_df)
                print('ADDED TO {}'.format(HF_STORE))
            except Exception as e:
                errors.append({"ticker": tick, "Error": e})
                print('ERROR READING TO FILE {}'.format(e))
        except Exception as e:
            errors.append({"ticker": tick, "Error": e})
            print('ERROR READING TO FILE {}'.format(e))
        # Lap
        run_time.lap()
        run_time.show_latest_lap_time()
    except Exception as e:
        h_store.close()
        errors.append({"ticker": tick, "Error": e})
        print('ERROR PROCESSING DATA {}'.format(e))
h_store.close()
print('\n\n')
run_time.end()
print('\nERROR COUNT -> {}'.format(len(errors)))
if len(errors) > 0:
    print('\tERRORS -> ')
    print(pd.DataFrame(errors))

# Check the final tmp table
tmp_df = pd.read_hdf(HF_STORE, key='data', mode='r')
print("")
print("FINAL HDFSTORE SIZE: {}".format(tmp_df.shape))
print("FINAL SIGNAL COUNTS: \n{}".format(tmp_df.signal.value_counts()))
print("FINAL NULL COUNTS: \n{}".format(tmp_df.isnull().sum()))
h_store.close()

# Delete the old h5 file and rename the TMP
replace_file(FT_ENG_W, HF_STORE)

# Remove 'date' from out_cols
out_cols.remove('date')

# Export a list of the features for this model
file_object = open(FT_ENG_COL_LIST, 'w')
feature_str = ''
for i in out_cols:
    feature_str += '{},'.format(i)
feature_str = feature_str[:-1]
file_object.write(feature_str)
file_object.close()
feature_str
