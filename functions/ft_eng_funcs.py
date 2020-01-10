#Import libraries
import pandas as pd
import numpy as np
import datetime as dt
# from rf_modules import *



####################
### EMA AND MACD ###
####################

#Function for calculating ema
def calc_ema(_s_in,_periods):
    """Function used to create EMA for a series
    
    args:
    -----
    _s_in - pandas series - series of float values
    _periods - int - value describing how far to look at for EMA calc
    
    returns:
    ------
    pandas series    
    """
    #Calc mod val
    _mod = 2/(_periods+1)
    #Calc sma
    _sma_s = [0] * _s_in.shape[0]
    for _i in range(0,_periods):
        _sma_s += _s_in.shift(_i) / _periods
    #Calc ema
    _ema_s = _sma_s.copy()
    _ema_s[(_ema_s > 0) & (np.isnan(_ema_s) == False)] = _mod*(_s_in - _ema_s.shift(1)) + _ema_s.shift(1)
    return _ema_s.copy()

#Function for calculating the MACD
def calc_macd(_ema_lng_s,_ema_sht_s,_sig_period:int):
    """Function used to create MACD for a series
    
    args:
    -----
    _ema_lng_s - pandas series - series of float values for the long term EMA 
    _ema_sht_s - pandas series - series of float values for the short term EMA
    _sig_period - int - value describing how far to look at for MACD calc
    
    returns:
    ------
    tuple of pandas series,pandas series,pandas series - MACD line, signal line, macd histogram  
    """
    #Make a df
    _tmp_df = pd.DataFrame([])
    _tmp_df['ema_lng'] = _ema_lng_s
    _tmp_df['ema_sht'] = _ema_sht_s
    #Calc the signal line
    _tmp_df['macd_line'] = _tmp_df['ema_sht'] - _tmp_df['ema_lng']
    _tmp_df['signal_line'] = calc_ema(_tmp_df['macd_line'],_sig_period)
    _tmp_df['macd_hist'] = _tmp_df['macd_line'] - _tmp_df['signal_line']
    return (_tmp_df['macd_line'].copy(),_tmp_df['signal_line'].copy(),_tmp_df['macd_hist'].copy())

#Calc the ema and macds for the data
def calc_ema_macd(_df_in):
    """Function used to call EMA and MACD functions
    
    args:
    -----
    _df_in - pandas dataframe - must have columns 'close' and 'date' 
    
    returns:
    ------
    pandas dataframe - with new columns for ema12,ema26,macd_line,signal_line,macd
    """
    _tick_df = _df_in.copy()
    try:
        #Add in the ema and macd
        _tick_df = _tick_df.sort_values(by='date')
        _tick_df['ema12'] = calc_ema(_tick_df['close'],12)
        _tick_df['ema26'] = calc_ema(_tick_df['close'],26)
        _tick_df['macd_line'],_tick_df['signal_line'],_tick_df['macd'] = calc_macd(_tick_df['ema26'],_tick_df['ema12'],9)
        return _tick_df
    except Exception as e:
        print('ERROR:{}'.format(e))
        return _tick_df



###################
### NORMALISING ###
###################

#Create a function which normalises a feature based only on the values which have come before it - avoids time series bias
def norm_time_s(_ind:int,_s_in,_window:int,_neg_vals:bool=False,_mode:str='max_min',_return_series:bool=False,_fill_window:bool=False):
    """Normalise a value within over a time period
    
    args:
    -----
    _ind - int - the index of this value in the series
    _s_in - pandas series - a series of values to be normalised
    _window - int - the number of values to look over
    _neg_vals - bool:False - is the output to accunt for the sign of values
    _mode  str:max_min - should the normalisation be done by max mins or standard deviation
    _return_series - bool:Fasle - Should the return be a series or a value
    _fill_window - bool:False - Should the returned series be bulked up to fit the window

    returns:
    ------
    float - normalised value in the window period
    """
    #Establish the index window
    _this_ind = _ind - _s_in.index.min()
    if _this_ind < _window:
        _st_ind = 0
    else:
        _st_ind = _this_ind - _window + 1
    _s = _s_in[_st_ind:_this_ind+1]
    if _return_series:
        _v = _s
    else:
        _v = _s_in[_ind]
    #Normalise the value
    if _mode == 'max_min':
        _min = np.nanmin(_s.values)
        _max = np.nanmax(_s.values)
        #If accounting for neg_vals then adjust _max and _min to allow this
        # This method allows values to be normalised and be relative to each other (IE -25 is half the magnitude of -50 and 50)
        if _neg_vals:
            _max = np.nanmax([np.abs(_min),_max])
            _min = 0
        _norm_val = (_v - _min) / (_max - _min)
    elif _mode == 'std':
        if _neg_vals:
            if _v < 0:
                _s = _s[_s <= 0]
            else:
                _s = _s[_s >= 0]
            _mean = np.nanmean(_s.values)
            _std = np.nanstd(_s.values)
            _norm_val = (_v - _mean) / _std
        else:
            _mean = np.nanmean(_s.values)
            _std = np.nanstd(_s.values)
            _norm_val = (_v - _mean) / _std
    else:
        raise ValueError('mode must be "std" or "max_min", {} given'.format(_mode))
    if _return_series and _fill_window:
            _leading_s = pd.Series([np.nan] * (_window - _norm_val.shape[0]))
            _norm_val = _leading_s.append(_norm_val)
    return _norm_val

#Run the functions
def norm_prices(_df_in,_norm_window:int):
    """Function used to normalise all prices in the dataframe
    
    args:
    -----
    _df_in - pandas dataframe - must contain values 'open','close','high','low','volume'
    _norm_window - int - the period over which values will be normalised
    
    returns:
    ------
    pandas dataframe - with normalised values for 'open','close','high','low','volume'
    """
    _df_out = _df_in.copy()
    #Normalise the columns which need it
    _norm_cols = [
        #Standard features
        "open"
        ,"close"
        ,"high"
        ,"low"
        ,"volume"
    ]
    #Reset the index
    _df_out.sort_values(['date'],ascending=True,inplace=True)
    #Normalise
    for _col in _norm_cols:
        _df_out["{}_orig".format(_col)] = _df_out[_col].copy() #Take a copy so as the values are changed this does not affect following calculations
        _df_out[_col] = [norm_time_s(_x,_df_out["{}_orig".format(_col)],_norm_window) for _x in _df_out.index]
    return _df_out

#Get in-row price change
def calc_changes(_s_in,_prev_s_in):
    """Function used to calculate the change between two values, absolute and percentage
    
    args:
    -----
    _s_in - pandas series - the current value to be compared
    _prev_s_in - pandas series - the base value to compare the values to
    
    returns:
    ------
    tuple - pandas series, pandas series - absolute change, percentage change
    """
    _s_change = _s_in - _prev_s_in
    _s_per_change = _s_change / _s_in
    return (_s_change,_s_per_change)



##########################
### BUY SELL FUNCTIONS ###
##########################

#Check if the target price is hit within the target_price_period
def min_gain_check(_var_s,_target_s,_periods:int=12):
    """Function used to check if the value meets the min gain criteria
    
    args:
    -----
    _var_s - pandas series - value to be compared
    _target_s - pandas series - the target value to be hit
    _periods - int - time period to check for gain over
    
    returns:
    ------
    pandas series - bools
    """
    _check_s = [False] * _var_s.shape[0]
    for _i in range(1,_periods+1):
        _tmp_check_s = _var_s.shift(-_i) > _target_s #True if price is >= limit
        _check_s = _check_s | _tmp_check_s
    return _check_s

def max_drop_check(_var_s,_target_s,_periods:int=12):
    """Function used to check if the value meets the max drop criteria
    
    args:
    -----
    _var_s - pandas series - value to be compared
    _target_s - pandas series - the target value to be hit
    _periods - int - time period to check for gain over
    
    returns:
    ------
    pandas series - bools
    """
    _check_s = [False] * _var_s.shape[0]
    for _i in range(1,_periods+1):
        _tmp_check_s = _var_s.shift(-_i) < _target_s #True if price is <= limit
        _check_s = _check_s | _tmp_check_s
    return _check_s

def close_vs_close(_var_s,_shift:int=1):
    """Function used to calculate the change over a given period
    
    args:
    -----
    _var_s - pandas series - values to be compared
    _shift - int - time period to shift _var_s over for comparison
    
    returns:
    ------
    pandas series - floats
    """
    _check_s = _var_s.shift(_shift) - _var_s
    return _check_s

#Create a function for finding buy signals
def get_buys(_var_s,_period,_min_gain,_max_drop):
    """Function used to find if a value meets the requirements for a buy signal
    
    args:
    -----
    _var_s - pandas series - values to be checked
    _period - int - window in which values must be reached or avoided
    _min_gain - float - the minimum gain to be hit (%)
    _max_drop - float - the maximum drop allowed (%)
    
    returns:
    ------
    pandas series - bools
    """
    
    #Check if the target price is hit within the period
    target_s = _var_s * (1+_min_gain)
    min_gain_s = min_gain_check(_var_s,target_s,_period) == True #Function returns True when min_gain is hit
    print('BUY min_gain_s -> {}'.format(min_gain_s[min_gain_s == True].shape))
    
    #Check if the sell price is hit within the period
    target_s = _var_s * (1+_max_drop)
    max_drop_s = max_drop_check(_var_s,target_s,_period) == False #Function returns False when does not go below target
    print('BUY max_drop_s -> {}'.format(max_drop_s[max_drop_s == True].shape))
    
    #Check if the following day is a positive change on today's close price
    close_vs_close_pos_s = close_vs_close(_var_s,-1) > 0
    print('BUY close_vs_close_pos_s -> {}'.format(close_vs_close_pos_s[max_drop_s == True].shape))
    
    #Find the buy signals
    s_out = min_gain_s & max_drop_s & close_vs_close_pos_s
    print('BUY ALL -> {}'.format(s_out[s_out == True].shape))
    
    return s_out

#Function for finding sell signals
def get_sells(_var_s,_period,_min_gain,_max_drop):
    """Function used to find if a value meets the requirements for a sell signal
    
    args:
    -----
    _var_s - pandas series - values to be checked
    _period - int - window in which values must be reached or avoided
    _min_gain - float - the minimum gain to be hit (%)
    _max_drop - float - the maximum drop allowed (%)
    
    returns:
    ------
    pandas series - bools
    """
    
    #Check if the target price is hit within the period
    target_s = _var_s * (1+_max_drop)
    max_drop_s = max_drop_check(_var_s,target_s,_period) == True #Function returns True when max_drop is hit
    print('SELL max_drop_s -> {}'.format(max_drop_s[max_drop_s == True].shape))
    
    #Perform if the target is crossed again
    target_s = _var_s * (1+_min_gain)
    min_gain_s = min_gain_check(_var_s,target_s,_period) == False #Function returns False when min_gain is not hit
    print('SELL min_gain_s -> {}'.format(min_gain_s[min_gain_s == True].shape))
    
    #Check if the following day is a negative change on today's close price
    close_vs_close_neg_s = close_vs_close(_var_s,-1) < 0
    print('SELL close_vs_close_pos_s -> {}'.format(close_vs_close_neg_s[max_drop_s == True].shape))
    
    #Find the sell signals
    s_out = max_drop_s & min_gain_s & close_vs_close_neg_s
    print('SELL ALL -> {}'.format(s_out[s_out == True].shape))
    
    return s_out



#####################
### MINS AND MAXS ###
#####################

#Mark minimums and maximums
def flag_mins(_s_in,_period:int=3,_gap:int=3,_cur:bool=False):
    """Function used to identify values in a series as mins
    
    args:
    -----
    _s_in - pandas series - values to be compared
    _period - int:3 - window to check values over
    _gap - int:3 - the number of period which must have elapsed before a min is 
        identified (prevents changing of min_flags on current week vs same week next week)
    _cur - bool:False - is this looking at current or past values
    
    returns:
    ------
    pandas series - bools
    """
    _s_out = 0
    #Adjust the series input if looking at the current values (IE not able to see the future)
    if _cur:
        _s_in = _s_in.shift(_gap)
    #Looking back - check within window
    for i in range(1,_period+1):
        _s_out += (_s_in > _s_in.shift(i)) | (_s_in.shift(i).isnull())
    
    #Looking forwards
    if _cur:
        #Check within gap
        for i in range(1,_gap+1):
            _s_out += (_s_in > _s_in.shift(-i))
    else:
        #Check within forwardlooking periods
        for i in range(1,_period+1):
            _s_out += (_s_in > _s_in.shift(-i)) | (_s_in.shift(-i).isnull())
    
    #Check end series
    _s_out = _s_out == 0
    
    return _s_out

def flag_maxs(_s_in,_period:int=3,_gap:int=0,_cur:bool=False):
    """Function used to identify values in a series as maxs
    
    args:
    -----
    _s_in - pandas series - values to be compared
    _period - int:3 - window to check values over
    _gap - int:3 - the number of period which must have elapsed before a min is 
        identified (prevents changing of min_flags on current week vs same week next week)
    _cur - bool:False - is this looking at current or past values
    
    returns:
    ------
    pandas series - bools
    """
    _s_out = 0
    #Adjust the series input if looking at the current values (IE not able to see the future)
    if _cur:
        _s_in = _s_in.shift(_gap)
    #Looking back - check within window
    for i in range(1,_period+1):
        _s_out += (_s_in < _s_in.shift(i)) | (_s_in.shift(i).isnull())
    
    #Looking forwards
    if _cur:
        #Check within gap
        for i in range(1,_gap+1):
            _s_out += (_s_in < _s_in.shift(-i))
    else:
        #Check within forwardlooking periods
        for i in range(1,_period+1):
            _s_out += (_s_in < _s_in.shift(-i)) | (_s_in.shift(-i).isnull())
    _s_out = _s_out == 0
    return _s_out

#Function to find last max and mins
def prev_max_min(_df_in,_var_col,_bool_col,_gap:int=0):
    """Function to find last max and mins
    
    args:
    -----
    _df_in - pandas dataframe - must contain 'date', _var_col and _bool_col as column names
    _var_col - str - the name of the column containing the current variables
    _bool_col - str - the name of the column containing the bool values defining max and min vlaues
    _gap - int:0 - the number of period which must have elapsed before a min is 
        identified (means that when you're in that week you can't tell if it's a min/max)
    
    returns:
    ------
    tuple - pandas series,pandas series,pandas series - last max/min value, last max/min date, last max/min index
    """
    _df_in["prev_val"] = _df_in.loc[_df_in[_bool_col].fillna(False),_var_col]  
    _df_in["prev_val"] = _df_in["prev_val"].fillna(method='ffill').shift(_gap)#Shift _gap allows offset
    _df_in["prev_marker_date"] = _df_in.loc[_df_in[_bool_col].fillna(False),"date"]
    _df_in["prev_marker_date"] = _df_in["prev_marker_date"].fillna(method='ffill').shift(_gap)#Shift _gap allows offset
    _df_in['index'] = _df_in.index
    _df_in["prev_marker_index"] = _df_in.loc[_df_in[_bool_col].fillna(False),'index']
    _df_in["prev_marker_index"] = _df_in["prev_marker_index"].fillna(method='ffill').shift(_gap)#Shift _gap allows offset
    return (_df_in["prev_val"],_df_in["prev_marker_date"],_df_in["prev_marker_index"])

#Function for finding the max within a given time period using indexes
def max_min_period(_s_in,_period:int=1,_normalise:bool=False,_max_min:str='max'):
    """Function for calculating the max and mins within a period
    
    args:
    -----
    _s_in - pandas series - the vlaues to be looked at
    _period - int:1 - the time window to look over
    _max_min - str:max - looking for the max or min
    _normalise - bool:False - should the returned value be normalised?
    
    returns:
    ------
    pandas series - floats
    """
    #Find the min index
    _min_i = _s_in.index.min()
    if _normalise:
        _s_max = pd.Series([_s_in.loc[x-_period if x-_period >= _min_i else _min_i:x].max() for x in _s_in.index])
        _s_min = pd.Series([_s_in.loc[x-_period if x-_period >= _min_i else _min_i:x].min() for x in _s_in.index])
        _s_out = pd.Series((_s_in - _s_min) / (_s_max - _s_min))
    else:
        #Get the max or min within a time period, ensuring not to go into negative indexes
        if _max_min == 'max':
            _s_out = pd.Series([_s_in.loc[x-_period if x-_period >= _min_i else _min_i:x].max() for x in _s_in.index])
        elif _max_min == 'min':
            _s_out = pd.Series([_s_in.loc[x-_period if x-_period >= _min_i else _min_i:x].min() for x in _s_in.index])
        else:
            raise ValueError('_max_min must be either \'max\' or \'min\'')
    _s_out.index = _s_in.index
    return _s_out



#################
### GRADIENTS ###
#################

#Function for finding the gradient of a variable overa set period
def gradient(_s_in,_period:int=1):
    """Function for finding the gradient of a variable over a set period
    
    args:
    -----
    _s_in - pandas series - the series from which the gradient will be found
    _period - int:1 - the period over which the gradient will be found
    
    returns:
    ------
    pandas series
    """
    _s_out = _s_in - _s_in.shift(_period)
    return _s_out



###########################
### PROPORTIONAL VALUES ###
###########################

#Calc vol as proportion of previous n-rows
def calc_prop_of_prev(_s_in,_periods:int = 4):
    """Function to this value as a proportion of the cum previous values
    
    args:
    -----
    _s_in - pandas series - values to be looked at
    _period - int - window to sum values over
    
    returns:
    ------
    pandas series - floats
    """
    _s_cum = _s_in.copy()
    for i in range(1,_periods):
        _s_cum += _s_in.shift(i)
    return _s_in / _s_cum

#Function for calculating the percentage change within a range
def per_change_in_range(_s_in,_period:int=1,**kwargs):
    """Function for calculating the percentage change of a value from it's max or min within a range
    
    args:
    -----
    _s_in - pandas series - the values to be looked at
    _period - int:1 - the time window to look over
    
    returns:
    ------
    pandas series - floats
    """
    return ((_s_in - max_min_period(_s_in,_period,_normalise=False,**kwargs)) / max_min_period(_s_in,_period,_normalise=False,**kwargs))

def avg_in_range(_s_in,_period:int=1,_inc_val:bool=True):
    """Function for calculating average within a range
    
    args:
    -----
    _s_in - pandas series - the values to be looked at
    _period - int:1 - the time window to look over
    _inc_val - bool:True - should the average include the subject value
    
    returns:
    ------
    pandas series - floats
    """
    if _inc_val:
        _s_out = [_s_in.iloc[x-_period+1:x+1].mean() if x-_period+1 > 0 else _s_in.iloc[:x+1].mean() for x in range(_s_in.shape[0])]
    else:
        _s_out = [_s_in.iloc[x-_period:x].mean() if x-_period > 0 else _s_in.iloc[:x].mean() for x in range(_s_in.shape[0])]

    return _s_out



#########################
### POSITIVE NEGATIVE ###
#########################

#Mark points of macd positive entry
def pos_entry(_s_in):
    """Function to check if this value is a new positive after a negative value
    
    args:
    -----
    _s_in - pandas series - values to be looked at
    
    returns:
    ------
    pandas series - bools
    """
    return (_s_in > _s_in.shift(1)) & (_s_in > 0) & (_s_in.shift(1) < 0)

def neg_entry(_s_in):
    """Function to check if this value is a new negative after a positive value
    
    args:
    -----
    _s_in - pandas series - values to be looked at
    
    returns:
    ------
    pandas series - bools
    """
    return (_s_in < _s_in.shift(1)) & (_s_in < 0) & (_s_in.shift(1) > 0)

#Create separate columns for pos and neg values - allows for normalisation
def pos_neg_cols(_s_in,_gt_lt = "GT"):
    """Function to separate columns for pos and neg values - allows for normalisation
    
    args:
    -----
    _s_in - pandas series - the vlaues ot be looked at
    _gt_lt - str:'GT' - defines if looking for positive or negative values
    
    returns:
    ------
    tuple - pandas series,pandas series - bools,floats
    """
    if _gt_lt.upper() == "GT":
        _bool_s = _s_in >= 0
    elif _gt_lt.upper() == "LT":
        _bool_s = _s_in <= 0
    _df_out = _s_in.to_frame()
    _df_out["_s_in"] = _s_in
    _df_out["_val"] = abs(_s_in[_bool_s])
    _val_s = _df_out["_val"].fillna(0,method=None)
    return (_bool_s,_val_s)



#######################
### PREVIOUS VALUES ###
#######################

def mk_prev_move_float(_s_in):
    """Function to find the the magnitude of the most recent value change.
    
    args:
    ------
    _s_in - pandas series - float values
    
    returns:
    ------
    pandas series - float values
    """
    _s_out = _s_in - _s_in.shift(1)
    _s_out[_s_out == 0] = np.nan
    _s_out = _s_out.fillna(method='ffill')
    return _s_out

def mk_prev_move_date(_s_in,_periods:int=7):
    """Function to find the time elapsed between two different changes.
    
    args:
    ------
    _s_in - pandas dataframe - datetime values
    _periods - int:7 - used to modify days of datetime into the period required
    
    
    returns:
    ------
    pandas series - int values
    """
    _s_out = _s_in - _s_in.shift(1)
    _s_check = pd.Series([np.floor(_x.days) for _x in _s_out])
    _s_check[_s_check == 0] = np.nan
    _s_check = _s_check.fillna(method='ffill')
    _s_check = [np.floor(_x/_periods) for _x in _s_check]
    return _s_check

#Create features for the cumulative sequential count of max/mins in a certain direction
def mk_move_cum(_s_in):
    """Function for counting the number of changes of the same sign sequentially.
    EG how many positive moves have there been in a row.
    
    args:
    ------
    _s_in - pandas series - floats
    
    returns:
    pandas series - floats
    """
    _li_out = []
    _prev_x = None
    #Loop through each value in _s_in
    for _i,_x in _s_in.iteritems():
        if np.isnan(_x) or _prev_x == None: #If this is the first value add it to the list
            _li_out.append(0)
        else:
            _prev_x = _prev_x if not np.isnan(_prev_x) else 0
            if ((_x < 0) & (_prev_x > 0)) or ((_x > 0) & (_prev_x < 0)): #If a sign change then reset to 0
                _li_out.append(0)
            else:
                if _prev_x != _x: #if there has been a change in value from this and the previous value increment it by 1
                    if _x > 0: #for positive value increment by 1
                        _li_out.append(_li_out[-1] + 1)                                    
                    else: #for negative values increment by -1
                        _li_out.append(_li_out[-1] - 1)
                else: #Otherwise just use the last added value
                    _li_out.append(_li_out[-1])
        _prev_x = _x
    return _li_out

#Create features showing the value change since the first min/max
def mk_long_prev_move_float(_ref_s,_val_s):
    """Function to find the value change since the first max/min move in the current sequential series.
    
    args:
    ------
    _ref_s - pandas series - the reference series from which changes will be detected
    _val_s - pandas series - the values series from which outputs will be created
    
    returns:
    ------
    pandas series - float values
    """
    _li_out = []
    _st_x = None
    _prev_x = None
    #Loop through each value in _s_in
    for _i,_x in _ref_s.iteritems():
        if np.isnan(_x) or _prev_x == None: #If this is the first value add it to the list
            _li_out.append(0)
        else:
            _prev_x = _prev_x if not np.isnan(_prev_x) else 0
            if ((_x < 0) & (_prev_x > 0)) or ((_x > 0) & (_prev_x < 0)): #If a sign change then reset to 0
                _li_out.append(0)
                _st_x = None
            else:
                if _st_x == None: #If _st_x has not been set yet set it to this value
                    _st_x = _val_s[_i]
                _li_out.append(_val_s[_i] - _st_x) #Now calculate the difference and add it to the list
        _prev_x = _x
    return _li_out

def mk_long_prev_move_date(_ref_s,_val_s,_periods:int=7):
    """Function to find the date change since the first max/min move in the current sequential series.
    
    args:
    ------
    _ref_s - pandas series - the reference series from which changes will be detected
    _val_s - pandas series - the values series from which outputs will be created
    _periods - int:7 - used to modify days of datetime into the period required
    
    returns:
    ------
    pandas series - int values
    """
    _li_out = []
    _st_x = None
    _prev_x = None
    #Loop through each value in _s_in
    for _i,_x in _ref_s.iteritems():
        if np.isnan(_x) or _prev_x == None: #If this is the first value add it to the list
            _li_out.append(0)
        else:
            _prev_x = _prev_x if not np.isnan(_prev_x) else 0
            if ((_x < 0) & (_prev_x > 0)) or ((_x > 0) & (_prev_x < 0)): #If a sign change then reset to 0
                _li_out.append(0)
                _st_x = None
            else:
                if _st_x == None: #If _st_x has not been set yet set it to this value
                    _st_x = _val_s[_i]
                _li_out.append(np.floor((_val_s[_i] - _st_x).days/_periods)) #Now calculate the difference and add it to the list
        _prev_x = _x
    return _li_out



######################
### COLUMN LENGTHS ###
######################

#Create a dictionary of max character lengths of fields for use later in h5 file appending
def get_col_len_s(_s_in):
    """Get the max length of value in the series
    
    args:
    -----
    _s_in - pandas series - series holding values to look at for max field lengths
    
    returns:
    ------
    float
    """
    _tmp_s = pd.Series([len(str(x)) for x in _s_in])
    return _tmp_s.max()
    
def get_col_len_df(_df_in):
    """Create a dictionary of max character lengths of fields for use later in h5 file appending
    
    args:
    -----
    _df_in - pandas dataframe - dataframe holding values to look at for max field lengths
    
    returns:
    ------
    dictionary
    """
    _col_lens = {}
    for _c in _df_in:
        _col_lens[_c] = get_col_len_s(_df_in[_c])
    return _col_lens


