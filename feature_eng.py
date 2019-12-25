"""Feature engineering for stock prices"""

#Import libraries
import pandas as pd
import numpy as np
import re
import tables
import os
import datetime as dt

from rf_modules import *

from functions.ft_eng_funcs import *
from config import CONFIG

#Programming note
#df.shift(1) looks 1 period into the past
#df.shift(-1) looks 1 period into the future



######################
### IMPORTING DATA ###
######################

#Import the ftse list
tick_ftse = pd.read_csv(CONFIG['files']['store_path'] + CONFIG['files']['tick_ftse'])
tick_ftse = tick_ftse.iloc[:,1:]
for col in tick_ftse:
    tick_ftse.rename(columns={col:clean_col_name(col)},inplace=True)
tick_ftse.head()
tick_ftse['ticker'] = [re.sub('(?=[0-9A-Z])*\.(?=[0-9A-Z]+)','-',tick) for tick in tick_ftse['ticker']]
tick_ftse['ticker'] = [re.sub('[^0-9A-Z\-]','',tick) for tick in tick_ftse['ticker']]

#Import and combine prices files
df_prices_w = pd.read_hdf(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w'])
#Rename columns
for col in df_prices_w:
    df_prices_w.rename(columns={col:clean_col_name(col)},inplace=True)
#Drop unwanted columns
try:
    df_prices_w.drop(columns=["unnamed_0","index"],inplace=True)
except Exception as e:
    print(e)
#Reformat columns where neccessary
df_prices_w["date"] = df_prices_w["date"].astype("datetime64")
print(df_prices_w.shape)
print(df_prices_w.dtypes)
print(df_prices_w.head())



#############################
### BUY HOLD SELL SIGNALS ###
#############################

#Set records into the correct order
df_prices_w = df_prices_w.sort_values(['ticker','date'],ascending=[True,True])
df_prices_w.reset_index(inplace=True,drop=True)

#Calc a column for will the price increase next week
df_prices_w['signal'] = (df_prices_w.close.shift(-CONFIG['feature_eng']['target_price_period']) - df_prices_w.close) > 0
print('df_prices_w.signal.value_counts() -> \n{}'.format(df_prices_w.signal.value_counts()))

#Get buy signals
df_prices_w['buy'] = get_buys(df_prices_w['close'],CONFIG['feature_eng']['target_price_period'],CONFIG['feature_eng']['min_gain'],CONFIG['feature_eng']['max_drop'])

#Get sell signals
df_prices_w['sell'] = get_sells(df_prices_w['close'],CONFIG['feature_eng']['target_price_period'],CONFIG['feature_eng']['min_gain'],CONFIG['feature_eng']['max_drop'])

#Get hold signals
df_prices_w["hold"] = (df_prices_w["buy"] == False) & (df_prices_w["sell"] == False)

print('BUY PERCENTAGE -> {:.2f}%'.format(df_prices_w[df_prices_w['buy'] == True].shape[0]*100/df_prices_w.shape[0]))
print('SELL PERCENTAGE -> {:.2f}%'.format(df_prices_w[df_prices_w['sell'] == True].shape[0]*100/df_prices_w.shape[0]))
print('HOLD PERCENTAGE -> {:.2f}%'.format(df_prices_w[df_prices_w['hold'] == True].shape[0]*100/df_prices_w.shape[0]))



#######################################
### FILTER OUT SHORT HISTORY SHARES ###
#######################################

#Remove tickers with fewer than 34 entries as this is where the MACD can be calculated
print('START ROW COUNT -> {}'.format(df_prices_w.shape[0]))
print('START TICK COUNT -> {}'.format(tick_ftse.shape[0]))
for tick in tick_ftse.ticker:
    print(tick,' ->',df_prices_w[df_prices_w.ticker == tick].shape[0])
    if df_prices_w[df_prices_w.ticker == tick].shape[0] < 34:
        #Remove from dataframe
        print('\tTOO FEW RECORDS FOR {}'.format(tick))
        df_prices_w = df_prices_w.loc[df_prices_w.ticker != tick]
        print('\tNEW ROW COUNT -> {}'.format(df_prices_w.shape[0]))
        #Remove from tick_ftse
        tick_ftse = tick_ftse.loc[tick_ftse.ticker != tick]
        print('\tNEW TICK COUNT -> {}'.format(tick_ftse.shape[0]))
print('\nEND ROW COUNT -> {}'.format(df_prices_w.shape[0]))
print('END TICK COUNT -> {}'.format(tick_ftse.shape[0]))



############################
### NORMALISE THE PRICES ###
############################

# Normalize the prices by ticker and time then create emas and macds for each ticker
print('NORALISING AND CALCULATING EMA & MACD VALUES')
error_li = []
run_time = process_time()
run_time.lap()
new_df = pd.DataFrame([])
for tick in tick_ftse.ticker:
    print('\nRUN FOR {} - {}'.format(tick,len(run_time.lap_li)))
    try:
        this_tick_df = df_prices_w[df_prices_w.ticker == tick]
        # this_tick_df = norm_prices(this_tick_df.copy(),5*52)
        #Calculate the ema and macd
        this_tick_df = calc_ema_macd(this_tick_df)
        #Append back on to the dataframe
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

#Relabel col names
for col in df_prices_w:
    df_prices_w.rename(columns={col:col.lower()},inplace=True)
    
df_prices_w["change_close"],df_prices_w["per_change_close"] = calc_changes(df_prices_w.close.copy(),df_prices_w.open.copy())
print(df_prices_w.head())

#Sort values by ticker and date to ensure sequential records
df_prices_w = df_prices_w.sort_values(['ticker','date'],ascending=[True,True])
df_prices_w.reset_index(inplace=True,drop=True)
print(df_prices_w.ticker.unique())
print(df_prices_w.head())

#Get column lengths
col_lens = get_col_len_df(df_prices_w)
print(col_lens)



#######################
### CREATE FEATURES ###
#######################

#Create a single function to run each stock through feature creation
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

    ### PRICES ###

    #Close change vs avg over set period
    #Divide by absolute to see if this is an extreme move and which direction it is in
    for period in CONFIG['feature_eng']['period_li']:
        _df_out['change_vs_{}_wk_avg'.format(period)] = _df_out['per_change_close'] / np.abs(avg_in_range(_df_out['per_change_close'],period))
    
    #Previous close benchmarked against tis close as 1.0 for last n records
    for n in range(1,CONFIG['feature_eng']['look_back_price_period']+1):
        _df_out['close_vs_shift_{}_bench'.format(n)] = _df_out['close'].shift(n) / _df_out['close']

    #Get gradient from previous 2 real min/max
    #Find turn points
    _df_out['real_close_min'] = flag_mins(_df_out['close'],_period=CONFIG['feature_eng']['period_high_volatility'],_cur=False)
    _df_out['real_close_max'] = flag_maxs(_df_out['close'],_period=CONFIG['feature_eng']['period_high_volatility'],_cur=False)
    for max_min in ['max','min']:
        #Find the last 2 mins
        _df_out["prev_{}_close".format(max_min)],_df_out["prev_{}_close_date".format(max_min)],_df_out["prev_{}_close_index".format(max_min)] = prev_max_min(_df_out[["date",'close',"real_close_{}".format(max_min)]].copy(),'close',"real_close_{}".format(max_min))
        _df_out["prev_{}_close_change".format(max_min)] = mk_prev_move_float(_df_out['prev_{}_close'.format(max_min)])
        _df_out["prev_{}_close_index_change".format(max_min)] = mk_prev_move_float(_df_out['prev_{}_close_index'.format(max_min)])
        #Calc the gradient
        _df_out['prev_{}_grad'.format(max_min)] = _df_out["prev_{}_close_change".format(max_min)] / _df_out["prev_{}_close_index_change".format(max_min)]
        #Count hte periods since the change
        _df_out['prev_{}_period_count'.format(max_min)] = _df_out.index - _df_out["prev_{}_close_index".format(max_min)]
        #Calc the projected value and diff to the actual value
        _df_out['prev_{}_projected_close'.format(max_min)] = _df_out["prev_{}_close".format(max_min)] + (_df_out['prev_{}_period_count'.format(max_min)] * _df_out['prev_{}_grad'.format(max_min)])
        _df_out['prev_{}_projected_close_diff'.format(max_min)] = _df_out["close"] - _df_out['prev_{}_projected_close'.format(max_min)]
        #Keep only the wanted columns - keep grad, period_count, and project_close_diff
        _df_out = _df_out.drop(columns=[
            "prev_{}_close".format(max_min)
            ,"prev_{}_close_date".format(max_min)
            ,"prev_{}_close_index".format(max_min)
            ,"prev_{}_close_change".format(max_min)
            ,"prev_{}_close_index_change".format(max_min)
            ,'prev_{}_projected_close'.format(max_min)
            ])


    #Calc vol as proportion of previous n-rows
    _df_out["prop_vol"] = calc_prop_of_prev(_df_out["volume"].copy().astype("float"),6)

    #Get period-period changes
    for col in ['close','volume','macd','ema26','signal_line','macd_line']:
        _df_out["change_{}_shift1".format(col)] = gradient(_df_out[col],1)
        
    #Compare close to the max/mins within 4,13,26,52 periods
    for col in ['close_orig','macd_line']:
        for max_min in ['max','min']:
            for period in [4,13,26,52]:
                _df_out["{}_per_change_{}_{}".format(col,max_min,period)] = per_change_in_range(_df_out[col],period,_max_min=max_min)
            
    #Mark points of macd positive entry
    _df_out["macd_pos_ent"] = pos_entry(_df_out["macd"])
    _df_out["macd_neg_ent"] = neg_entry(_df_out["macd"])
    
    #Create max min columns
    def mk_cols_max_min(tmp_df,col,period:int=4,gap:int=2):
        #Historic max mins
        tmp_df["{}_min".format(col)] = flag_mins(tmp_df[col],period,gap)
        tmp_df["{}_max".format(col)] = flag_maxs(tmp_df[col],period,gap)
        
    #Find previous max and mins, then look at:
        # - how many positive or negative moves in a row there has been
        # - what the move since the last (n-1) max/min was
        # - what the gradient is since the last (n-1) max/min
        # - what the move since the first max/min was
        # - what the gradient since the first max/min was
    def mk_cols_prev_max_min(tmp_df,col,period:int=4):
        #GETTING THE MAX/MINS - includes "gap" to account for time lag before declaring something as min/max
        tmp_df["prev_max_{}".format(col)],tmp_df["prev_max_{}_date".format(col)] = prev_max_min(tmp_df[["date",col,"{}_max".format(col)]].copy(),col,"{}_max".format(col))
        tmp_df["prev_min_{}".format(col)],tmp_df["prev_min_{}_date".format(col)] = prev_max_min(tmp_df[["date",col,"{}_min".format(col)]].copy(),col,"{}_min".format(col))
#         #Shift the max min columns by n periods to not leak future information
#         tmp_df["prev_max_{}".format(col)] = tmp_df["prev_max_{}".format(col)].shift(period)
#         tmp_df["prev_min_{}".format(col)] = tmp_df["prev_min_{}".format(col)].shift(period)
#         tmp_df["prev_max_{}_date".format(col)] = tmp_df["prev_max_{}_date".format(col)].shift(period)
#         tmp_df["prev_min_{}_date".format(col)] = tmp_df["prev_min_{}_date".format(col)].shift(period)
        #WHAT WAS THE MOVE SINCE THE LAST (N-1) MAX/MIN
        tmp_df['prev_max_move_{}'.format(col)] = mk_prev_move_float(tmp_df["prev_max_{}".format(col)])
        tmp_df['prev_max_date_move_{}'.format(col)] = mk_prev_move_date(tmp_df["prev_max_{}_date".format(col)])        
        tmp_df['prev_min_move_{}'.format(col)] = mk_prev_move_float(tmp_df["prev_min_{}".format(col)])
        tmp_df['prev_min_date_move_{}'.format(col)] = mk_prev_move_date(tmp_df["prev_min_{}_date".format(col)])
        #WHAT IS THE GRADIENT SINCE THE LAST (N-1) MAX/MIN
        tmp_df['prev_max_grad_{}'.format(col)] = tmp_df['prev_max_move_{}'.format(col)] / tmp_df['prev_max_date_move_{}'.format(col)]
        tmp_df['prev_min_grad_{}'.format(col)] = tmp_df['prev_min_move_{}'.format(col)] / tmp_df['prev_min_date_move_{}'.format(col)]
        #HOW MANY PROGRESSIVE MAX/MINS IN A ROW HAVE THERE BEEN - UP OR DOWN FOR BOTH OPTIONS
        tmp_df['max_move_cum_{}'.format(col)] = mk_move_cum(tmp_df['prev_max_move_{}'.format(col)])
        tmp_df['min_move_cum_{}'.format(col)] = mk_move_cum(tmp_df['prev_min_move_{}'.format(col)])
        #WHAT WAS THE MOVE SINCE THE FIRST (N=0) MAX/MIN
        tmp_df['long_prev_max_move_{}'.format(col)] = mk_long_prev_move_float(tmp_df['prev_max_move_{}'.format(col)],tmp_df['prev_max_{}'.format(col)])
        tmp_df['long_prev_min_move_{}'.format(col)] = mk_long_prev_move_float(tmp_df['prev_min_move_{}'.format(col)],tmp_df['prev_min_{}'.format(col)])
        #WHAT WAS THE TIMEDELTA SINCE THE FIRST (N=0) MAX/MIN
        tmp_df['long_prev_max_move_date_{}'.format(col)] = mk_long_prev_move_date(tmp_df['prev_max_move_{}'.format(col)],tmp_df['prev_max_{}_date'.format(col)])
        tmp_df['long_prev_min_move_date_{}'.format(col)] = mk_long_prev_move_date(tmp_df['prev_min_move_{}'.format(col)],tmp_df['prev_min_{}_date'.format(col)])
        #WHAT IS THE GRADIENT SINCE THE FIRST (N=0) MAX/MIN
        tmp_df['long_max_grad_{}'.format(col)] = tmp_df['long_prev_max_move_{}'.format(col)] / tmp_df['long_prev_max_move_date_{}'.format(col)]
        tmp_df['long_min_grad_{}'.format(col)] = tmp_df['long_prev_min_move_{}'.format(col)] / tmp_df['long_prev_min_move_date_{}'.format(col)]
        #WHAT IS THE MAX MIN CONVERGENCE/DIVERGENCE
        tmp_df['prev_grad_conv_{}'.format(col)] = tmp_df['prev_min_grad_{}'.format(col)] - tmp_df['prev_max_grad_{}'.format(col)]
        tmp_df['long_grad_conv_{}'.format(col)] = tmp_df['long_min_grad_{}'.format(col)] - tmp_df['long_max_grad_{}'.format(col)]
        
    #Calc the value changes and percentage changes of these movements
    def mk_cols_prev_max_min_change(tmp_df,col):
        tmp_df["max_change_{}".format(col)],tmp_df["max_per_change_{}".format(col)] = calc_changes(tmp_df[col].copy(),tmp_df["prev_max_{}".format(col)].copy())
        tmp_df["min_change_{}".format(col)],tmp_df["min_per_change_{}".format(col)] = calc_changes(tmp_df[col].copy(),tmp_df["prev_min_{}".format(col)].copy())
        
    #Mark date change since max and mins and convert to periods
    def mk_cols_prev_max_min_date_change(tmp_df,col,period:int=7):
        tmp_df["prev_max_{}_date_change".format(col)] = tmp_df["date"] - tmp_df["prev_max_{}_date".format(col)]
        tmp_df["prev_min_{}_date_change".format(col)] = tmp_df["date"] - tmp_df["prev_min_{}_date".format(col)]
        #Convert all to period changes
        tmp_df["prev_max_{}_date_change".format(col)] = [np.floor(x.days/period) for x in tmp_df["prev_max_{}_date_change".format(col)]]
        tmp_df["prev_min_{}_date_change".format(col)] = [np.floor(x.days/period) for x in tmp_df["prev_min_{}_date_change".format(col)]]
    
    #Run function for columns - high volatility
    for col in ['close','signal_line','volume']:
        mk_cols_max_min(_df_out,col,period_high_volatility,gap_high_volatility)
        mk_cols_prev_max_min(_df_out,col,period_high_volatility)
        mk_cols_prev_max_min_change(_df_out,col) 
        mk_cols_prev_max_min_date_change(_df_out,col,7)
    #Run function for columns - low volatility
    for col in ['macd','ema26','macd_line']:
        mk_cols_max_min(_df_out,col,period_low_volatility,gap_low_volatility)
        mk_cols_prev_max_min(_df_out,col,period_low_volatility)
        mk_cols_prev_max_min_change(_df_out,col) 
        mk_cols_prev_max_min_date_change(_df_out,col,7)
    
    return _df_out

#Define the columns for the output
out_cols = [
    #NON-NORMALISED COLS
    "ticker"
    ,"date"
    #NORMALISED COLS
    #Standard features
    ,"open"
    ,"close"
    ,"high"
    ,"low"
    ,"volume"
    ,"change_price"
    ,"per_change_price"
    ,"ema26"
    ,"macd"
    ,"signal_line"
    ,"macd_line"
]
#Append additional columns for key areas
for col in ['close_orig','macd_line']:
    for max_min in ['max','min']:
        for period in [4,13,26,52]:
            out_cols.append("{}_per_change_{}_{}".format(col,max_min,period))
for col in ['close','macd','ema26','signal_line','macd_line','volume']:
    #Shifted features
    out_cols.append("change_{}_shift1".format(col))
    #Max/min flags
    out_cols.append("{}_max".format(col))    
    out_cols.append("{}_min".format(col))    
    #Prev max/min features
    out_cols.append("prev_max_{}".format(col))
    out_cols.append("prev_min_{}".format(col))
    #date changes
    out_cols.append("prev_max_{}_date_change".format(col))
    out_cols.append("prev_min_{}_date_change".format(col))
    #Min max change features
    out_cols.append("max_change_{}".format(col))
    out_cols.append("min_change_{}".format(col))
    #prev max/mins (n-1) - compared to previous
    out_cols.append('prev_max_grad_{}'.format(col))
    out_cols.append('prev_min_grad_{}'.format(col))
    out_cols.append('prev_grad_conv_{}'.format(col))
    #prev max/mins (n=0) - compared to first in this run
    out_cols.append('max_move_cum_{}'.format(col))
    out_cols.append('min_move_cum_{}'.format(col))
    out_cols.append('long_prev_max_move_date_{}'.format(col))
    out_cols.append('long_prev_min_move_date_{}'.format(col))
    out_cols.append('long_max_grad_{}'.format(col))
    out_cols.append('long_min_grad_{}'.format(col))
    out_cols.append('long_grad_conv_{}'.format(col))
#Append signal
out_cols.append("signal")

#A conversion for all variables ot the correct dtype
conv_di = {
    #NON-NORMALISED COLS
    "ticker":'object'
    ,"date":'datetime64'
    #NORMALISED COLS
    #Standard features
    ,"open":'float64'
    ,"close":'float64'
    ,"high":'float64'
    ,"low":'float64'
    ,"volume":'float64'
    ,"change_price":'float64'
    ,"per_change_price":'float64'
    ,"ema26":'float64'
    ,"macd":'float64'
    ,"signal_line":'float64'
    ,"macd_line":'float64'
}
#Append additional columns for key areas
for col in ['close_orig','macd_line']:
    for max_min in ['max','min']:
        for period in [4,13,26,52]:
            conv_di["{}_per_change_{}_{}".format(col,max_min,period)] = 'float64'
for col in ['close','macd','ema26','signal_line','macd_line','volume']:
    #Shifted features
    conv_di["change_{}_shift1".format(col)] = 'float64'
    #Max/min flags
    conv_di["{}_max".format(col)] = 'bool'
    conv_di["{}_min".format(col)] = 'bool'
    #Prev max/min features
    conv_di["prev_max_{}".format(col)] = 'float64'
    conv_di["prev_min_{}".format(col)] = 'float64'
    #date changes
    conv_di["prev_max_{}_date_change".format(col)] = 'float64'
    conv_di["prev_min_{}_date_change".format(col)] = 'float64'
    #Min max change features
    conv_di["max_change_{}".format(col)] = 'float64'
    conv_di["min_change_{}".format(col)] = 'float64'
    #prev max/mins (n-1) - compared to previous
    conv_di['prev_max_grad_{}'.format(col)] = 'float64'
    conv_di['prev_min_grad_{}'.format(col)] = 'float64'
    conv_di['prev_grad_conv_{}'.format(col)] = 'float64'
    #prev max/mins (n=0) - compared to first in this run
    conv_di['max_move_cum_{}'.format(col)] = 'int64'
    conv_di['min_move_cum_{}'.format(col)] = 'int64'
    conv_di['long_prev_max_move_date_{}'.format(col)] = 'float64'
    conv_di['long_prev_min_move_date_{}'.format(col)] = 'float64'
    conv_di['long_max_grad_{}'.format(col)] = 'float64'
    conv_di['long_min_grad_{}'.format(col)] = 'float64'
    conv_di['long_grad_conv_{}'.format(col)] = 'float64'
#Append signal
conv_di["signal"] = 'object'

#Then loop the tickers and combine these into one large dataset
hf_store_name = CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_w_tmp']
h_store = pd.HDFStore(hf_store_name)
errors = []
run_time = process_time()
for tick in tick_ftse["ticker"]:
# for tick in ['SBRY','AJB']: #TEMP
    try:
        print("\n{}".format(len(run_time.lap_li)))
        print("RUN FOR {}".format(tick))
        #Isolate this ticker
        this_tick_df = df_prices_w[df_prices_w["ticker"] == re.sub('[^a-zA-Z0-9\-]','',tick)].copy()
        print("shape before -> {}".format(this_tick_df.shape))
        #Create the features
        this_tick_df = create_features(this_tick_df)
        #Clarify col_lens with cur cols in data
        this_col_lens = get_col_len_df(this_tick_df)
        min_itemsize_di = {}
        for col in out_cols:
            if col in col_lens:
                if this_col_lens[col] > col_lens[col]:
                    col_lens[col] = this_col_lens[col]
            else:
                col_lens[col] = this_col_lens[col]
            min_itemsize_di = col_lens[col]
        print("shape after -> {}".format(this_tick_df.shape))
        #Create function for appending to hdf file
        def append_to_hdf(df_in):
            df_in[out_cols].to_hdf(hf_store_name,key='weekly_data',append=True,min_itemsize=min_itemsize_di)
        #Append this data to the group
        try:
            append_to_hdf(this_tick_df)
            print('ADDED TO {}'.format(hf_store_name))
        except ValueError:
            print('WARNING -> Attempting to change dtypes')
            #Try changing the dtypes
            try:
                for col in out_cols:
                    # print(r'CONVERT {} FROM {} TO {}'.format(col,this_tick_df[col].dtype,conv_di[col]))
                    this_tick_df[col] = this_tick_df[col].astype(conv_di[col])
                append_to_hdf(this_tick_df)
                print('ADDED TO {}'.format(hf_store_name))
            except Exception as e:
                errors.append({"ticker":tick,"Error":e})
                print('ERROR READING TO FILE {}'.format(e))
        except Exception as e:
            errors.append({"ticker":tick,"Error":e})
            print('ERROR READING TO FILE {}'.format(e))
        #Lap
        run_time.lap()
        run_time.show_latest_lap_time()
    except Exception as e:
        h_store.close()
        errors.append({"ticker":tick,"Error":e})
        print('ERROR PROCESSING DATA {}'.format(e))
h_store.close()
print('\n\n')
run_time.end()
print('\nERROR COUNT -> {}'.format(len(errors)))
if len(errors) > 0:
    print('\tERRORS -> ')
    display(pd.DataFrame(errors))

#close any open h5 files
tables.file._open_files.close_all()

#Check the final tmp table
tmp_df = pd.read_hdf(hf_store_name,key='weekly_data',mode='r')
print("")
print("FINAL HDFSTORE SIZE: {}".format(tmp_df.shape))
print("FINAL SIGNAL COUNTS: {}".format(tmp_df.signal.value_counts()))
h_store.close()
print(tmp_df.head(50))

#close any open h5 files
tables.file._open_files.close_all()

#Delete the old h5 file and rename the TMP
try:
    os.remove(CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_w'])
    print('\nSUCCESSFULLY REMOVED {}'.format(CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_w']))
except Exception as e:
    print('\nERROR - REMOVING:{}'.format(e))
try:
    os.rename(CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_w_tmp'],CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_w'])
    print('\nSUCCESSFULLY RENAMED {} TO {}'.format(CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_w_tmp'],CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_w']))
except Exception as e:
    print('\nERROR - RENAMING:{}'.format(e))

#Remove 'date' from out_cols
out_cols.remove('date')

#Export a list of the features for this model
file_object = open(CONFIG['files']['store_path'] + CONFIG['files']['ft_eng_col_list'],'w')
feature_str = ''
for i in out_cols:
    feature_str += '{},'.format(i)
feature_str = feature_str[:-1]
file_object.write(feature_str)
file_object.close()
feature_str