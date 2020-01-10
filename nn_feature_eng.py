"""
Featue engineering for the training of a time series neaural network

The goal is to output an array of shape (n,m,p).
n is the number of datasets created over all - all datasets must be bulked out to this size.
m is the step in this set of data, it will be of whatever length we decide.
p is the number of features being created.
"""

#import modules
import pandas as pd
import numpy as np

from config import CONFIG
from functions.ft_eng_funcs import *
from rf_modules import *

#import the price data
prices_w = pd.read_hdf(CONFIG['files']['store_path']+CONFIG['files']['hist_prices_w'])

########################
### IDENTIFY TICKERS ###
########################
tickers = prices_w.ticker.unique()


######################
### BUILD FEATURES ###
######################
df_in = prices_w #TEMP
def build_ft_arrays(df_in):
    """Function to build the features for the given dataset.
    Idealy dataset input should be for a single ticker in ascending datetime order
    as this function will create outputs base don that assumption.

    args:
    ------
    df_in - pandas dataframe - all the price data ordered by datetime value

    returns:
    ------
    numpy array,numpy array - features []
    """
    def pos_fts(col,reverse:bool=False):
        """Function to create the features if all values are positive
        
        args:
        ------
        col - str - the name of the columns we're using as a base
        reverse - bool:False - will flip the values so that low becomes high, 
            used when dealing with negatives where you want larger negatives 
            to have more impact.

        returns:
        ------
        list - list of outputs
        """
        li = [norm_time_s(x,df_in[col],CONFIG['nn_ft_eng']['ft_periods'],_return_series=True,_fill_window=True).tolist() for x in df_in.index]
        if reverse:
            li = [1 - np.array(x) for x in li]
        return li

    def pos_neg_fts(col):
        """Function to create the positive negative features
        
        args:
        ------
        col - str - the name of the columns we're using as a base

        returns:
        ------
        3 lists - list of absolute outputs,list of binaries showing positive
        """
        raw_li = [norm_time_s(x,df_in[col],CONFIG['nn_ft_eng']['ft_periods'],_return_series=True,_fill_window=True,_neg_vals=True).tolist() for x in df_in.index]
        abs_li = [np.abs(x).tolist() for x in raw_li]
        pos_binary_li = [(np.array(x) > 0).astype(int).tolist() for x in raw_li]
        neg_binary_li = [(np.array(x) < 0).astype(int).tolist() for x in raw_li]
        return abs_li,pos_binary_li,neg_binary_li

    ### CLOSE ###
    # Normalise over time period
    # For each time step look back over a set number of steps and normalise
    close_li = pos_fts('close')

    ### MACD ###
    # Calc the MACD
    df_in = calc_ema_macd(df_in)
    # Get the lists
    macd_li,macd_pos_li,macd_neg_li = pos_neg_fts('macd')

    ### CLOSE VS CLOSE(PREV) ###
    # Create the column in the dataset
    df_in['cl_vs_cl'] = df_in.close - df_in.close.shift(1)
    # Get the lists
    cl_vs_cl_li,cl_vs_cl_pos_li,cl_vs_cl_neg_li = pos_neg_fts('cl_vs_cl')

    ### CLOSE VS OPEN ###
    # Create the column in the dataset
    df_in['cl_vs_op'] = df_in.close - df_in.open
    # Get the lists
    cl_vs_op_li,cl_vs_op_pos_li,cl_vs_op_neg_li = pos_neg_fts('cl_vs_op')

    ### CLOSE VS HIGH ###
    # Create the column in the dataset
    df_in['cl_vs_hi'] = df_in.close - df_in.high
    # Get the lists
    cl_vs_hi_li = pos_fts('cl_vs_hi',reverse=True)

    ### CLOSE VS LOW ###
    # Create the column in the dataset
    df_in['cl_vs_lo'] = df_in.close - df_in.low
    # Get the lists
    cl_vs_lo_li = pos_fts('cl_vs_lo',reverse=False)

    ### CALC THE GRADIENT FEATURES ###
    #Get gradient from previous 2 real min/max
    #Find turn points
    df_in['real_close_min'] = flag_mins(df_in['close'],_period=CONFIG['feature_eng']['period_high_volatility'],_cur=False)
    df_in['real_close_max'] = flag_maxs(df_in['close'],_period=CONFIG['feature_eng']['period_high_volatility'],_cur=False)
    for max_min in ['max','min']:
        #Find the last 2 mins
        df_in["prev_{}_close".format(max_min)],df_in["prev_{}_close_date".format(max_min)],df_in["prev_{}_close_index".format(max_min)] = prev_max_min(df_in[["date",'close',"real_close_{}".format(max_min)]].copy(),'close',"real_close_{}".format(max_min),CONFIG['feature_eng']['gap_high_volatility'])
        df_in["prev_{}_close_change".format(max_min)] = mk_prev_move_float(df_in['prev_{}_close'.format(max_min)])
        df_in["prev_{}_close_index_change".format(max_min)] = mk_prev_move_float(df_in['prev_{}_close_index'.format(max_min)])
        #Calc the gradient
        df_in['prev_{}_close_grad'.format(max_min)] = df_in["prev_{}_close_change".format(max_min)] / df_in["prev_{}_close_index_change".format(max_min)]
        #Count the periods since the change
        df_in['prev_{}_close_period_count'.format(max_min)] = df_in.index - df_in["prev_{}_close_index".format(max_min)]
        #Calc the projected value and diff to the actual value
        df_in['prev_{}_projected_close'.format(max_min)] = df_in["prev_{}_close".format(max_min)] + (df_in['prev_{}_close_period_count'.format(max_min)] * df_in['prev_{}_close_grad'.format(max_min)])
        df_in['prev_{}_projected_close_diff'.format(max_min)] = (df_in["close"] - df_in['prev_{}_projected_close'.format(max_min)]) / df_in["close"]
        #Keep only the wanted columns - keep grad, period_count, and project_close_diff
        df_in = df_in.drop(columns=[
            "prev_{}_close".format(max_min)
            ,"prev_{}_close_date".format(max_min)
            ,"prev_{}_close_index".format(max_min)
            ,"prev_{}_close_change".format(max_min)
            ,"prev_{}_close_index_change".format(max_min)
            ,'prev_{}_projected_close'.format(max_min)
            ])

    ### MAX CLOSE GRAD DIFF ###
    # Normalise over time period
    max_cl_grad_diff_li,max_cl_grad_pos_diff_li,max_cl_grad_diff_neg_li = pos_neg_fts('prev_max_projected_close_diff')

    ### MIN CLOSE GRAD DIFF ###
    # Normalise over time period
    min_cl_grad_diff_li,min_cl_grad_diff_pos_li,min_cl_grad_diff_neg_li = pos_neg_fts('prev_min_projected_close_diff')

    ### COMBINE THE FEATURES ###
    #The fetaures need to be made so that the nth terms of each list are blocked together
    features = []
    for i in range(len(close_li)):
        tmp_li = []
        for j in range(len(close_li[i])):
            tmp_li.append([
                close_li[i][j]
                ,macd_li[i][j]
                ,macd_pos_li[i][j]
                ,macd_neg_li[i][j]
                ,cl_vs_cl_li[i][j]
                ,cl_vs_cl_pos_li[i][j]
                ,cl_vs_cl_neg_li[i][j]
                ,cl_vs_op_li[i][j]
                ,cl_vs_op_pos_li[i][j]
                ,cl_vs_op_neg_li[i][j]
                ,cl_vs_hi_li[i][j]
                ,cl_vs_lo_li[i][j]
                ,max_cl_grad_diff_li[i][j]
                ,max_cl_grad_pos_diff_li[i][j]
                ,max_cl_grad_diff_neg_li[i][j]
                ,min_cl_grad_diff_li[i][j]
                ,min_cl_grad_diff_pos_li[i][j]
                ,min_cl_grad_diff_neg_li[i][j]
            ])
        features.append(tmp_li)
    #Convert to a numpy array
    features = np.array(features)

    #############################
    ### BUILD TARGET VARIABLE ###
    #############################

    # Use the value of the EMA12 close n periods from this one relative to this EMA12
    target = np.array((df_in.ema12.shift(-CONFIG['nn_ft_eng']['target_periods']) / df_in.ema12).tolist())

    ### NOTE TO SELF - Need to remove rows with nan values (IE the first few features and the last few targets)

    return features,target

#Setup a timing class
run_time = process_time()
features = np.array([])
target = np.array([])
tickers = prices_w.ticker.unique()
for tick in tickers:
    print('\nRUNNING LAP {}/{} - TICKER -> {}'.format(len(run_time.lap_li) + 1,len(tickers),tick))
    this_tick_df = prices_w[prices_w.ticker == tick]
    tick_ft,tick_tar = build_ft_arrays(this_tick_df)
    print('SHAPE OF features -> {}'.format(tick_ft.shape))
    print('SHAPE OF target -> {}'.format(tick_tar.shape))
    if features.shape[0] == 0:
        features = tick_ft
    else:
        features = np.append(features,tick_ft,axis=0)
    if target.shape[0] == 0:
        target = tick_tar
    else:
        target = np.append(target,tick_tar)
    #Create a lap
    run_time.lap()
    run_time.show_latest_lap_time()
print('\n\nFEATURES CREATED')
run_time.end()
print('FINAL SHAPE OF features -> {}'.format(features.shape))
print('FINAL SHAPE OF target -> {}'.format(target.shape))


##############
### EXPORT ###
##############
np.save(CONFIG['files']['store_path'] + CONFIG['files']['nn_ft_numpy'],features)
np.save(CONFIG['files']['store_path'] + CONFIG['files']['nn_tar_numpy'],target)