import pandas as pd
import numpy as np

from config import CONFIG
from libs.ft_eng_funcs import calc_ema_macd, calc_ema

prices_w_df = pd.read_hdf(CONFIG['files']['store_path']+CONFIG['files']['hist_prices_w'])
prices_d_df = pd.read_hdf(CONFIG['files']['store_path']+CONFIG['files']['hist_prices_d'])

tmp_w_df = prices_w_df[prices_w_df.ticker == 'BARC'][['date','close']].sort_values(['date'])
tmp_d_df = prices_d_df[prices_d_df.ticker == 'BARC'][['date','close']].sort_values(['date'])

# tmp_w_df = calc_ema_macd(tmp_w_df)

# def calc_ema(_s_in,_periods):
#     """Function used to create EMA for a series
    
#     args:
#     -----
#     _s_in - pandas series - series of float values
#     _periods - int - value describing how far to look at for EMA calc
    
#     returns:
#     ------
#     pandas series    
#     """
#     #Calc mod val
#     _mod = 2/(_periods+1)
#     #Weighting series
#     _weight_ar = np.array(range(0,_periods))
#     _weight_ar = (1-_mod)**_weight_ar
#     #Calc cum emas
#     _ema_s = [0] * _s_in.shape[0]
#     for _i in range(0,_periods):
#         _ema_s += _s_in.shift(_i) * _weight_ar[_i]
#     #Calc ema
#     _ema_s =  _ema_s / np.sum(_weight_ar)
#     return _ema_s.copy()

tmp_s = pd.Series(range(20,41))
tmp_ema = calc_ema(tmp_s,15)
tmp_ema

tmp_w_df = calc_ema_macd(tmp_w_df)

tmp_w_df.tail(15)