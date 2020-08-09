"""
Creating and running a fund
This script will import the vrious models and run them as
funds from 01-01-2014 to find the best performing fund/model.
"""

#Import models
import numpy as np
import pandas as pd
import math
import os
import tables
from rf_modules import *
from config import CONFIG
from libs.run_fund_funcs import *

#Establish trading variables
#All figures in pence
fund_vars = CONFIG['fund_vars']

#Import and combine prices files
df_signals_lgb = pd.read_hdf(CONFIG['files']['store_path'] + CONFIG['files']['signals'])
print('SHAPE: {}'.format(df_signals_lgb.shape))
print('df_signals_lgb.columns -> {}'.format(df_signals_lgb.columns))
print('df_signals_lgb.head() -> {}'.format(df_signals_lgb.head()))

#Order the data by date (asc) and buy probability (desc)
signal_df = df_signals_lgb[['ticker','date','open','high','low','close','open_shift_neg1','signal','signal_prob']].copy()
signal_df.sort_values(['date','signal_prob'],ascending=[True,False],inplace=True)
signal_df.reset_index(drop=True,inplace=True)
print(signal_df.signal.value_counts())
print('signal_df.head() -> {}'.format(signal_df.head()))

#Run through rows and buy and sell according to signals and holdings
lgb_fund = run_fund(signal_df,_buy_signal=CONFIG['lgbm_training']['buy_signal'],_sell_signal=CONFIG['lgbm_training']['sell_signal'],**fund_vars)

#Show summary
print('lgb_fund.st_val:£{:,.2f}'.format(lgb_fund.st_val/100))
print('lgb_fund.available:£{:,.2f}'.format(lgb_fund.available/100))
print('lgb_fund.codb:£{:,.2f}'.format(lgb_fund.codb/100))
print('lgb_fund.invested_value:£{:,.2f}'.format(lgb_fund.invested_value/100))
lgb_ledger_df = pd.DataFrame(lgb_fund.ledger,columns=[
            'trade_type'
            ,'signal_prob'
            ,'ticker'
            ,'trade_date'
            ,'spread'
            ,'price'
            ,'ask_price'
            ,'bid_price'
            ,'share_vol'
            ,'trade_value'
            ,'stamp_duty'
            ,'trade_cost'
            ,'spread_cost'
            ,'holding_value'
            ,'ledger_value'
            ,'invested_pre_trade'
            ,'invested_post_trade'
            ,'available_pre_trade'
            ,'available_post_trade'])
print('TRADE COUNT:{:,}'.format(len(lgb_ledger_df)))

#Get the completed trades
lgb_trades_df = completed_trades(lgb_ledger_df,lgb_fund.st_val,lgb_fund.cur_holdings)

#Export
lgb_ledger_df.to_csv(CONFIG['files']['store_path'] + CONFIG['files']['fund_ledger'])