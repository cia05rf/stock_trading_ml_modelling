"""Config file for running scripts"""

import datetime as dt
import numpy as np

CONFIG = {
    'files':{
        'store_path':r"C:\Users\Robert\Documents\python_scripts\stock_trading_ml_modelling\historical_prices"
        ,'tick_ftse':r"\tick_ftse.csv"
        ,'hist_prices_d':r"\all_hist_prices_d.h5"
        ,'hist_prices_d_tmp':r"\all_hist_prices_d_TMP.h5"
        ,'hist_prices_w':r"\all_hist_prices_w.h5"
        ,'hist_prices_w_tmp':r"\all_hist_prices_w_TMP.h5"
        ,'ft_eng_w_tmp':r'\all_hist_prices_w_ft_eng2_TMP.h5'
        ,'ft_eng_w':r'\all_hist_prices_w_ft_eng2.h5'
        ,'ft_eng_col_list':r'\feature_engineering_feature_list.txt'
        ,'lgb_model':r'\lgb_model.joblib'
        ,'lgb_model_feature_list':r'\lgb_model_feature_list.txt'
        ,'signals':r'\historic_lgb_bsh_signals.h5'
        ,'signals_tmp':r'\historic_lgb_bsh_signals_TMP.h5'
        ,'fund_ledger':r'\fund_ledger_lgb.csv'
    }
    ,'web_scrape':{
        'mode':'update' #Set to 'update' or 'full'
    }
    ,'feature_eng':{
        'min_records':30
        ,'period_li':[4,13,26]
        ,'look_back_price_period':8
        ,'norm_window':1*52
        ,'target_price_period':6
        ,'min_gain':0.1
        ,'max_drop':-0.05
        ,'period_high_volatility':5
        ,'period_low_volatility':3
        ,'gap_high_volatility':3
        ,'gap_low_volatility':2
    }
    ,'lgbm_training':{
        'target_cols':'signal'
        ,'rem_inf':False
        ,'date_lim':dt.datetime(2014,1,1)
        ,'rand_seed':0
        ,'use_custom_loss_function':True #Set tot True if using a custom loss function
        ,'use_custom_eval_set':True #Set to True if using a custom evaluation set
        ,'custom_metric':'ppv'
        ,'buy_signal':'buy'
        ,'sell_signal':'sell'
        ,'mod_fixed_params':{
            'boosting_type':'gbdt'
            ,'random_state':0
            ,'silent':False
            ,'objective':'binary'
            ,'min_samples_split':2000 #Should be between 0.5-1% of samples
            ,'min_samples_leaf':500
            ,'n_estimators':20
            ,'subsample':0.8
        }
        ,'search_params':{
            'fixed':{
                'cv':3
                ,'n_iter':80
                # 'cv':2
                # ,'n_iter':1
                ,'verbose':True
                ,'random_state':0
            }
            ,'variable':{
                'learning_rate':[0.1,0.01,0.005]
                ,'num_leaves':np.linspace(10,1010,100,dtype=int)
                ,'max_depth':np.linspace(2,8,6,dtype=int)
            }
        }
        ,'fit_params':{
            'verbose':True
        }
    }
    ,'fund_vars':{
        '_fund_value_st':1000000 #£10,000
        ,'_trade_cost':250 #£2.50
        ,'_investment_limit_min_val':100000 #£1,000
        ,'_investment_limit_max_per':0.1 #10%
        ,'_spread':0.01 #1%
    }
}