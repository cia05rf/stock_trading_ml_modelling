"""Config file for running scripts"""

import datetime as dt
from numpy import linspace
from skopt import space

CONFIG = {
    "web_addr":{
        "ftse100":r'https://www.londonstockexchange.com/indices/ftse-100/constituents/table?page={}',
        "ftse250":r'https://www.londonstockexchange.com/indices/ftse-250/constituents/table?page={}',
        "share_price":r'https://finance.yahoo.com/quote/{}/history?period1={}&period2={}&interval={}&filter=history&frequency={}',
        "holidays":r'http://www.calendar-uk.co.uk/holidays/{}/',
    },
    "scrape":{
        "max_days":140,
    },
    'files':{
        "store_path":r"data"
        ,"log_path":r"logs"
        ,"tick_ftse":r"tick_ftse.csv"
        ,"hist_prices_d":r"all_hist_prices_d.h5"
        ,"hist_prices_d_tmp":r"all_hist_prices_d_TMP.h5"
        ,"hist_prices_w":r"all_hist_prices_w.h5"
        ,"hist_prices_w_tmp":r"all_hist_prices_w_TMP.h5"
        ,"prices_db":r"prices.db"
        ,"ft_eng_w_tmp":r"all_hist_prices_w_ft_eng2_TMP.h5"
        ,"ft_eng_w":r"all_hist_prices_w_ft_eng2.h5"
        ,"ft_eng_col_list":r"feature_engineering_feature_list.txt"
        ,"lgb_model":r"lgb_model.joblib"
        ,"lgb_model_feature_list":r"lgb_model_feature_list.txt"
        ,"signals":r"historic_lgb_bsh_signals.h5"
        ,"signals_tmp":r"historic_lgb_bsh_signals_TMP.h5"
        ,"fund_ledger":r"fund_ledger_lgb.csv"
        ,"ws_update_prices_log":r"update_db_historic_prices_LOG.log"
        ,"ws_update_tickers_log":r"update_db_tickers_LOG.log"
        ,"ws_update_signals_log":r"update_db_historic_bsh_LOG.log"
        ,"nn_ft_numpy":r"nn_ft"
        ,"nn_tar_numpy":r"nn_tar"
    }
    ,'web_scrape':{
        'mode':'update' #Set to 'update' or 'full'
    }
    ,'nn_ft_eng':{
        'ft_periods':6 #TEMP - change to 52 eventually
        ,'target_periods':6
    }
    ,'feature_eng':{
        'min_records':30
        ,'period_li':[4,13,26]
        ,'look_back_price_period':8
        ,'norm_window':3*52
        ,'target_price_period':6
        ,'min_gain':0.1
        ,'max_drop':-0.05
        ,'period_high_volatility':5
        ,'period_low_volatility':1
        ,'gap_high_volatility':3
        ,'gap_low_volatility':1
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
            # ,'min_samples_split':2000 #Should be between 0.5-1% of samples
            # ,'min_samples_leaf':500
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
                ,'num_leaves':linspace(10,1010,100,dtype=int)
                ,'max_depth':linspace(2,8,6,dtype=int)
                ,'min_samples_split':linspace(200,2200,10,dtype=int)
                ,'min_samples_leaf':linspace(50,550,10,dtype=int)
            }
        }
        ,'skopt_params':[
            space.Real(0.01,0.5,name='learning_rate',prior='log-uniform')
            ,space.Integer(1,30,name='max_depth')
            ,space.Integer(2,100,name='num_leaves')
            ,space.Integer(200,2000,name='min_samples_split')
            ,space.Integer(50,500,name='min_samples_leaf')
            ,
        ]
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
    ,'db_update':{
        'prices':'full'#'update' or 'full'
        ,'signals':'full'#'update' or 'full'
    },
    "public_holidays":[
        "New Year's Day",
        "Good Friday",
        "Easter Monday",
        "May Day Bank Holiday",
        "Spring Bank Holiday",
        "Summer Bank Holiday",
        "Christmas day",
        "Boxing Day",
    ]
}