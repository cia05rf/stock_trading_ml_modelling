"""Config fule for running scripts"""

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
    }
    ,'web_scrape':{
        'mode':'update' #Set to 'update' or 'full'
    }
    ,'feature_eng':{
        'min_records':30
        ,'period_li':[4,13,26]
        ,'look_back_price_period':8
        ,'norm_window':5*52
        ,'target_price_period':12
        ,'min_gain':0.1
        ,'max_drop':-0.05
        ,'period_high_volatility':5
        ,'period_low_volatility':3
        ,'gap_high_volatility':3
        ,'gap_low_volatility':2
    }
}