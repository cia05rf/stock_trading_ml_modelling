"""Config file for running scripts"""
import json
import os
import re

CONFIG = []

with open("./stock_trading_ml_modelling/config.json", "r") as f:
    CONFIG = json.loads(f.read())

# Overwrite with local config if present
if os.path.isfile("./stock_trading_ml_modelling/local.config.json"):
    with open("local.config.json", "r") as f:
        local_config = json.loads(f.read())
        for k, v in local_config.items():
            # Replace the first part APPSETTING_
            if k in CONFIG:
                CONFIG[k] = v

# Overwrite with env vars if duplicated
for k, v in os.environ.items():
    # Replace the first part APPSETTING_
    k = re.sub(r"^APPSETTING_", "", k)
    if k in CONFIG:
        CONFIG[k] = v

# Assign to variables
# Scraping
WEB_SCRAPE_MODE = CONFIG.get("web_scrape", {}).get("mode", "update")
WEB_SCRAPE_MAX_DAYS = CONFIG.get("web_scrape", {}).get("max_days", 140)
WEB_ADDRS = CONFIG.get("web_addrs", {})
# Files
STORE_PATH = CONFIG.get("files", {}).get("store_path", "./data")
HIST_PRICES_D = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("hist_prices_d", "hist_prices_d.h5"))
HIST_PRICES_W = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("hist_prices_w", "hist_prices_w.h5"))
TICK_FTSE = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("tick_ftse", "tick_ftse.csv"))
HF_STORE = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("ft_eng_w_tmp", "ft_eng_w.h5"))
FT_ENG_W = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("ft_eng_w", "ft_eng_w.txt"))
FT_ENG_COL_LIST = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("ft_eng_w", "ft_eng_col_list.txt"))
LGB_MODEL = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("lgb_model", "lgb_model.joblib"))
LGB_MODEL_FEATURE_LIST = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("lgb_model_feature_list", "lgb_model_feature_list.txt"))
SIGNALS = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("signals", "signals.h5"))
NN_FT_NUMPY = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("nn_ft_numpy", "nn_ft.npy"))
NN_TAR_NUMPY = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("nn_tar_numpy", "nn_ta.npy"))
FUND_LEDGE = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("nn_tar_numpy", "nn_ta.npy"))
WS_UPDATE_PRICES_LOG = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("ws_update_prices_log", "update_db_historic_prices_LOG.logger"))
WS_UPDATE_TICKERS_LOG = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("ws_update_tickers_log", "update_db_tickers_LOG.logger"))
WS_UPDATE_SIGNALS_LOG = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("ws_update_signals_log", "update_db_historic_bsh_LOG.logger"))
# Database
DB_PATH = os.path.join(STORE_PATH, CONFIG.get("files", {}).get("prices_db", "prices.db"))
DB_UPDATE_PRICES = CONFIG.get("db_update", {}).get("prices", "full")
DB_UPDATE_SIGNALS = CONFIG.get("db_update", {}).get("signals", "full")
# nn ft eng
NN_FT_PERIODS = CONFIG.get("nn_ft_eng", {}).get("ft_periods", 6)
NN_TARGET_PERIODS = CONFIG.get("nn_ft_eng", {}).get("target_periods", 6)
# ft eng
TARGET_PRICE_PERIOD = CONFIG.get("feature_eng", {}).get("target_price_period", 5)
MIN_GAIN = CONFIG.get("feature_eng", {}).get("min_gain", 5)
MAX_DROP = CONFIG.get("feature_eng", {}).get("max_drop", 5)
PERIOD_LI = CONFIG.get("feature_eng", {}).get("period_li", 5)
LOOK_BACK_PRICE_PERIOD = CONFIG.get("feature_eng", {}).get("look_back_price_period", 5)
PERIOD_HIGH_VOLATILITY = CONFIG.get("feature_eng", {}).get("period_high_volatility", 5)
PERIOD_LOW_VOLATILITY = CONFIG.get("feature_eng", {}).get("period_low_volatility", 5)
GAP_HIGH_VOLATILITY = CONFIG.get("feature_eng", {}).get("gap_high_volatility", 5)
GAP_LOW_VOLATILITY = CONFIG.get("feature_eng", {}).get("gap_low_volatility", 5)
NORM_WINDOW = CONFIG.get("feature_eng", {}).get("norm_window", 52)
# lgbm
LGBM_REM_INF = CONFIG.get("lgbm_training", {}).get("rem_inf", True)
LGBM_USE_CUST_LOSS_FUNC = CONFIG.get("lgbm_training", {}).get("use_custom_loss_function", True)
LGBM_USE_CUST_EVAL_SET = CONFIG.get("lgbm_training", {}).get("use_custom_eval_set", True)
LGBM_FIXED_PARAMS = CONFIG.get("lgbm_training", {}).get("fixed_params", {})
LGBM_SEARCH_PARAMS = CONFIG.get("lgbm_training", {}).get("search_params", {})
LGBM_FIT_PARAMS = CONFIG.get("lgbm_training", {}).get("fit_params", {})
# training
BUY_SIGNAL = CONFIG.get("lgbm", {}).get("buy_signal", "buy")
SELL_SIGNAL = CONFIG.get("lgbm", {}).get("sell_signal", "sell")
DATE_LIM = CONFIG.get("training_params", {}).get("date_lim", "2014-01-01")
RAND_SEED = CONFIG.get("training_params", {}).get("rand_seed", 42)
TARGET_COL = CONFIG.get("training_params", {}).get("target_col", "signal")
CUSTOM_METRIC = CONFIG.get("training_params", {}).get("custom_metric", "ppv")
BUY_SIGNAL = CONFIG.get("training_params", {}).get("buy_signal", "buy")
SELL_SIGNAL = CONFIG.get("training_params", {}).get("sell_signal", "sell")
# Fund
FUND_FUND_VALUE_ST = CONFIG.get("fund", {}).get("fund_value_st", 1000000)
FUND_TRADE_COST = CONFIG.get("fund", {}).get("trade_cost", 250)
FUND_LIMIT_MIN_VAL = CONFIG.get("fund", {}).get("limit_min_val", 100000)
FUND_LIMIT_MAX_PE = CONFIG.get("fund", {}).get("limit_max_pe", 0.1)
FUND_SPREAD = CONFIG.get("fund", {}).get("spread", 0.01)
# Misc
PUBLIC_HOLS = CONFIG.get("public_holidays", [])