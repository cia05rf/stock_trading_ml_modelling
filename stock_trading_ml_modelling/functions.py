"""Master functions"""
import datetime as dt
from pathlib import Path

from stock_trading_ml_modelling.database.models import engine
from stock_trading_ml_modelling.database.models import Session as session
from stock_trading_ml_modelling.database.models.prices import create_db
from stock_trading_ml_modelling.scrapping import full_scrape
from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.libs.stock_filtering import filter_stocks
from stock_trading_ml_modelling.manage_data import remove_duplicate_daily_prices, \
    remove_duplicate_weekly_prices, fill_price_gaps

from stock_trading_ml_modelling.config import CONFIG

def create_database():
    create_db(engine)

def run_full_scrape():
    logger.set_logger("_run_full_scrape")
    full_scrape()

def remove_duplicate_prices():
    logger.set_logger("_remove_duplicate_prices")
    #Remove daily price duplicates
    remove_duplicate_daily_prices()
    #Remove weekly price duplicates
    remove_duplicate_weekly_prices()

def fill_all_price_gaps():
    logger.set_logger("_fill_price_gaps")
    fill_price_gaps()

def find_buys():
    logger.set_logger("_find_buys")
    buy_df = filter_stocks()
    today_str = dt.datetime.strftime(dt.datetime.today(), "%Y%m%d")
    buy_df.to_csv(Path(f"out/buys{today_str}.csv"), index=None)

if __name__ == "__main__":
    # create_database()
    # remove_duplicate_prices()
    run_full_scrape()
    # fill_all_price_gaps()
    find_buys()
    exit()
