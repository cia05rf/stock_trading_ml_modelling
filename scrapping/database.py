"""Functions for adding data to the database"""
import pandas as pd

from prices.get_data import all_df, fetch_tickers, fetch_ticker_markets, \
    fetch_daily_prices, fetch_weekly_prices
from prices.add_data import add_ticker_df, add_ticker_market_df, add_daily_price_df, \
    add_weekly_price_df
from libs.logs import NoLog

def create_new_tickers(tick_scrape, log=NoLog()):
    """Function to find tickers whcih have not bee seen before and add to the
    database.
    
    args:
    ----
    tick_ftse - pandas dataframe - all the scraped tickers

    returns:
    ----
    pandas dataframe - extract from database afetr update
    """
    #Check if ticker exists, if not add it to the ticker table
    tick_db = all_df(fetch_tickers())
    #add the id to the tick_ftse table
    new_tick = pd.merge(
        tick_scrape,
        tick_db[["id","ticker"]],
        on=["ticker"],
        how="left"
        )
    #find tickers which don't exist
    new_tick = new_tick[new_tick.id.isnull()]
    log.info(f"{new_tick.shape[0]} items to add to ticker")
    #add to db
    add_ticker_df(new_tick)
    #fetch updated table
    tick_db = all_df(fetch_tickers())
    return tick_db

def create_new_ticker_markets(tick_db=pd.DataFrame([]), log=NoLog()):
    """Function to find tickers whcih have not bee seen before and add to the
    database.
    
    args:
    ----
    tick_ftse - pandas dataframe - all the scraped tickers

    returns:
    ----
    pandas dataframe - extract from database afetr update
    """
    if not tick_db.shape[0]:
        tick_db = all_df(fetch_tickers())
    #Check if ticker mrket exists, if not add it to the ticker_market table
    tick_market_db = all_df(fetch_ticker_markets())
    #find ticker markets which don't exist
    new_tick_market = pd.merge(
        tick_db.rename(columns={"id":"ticker_id"}),
        tick_market_db[["id","ticker_id"]],
        on=["ticker_id"],
        how="left"
        )
    new_tick_market = new_tick_market[new_tick_market.id.isnull()]
    log.info(f"{new_tick_market.shape[0]} items to add to ticker_market")
    #add to db
    add_ticker_market_df(new_tick_market)
    #fetch updated table
    tick_market_db = all_df(fetch_ticker_markets())
    return tick_market_db