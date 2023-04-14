"""Functions for adding data to the database"""
import pandas as pd

from stock_trading_ml_modelling.database.get_data import sqlaq_to_df
from stock_trading_ml_modelling.database import ticker, ticker_market, daily_price, weekly_price
from stock_trading_ml_modelling.utils.log import logger

def create_new_tickers(tick_scrape):
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
    tick_db = sqlaq_to_df(ticker.fetch())
    #add the id to the tick_ftse table
    new_tick = pd.merge(
        tick_scrape,
        tick_db[["id","ticker"]],
        on=["ticker"],
        how="left"
        )
    #find tickers which don't exist
    new_tick = new_tick[new_tick.id.isnull()]
    logger.info(f"{new_tick.shape[0]} items to add to ticker")
    #add to db
    ticker.add_df(new_tick)
    #fetch updated table
    tick_db = sqlaq_to_df(ticker.fetch())
    return tick_db

def create_new_ticker_markets(tick_db=pd.DataFrame([])):
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
        tick_db = sqlaq_to_df(ticker.fetch())
    #Check if ticker mrket exists, if not add it to the ticker_market table
    tick_market_db = sqlaq_to_df(ticker_market.fetch())
    #find ticker markets which don't exist
    new_tick_market = pd.merge(
        tick_db.rename(columns={"id":"ticker_id"}),
        tick_market_db[["id","ticker_id"]],
        on=["ticker_id"],
        how="left"
        )
    new_tick_market = new_tick_market[new_tick_market.id.isnull()]
    logger.info(f"{new_tick_market.shape[0]} items to add to ticker_market")
    #add to db
    ticker_market.add_df(new_tick_market)
    #fetch updated table
    tick_market_db = sqlaq_to_df(ticker_market.fetch())
    return tick_market_db