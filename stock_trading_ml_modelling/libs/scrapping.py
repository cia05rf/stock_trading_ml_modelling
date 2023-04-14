#Import libraries
import pandas as pd
import numpy as np
import datetime as dt

from stock_trading_ml_modelling.config import CONFIG
from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.utils.date import create_sec_ref_li, conv_dt
from stock_trading_ml_modelling.utils.str_formatting import str_to_float_format
from stock_trading_ml_modelling.database import daily_price, weekly_price
from stock_trading_ml_modelling.libs.manage_data import split_day_prices, split_week_prices

from stock_trading_ml_modelling.scrapping.scrapes import ScrapePrices 

#Get the price history for a specific ticker
def get_day_prices(ticker:str, st_date:None, en_date:None):
    """Function fr gtting daily stock prices from webscrapping
    
    args:
    ------
    ticker - str - the identifier for the stock being looked at needs to math Yahoo.co.uk
    sec_ref_li - the list of time periods to scrape

    returns:
    ------
    pandas dataframe - contains all required prices

    """
    logger.info(f'Getting DAILY prices for -> {ticker} from {str(st_date)} to {str(en_date)}')
    #Perform async scrapes
    tick_df = ScrapePrices(ticker, st_date, en_date).scrape()
    #Check for rows - if none then return
    if not tick_df.shape[0]:
        logger.warning("Early exit due to no new records being found")
        return False, None
    #Reformat strings to floats
    tick_df['open'] = [str_to_float_format(v) for v in tick_df.open]
    tick_df['high'] = [str_to_float_format(v) for v in tick_df.high]
    tick_df['low'] = [str_to_float_format(v) for v in tick_df.low]
    tick_df['close'] = [str_to_float_format(v) for v in tick_df.close]
    tick_df['adj_close'] = [str_to_float_format(v) for v in tick_df.adj_close]
    tick_df['volume'] = [str_to_float_format(v) for v in tick_df.volume]
    tick_df['change'] = tick_df.close - tick_df.open
    #Reformat date
    tick_df['date'] = [conv_dt(v, date_or_time="short_date") for v in tick_df.date]
    #Add the ticker series
    tick_df['ticker'] = ticker
    #Mark the week identifier
    tick_df['isocalendar'] = [x.isocalendar()[:2] for x in tick_df['date']]
    min_wk_day = tick_df.loc[tick_df['open'] > 0, ['date','isocalendar']] \
        .groupby('isocalendar') \
        .min() \
        .reset_index() \
        .rename(columns={'date':'week_start_date'})
    tick_df = pd.merge(tick_df, min_wk_day, on=['isocalendar'])
    #CLEANING - Remove any rows with no prices
    tick_df = tick_df[tick_df.open > 0]
    #CLEANING - Copy row above where the change has been more than 90%
    tick_df['cl_change'] = (tick_df.close - tick_df.close.shift(1)) / tick_df.close.shift(1)
    mask = tick_df['cl_change'] < -0.9
    tick_df.loc[mask,'open'] = tick_df.open.shift(-1).copy().loc[mask]
    tick_df.loc[mask,'close'] = tick_df.close.shift(-1).copy().loc[mask]
    tick_df.loc[mask,'high'] = tick_df.high.shift(-1).copy().loc[mask]
    tick_df.loc[mask,'low'] = tick_df.low.shift(-1).copy().loc[mask]
    #Fill missing values
    tick_df = tick_df.fillna(0)
    tick_df = tick_df[['ticker','date','week_start_date','open','close','high','low','change','volume']]
    return True, tick_df

def process_daily_prices(
    ticker,
    ticker_id,
    st_date=None,
    en_date=None,
    split_from_date=None,
    split_to_date=None
    ):
    """Function to scrape prices for a ticker between selected dates, then
    split into update and add records, then perform those split/add functions 
    on the db.
    
    args:
    ----
    ticker - str - the ticker to use in scrape
    ticker_id - int - the ticker id in the db
    st_date - datetime - the date to start the scrape
    en_date - datetime - the date to end the scrape
    split_from_date - datetime - the date to start the split
    split_to_date - datetime - the date to end the split
    logger - logger
    """
    #Get new price data if neccesary
    if not st_date  or st_date < en_date:
        check, new_prices_df = get_day_prices(ticker, st_date, en_date, )
        if check:
            new_prices_df['ticker_id'] = ticker_id
            update_df, append_df = split_day_prices(
                new_prices_df,
                ticker_ids=[ticker_id],
                from_date=split_from_date,
                to_date=split_to_date
            )
            #Update existing prices in the sql database
            daily_price.update_df(update_df)
            logger.info(f"\nUPDATED {update_df.shape[0]} RECORDS IN daily_price: \n\tFROM {update_df.date.min()} \n\tTO {update_df.date.max()}")
            #Add new prices to the sql database
            daily_price.add_df(append_df)
            logger.info(f"\nADDED {append_df.shape[0]} NEW RECORDS TO daily_price: \n\tFROM {append_df.date.min()} \n\tTO {append_df.date.max()}")
        else:
            logger.info('No new records found')
    else:
        logger.info('No new records to collect')
    
def process_weekly_prices(
    ticker_id,
    split_from_date=None,
    split_to_date=None
    ):
    """Function to fetch prices for a ticker between selected dates, then
    split into update and add records, then perform those split/add functions 
    on the db.
    
    args:
    ----
    ticker_id - int - the ticker id in the db
    split_from_date - datetime - the date to start the split
    split_to_date - datetime - the date to end the split
    logger - logger
    """
    #Get new price data if neccesary
    update_df, append_df = split_week_prices(
        ticker_ids=[ticker_id],
        from_date=split_from_date,
        to_date=split_to_date,
        
    )

    #Update existing records
    weekly_price.update_df(update_df)
    logger.info(f"\nUPDATED {update_df.shape[0]} RECORDS IN weekly_price: \n\tFROM {update_df.date.min()} \n\tTO {update_df.date.max()}")

    #Add new prices to the sql database
    weekly_price.add_df(append_df)
    logger.info(f"\nADDED {append_df.shape[0]} NEW RECORDS TO weekly_price: \n\tFROM {append_df.date.min()} \n\tTO {append_df.date.max()}")
