#Import libraries
import pandas as pd
import numpy as np
import datetime as dt

from config import CONFIG
from libs.logs import NoLog
from utils.date import create_sec_ref_li, conv_dt
from utils.str_formatting import str_to_float_format
from prices.add_data import add_daily_price_df, add_weekly_price_df
from prices.update_data import update_daily_prices, update_weekly_prices
from libs.manage_data import split_day_prices, split_week_prices

from scrapping.scrapes import scrape_prices 

#Get the price history for a specific ticker
def get_day_prices(ticker:str, st_date:None, en_date:None, log=NoLog()):
    """Function fr gtting daily stock prices from webscrapping
    
    args:
    ------
    ticker - str - the identifier for the stock being looked at needs to math Yahoo.co.uk
    sec_ref_li - the list of time periods to scrape

    returns:
    ------
    pandas dataframe - contains all required prices

    """
    #Get the time loops
    sec_ref_li = create_sec_ref_li(st_date, en_date, days=CONFIG["scrape"]["max_days"])
    log.info('Getting DAILY prices for -> {}'.format(ticker))
    tick_df = pd.DataFrame([])
    log.info('Number of webscrapes to perform -> {}'.format(len(sec_ref_li)))
    #For each time frame perform a scrape
    for i,secs in enumerate(sec_ref_li):
        log.info('Making call {} -> {} - {}'.format(i, dt.datetime.fromtimestamp(secs[0]), dt.datetime.fromtimestamp(secs[1])))
        _, new_tick_df = scrape_prices(ticker, st_secs=secs[0], en_secs=secs[1], log=log)
        log.info(f"{new_tick_df.shape[0]} new records found")
        #break loop if no new records
        if not new_tick_df.shape[0]:
            break
        tick_df = tick_df.append(new_tick_df)
    #Check for rows - if none then return
    if not tick_df.shape[0]:
        log.warning("Early exit due to no new records being found")
        return False, None
    #Reformat strings to floats
    tick_df['open'] = [str_to_float_format(v) for v in tick_df.open]
    tick_df['high'] = [str_to_float_format(v) for v in tick_df.high]
    tick_df['low'] = [str_to_float_format(v) for v in tick_df.low]
    tick_df['close'] = [str_to_float_format(v) for v in tick_df.close]
    tick_df['adj_close'] = [str_to_float_format(v) for v in tick_df.adj_close]
    tick_df['volume'] = [str_to_float_format(v) for v in tick_df.volume]
    tick_df['change'] = tick_df.open - tick_df.close
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
    split_to_date=None,
    log=NoLog()
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
    log - logger
    """
    #Get new price data if neccesary
    if not st_date  or st_date < en_date:
        check, new_prices_df = get_day_prices(ticker, st_date, en_date, log=log)
        if check:
            new_prices_df['ticker_id'] = ticker_id
            update_df, append_df = split_day_prices(
                new_prices_df,
                ticker_ids=[ticker_id],
                from_date=split_from_date,
                to_date=split_to_date
            )
            #Update existing prices in the sql database
            update_daily_prices(update_df)
            log.info(f"\nUPDATED {update_df.shape[0]} RECORDS IN daily_price: \n\tFROM {update_df.date.min()} \n\tTO {update_df.date.max()}")
            #Add new prices to the sql database
            add_daily_price_df(append_df)
            log.info(f"\nADDED {append_df.shape[0]} NEW RECORDS TO daily_price: \n\tFROM {append_df.date.min()} \n\tTO {append_df.date.max()}")
        else:
            log.info('No new records found')
    else:
        log.info('No new records to collect')
    
def process_weekly_prices(
    ticker_id,
    split_from_date=None,
    split_to_date=None,
    log=NoLog()
    ):
    """Function to fetch prices for a ticker between selected dates, then
    split into update and add records, then perform those split/add functions 
    on the db.
    
    args:
    ----
    ticker_id - int - the ticker id in the db
    split_from_date - datetime - the date to start the split
    split_to_date - datetime - the date to end the split
    log - logger
    """
    #Get new price data if neccesary
    update_df, append_df = split_week_prices(
        ticker_ids=[ticker_id],
        from_date=split_from_date,
        to_date=split_to_date,
        log=log
    )

    #Update existing records
    update_weekly_prices(update_df)
    log.info(f"\nUPDATED {update_df.shape[0]} RECORDS IN weekly_price: \n\tFROM {update_df.date.min()} \n\tTO {update_df.date.max()}")

    #Add new prices to the sql database
    add_weekly_price_df(append_df)
    log.info(f"\nADDED {append_df.shape[0]} NEW RECORDS TO weekly_price: \n\tFROM {append_df.date.min()} \n\tTO {append_df.date.max()}")
