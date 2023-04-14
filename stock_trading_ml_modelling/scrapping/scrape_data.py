import pandas as pd
import re
import datetime as dt

from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.utils.str_formatting import zero_pad_single
from stock_trading_ml_modelling.config import PUBLIC_HOLS

from stock_trading_ml_modelling.scrapping.scrapes import ScrapeTickers, ScrapeBankHolidays

def get_tickers():
    logger.info("\nSCRAPPING TICKERS")
    #This section will scrap the ticker values for the FTSE 100 and FTSE 250 and store them in dataframes 'tick_ftse100' and 'tick_ftse250'.
    #Finally concatenate into 1 dataframe 'tick_ftse'.

    #{Perform async scrape
    logger.info("\nFTSE 100")
    tick_ftse100 = ScrapeTickers("ftse100").scrape()

    #Collect the rows of data
    logger.info("\nFTSE 250")
    tick_ftse250 = ScrapeTickers("ftse250").scrape()

    #Combine into 1 dataframe
    tick_ftse = pd.concat([tick_ftse100,tick_ftse250])
    tick_ftse.sort_values(['ticker'])
    tick_ftse['ticker'] = [re.sub(r'(?=[0-9A-Z])*\.(?=[0-9A-Z]+)','-',tick) for tick in tick_ftse['ticker']]
    tick_ftse['ticker'] = [re.sub(r'[^0-9A-Z\-]','',tick) for tick in tick_ftse['ticker']]
    tick_ftse['last_seen_date'] = dt.date.today()

    return tick_ftse

def get_public_holidays(year):
    """Function for getting bank holidays and converting to datetime objects"""
    dates = ScrapeBankHolidays.scrape(year)
    #Keep the public holiday dates
    dates = [d for d in dates if d[1] in PUBLIC_HOLS]
    #Convert numbers to zero padded
    dates = [zero_pad_single(d[0]) for d in dates]
    #Convert the dates
    dates = [dt.datetime.strptime(f'{d} {year}', r"%B %d %Y") for d in dates]
    return dates
