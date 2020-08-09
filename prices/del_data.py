"""File to delete data from prices database"""
import pandas as pd

from config import CONFIG
from libs.logs import NoLog
from models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice
from models import Session as session

def del_daily_prices(
    ids=[],
    ticker_ids=[],
    from_date=None,
    to_date=None,
    del_all=False,
    log=NoLog()
    ):
    """Function to delete records from the daily prices table.
    
    args:
    ----
    ids - list:[] - the ids of the records to be extracted
    ticker_ids - list:[] - the ticker ids of the records to be extracted
    from_date - dtatime:None - the min date for filtering records
    to_date - dtatime:None - the max date for filtering records
    del_all - bool:False - safety to prevet deleting the whole table

    returns:
    ----
    sqlalchemy query 
    """
    try:
        #Preform check to prevent del_all
        if not del_all and not len(ids) and not len(ticker_ids) and not from_date and not to_date:
            log.warning("Delete not performed as no attributes given and del_all is False")
            return False
        query = session.query(DailyPrice)
        if len(ids):
            query = query.filter(DailyPrice.id.in_(ids))
        if len(ticker_ids):
            query = query.filter(DailyPrice.ticker_id.in_(ticker_ids))
        if from_date:
            query = query.filter(DailyPrice.date >= from_date)
        if to_date:
            query = query.filter(DailyPrice.date <= to_date)
        query.delete(synchronize_session=False)
        session.commit()
        return True
    except:
        return False

def del_weekly_prices(
    ids=[],
    ticker_ids=[],
    from_date=None,
    to_date=None,
    del_all=False,
    log=NoLog()
    ):
    """Function to delete records from the weekly prices table.
    
    args:
    ----
    ids - list:[] - the ids of the records to be extracted
    ticker_ids - list:[] - the ids of the records to be extracted
    from_date - dtatime:None - the min date for filtering records
    to_date - dtatime:None - the max date for filtering records
    del_all - bool:False - safety to prevet deleting the whole table

    returns:
    ----
    sqlalchemy query 
    """
    try:
        #Preform check to prevent del_all
        if not del_all and not len(ids) and not len(ticker_ids) and not from_date and not to_date:
            log.warning("Delete not performed as no attributes given and del_all is False")
            return False
        query = session.query(WeeklyPrice)
        if len(ids):
            query = query.filter(WeeklyPrice.id.in_(ids))
        if len(ticker_ids):
            query = query.filter(WeeklyPrice.ticker_id.in_(ticker_ids))
        if from_date:
            query = query.filter(WeeklyPrice.date >= from_date)
        if to_date:
            query = query.filter(WeeklyPrice.date <= to_date)
        query.delete(synchronize_session=False)
        session.commit()
        return True
    except:
        return False