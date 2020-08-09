"""File to fetch data from prices database"""
import pandas as pd
from sqlalchemy import func, and_

from config import CONFIG
from models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice
from models import Session as session

#Converting query to dataframe
def all_df(query, session=session):
    out_df = pd.read_sql(query.statement, con=session.bind)
    return out_df

def first_df(query, session=session):
    out_df = pd.read_sql(query.statement, con=session.bind)
    return out_df.iloc[0]

def fetch_tickers( 
    ticker_ids=[],
    from_date=None,
    to_date=None
    ):
    """Function to create a query to grab all sub-jobs from the offers db.
    Open sub-jobs are set to status_id 1.
    
    args:
    ----
    session - sqla session
    ticker_ids - list:[] - the ids of the records to be extracted
    from_date - dtatime:None - the min date for filtering records
    to_date - dtatime:None - the max date for filtering records

    returns:
    ----
    sqlalchemy query 
    """
    query = session.query(Ticker)
    if len(ticker_ids):
        query = query.filter(Ticker.id.in_(ticker_ids))
    if from_date:
        query = query.filter(Ticker.last_seen_date >= from_date)
    if to_date:
        query = query.filter(Ticker.last_seen_date <= to_date)
    return query    

def fetch_ticker_markets( 
    ticker_ids=[],
    from_date=None,
    to_date=None
    ):
    """Function to create a query to grab all sub-jobs from the offers db.
    Open sub-jobs are set to status_id 1.
    
    args:
    ----
    ticker_ids - list:[] - the ids of the records to be extracted

    returns:
    ----
    sqlalchemy query 
    """
    query = session.query(TickerMarket)
    if len(ticker_ids):
        query = query.filter(TickerMarket.ticker_id.in_(ticker_ids))
    if from_date:
        query = query.filter(TickerMarket.first_seen_date >= from_date)
    if to_date:
        query = query.filter(TickerMarket.first_seen_date <= to_date)
    return query

def fetch_daily_prices( 
    ticker_ids=[],
    from_date=None,
    to_date=None
    ):
    """Function to create a query to grab all sub-jobs from the offers db.
    Open sub-jobs are set to status_id 1.
    
    args:
    ----
    ticker_ids - list:[] - the ids of the records to be extracted

    returns:
    ----
    sqlalchemy query 
    """
    query = session.query(DailyPrice)
    if len(ticker_ids):
        query = query.filter(DailyPrice.ticker_id.in_(ticker_ids))
    if from_date:
        query = query.filter(DailyPrice.date >= from_date)
    if to_date:
        query = query.filter(DailyPrice.date <= to_date)
    return query

def fetch_weekly_prices( 
    ticker_ids=[],
    from_date=None,
    to_date=None
    ):
    """Function to create a query to grab all sub-jobs from the offers db.
    Open sub-jobs are set to status_id 1.
    
    args:
    ----
    ticker_ids - list:[] - the ids of the records to be extracted

    returns:
    ----
    sqlalchemy query 
    """
    query = session.query(WeeklyPrice)
    if len(ticker_ids):
        query = query.filter(WeeklyPrice.ticker_id.in_(ticker_ids))
    if from_date:
        query = query.filter(WeeklyPrice.date >= from_date)
    if to_date:
        query = query.filter(WeeklyPrice.date <= to_date)
    return query

def fetch_latest_daily_prices(
    session,
    ticker_ids=[],
    from_date=None,
    to_date=None
    ):
    """Function to get that last entry for each item
    
    args:
    ----
    session - sqla session
    ticker_ids - list:[] - the ids of the records to be extracted
    from_date - dtatime:None - the min date for filtering records
    to_date - dtatime:None - the max date for filtering records

    returns:
    ----
    sqla query
    """
    #create the sub-query
    subq = session.query(
            DailyPrice.ticker_id,
            func.max(DailyPrice.date).label("max_date")
        )
    #filter for dates
    if from_date:
        subq = subq.filter(DailyPrice.date >= from_date)
    if to_date:
        subq = subq.filter(DailyPrice.date <= to_date)
    #order the results
    subq = subq.order_by(DailyPrice.ticker_id, DailyPrice.date.desc()) \
        .group_by(DailyPrice.ticker_id) \
        .subquery("t2")
    #build the main query
    query = session.query(Ticker, subq.c.max_date) \
        .outerjoin(
            subq,
            subq.c.ticker_id == Ticker.id 
        )
    #filter on ticker ids wanted
    if len(ticker_ids):
        query = query.filter(Ticker.id.in_(ticker_ids))
    return query

def fetch_latest_weekly_prices(
    session,
    ticker_ids=[],
    from_date=None,
    to_date=None
    ):
    """Function to get that last entry for each item
    
    args:
    ----
    session - sqla session
    ticker_ids - list:[] - the ids of the records to be extracted
    from_date - dtatime:None - the min date for filtering records
    to_date - dtatime:None - the max date for filtering records

    returns:
    ----
    sqla query
    """
    #create the sub-query
    subq = session.query(
            WeeklyPrice.ticker_id,
            func.max(WeeklyPrice.date).label("max_date")
        )
    #filter for dates
    if from_date:
        subq = subq.filter(WeeklyPrice.date >= from_date)
    if to_date:
        subq = subq.filter(WeeklyPrice.date <= to_date)
    #order the results
    subq = subq.order_by(WeeklyPrice.ticker_id, WeeklyPrice.date.desc()) \
        .group_by(WeeklyPrice.ticker_id) \
        .subquery("t2")
    #build the main query
    query = session.query(Ticker, subq.c.max_date) \
        .outerjoin(
            subq,
            subq.c.ticker_id == Ticker.id 
        )
    #filter on ticker ids wanted
    if len(ticker_ids):
        query = query.filter(Ticker.id.in_(ticker_ids))
    return query