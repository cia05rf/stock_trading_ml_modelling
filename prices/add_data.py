"""Functions for adding data to the prices database"""
from models import Session as session
from models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice

def add_df(records_df, DestClass, fields=[], session=session):
    """Generic function for adding to a table from a dataframe.
    
    args:
    ----
    records_df - pandas dataframe - the records to be added to the database 
    DestClass - sqla table class - the class of the receiving table
    session - sqla session:None - the offers db session object

    returns:
    ----
    None
    """
    if len(fields) > 0:
        records_df = records_df[fields]
    objects = [DestClass(**r) for _,r in records_df.iterrows()]
    session.bulk_save_objects(objects)
    session.commit()

def add_ticker_df(df, session=session):
    """Function to add data to the database.
    
    args:
    ----
    df - pandas dataframe - the data to be added to the database
    session - sqla session:None - the db session object

    returns:
    ----
    None
    """
    if df.shape[0]:
        keep_cols = ['ticker','company']
        if 'last_seen_date' in df.columns:
            keep_cols.append('last_seen_date')
        df = df[keep_cols] \
            .drop_duplicates()
        add_df(df, Ticker, session=session)

def add_ticker_market_df(df, session=session):
    """Function to add data to the database.
    
    args:
    ----
    df - pandas dataframe - the data to be added to the database
    session - sqla session:None - the db session object

    returns:
    ----
    None
    """
    if df.shape[0]:
        keep_cols = ['market','ticker_id']
        if 'first_seen_date' in df.columns:
            keep_cols.append('first_seen_date')
        df = df[keep_cols] \
            .drop_duplicates()
        add_df(df, TickerMarket, session=session)

def add_daily_price_df(df, session=session):
    """Function to add data to the database.
    
    args:
    ----
    df - pandas dataframe - the data to be added to the database
    session - sqla session:None - the db session object

    returns:
    ----
    None
    """
    if df.shape[0]:
        df = df[['date','open','high','low','close','change','volume','week_start_date','ticker_id']] \
            .drop_duplicates()
        add_df(df, DailyPrice, session=session)

def add_weekly_price_df(df, session=session):
    """Function to add data to the database.
    
    args:
    ----
    df - pandas dataframe - the data to be added to the database
    session - sqla session:None - the db session object

    returns:
    ----
    None
    """
    if df.shape[0]:
        df = df[['date','open','high','low','close','change','volume','ticker_id']] \
            .drop_duplicates()
        add_df(df, WeeklyPrice, session=session)