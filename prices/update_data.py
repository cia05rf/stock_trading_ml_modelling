"""Functions for updating data in the prices database"""
from models import Session as session
from models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice

def _update_df(df, DestClass, session=session):
    """Function for updating records from a dataframe"""
    session.bulk_update_mappings(DestClass, df.to_dict(orient="records"))
    session.commit()

def update_ticker_df(df, session=session):
    """Function for updating records from a dataframe"""
    return _update_df(df, Ticker, session=session)

def update_weekly_prices(df, session=session):
    """Function for updating records from a dataframe"""
    return _update_df(df, WeeklyPrice, session=session)

def update_daily_prices(df, session=session):
    """Function for updating records from a dataframe"""
    return _update_df(df, DailyPrice, session=session)
