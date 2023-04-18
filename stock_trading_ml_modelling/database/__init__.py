"""Create sub-classes for querying the database"""
from sqlalchemy import func, and_
import logging

from stock_trading_ml_modelling.database.models import Session as session
from stock_trading_ml_modelling.database.models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice
from stock_trading_ml_modelling.database.add_data import _add_df
from stock_trading_ml_modelling.database.update_data import _update_df


class TickerCl:
    def __init__(self):
        pass

    def add_df(self, df, session=session):
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
            _add_df(df, Ticker, session=session)

    def fetch(self,
        ticker_ids=[],
        from_date=None,
        to_date=None
        ):
        """Function to create a query to grab all tickers from the offers db.
        
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

    def update_df(self, df, session=session):
        """Function for updating records from a dataframe"""
        return _update_df(df, Ticker, session=session)

class TickerMarketCl:
    def __init__(self):
        pass

    def add_df(self, df, session=session):
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
            _add_df(df, TickerMarket, session=session)

    def fetch(self,
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

    def update_df(self, df, session=session):
        """Function for updating records from a dataframe"""
        return _update_df(df, TickerMarket, session=session)

class DailyPriceCl:
    def __init__(self):
        pass

    def add_df(self, df, session=session):
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
            _add_df(df, DailyPrice, session=session)

    def fetch(self,
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

        
    def fetch_latest(self,
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

    def update_df(self, df, session=session):
        """Function for updating records from a dataframe"""
        return _update_df(df, DailyPrice, session=session)
        
    def remove(self,
        ids=[],
        ticker_ids=[],
        from_date=None,
        to_date=None,
        del_all=False
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
                logging.warning("Delete not performed as no attributes given and del_all is False")
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

class WeeklyPriceCl:
    def __init__(self):
        pass

    def add_df(self, df, session=session):
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
            _add_df(df, WeeklyPrice, session=session)

    def fetch(self,
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

    def fetch_latest(self,
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

    def update_df(self, df, session=session):
        """Function for updating records from a dataframe"""
        return _update_df(df, WeeklyPrice, session=session)

    def remove(self,
        ids=[],
        ticker_ids=[],
        from_date=None,
        to_date=None,
        del_all=False
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
                logging.warning("Delete not performed as no attributes given and del_all is False")
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

ticker = TickerCl()
ticker_market = TickerMarketCl()
daily_price = DailyPriceCl()
weekly_price = WeeklyPriceCl()