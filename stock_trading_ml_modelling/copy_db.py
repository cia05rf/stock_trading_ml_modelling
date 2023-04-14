import pandas as pd
from pathlib import Path
from sqlalchemy.orm import sessionmaker, scoped_session
from tqdm import tqdm

from stock_trading_ml_modelling.config import STORE_PATH
from sqlalchemy import create_engine
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df
from stock_trading_ml_modelling.database import ticker, ticker_market, daily_price, weekly_price
from stock_trading_ml_modelling.database.add_data import _add_df
from stock_trading_ml_modelling.database.models import engine, Session as session
from stock_trading_ml_modelling.database.models.prices import create_db, Ticker, TickerMarket, DailyPrice, WeeklyPrice

eng_old = create_engine(
    f'sqlite:///{str(STORE_PATH / "prices_old.db")}'
)
old_session = scoped_session(sessionmaker(bind=eng_old, expire_on_commit=False))

#Create the new db
create_db(engine)

#ticker
ticker_df = sqlaq_to_df(ticker.fetch(), session=old_session)
#add to the new database
_add_df(ticker_df, Ticker)

#ticker_market
ticker_market_df = sqlaq_to_df(ticker_market.fetch(), session=old_session)
#add to the new database
_add_df(ticker_market_df, TickerMarket)

#daily_price
for id in tqdm(ticker_df.id, total=ticker_df.shape[0]):
    daily_price_df = sqlaq_to_df(daily_price.fetch(ticker_ids=[id]), session=old_session)
    #add to the new database
    _add_df(daily_price_df, DailyPrice)

#weekly_price
for id in tqdm(ticker_df.id, total=ticker_df.shape[0]):
    weekly_price_df = sqlaq_to_df(weekly_price.fetch(ticker_ids=[id]), session=old_session)
    #add to the new database
    _add_df(weekly_price_df, WeeklyPrice)

