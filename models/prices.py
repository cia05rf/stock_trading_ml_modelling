"""Set of functions for the SQLAlchemy classes

"""
from sqlalchemy import Column, Sequence, Integer, String, Float, Date, ForeignKey, \
    Enum, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime as dt

from models import Base

class Ticker(Base):
    __tablename__ = 'ticker'
    id = Column(Integer, nullable=False, primary_key=True, autoincrement=True)
    ticker = Column(String, nullable=False)
    company = Column(String, nullable=False)
    last_seen_date = Column(Date, default=dt.today)
    #perent
    markets = relationship('TickerMarket', backref='ticker')
    daily_prices = relationship('DailyPrice', backref='ticker')
    weekly_prices = relationship('WeeklyPrice', backref='ticker')
    #constraints
    UniqueConstraint('ticker', 'company', name='uix_1')

class TickerMarket(Base):
    __tablename__ = 'ticker_market'
    id = Column(Integer, nullable=False, primary_key=True, autoincrement=True)
    market = Column(String, nullable=False)
    first_seen_date = Column(Date, default=dt.today)
    #child
    ticker_id = Column(Integer, ForeignKey('ticker.id'))

class DailyPrice(Base):
    __tablename__ = 'daily_price'
    id = Column(Integer, nullable=False, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    week_start_date = Column(Date, nullable=False)
    #child
    ticker_id = Column(Integer, ForeignKey('ticker.id'))
    #constraints
    UniqueConstraint('ticker_id', 'date', name='uix_1')


class WeeklyPrice(Base):
    __tablename__ = 'weekly_price'
    id = Column(Integer, nullable=False, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    #child
    ticker_id = Column(Integer, ForeignKey('ticker.id'))
    #constraints
    UniqueConstraint('ticker_id', 'date', name='uix_1')
    
def create_db(engine):
    Base.metadata.create_all(engine)