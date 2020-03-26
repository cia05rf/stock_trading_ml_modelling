"""Set of functions for the SQLAlchemy classes and functions

"""
from sqlalchemy import create_engine
from sqlalchemy import Column, Sequence, Integer, String, Float, Date, ForeignKey, Enum
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime as dt

def start_engine(db_name:str):
    """
    Create an engine and start a session

    args:
    ----
    db_name - str - the filepath and name of the database

    returns:
    ----
    engine, session - sqlalchemy engine, sqlalchemy session
    """
    engine = create_engine(f'sqlite:///{db_name}')
    session = sessionmaker(bind=engine)
    return engine, session

Base = declarative_base()

class Ticker(Base):
    __tablename__ = 'ticker'
    id = Column(Integer, Sequence("t_id_seq"), primary_key=True)
    ticker = Column(String, nullable=False)
    company = Column(String, nullable=False)
    last_seen_date = Column(Date, nullable=False, default=dt.today)
    
    market = relationship('TickerMarket', back_populates='market')

class TickerMarket(Base):
    __tablename__ = 'ticker_market'
    id = Column(Integer, Sequence("tm_id_seq"), primary_key=True)
    market = Column(String, nullable=False)
    first_seen_date = Column(Date, nullable=False, default=dt.today)
    
    ticker = relationship('Ticker', back_populates='market')
    ticker_id = Column(Integer, ForeignKey('ticker.id'))

class DailyPrice(Base):
    __tablename__ = 'daily_price'
    id = Column(Integer, Sequence("dp_id_seq"), primary_key=True)
    date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    week_start_date = Column(Date, nullable=False)
    
    ticker = relationship("Ticker")
    ticker_id = Column(Integer, ForeignKey('ticker.id'))

class WeeklyPrice(Base):
    __tablename__ = 'weekly_price'
    id = Column(Integer, Sequence("wp_id_seq"), primary_key=True)
    date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    
    ticker = relationship("Ticker")
    ticker_id = Column(Integer, ForeignKey('ticker.id'))
    
def create_db(db_name):
    engine, session = start_engine(db_name)
    Base.metadata.create_all(engine)
    return engine, session