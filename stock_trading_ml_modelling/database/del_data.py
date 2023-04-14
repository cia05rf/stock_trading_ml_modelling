"""File to delete data from stock_trading_ml_modelling.database database"""
import pandas as pd

from stock_trading_ml_modelling.config import CONFIG
from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.database.models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice
from stock_trading_ml_modelling.database.models import Session as session
