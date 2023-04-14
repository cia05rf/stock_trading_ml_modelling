
import datetime as dt
import pandas as pd

from stock_trading_ml_modelling.utils.date import create_full_year_days, calc_wk_st_date
from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.scrapping.scrape_data import get_public_holidays
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df
from stock_trading_ml_modelling.database import daily_price, weekly_price

def filter_year_dates(year, year_dates):
    """Function to filter out weekends and bank holidays from year dates
    
    args:
    ----
    year_dates - list - list of date objects

    returns:
    ----
    pandas dataframe
    """
    #Mark days as required
    year_dates = pd.DataFrame(year_dates, columns=["date"])
    #Remove weekends
    year_dates["weekday"] = [v.weekday() for v in year_dates.date]
    year_dates = year_dates[~year_dates.weekday.isin([5,6])] \
        .drop(columns=["weekday"])
    #Remove bank holidays
    #Get bank holidys in this year
    public_holidays = get_public_holidays(year, )
    public_holidays = pd.DataFrame(public_holidays, columns=["date"])
    public_holidays["flag"] = True
    year_dates = pd.merge(year_dates, public_holidays, on=["date"], how="left")
    year_dates = year_dates[year_dates.flag.isnull()] \
        .drop(columns=["flag"])
    return year_dates

def create_filtered_year_dates(year, from_date=None, to_date=None):
    """Function to create a full year of dates fltering out days when the UKX 
    is closed
    
    args:
    ----
    year - int - the year to be looked at

    returns:
    ----
    pandas dataframe
    """
    #Get a full year of dates
    year_dates = create_full_year_days(year, from_date, to_date)
    #Remove bank holidays and weekends - and convert to dataframe
    year_dates = filter_year_dates(year, year_dates, )
    return year_dates

#Create a weekly table
def daily_to_weekly_price_conversion(dp_df):
    """Function to convert the daily prices into weekly prices
    
    args:
    ------
    dp_df - pandas dataframe - the daily prices

    returns:
    ------
    pandas dataframe
    """
    logger.info('Converting daily prices to weekly prices')
    #Mark the week identifier
    dp_df['isocalendar'] = [x.isocalendar()[:2] for x in dp_df['date']]
    #Get highs and lows
    high_df = dp_df.loc[dp_df['high'] > 0, ['high','ticker_id','isocalendar']] \
        .groupby(['ticker_id','isocalendar'], as_index=False) \
        .max()
    low_df = dp_df.loc[dp_df['low'] > 0, ['low','ticker_id','isocalendar']] \
        .groupby(['ticker_id','isocalendar'], as_index=False) \
        .min()
    #Get total volume for the week
    vol_df = dp_df.loc[dp_df['volume'] > 0, ['volume','ticker_id','isocalendar']] \
        .groupby(['ticker_id','isocalendar'], as_index=False) \
        .sum()
    #Get max and min week days
    max_wk_day = dp_df.loc[dp_df['close'] > 0, ['date','ticker_id','isocalendar']] \
        .groupby(['ticker_id','isocalendar'], as_index=False) \
        .max()
    min_wk_day = dp_df.loc[dp_df['open'] > 0, ['date','ticker_id','isocalendar']] \
        .groupby(['ticker_id','isocalendar'], as_index=False) \
        .min()
    #Get open price
    open_df = pd.merge(dp_df[['date','open']], min_wk_day, on='date')
    #Get close price
    close_df = pd.merge(dp_df[['date','close']], max_wk_day, on='date')
    #Form the final df
    wp_df = dp_df[['ticker_id','isocalendar']]
    wp_df = pd.merge(wp_df, min_wk_day, on=['ticker_id','isocalendar'], how="left") #date
    wp_df = pd.merge(wp_df, high_df, on=['ticker_id','isocalendar'], how="left") #high
    wp_df = pd.merge(wp_df, low_df, on=['ticker_id','isocalendar'], how="left") #low
    wp_df = pd.merge(wp_df, vol_df, on=['ticker_id','isocalendar'], how="left") #volume
    wp_df = pd.merge(wp_df, open_df[['ticker_id','isocalendar','open']], on=['ticker_id','isocalendar'], how="left") #open
    wp_df = pd.merge(wp_df, close_df[['ticker_id','isocalendar','close']], on=['ticker_id','isocalendar'], how="left") #close
    wp_df['change'] = wp_df['close'] - wp_df['open']
    wp_df = wp_df.drop_duplicates() \
        .reset_index(drop=True)
    #Get the monday of each week
    wp_df['date'] = [calc_wk_st_date(x) for x in wp_df.date]
    wp_df = wp_df.drop(columns=['isocalendar'])
    #Fill missing values
    wp_df = wp_df.fillna(0)
    return True, wp_df

def split_day_prices(new_dp_df, ticker_ids=[], from_date=None, to_date=None):
    """Function to split daily prices into update and append records
    
    args:
    ------
    new_dp_df - pandas dataframe - the dataframe with daily prices which have been scraped
    ticker_ids - list:[] - the ids of tickers to be fetched (if [] then all are fetched)
    from_date - datetime:None - the min date of prices to be fetched (if None then all are fetched)
    to_date - datetime:None - the max date of prices to be fetched (if None then all are fetched)

    returns:
    ------
    pandas dataframe, pandas dataframe
    """
    #Grab the existing data from the table
    dp_df = sqlaq_to_df(daily_price.fetch(
        ticker_ids=ticker_ids,
        from_date=from_date,
        to_date=to_date
        ))
    dp_df["date"] = dp_df.date.astype("datetime64")
    #Mark for insert or update
    dp_df["flag"] = 1
    new_dp_df = pd.merge(new_dp_df, dp_df[["ticker_id","date","id","flag"]], on=["ticker_id",'date'], how='left')
    mask = new_dp_df.flag == 1
    update_df = new_dp_df[mask]
    append_df = new_dp_df[~mask]
    return [
        update_df,
        append_df
    ]

def split_week_prices(ticker_ids=[], from_date=None, to_date=None):
    """Function to convert the daily prices into weekly prices and then split
    into update and append records
    
    args:
    ------
    ticker_ids - list:[] - the ids of tickers to be fetched (if [] then all are fetched)
    from_date - datetime:None - the min date of prices to be fetched (if None then all are fetched)
    to_date - datetime:None - the max date of prices to be fetched (if None then all are fetched)

    returns:
    ------
    pandas dataframe, pandas dataframe
    """
    dp_df = sqlaq_to_df(daily_price.fetch(
        ticker_ids=ticker_ids,
        from_date=from_date,
        to_date=to_date
        ))
    _, new_wp_df = daily_to_weekly_price_conversion(dp_df, )

    #Grab the existing data from the table
    wp_df = sqlaq_to_df(weekly_price.fetch(
        ticker_ids=ticker_ids,
        from_date=from_date,
        to_date=to_date
        ))
    wp_df["date"] = wp_df.date.astype("datetime64")
    #Mark for insert or update
    wp_df["flag"] = 1
    new_wp_df = pd.merge(new_wp_df, wp_df[["ticker_id","date","id","flag"]], on=["ticker_id",'date'], how='left')
    mask = new_wp_df.flag == 1
    update_df = new_wp_df[mask]
    append_df = new_wp_df[~mask]
    return [
        update_df,
        append_df
    ]





