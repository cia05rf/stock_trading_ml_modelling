"""Functions for managing the database"""
from tqdm import tqdm
import datetime as dt
import pandas as pd

from stock_trading_ml_modelling.config import WEB_SCRAPE_MAX_DAYS
from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.utils.date import calc_date_window
from stock_trading_ml_modelling.utils.timing import ProcessTime
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df
from stock_trading_ml_modelling.database import ticker, ticker_market, daily_price, weekly_price
from stock_trading_ml_modelling.libs.scrapping import get_day_prices, process_daily_prices, process_weekly_prices

from stock_trading_ml_modelling.libs.manage_data import create_filtered_year_dates

def _find_duplicates(prices):
    """Find duplicate prices
    
    args:
    ----
    prices - pandas dataframe

    returns:
    ----
    pands dataframe
    """
    #Find duplicate dates
    dates = prices.date.value_counts()
    dates.name = "date_count"
    dates.index.name = "date"
    dates = dates[dates > 1] \
        .to_frame() \
        .reset_index()
    #Isolate those dates in the prices dataframe
    dups = prices[prices.date.isin(dates.date)]
    dups = dups.sort_values(["date","volume"], ascending=[True,False])
    #Keep the first for each date
    mask = dups.date != dups.date.shift(1)
    del_df = dups[~mask]
    return del_df

def remove_duplicate_daily_prices():
    """Function for removing any duplicated prices in the database
    keeping the most recent (found by highest volume).
    """
    #Fetch all the tickers
    tickers = sqlaq_to_df(ticker.fetch())
    #Loop through tickers
    for id in tqdm(tickers.id, total=tickers.shape[0], desc="Remove duplicate daily prices"):
        #Fetch all prices
        dp = sqlaq_to_df(daily_price.fetch(ticker_ids=[id]))
        #Find duplicate dates
        dp_del = _find_duplicates(dp)
        #Delete the others
        if daily_price.remove(dp_del.id.to_list()):
            logger.info(f"Delted {dp_del.shape[0]} records from daily_price, ticker_id -> {id}")
        else:
            logger.error(f"Unable to delete {dp_del.shape[0]} records from daily_price, ticker_id -> {id}")

def remove_duplicate_weekly_prices():
    """Function for removing any duplicated prices in the database
    keeping the most recent (found by highest volume).
    """
    #Fetch all the tickers
    tickers = sqlaq_to_df(ticker.fetch())
    #Loop through tickers
    for id in tqdm(tickers.id, total=tickers.shape[0], desc="Remove duplicate weekly prices"):
        #Fetch all prices
        wp = sqlaq_to_df(weekly_price.fetch(ticker_ids=[id]))
        #Find duplicate dates
        wp_del = _find_duplicates(wp)
        #Delete the others
        if weekly_price.remove(wp_del.id.to_list()):
            logger.info(f"Delted {wp_del.shape[0]} records from weekly_price, ticker_id -> {id}")
        else:
            logger.error(f"Unable to delete {wp_del.shape[0]} records from weekly_price, ticker_id -> {id}")

def fill_price_gaps(
    from_date=dt.datetime(1970,1,1),
    to_date=dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    ):
    """Function for finding missing prices in tickers and filling them.
    Looks at both daily and weekly price tables.
    """
    #Create a collection of years
    years = []
    cur_year = from_date.year
    while cur_year <= to_date.year:
        years.append(cur_year)
        cur_year += 1
    #Loop each year
    all_year_dates = pd.DataFrame([])
    for year in tqdm(years, total=len(years), desc="Loop through years to find dates"):
        #establish bounding dates
        year_from_date = None if year != from_date.year else from_date
        year_to_date = None if year != to_date.year else to_date
        #Get filtered year dates
        year_dates = create_filtered_year_dates(year, from_date=year_from_date, to_date=year_to_date, )
        #Add to the full list
        all_year_dates = pd.concat([all_year_dates, year_dates])
    #Order the dates (just in case)
    all_year_dates = all_year_dates.sort_values(["date"]) \
        .reset_index(drop=True)
    #Fetch all the tickers
    tickers = sqlaq_to_df(ticker.fetch())
    #Loop through tickers
    errors = []
    run_time = ProcessTime()
    for _,r in tqdm(tickers[["id","ticker"]].iterrows(), total=tickers.shape[0], desc="Filling in gaps"):
        logger.info(f"Filling gaps in {r.id} -> {r.ticker}")
        try:
            #Fetch all prices
            dp = sqlaq_to_df(daily_price.fetch(ticker_ids=[r.id]))
            dp["date"] = dp.date.astype("datetime64[ns]")
            #Identify missing dates
            missing_dates = pd.merge(all_year_dates, dp[["date","id"]], on=["date"], how="left")
            #Identify the start date and remove all missing date before that
            start_date = missing_dates[~missing_dates.id.isnull()].date.min()
            missing_dates = missing_dates[missing_dates.date > start_date]
            #Remove all other items which have dates
            missing_dates = missing_dates[missing_dates.id.isnull()]
            #Order remaining dates
            missing_dates = missing_dates.sort_values("date")
            #Create groupings no larger than max_days (in config)
            st_d = None
            date_groups = []
            missing_dates = missing_dates.date.to_list()
            if len(missing_dates):
                for i,d in enumerate(missing_dates):
                    if not st_d:
                        st_d = d
                    else:
                        #Append when group gets too big
                        if (d - st_d).days > WEB_SCRAPE_MAX_DAYS:
                            date_groups.append([st_d, missing_dates[i-1]])
                            #Update the start date
                            st_d = d
                #Append the last item
                date_groups.append([st_d, d])
                #Scrape the missing prices
                logger.info('Number of webscrapes to perform -> {}'.format(len(date_groups)))
                #For each time frame perform a scrape
                try: #Try loop so as not to miss all following date groups
                    for i,dates in enumerate(date_groups):
                        logger.info(f"Running dates {i} -> {dt.datetime.strptime(str(dates[0])[:10], '%Y-%m-%d')} - {dt.datetime.strptime(str(dates[1])[:10], '%Y-%m-%d')}")
                        process_daily_prices(
                            r.ticker,
                            r.id,
                            st_date=dates[0],
                            en_date=dates[1],
                            
                            )
                except Exception as e:
                    logger.error(e)
                    errors.append({'ticker_id':r.id, 'ticker':r.ticker, "error":e, "st_date":dates[0], "en_dates":dates[1]})
                #Run an update on th weekly prices
                process_weekly_prices(
                    r.id,
                    
                    )
        except Exception as e:
            logger.error(e)
            errors.append({'ticker_id':r.id, 'ticker':r.ticker, "error":e})
        #Lap
        logger.info(run_time.lap())
        logger.info(run_time.show_latest_lap_time(show_time=True))
    logger.info(f"GAP FILL RUN TIME - {run_time.end()}")

    logger.info(f'\nGAP FILL ERROR COUNT -> {len(errors)}')
    if len(errors) > 0:
        logger.info('GAP FILL ERRORS ->')
        for e in errors:
            logger.error(e)