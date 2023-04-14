"""Script for scrapping the stock price histories of stock accross the 
FTSE 350

# Daily web scrape of stock prices
This code accesses site to retrieve stock price information daily ad add it to the h5 file. The steps are:
1. Get FTSE 100 and FTSE 250 lists from wikipedia, then compile into one list.
2. open pipe to h5 file holding current price data.
3. Loop though the tickers and for each one; 
    1. find the most recent price date.
    2. convert this into a time stamp to be used on Yahoo finance.
    3. go to Yahoo Finance and get all the prices between the last time stamp and the current timestamp.
4. Add these new prices to the h5 file.
5. Create a new (empty) h5 file and transfer all the data from the old file into the new one.
6. Delete the old h5 file and rename the new one.

The sources of data are:
- https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=NMX&page=1 -> list of FTSE 350 company stock tickers
- https://finance.yahoo.com/quote/{stock-ticker}/history?period1={start-time-mark}&period2={end-time-mark}&interval={interval}&filter=history&frequency={frequency} -> Example web address to retrieve information from Yahoo finance
    - Data on this page is scroll loaded so many time indexes must be used toretrieve the dcorrect data
    - Up to 145 records can be seen from initial page load note that this includes dividends so limit to 140 for safety

The inputs required for scrapping are:
 - {stock-ticker} -> this is the ticker taken from wiki with '.L' appended to it
 - {start-tme-mark} -> This is the time in seconds since 01/01/1970 at which you would like the data retrieval to start, data retrieved is inclusive of this time
 - {end-tme-mark} -> This is the time in seconds since 01/01/1970, data retrieved is inclusive of this time
 - {interval} & {frequency} -> This is the interval for which values are given, the two must match
     - 1d = 1 every 1 days
     - 1wk = 1 every week
     - 1mo = 1 eveery month
"""

# Import libraries
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import requests as rq
import re
import datetime as dt
import os
import sys

from stock_trading_ml_modelling.config import HIST_PRICES_D, HIST_PRICES_W, \
    WEB_SCRAPE_MODE, TICK_FTSE
from stock_trading_ml_modelling.libs.scrapping import get_day_prices, \
    process_weekly_prices
from stock_trading_ml_modelling.utils.file import replace_file
from stock_trading_ml_modelling.utils.timing import ProcessTime
from stock_trading_ml_modelling.utils.ft_eng import get_col_len_df
from stock_trading_ml_modelling.utils.date import create_sec_ref_li

######################################################
### DELETE THE OLD TEMPORARY FILES (IF THEY EXIST) ###
######################################################

# Delete the old h5 files
hist_price_split = HIST_PRICES_D.split(".")
tmp_file_d = hist_price_split[0] + "_tmp." + hist_price_split[1]
hist_price_split = HIST_PRICES_W.split(".")
tmp_file_w = hist_price_split[0] + "_tmp." + hist_price_split[1]
for f in [tmp_file_d, tmp_file_w]:
    try:
        os.remove(tmp_file_d)
        print('\nSUCCESSFULLY REMOVED {}'.format(tmp_file_d))
    except Exception as e:
        print('\nERROR - REMOVING:{}'.format(e))


#########################
### SCRAPPING TICKERS ###
#########################

# This section will scrap the ticker values for the FTSE 100 and FTSE 250 and store them in dataframes 'tick_ftse100' and 'tick_ftse250'.
# Finally concatenate into 1 dataframe 'tick_ftse'.

# Fetch the data for ftse 100
web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=UKX&page=1'
resp = rq.get(web_add)
parser = bs(resp.content, 'html.parser')
# Find how many pages there are to collect
par_elem = parser.find_all('div', id='pi-colonna1-display')[0]
num_pages = int(re.sub('[^0-9]', '', par_elem.find_all('p')[0].text[-3:]))

# Collect the rows of data
row_li = []
for page in range(1, num_pages+1):
    web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=UKX&page={}'.format(
        page)
    resp = rq.get(web_add)
    parser = bs(resp.content, 'html.parser')
    # Find how many pages there are to collect
    par_elem = parser.find_all('div', id='pi-colonna1-display')[0]
    # Collect the table
    table = par_elem.find_all('table')[0]
    # Collect the rows of data
    for row in table.tbody.find_all('tr'):
        temp_row = []
        for cell in row.find_all('td')[:2]:
            temp_row.append(re.sub('\n', '', cell.text.upper()))
        row_li.append(temp_row)
print('count -> {}'.format(len(row_li)))
# Create a dataframe
tick_ftse100 = pd.DataFrame(data=row_li, columns=['ticker', 'company'])
tick_ftse100['index'] = 'FTSE100'
print('tick_ftse100.head() -> \n{}'.format(tick_ftse100.head()))

# Fetch the data for ftse 250
web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=MCX&page=1'
resp = rq.get(web_add)
parser = bs(resp.content, 'html.parser')
# Find how many pages there are to collect
par_elem = parser.find_all('div', id='pi-colonna1-display')[0]
num_pages = int(re.sub('[^0-9]', '', par_elem.find_all('p')[0].text[-3:]))

# Collect the rows of data
row_li = []
for page in range(1, num_pages+1):
    web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=MCX&page={}'.format(
        page)
    resp = rq.get(web_add)
    parser = bs(resp.content, 'html.parser')
    # Find how many pages there are to collect
    par_elem = parser.find_all('div', id='pi-colonna1-display')[0]
    # Collect the table
    table = par_elem.find_all('table')[0]
    # Collect the rows of data
    for row in table.tbody.find_all('tr'):
        temp_row = []
        for cell in row.find_all('td')[:2]:
            temp_row.append(re.sub('\n', '', cell.text.upper()))
        row_li.append(temp_row)
print('count -> {}'.format(len(row_li)))
# Create a dataframe
tick_ftse250 = pd.DataFrame(data=row_li, columns=['ticker', 'company'])
tick_ftse250['index'] = 'FTSE250'
print('tick_ftse250.head() -> \n{}'.format(tick_ftse250.head()))

# Combine into 1 dataframe
tick_ftse = pd.concat([tick_ftse100, tick_ftse250])
print('shape -> {}'.format(tick_ftse.shape))
print('value_counts -> \n{}'.format(tick_ftse.ticker.value_counts()))
tick_ftse.sort_values(['ticker'])
tick_ftse['ticker'] = [re.sub(
    '(?=[0-9A-Z])*\.(?=[0-9A-Z]+)', '-', tick) for tick in tick_ftse['ticker']]
tick_ftse['ticker'] = [re.sub('[^0-9A-Z\-]', '', tick)
                       for tick in tick_ftse['ticker']]


##################################
### GET THE LATEST PRICE DATES ###
##################################

# Get the latest price file.
# Loop through the tickers in tick_ftse and for each one get the latest date of scrape.
# Convert this date into a timestamp and add to the dataframe.

# open the price file
hist_prices_df = pd.read_hdf(HIST_PRICES_D)
# Convert date


def conv_date(_str_in):
    if type(_str_in) == str:
        return dt.datetime.strptime(_str_in, '%Y-%m-%d')
    else:
        return _str_in


hist_prices_df.date = [conv_date(x) for x in hist_prices_df.date]
print('hist_prices_df.dtypes -> \n{}'.format(hist_prices_df.dtypes))
# If there are any missing week_start_date fields then calculate them
if hist_prices_df.week_start_date.isnull().sum() > 0:
    hist_prices_df['isocalendar'] = [x.isocalendar()[:2]
                                     for x in hist_prices_df['date']]
    min_wk_day = hist_prices_df.loc[hist_prices_df.open > 0, ['ticker', 'date', 'isocalendar']].groupby(
        ['ticker', 'isocalendar']).min().reset_index().rename(columns={'date': 'week_start_date'})
    hist_prices_df = pd.merge(hist_prices_df.drop(columns=['week_start_date']), min_wk_day, on=[
                              'ticker', 'isocalendar']).drop(columns=['isocalendar'])
print('hist_prices_df.head() -> \n{}'.format(hist_prices_df.head()))

############################
### SCRAPE STOCK HISTORY ###
############################

# Working backwards through time from now until the last price scrape date collect all the daily data for a stock. Daily is used as it can later be summarised into weekly or monthly if required.
# This will then be put into a dataframe containing:
# - ticker
# - Company name
# - date
# - open
# - high
# - low
# - close
# - Adjusted close (for divs and splits)
# - volume

# Get the lengths of each column in the DF
col_lens = get_col_len_df(hist_prices_df)
# col_lens = {'ticker': 4,
#  'date': 19,
#  'open': 8,
#  'close': 8,
#  'high': 8,
#  'low': 8,
#  'change': 22,
#  'volume': 12,
#  'ema12': 18,
#  'ema26': 18,
#  'macd_line': 23,
#  'signal_line': 23,
#  'macd': 23}
print('col_lens -> \n{}'.format(col_lens))

# Setup stores  # open the price file
hf_d = pd.HDFStore(tmp_file_d)
group_name_d = r'data'
hf_w = pd.HDFStore(tmp_file_w)
group_name_w = r'data'

# Scrape daily price data
out_cols = ['ticker', 'date', 'open', 'close', 'high',
            'low', 'change', 'volume', 'week_start_date']
errors = []
run_time = ProcessTime()
for tick in tick_ftse['ticker']:
    try:
        print('\n{} RUNNING FOR -> {}'.format(len(run_time.lap_li), tick))

        # Get the existing prices
        tick_df = hist_prices_df[hist_prices_df.ticker == tick][out_cols]
        print('DAILY SHAPE BEFORE -> {}'.format(tick_df.shape))

        # DAILY PRICES
        # Establish the date to start scrapping on
        if str(WEB_SCRAPE_MODE).lower() == 'update':
            st_date = tick_df.date.max()
            if pd.isnull(st_date):
                # Treat as a new share
                st_date = dt.datetime(1970, 1, 1)
        elif str(WEB_SCRAPE_MODE).lower() == 'full':
            st_date = dt.datetime(1970, 1, 1)
        else:
            raise Exception(
                'ValueError', 'web scrape mode must be either \'update\' or \'full\'')

        # Establish the end date for scrapping
        en_date = dt.date.today()+dt.timedelta(days=1)
        en_date = dt.datetime(en_date.year, en_date.month,
                              en_date.day, 0, 0, 0)
        # Match to sat if sunday
        if en_date.weekday() == 6:
            en_date = en_date-dt.timedelta(days=(en_date.weekday()-5))

        # Get new price data if neccesary
        if st_date < en_date:
            new_tick_df = get_day_prices(
                tick, create_sec_ref_li(st_date, en_date))
            # Join onto existing data if exists, if not create a new df for it
            if tick_df.shape[0] > 0:
                tick_df = tick_df.append(new_tick_df)
            else:
                tick_df = new_tick_df
            # Drop duplicates
            tick_df = tick_df.drop_duplicates()
        else:
            print('No new records to collect')

        print('DAILY FINAL SHAPE -> {}'.format(tick_df.shape))

        # Clarify col_lens with cur cols in data
        col_lens_tmp = {}
        for col in tick_df:
            if col in col_lens:
                col_lens_tmp[col] = col_lens[col]
        col_lens = col_lens_tmp

        # Add to daily h5 file
        tick_df.to_hdf(HIST_PRICES_D, key=group_name_d,
                       append=True, min_itemsize=col_lens)

        # WEEKLY PRICES
        # Convert to weekly prices
        resp = process_weekly_prices(tick_df)
        if resp[0]:
            df_w = resp[1]
        else:
            # Raise error
            raise resp[1]

        # Drop duplicates
        df_w = df_w.drop_duplicates()

        print('WEEKLY FINAL SHAPE -> {}'.format(df_w.shape))

        # Add to weekly h5 file
        col_lens_w = col_lens.copy()
        try:
            del col_lens_w['week_start_date']
        except KeyError:
            print('ERROR - Could not find the key "week_start_date"')
        df_w.to_hdf(HIST_PRICES_W, key=group_name_w,
                    append=True, min_itemsize=col_lens_w)

        # Lap
        run_time.lap()
        run_time.show_latest_lap_time(show_time=True)
    except Exception as e:
        print('ERROR -> {}'.format(e))
        errors.append(e)
hf_d.close()
hf_w.close()
print('\n\n')
run_time.end()
print('\nERROR COUNT -> {}'.format(len(errors)))
if len(errors) > 0:
    print('ERRORS -> {}'.format(errors))


####################################
### RENAMING AND DELETEING FILES ###
####################################

# Delete the old h5 file and rename the TMP
replace_file(HIST_PRICES_D, tmp_file_d)
replace_file(HIST_PRICES_W, tmp_file_w)


##################################
### EXPORTING THE FTSE TICKERS ###
##################################
# Export the ftse list
tick_ftse.to_csv(path_or_buf=TICK_FTSE)


#######################
### END THE PROGRAM ###
#######################
sys.exit()

# import pandas as pd
# from stock_trading_ml_modelling.config import CONFIG
# _tick_df = pd.read_hdf(HIST_PRICES_D,key='data')
# # w_df = pd.read_hdf(HIST_PRICES_W,key='data')

# _tick_df = _tick_df[['ticker','date','open','close','high','low','change','volume']]
# # w_df = w_df[['ticker','date','open','close','high','low','change','volume']]

# #Mark the week identifier
# _tick_df['isocalendar'] = [x.isocalendar()[:2] for x in _tick_df['date']]
# _min_wk_day = _tick_df.loc[_tick_df['open'] > 0,['date','ticker','isocalendar']].groupby(['ticker','isocalendar']).min().reset_index().rename(columns={'date':'week_start_date'})
# _tick_df = pd.merge(_tick_df,_min_wk_day,on=['ticker','isocalendar'])
# #CLEANING - Remove any rows with zero volume
# _tick_df = _tick_df[_tick_df['volume'] > 0]
# #CLEANING - Copy row above where the change has been more than 90%
# _tick_df['cl_change'] = (_tick_df['close'] - _tick_df['close'].shift(1))/_tick_df['close'].shift(1)
# _check_s = _tick_df['cl_change'] < -0.9
# _tick_df.loc[_check_s,'open'] = _tick_df['open'].shift(-1).copy().loc[_check_s]
# _tick_df.loc[_check_s,'close'] = _tick_df['close'].shift(-1).copy().loc[_check_s]
# _tick_df.loc[_check_s,'high'] = _tick_df['high'].shift(-1).copy().loc[_check_s]
# _tick_df.loc[_check_s,'low'] = _tick_df['low'].shift(-1).copy().loc[_check_s]
# _tick_df = _tick_df.loc[:,['ticker','date','week_start_date','open','close','high','low','change','volume']]

# _tick_df.to_hdf(HIST_PRICES_D,key='data')
# # w_df.to_hdf(HIST_PRICES_W,key='data')
