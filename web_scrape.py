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

#Import libraries
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import requests as rq
import re
import datetime as dt
import os
import tables
import sys

from rf_modules import *
from config import CONFIG
from functions.web_scrape_funcs import *
from functions.ft_eng_funcs import calc_ema,calc_macd,calc_ema_macd,get_col_len_df

######################################################
### DELETE THE OLD TEMPORARY FILES (IF THEY EXIST) ###
######################################################

#close any open h5 files
tables.file._open_files.close_all()

#Delete the old h5 files
try:
    os.remove(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_d_tmp'])
    print('\nSUCCESSFULLY REMOVED {}'.format(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_d_tmp']))
except Exception as e:
    print('\nERROR - REMOVING:{}'.format(e))
try:
    os.remove(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w_tmp'])
    print('\nSUCCESSFULLY REMOVED {}'.format(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w_tmp']))
except Exception as e:
    print('\nERROR - REMOVING:{}'.format(e))


#########################
### SCRAPPING TICKERS ###
#########################

#This section will scrap the ticker values for the FTSE 100 and FTSE 250 and store them in dataframes 'tick_ftse100' and 'tick_ftse250'.
#Finally concatenate into 1 dataframe 'tick_ftse'.

#Fetch the data for ftse 100
web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=UKX&page=1'
resp = rq.get(web_add)
parser = bs(resp.content,'html.parser')
#Find how many pages there are to collect
par_elem = parser.find_all('div',id='pi-colonna1-display')[0]
num_pages = int(re.sub('[^0-9]','',par_elem.find_all('p')[0].text[-3:]))

#Collect the rows of data
row_li = []
for page in range(1,num_pages+1):
    web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=UKX&page={}'.format(page)
    resp = rq.get(web_add)
    parser = bs(resp.content,'html.parser')
    #Find how many pages there are to collect
    par_elem = parser.find_all('div',id='pi-colonna1-display')[0]
    #Collect the table
    table = par_elem.find_all('table')[0]
    #Collect the rows of data
    for row in table.tbody.find_all('tr'):
        temp_row = []
        for cell in row.find_all('td')[:2]:
            temp_row.append(re.sub('\n','',cell.text.upper()))
        row_li.append(temp_row)
print('count -> {}'.format(len(row_li)))
#Create a dataframe
tick_ftse100 = pd.DataFrame(data=row_li,columns=['ticker','company'])
tick_ftse100['index'] = 'FTSE100'
print('tick_ftse100.head() -> \n{}'.format(tick_ftse100.head()))

#Fetch the data for ftse 250
web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=MCX&page=1'
resp = rq.get(web_add)
parser = bs(resp.content,'html.parser')
#Find how many pages there are to collect
par_elem = parser.find_all('div',id='pi-colonna1-display')[0]
num_pages = int(re.sub('[^0-9]','',par_elem.find_all('p')[0].text[-3:]))

#Collect the rows of data
row_li = []
for page in range(1,num_pages+1):
    web_add = r'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=MCX&page={}'.format(page)
    resp = rq.get(web_add)
    parser = bs(resp.content,'html.parser')
    #Find how many pages there are to collect
    par_elem = parser.find_all('div',id='pi-colonna1-display')[0]
    #Collect the table
    table = par_elem.find_all('table')[0]
    #Collect the rows of data
    for row in table.tbody.find_all('tr'):
        temp_row = []
        for cell in row.find_all('td')[:2]:
            temp_row.append(re.sub('\n','',cell.text.upper()))
        row_li.append(temp_row)
print('count -> {}'.format(len(row_li)))
#Create a dataframe
tick_ftse250 = pd.DataFrame(data=row_li,columns=['ticker','company'])
tick_ftse250['index'] = 'FTSE250'
print('tick_ftse250.head() -> \n{}'.format(tick_ftse250.head()))

#Combine into 1 dataframe
tick_ftse = pd.concat([tick_ftse100,tick_ftse250])
print('shape -> {}'.format(tick_ftse.shape))
print('value_counts -> \n{}'.format(tick_ftse.ticker.value_counts()))
tick_ftse.sort_values(['ticker'])
tick_ftse['ticker'] = [re.sub('(?=[0-9A-Z])*\.(?=[0-9A-Z]+)','-',tick) for tick in tick_ftse['ticker']]
tick_ftse['ticker'] = [re.sub('[^0-9A-Z\-]','',tick) for tick in tick_ftse['ticker']]


##################################
### GET THE LATEST PRICE DATES ###
##################################

#Get the latest price file.
#Loop through the tickers in tick_ftse and for each one get the latest date of scrape.
#Convert this date into a timestamp and add to the dataframe.

#open the price file
hist_prices_df = pd.read_hdf(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_d'])
print('hist_prices_df.head() -> \n{}'.format(hist_prices_df.head()))

#Convert date
def conv_date(_str_in):
    if type(_str_in) == str:
        return dt.datetime.strptime(_str_in,'%Y-%m-%d')
    else:
        return _str_in
hist_prices_df.date = [conv_date(x) for x in hist_prices_df.date]
print('hist_prices_df.dtypes -> \n{}'.format(hist_prices_df.dtypes))


############################
### SCRAPE STOCK HISTORY ###
############################

#Working backwards through time from now until the last price scrape date collect all the daily data for a stock. Daily is used as it can later be summarised into weekly or monthly if required.
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

#Get the lengths of each column in the DF
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

#Setup stores
hf_store_name_d = CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_d_tmp']#open the price file
hf_d = pd.HDFStore(hf_store_name_d)
group_name_d = r'data'
hf_store_name_w = CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w_tmp']
hf_w = pd.HDFStore(hf_store_name_w)
group_name_w = r'data'

#close any open h5 files
tables.file._open_files.close_all()

#Scrape daily price data
out_cols = ['ticker','date','open','close','high','low','change','volume']
errors = []
run_time = process_time()
for tick in tick_ftse['ticker']:
    try:
        print('\n{} RUNNING FOR -> {}'.format(len(run_time.lap_li),tick))
        
        #Get the existing prices
        tick_df = hist_prices_df[hist_prices_df.ticker == tick][out_cols]
        print('DAILY SHAPE BEFORE -> {}'.format(tick_df.shape))
        
        #DAILY PRICES
        #Establish the date to start scrapping on
        if str(CONFIG['web_scrape']['mode']).lower() == 'update':
            st_date = tick_df.date.max()
            if pd.isnull(st_date):
                #Treat as a new share
                st_date = dt.datetime(1970,1,1)
        elif str(CONFIG['web_scrape']['mode']).lower() == 'full':
            st_date = dt.datetime(1970,1,1)
        else:
            raise Exception('ValueError','CONFIG[web_scrape][mode] must be either \'update\' or \'full\'')

        #Establish the end date for scrapping
        en_date = dt.date.today()+dt.timedelta(days=1)
        en_date = dt.datetime(en_date.year,en_date.month,en_date.day,0,0,0)
        #Match to friday if sat or sunday
        if en_date.weekday() in (5,6):
            en_date = en_date-dt.timedelta(days=(en_date.weekday()-4))
        
        #Get new price data if neccesary
        if st_date < en_date:
            new_tick_df = get_price_hist_d(tick,create_sec_ref_li(st_date,en_date))
            #Join onto existing data if exists, if not create a new df for it
            if tick_df.shape[0] > 0:
                tick_df = tick_df.append(new_tick_df)
            else:
                tick_df = new_tick_df
            #Drop duplicates
            tick_df = tick_df.drop_duplicates()
        else:
            print('No new records to collect')

        #Calc emas and macd
        tick_df = calc_ema_macd(tick_df)
        print('DAILY FINAL SHAPE -> {}'.format(tick_df.shape))
        
        #Clarify col_lens with cur cols in data
        col_lens_tmp = {}
        for col in tick_df:
            if col in col_lens:
                col_lens_tmp[col] = col_lens[col]
                
        #Add to daily h5 file
        tick_df.to_hdf(hf_store_name_d,key=group_name_d,append=True,min_itemsize=col_lens_tmp)
        
        #WEEKLY PRICES
        #Convert to weekly prices
        df_w = get_price_hist_w(tick_df)
        
        #Drop duplicates
        df_w = df_w.drop_duplicates()

        #Calc emas and macd
        df_w = calc_ema_macd(df_w)
        print('WEEKLY FINAL SHAPE -> {}'.format(df_w.shape))

        #Add to weekly h5 file
        df_w.to_hdf(hf_store_name_w,key=group_name_w,append=True,min_itemsize=col_lens)
        
        #Lap
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

#Delete the old h5 file and rename the TMP
replace_file(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_d'],CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_d_tmp'])
replace_file(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w'],CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w_tmp'])


##################################
### EXPORTING THE FTSE TICKERS ###
##################################
#Export the ftse list
tick_ftse.to_csv(path_or_buf=CONFIG['files']['store_path'] + CONFIG['files']['tick_ftse'])


#######################
### END THE PROGRAM ###
#######################
sys.exit()