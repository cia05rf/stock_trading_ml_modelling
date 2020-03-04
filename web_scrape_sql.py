"""Script for scrapping the stock price histories of stock accross the 
FTSE 350

# Daily web scrape of stock prices
This code accesses site to retrieve stock price information daily ad add it to the h5 file. The steps are:
1. Get FTSE 100 and FTSE 250 lists from wikipedia, then compile into one list.
2. open pipe to sql file holding current price data.
3. Loop though the tickers and for each one; 
    1. find the most recent price date.
    2. convert this into a time stamp to be used on Yahoo finance.
    3. go to Yahoo Finance and get all the prices between the last time stamp and the current timestamp.
4. Add these new prices to the sql file.

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
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import requests as rq
import re
import datetime as dt
import sqlite3
import os

from functions.sql_funcs import start_engine
from rf_modules import *
from config import CONFIG
from functions.web_scrape_funcs import *
from functions.ft_eng_funcs import calc_ema,calc_macd,calc_ema_macd,get_col_len_df


db_file = CONFIG['files']['store_path'] + CONFIG['files']['prices_db']

engine, session = start_engine(db_file)
conn = sqlite3.connect(db_file)
cur = conn.cursor()


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
tick_ftse100['market'] = 'FTSE100'
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
tick_ftse250['market'] = 'FTSE250'
print('tick_ftse250.head() -> \n{}'.format(tick_ftse250.head()))

#Combine into 1 dataframe
tick_ftse = pd.concat([tick_ftse100,tick_ftse250])
print('shape -> {}'.format(tick_ftse.shape))
print('value_counts -> \n{}'.format(tick_ftse.ticker.value_counts()))
tick_ftse.sort_values(['ticker'])
tick_ftse['ticker'] = [re.sub('(?=[0-9A-Z])*\.(?=[0-9A-Z]+)','-',tick) for tick in tick_ftse['ticker']]
tick_ftse['ticker'] = [re.sub('[^0-9A-Z\-]','',tick) for tick in tick_ftse['ticker']]
tick_ftse['last_seen_date'] = dt.date.today()

#If the ticker market combo doesn't exist add it to ticker_market
sql = """
    SELECT 
        t.*,
        tm.market,
        tm.'index' AS ticker_market_id
    FROM ticker AS t
    LEFT JOIN ticker_market AS tm
        ON tm.ticker_id = t.'index'
"""
db_tick_df = pd.read_sql(sql, con=conn)
new_tick_market_df = pd.merge(tick_ftse, db_tick_df[['ticker','index','market','ticker_market_id']].rename(columns={'index':'ticker_id'}), on=['ticker','market'], how='left')
new_tick_market_df = new_tick_market_df[new_tick_market_df.ticker_market_id.isnull()]
new_tick_market_df['first_seen_date'] = dt.date.today()
db_cols = ['market','first_seen_date','ticker_id']
new_tick_market_df[db_cols].to_sql('ticker_market', con=conn, if_exists='append')

#If the ticker doesn't exist add it to both ticker and ticker_market
new_tick_df = pd.merge(tick_ftse, db_tick_df[['ticker','index']].rename(columns={'index':'ticker_id'}), on=['ticker'], how='left')
new_tick_df = new_tick_df[new_tick_df.ticker_id.isnull()]
db_cols = ['ticker','company','last_seen_date']
new_tick_df[db_cols].to_sql('ticker', con=conn, if_exists='append')
sql = """
    SELECT 
        t.*
    FROM ticker AS t
"""
db_tick_df = pd.read_sql(sql, con=conn)
new_tick_df['first_seen_date'] = dt.date.today()
new_tick_df = pd.merge(new_tick_df.drop(columns=['ticker_id']), db_tick_df, on=['ticker']).rename(columns={'index':'ticker_id'})
db_cols = ['market','first_seen_date','ticker_id']
new_tick_df[db_cols].to_sql('ticker_market', con=conn, if_exists='append')


####################
### DAILY PRICES ###
####################

#Make a call for all the latest dates
sql = """
    SELECT dp1.* FROM daily_price AS dp1
    INNER JOIN (
        SELECT 
            ticker_id,
            MAX(week_start_date) AS max_week_start_date
        FROM daily_price
        GROUP BY ticker_id
    ) AS dp2
        ON dp2.ticker_id = dp1.ticker_id
        AND dp2.max_week_start_date <= dp1.date
"""
latest_dates_df = pd.read_sql(sql, con=conn)
#Convert date
def conv_date(_str_in):
    if type(_str_in) == str:
        return dt.datetime.strptime(_str_in,'%Y-%m-%d')
    else:
        return _str_in
latest_dates_df.date = [conv_date(x[:10]) for x in latest_dates_df.date]
latest_dates_df.week_start_date = [conv_date(x[:10]) for x in latest_dates_df.week_start_date]
print('latest_dates_df.dtypes -> \n{}'.format(latest_dates_df.dtypes))

#Establish the end date
#Establish the end date for scrapping
en_date = dt.date.today()+dt.timedelta(days=1)
en_date = dt.datetime(en_date.year,en_date.month,en_date.day,0,0,0)
#Match to sat if sunday
if en_date.weekday() == 6:
    en_date = en_date-dt.timedelta(days=(en_date.weekday()-5))
en_date = np.datetime64(en_date)


#Loop through the tickers in tick_ftse and for each one get the latest date of scrape.
#Convert this date into a timestamp.
#Scrape all new data and add to the database.
sql = """
    SELECT 
        t.*
    FROM ticker AS t
"""
db_tick_df = pd.read_sql(sql, con=conn)
db_cols = ["date","open","high","low","close","change","volume","week_start_date","ticker_id"]
dp_errors = []
run_time = process_time()
for _,row in db_tick_df.iterrows():
    try:
        print(f'\n{len(run_time.lap_li)} RUNNING FOR -> {row.ticker}')
        #Get the latest date
        st_date = latest_dates_df[latest_dates_df.ticker_id == row['index']].date.values[0]
        #Add 1 day to st_date
        st_date = st_date + np.timedelta64(1,'D')
        #Establish the date to start scrapping on
        if str(CONFIG['web_scrape']['mode']).lower() == 'update':
            if np.isnan(st_date):
                #Treat as a new share
                st_date = dt.datetime(1970,1,1)
        elif str(CONFIG['web_scrape']['mode']).lower() == 'full':
            st_date = dt.datetime(1970,1,1)
            #Remove all the existing data
            sql = f"""
                DELETE FROM daily_price WHERE 'index' == {row['index']}
            """
            cur.execute(sql)
        else:
            raise Exception('ValueError','CONFIG[web_scrape][mode] must be either \'update\' or \'full\'')

        #Get new price data if neccesary
        if st_date < en_date:
            new_prices_df = get_price_hist_d(row.ticker,create_sec_ref_li(st_date,en_date))
            new_prices_df['ticker_id'] = row['index']
            #Add new prices to the sql database
            new_prices_df[db_cols].to_sql('daily_price', con=conn, if_exists='append')
            print(f"ADDED {new_prices_df.shape[0]} NEW RECORDS TO daily_price: \n\tFROM {new_prices_df.date.min()} \n\tTO {new_prices_df.date.max()}")
        else:
            print('No new records to collect')
            continue
    except Exception as e:
        print('ERROR -> {e}')
        dp_errors.append({'ticker':row.ticker,"error":e})
    #Lap
    run_time.lap()
    run_time.show_latest_lap_time(show_time=True)

print('\n\n')
run_time.end()
print(f'\nERROR COUNT -> {len(dp_errors)}')
if len(dp_errors) > 0:
    print('ERRORS ->')
    for e in dp_errors:
        print(e)


#####################
### WEEKLY PRICES ###
#####################

#Make a call for all the latest dates
sql = """
    SELECT dp1.* FROM weekly_price AS dp1
    INNER JOIN (
        SELECT 
            ticker_id,
            MAX(date) AS max_date
        FROM weekly_price
        GROUP BY ticker_id
    ) AS dp2
        ON dp2.ticker_id = dp1.ticker_id
        AND dp2.max_date <= dp1.date
"""
latest_dates_df = pd.read_sql(sql, con=conn)
latest_dates_df.date = [conv_date(x[:10]) for x in latest_dates_df.date]
print('latest_dates_df.dtypes -> \n{}'.format(latest_dates_df.dtypes))

#Loop through the tickers in tick_ftse and for each one get the latest date of scrape.
#Convert this date into a timestamp.
#Scrape all new data and add to the database.
db_cols = ["date","open","high","low","close","change","volume","ticker_id"]
wp_errors = []
run_time = process_time()
for _,row in db_tick_df.iterrows():
    try:
        print(f'\n{len(run_time.lap_li)} RUNNING FOR -> {row.ticker}')
        #Get the latest date
        latest_record = latest_dates_df[latest_dates_df.ticker_id == row['index']]
        st_date = latest_record.date.values[0]

        #Get new price data if neccesary
        if st_date < en_date:
            sql = f"""
                SELECT * 
                FROM daily_price
                WHERE 
                    ticker_id = {row['index']}
                    AND week_start_date >= '{st_date}'
            """
            dp_df = pd.read_sql(sql, con=conn)
            dp_df.date = [conv_date(x[:10]) for x in dp_df.date]
            dp_df.week_start_date = [conv_date(x[:10]) for x in dp_df.week_start_date]
            dp_df['ticker'] = row.ticker
            resp = get_price_hist_w(dp_df)
            if resp[0]:
                new_prices_df = resp[1]
            else:
                #Raise error
                raise resp[1]
            new_prices_df['ticker_id'] = row['index']

            #Grab the existing data from the table
            sql = f"""
                SELECT *
                FROM weekly_price
                WHERE 
                    ticker_id = {row['index']}
                    AND date >= '{st_date}'
            """
            wp_df = pd.read_sql(sql, con=conn)
            wp_df.date = [conv_date(x[:10]) for x in wp_df.date]
            #Compare the existing records to new records
            comp_df = pd.merge(new_prices_df, wp_df, on='date', how='left', suffixes=("","_old"))
            append_mask = pd.Series([False] * comp_df.shape[0])
            update_mask = pd.Series([False] * comp_df.shape[0])
            for col in ['open','high','low','close','volume']:
                #Look for new records
                missing = (comp_df[f"{col}_old"].isnull())
                append_mask = append_mask | missing
                #Look for changed records
                change = (comp_df[f"{col}_old"] == comp_df[col]) & (~comp_df[f"{col}_old"].isnull())
                update_mask = update_mask | change

            #Add new prices to the sql database
            append_df = comp_df[append_mask]
            append_df[db_cols].to_sql('daily_price', con=conn, if_exists='append')
            print(f"ADDED {append_df.shape[0]} NEW RECORDS TO daily_price: \n\tFROM {append_df.date.min()} \n\tTO {append_df.date.max()}")

            #Update changed prices on the sql database
            update_df = comp_df[update_mask]
            sql = f"""
                DELETE FROM weekly_price
                WHERE ticker_id = {row['index']}
                AND date IN ({','.join(f"'{str(x)[:10]}'" for x in update_df.date.to_list())})
            """
            cur.execute(sql)
            update_df[db_cols].to_sql('daily_price', con=conn, if_exists='append')
            print(f"DELETED AND ADDED {update_df.shape[0]} NEW RECORDS TO daily_price: \n\tFROM {update_df.date.min()} \n\tTO {update_df.date.max()}")
        else:
            print('No new records to collect')
            continue
    except Exception as e:
        print('ERROR -> {e}')
        wp_errors.append({'ticker':row.ticker,"error":e})
    #Lap
    run_time.lap()
    run_time.show_latest_lap_time(show_time=True)

print('\n\n')
run_time.end()
print(f'\nERROR COUNT -> {len(wp_errors)}')
if len(wp_errors) > 0:
    print('ERRORS ->')
    for e in wp_errors:
        print(e)


#######################################
### UPDATE ALL TICKERS LAST SEEN ON ###
#######################################

sql = f"""
    UPDATE ticker
    SET last_seen_date = (
        SELECT
            MAX(date)
        FROM daily_price
        WHERE daily_price.ticker_id = ticker.'index'
    )
    WHERE
        EXISTS (
            SELECT *
            FROM daily_price
            WHERE daily_price.ticker_id = ticker.'index'
        )
"""
cur.execute(sql)