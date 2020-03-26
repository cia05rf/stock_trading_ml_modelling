"""
Script for createing a new database from existing price data
held in a h5 file
"""
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import requests as rq
import re
import datetime as dt
import sqlite3
import os

from functions.sql_funcs import create_db
from config import CONFIG

db_file = CONFIG['files']['store_path'] + CONFIG['files']['prices_db']

#Delete the old files
try:
    os.remove(db_file)
    print(f'\nSUCCESSFULLY REMOVED {db_file}')
except Exception as e:
    print(f'\nERROR - REMOVING:{e}')


engine, session = create_db(db_file)
conn = sqlite3.connect(db_file)


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

#Put into sql table ticker
tick_ftse['last_seen_date'] = dt.date.today()
db_cols = ['ticker','company','last_seen_date']
tick_ftse[db_cols].reset_index(drop=True).to_sql('ticker', con=conn, index=False, if_exists='append')

#Get tickers from db for index values
#Join in the ticker ids
sql = """
    SELECT * FROM ticker
"""
tick_df = pd.read_sql(sql, con=conn)
#Keep only the latest record
max_dates = tick_df[['ticker','last_seen_date']].groupby(['ticker']).max().reset_index().rename(columns={'last_seen_date':'max_date'})
tick_df = pd.merge(tick_df,max_dates, on='ticker')
tick_df = tick_df[tick_df.last_seen_date == tick_df.max_date]

#Put into sql table ticker_market
tick_ftse['first_seen_date'] = dt.date.today()
tick_market = pd.merge(tick_ftse, tick_df[['id','ticker']].rename(columns={'id':'ticker_id'}), on='ticker')
db_cols = ['market','first_seen_date','ticker_id']
tick_market[db_cols].reset_index(drop=True).to_sql('ticker_market', con=conn, index=False, if_exists='append')


####################
### DAILY PRICES ###
####################

#Read in all the existing data for daily prices
hist_prices_df = pd.read_hdf(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_d'])
#Convert date
def conv_date(_str_in):
    if type(_str_in) == str:
        return dt.datetime.strptime(_str_in,'%Y-%m-%d')
    else:
        return _str_in
hist_prices_df.date = [conv_date(x) for x in hist_prices_df.date]
hist_prices_df = pd.merge(hist_prices_df, tick_df[['ticker','id']].rename(columns={'id':'ticker_id'}), on='ticker')
#Put into db
db_cols = ['date','open','high','low','close','change','volume','week_start_date','ticker_id']
hist_prices_df[db_cols].sort_values(['ticker_id','date']).reset_index(drop=True).to_sql('daily_price', con=conn, index=False, if_exists='append')


#####################
### WEEKLY PRICES ###
#####################

#Read in all the existing data for daily prices
hist_prices_df = pd.read_hdf(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w'])
#Convert date
hist_prices_df.date = [conv_date(x) for x in hist_prices_df.date]
hist_prices_df = pd.merge(hist_prices_df, tick_df[['ticker','id']].rename(columns={'id':'ticker_id'}), on='ticker')
#Put into db
db_cols = ['date','open','high','low','close','change','volume','ticker_id']
hist_prices_df[db_cols].sort_values(['ticker_id','date']).reset_index(drop=True).to_sql('weekly_price', con=conn, index=False, if_exists='append')


#############################
### CLOSE THE CONNECTIONS ###
#############################

session.close_all()
conn.close()