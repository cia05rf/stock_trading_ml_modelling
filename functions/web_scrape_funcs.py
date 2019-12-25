#Import libraries
import pandas as pd
import numpy as np
import datetime as dt
from bs4 import BeautifulSoup as bs
import requests as rq

from rf_modules import *

#Create a list of time intervals to be used with 140 days in each item
def create_sec_ref_li(_st_date:int,_en_date:int,_days:int=140):
    """Function to create a listof time intervals with a set number of days
    within each interval.

    Used for webscrape calls (too big a time frame causes the scrape to crash).

    args:
    ------
    _st_date - int - the first date in the time period you with to scrape (inclusive)
    _en_date - int - the first date in the time period you with to scrape (inclusive)
    _days - int:140 - the time window size you wish returned in days

    returns:
    ------
    list of tuples (datetime,datetime)

    """
    #make sure the dates are different
    if _st_date == _en_date:
        return []
    #Establish the day ref of the dates compared to 01/01/1970
    _ep_date = pd.to_datetime(dt.datetime(1970,1,1),errors='coerce')
    _en_date = pd.to_datetime(_en_date,errors='coerce')
    _st_date = pd.to_datetime(_st_date,errors='coerce')
    print('_st_date: ' + str(_st_date))
    print('_en_date: ' + str(_en_date))
    _st_days = (_st_date - _ep_date).days
    _en_days = (_en_date - _ep_date).days
    #Loop adding to a list until reaching 0
    _sec_ref_li = []
    while _en_days > _st_days:
        if _en_days - _days > _st_days:
            _sec_ref_li.append([(_en_days - _days)*86400,_en_days*86400])
        else:
            _sec_ref_li.append([_st_days*86400,_en_days*86400])        
        _en_days += -_days
    return _sec_ref_li

#Get the price history for a specific ticker
def get_price_hist_d(_tick:str,_sec_ref_li:list):
    """Function fr gtting daily stock prices from webscrapping
    
    args:
    ------
    _tick - str - the identifier for the stock being looked at needs to math Yahoo.co.uk
    _sec_ref_li - the list of time periods to scrape

    returns:
    ------
    pandas dataframe - contains all required prices

    """
    
    try:
        print('Getting DAILY prices for -> {}'.format(_tick))
        _tick_df = pd.DataFrame([])
        _cols = []
        print('Number of webscrapes to perform -> {}'.format(len(_sec_ref_li)))
        i = 0
        for _secs in _sec_ref_li:
            i += 1
            print('Making call {} -> {} - {}'.format(i,dt.datetime.fromtimestamp(_secs[0]),dt.datetime.fromtimestamp(_secs[1])))
            try:
                _web_add = 'https://finance.yahoo.com/quote/{0}/history?period1={1}&period2={2}&interval={3}&filter=history&frequency={3}'.format(re.sub('\.','-',_tick)+'.L',_secs[0],_secs[1],'1d')
#                 print('_web_add: {}'.format(_web_add))
                
                _resp = rq.get(_web_add)
                if int(_resp.status_code) != 200:
                    print('status code -> {}'.format(_resp.status_code))
                    continue
                _parser = bs(_resp.content,'html.parser')
                #Get the table
                _table = _parser.find_all('table',attrs={'data-test':'historical-prices'})[0]
                #Grab the data rows
                _rows = _table.find_all('tbody')[0].find_all('tr')
                #If there are no dates there's no point going back further
                if len(_rows) == 0:
                    print('No more records to collect')
                    break
                #Put the rows into the dataframe
                for _r in _rows:
                    if len(_tick_df) == 0:
                        _cols = [clean_col_name(x.text) for x in _table.find_all('th')]
                        _tick_df = pd.DataFrame([],columns=_cols)
                    if len(_r.find_all('td')) == len(_cols):
                        _tick_df = _tick_df.append(pd.Series([x.text for x in _r.find_all('td')],index=_cols),ignore_index=True)
                    else:
                        continue
            except Exception as e:
                print('ERROR - CONTINUE -> {}'.format(e))
                continue
        #Check for rows - if none then return
        if len(_tick_df) == 0:
            return _tick_df
        #Reformat
        def float_format(_str_in):
            if type(_str_in) == str:
                _str_in = _str_in.strip()
                _str_in = re.sub('[^0-9.]','',_str_in)
                if _str_in == '':
                    _str_in = 0
                return _str_in
            else:
                return _str_in
        _tick_df.loc[:,'open'] = _tick_df.loc[:,'open'].apply(float_format).astype(float)
        _tick_df.loc[:,'high'] = _tick_df.loc[:,'high'].apply(float_format).astype(float)
        _tick_df.loc[:,'low'] = _tick_df.loc[:,'low'].apply(float_format).astype(float)
        _tick_df.loc[:,'close'] = _tick_df.loc[:,'close'].apply(float_format).astype(float)
        _tick_df.loc[:,'adj_close'] = _tick_df.loc[:,'adj_close'].apply(float_format).astype(float)
        _tick_df.loc[:,'volume'] = _tick_df.loc[:,'volume'].apply(float_format).astype(float)
        _tick_df.loc[:,'change'] = _tick_df.loc[:,'close'] - _tick_df.loc[:,'open']
        def conv_date(_str_in):
            if type(_str_in) == str:
                return dt.datetime.strptime(_str_in,'%b %d, %Y')
            else:
                return _str_in
        _tick_df.loc[:,'date'] = _tick_df.loc[:,'date'].apply(conv_date)
        #Add the ticker series
        _tick_df.loc[:,'ticker'] = _tick    
        #Mark the week identifier
        _tick_df['isocalendar'] = [x.isocalendar()[:2] for x in _tick_df['date']]
        _min_wk_day = _tick_df.loc[_tick_df['open'] > 0,['date','isocalendar']].groupby('isocalendar').min().rename(columns={'date':'week_start_date'}).reset_index()
        _tick_df = pd.merge(_tick_df,_min_wk_day,left_on='isocalendar',right_on='isocalendar')
        #CLEANING - Remove any rows with zero volume
        _tick_df = _tick_df[_tick_df['volume'] > 0]
        #CLEANING - Copy row above where the change has been more than 90%
        _tick_df['cl_change'] = (_tick_df['close'] - _tick_df['close'].shift(1))/_tick_df['close'].shift(1)
        _check_s = _tick_df['cl_change'] < -0.9
        _tick_df.loc[_check_s,'open'] = _tick_df['open'].shift(-1).copy().loc[_check_s]
        _tick_df.loc[_check_s,'close'] = _tick_df['close'].shift(-1).copy().loc[_check_s]
        _tick_df.loc[_check_s,'high'] = _tick_df['high'].shift(-1).copy().loc[_check_s]
        _tick_df.loc[_check_s,'low'] = _tick_df['low'].shift(-1).copy().loc[_check_s]
        _tick_df = _tick_df.loc[:,['ticker','date','week_start_date','open','close','high','low','change','volume']]
        return _tick_df
    except Exception as e:
        print('ERROR -> {}'.format(e))
        return False


#Create a weekly table
def get_price_hist_w(_df_d):
    """Function to convert the daily prices into weely prices
    
    args:
    ------
    _df_d - pandas dataframe - the daily prices

    returns:
    ------
    pandas dataframe
    """
    print('Converting daily prices to weekly prices')
    try:
        #Create a copy of the data
        _df_d = _df_d.copy()
        #Mark the week identifier
        _df_d['isocalendar'] = [x.isocalendar()[:2] for x in _df_d['date']]
        #Get highs and lows
        _high_df = _df_d.loc[_df_d['high'] > 0,['high','isocalendar']].groupby('isocalendar').max().reset_index()
        _low_df = _df_d.loc[_df_d['low'] > 0,['low','isocalendar']].groupby('isocalendar').min().reset_index()
        #Get total volume for the week
        _vol_df = _df_d.loc[_df_d['volume'] > 0,['volume','isocalendar']].groupby('isocalendar').sum().reset_index()
        #Get open price
        _min_wk_day = _df_d.loc[_df_d['open'] > 0,['date','isocalendar']].groupby('isocalendar').min().reset_index()
        _open_df = pd.merge(_df_d[['date','open']],_min_wk_day,left_on='date',right_on='date')
        #Get close price
        _max_wk_day = _df_d.loc[_df_d['close'] > 0,['date','isocalendar']].groupby('isocalendar').max().reset_index()
        _close_df = pd.merge(_df_d[['date','close']],_max_wk_day,left_on='date',right_on='date').reset_index()
        #Form the final df
        _wk_df = pd.merge(_df_d[['ticker','isocalendar']],_min_wk_day,left_on='isocalendar',right_on='isocalendar') #date
        _wk_df = pd.merge(_wk_df,_high_df,left_on='isocalendar',right_on='isocalendar') #high
        _wk_df = pd.merge(_wk_df,_low_df,left_on='isocalendar',right_on='isocalendar') #low
        _wk_df = pd.merge(_wk_df,_vol_df,left_on='isocalendar',right_on='isocalendar') #volume
        _wk_df = pd.merge(_wk_df,_open_df[['isocalendar','open']],left_on='isocalendar',right_on='isocalendar') #open
        _wk_df = pd.merge(_wk_df,_close_df[['isocalendar','close']],left_on='isocalendar',right_on='isocalendar') #close
        _wk_df['change'] = _wk_df['close'] - _wk_df['open']
        _wk_df = _wk_df.drop_duplicates().reset_index(drop=True)
        #Get the monday of each week
        _wk_df['weekday'] = [dt.date.weekday(x) for x in _wk_df['date']]
        _wk_df['date'] = _wk_df['date'] - pd.Series([dt.timedelta(days=x) for x in _wk_df['weekday']])
        _wk_df.drop(columns=['isocalendar','weekday'],inplace=True)
        return _wk_df
    except Exception as e:
        print('ERROR -> {}'.format(e))
        return False