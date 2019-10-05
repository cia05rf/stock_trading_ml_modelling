###############################################
#   CODE FOR CREATING TRADE RECORDS
#   ON THE SERVER
#
###############################################

#STEPS:
#   1.Extract all the 'buy' and 'sell' signals from fresha18_trading.historic_bsh_signals and put into a dataframe
#   2.Identify the first signal in each series
#   3.Use this set of buy and sell signals to create a list of trades and dates.
#     Trades include the fields:
#        - ticker
#        - buy_date
#        - buy_proba
#        - buy_price
#        - sell_date
#        - sell_proba
#        - sell_price

#SETUP LOGGING FILE
import logging
log_file = r'C:\xampp\htdocs\freshandeasyfood\trading\python_parsers\update_db_historic_trades_LOG.log'    
logging.basicConfig(filename=log_file, filemode="w", level=logging.DEBUG)   
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logging.getLogger("").addHandler(console)

logging.info('OPENING FILE...')

#STEP 1 - READ IN THE DATAFRAME OF SIGNALS
import pandas as pd
import h5py
import datetime as dt
import mysql.connector as mysql
import re
logging.info('Libraries loaded')

#SQL FOR FUTURE USE
# WITH non_hold_signals AS (
#     SELECT DISTINCT
#         ticker_id
#         ,bsh_date
#         ,bsh_signal
#         ,bsh_signal_proba
#     FROM historic_bsh_signals
#     WHERE bsh_signal <> 'hold'
# )
# ,signal_changes AS (
#     SELECT
#         a.ticker_id
#         ,a.bsh_date
#         ,a.bsh_signal
#     	,a.bsh_signal_proba
#         ,a.prev_date
#         ,b.bsh_signal AS prev_signal
#     	,b.bsh_signal_proba AS prev_signal_proba
#     FROM (
#         SELECT
#             a.ticker_id
#             ,a.bsh_date
#             ,a.bsh_signal
#         	,a.bsh_signal_proba
#             ,MAX(b.bsh_date) AS prev_date
#         FROM non_hold_signals AS a
#         LEFT JOIN non_hold_signals AS b
#             ON b.ticker_id = a.ticker_id
#             AND b.bsh_date < a.bsh_date
#         GROUP BY 
#             a.ticker_id
#             ,a.bsh_date
#             ,a.bsh_signal
#         	,a.bsh_signal_proba
#     ) AS a
#     INNER JOIN non_hold_signals AS b
#         ON b.ticker_id = a.ticker_id
#         AND b.bsh_date = a.prev_date
#         AND b.bsh_signal <> a.bsh_signal
# )
# ,buy_sell_dates AS (
# 	SELECT
#         a.ticker_id
#     	,a.prev_date AS buy_date
#     	,a.prev_signal_proba AS buy_proba
#         ,a.bsh_date AS sell_date
#     	,a.bsh_signal_proba AS sell_proba
#     FROM signal_changes AS a
#     WHERE a.prev_signal = 'buy'
#     	AND a.bsh_signal = 'sell'
# )
# ,open_dates_buy AS (
#     SELECT
#         a.ticker_id
#     	,a.buy_date
#     	,MIN(b.price_date) AS buy_open_date
#     FROM buy_sell_dates AS a
#     LEFT JOIN historic_prices_w AS b
#     	ON b.ticker_id = a.ticker_id
#     	AND b.price_date > a.buy_date
#    	GROUP BY 
#         a.ticker_id
#     	,a.buy_date
# )
# ,open_dates_sell AS (
#     SELECT
#         a.ticker_id
#     	,a.sell_date
#     	,MIN(b.price_date) AS sell_open_date
#     FROM buy_sell_dates AS a
#     LEFT JOIN historic_prices_w AS b
#     	ON b.ticker_id = a.ticker_id
#     	AND b.price_date > a.sell_date
#    	GROUP BY 
#         a.ticker_id
#     	,a.sell_date
# )
# ,final AS (
# 	SELECT
#         a.ticker_id
#     	,a.buy_date
#     	,a.buy_proba
#     	,c.price_open AS buy_price
#         ,a.sell_date
#     	,a.sell_proba
#     	,e.price_open AS sell_price
#     FROM buy_sell_dates AS a
#     LEFT JOIN open_dates_buy AS b
#     	ON b.ticker_id = a.ticker_id
#     	AND b.buy_date = a.buy_date
#     LEFT JOIN historic_prices_w AS c
#     	ON c.ticker_id = a.ticker_id
#     	AND c.price_date = b.buy_open_date
#     LEFT JOIN open_dates_sell AS d
#     	ON d.ticker_id = a.ticker_id
#     	AND d.sell_date = a.sell_date
#     LEFT JOIN historic_prices_w AS e
#     	ON e.ticker_id = a.ticker_id
#     	AND e.price_date = d.sell_open_date
# )
# SELECT 
# 	* 
# FROM final
# ORDER BY 
# 	buy_date
#     ,buy_proba DESC

#Setup the variables
path = r'C:\Users\Robert\Documents\python_scripts\stock_trading_ml_modelling\historical_prices'

#Read in the file
logging.info('Reading in weekly prices file...')
f = h5py.File(path + r'\all_hist_prices_w.h5',mode='r')
grp = f['weekly_data'] #Group name given to weekly data in this file
logging.info('Successfully read in data')
#Get the column headers
col_li = list(grp['_i_table'])
#Get the data
data_df = pd.DataFrame(grp['table'][:],columns=col_li) #[:] for get all rows, fails without this
#Convert date
def conv_msec_to_dt(x):
    base_datetime = dt.datetime( 1970,1,1)
    delta = dt.timedelta(0,0,0, x)
    return base_datetime + delta
data_df.date = [conv_msec_to_dt(x/10**6) for x in data_df.date] #Converting microseconds to date (div by 10**6)
#Convert the ticker
data_df.ticker = [str(x,'utf-8') for x in data_df.ticker]
logging.info('\nEXAMPLE OF data_df -> \n{}'.format(data_df.head()))
#Get a list of tickers
tick_li = data_df['ticker'].unique().tolist()
logging.info('\nUNIQUE TICKERS -> \n{}'.format(tick_li))

#STEP 2 - CONNECT TO THE DATABASE
#Setup the connection
from rf_db_conx import db_conx
db = db_conx()

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

#STEPS 2 & 3 - LOOP THROUGH THE TICKERS AND CREATE AN SQL TEMPLATE WHIC ADDS TO THE DATABASE
errors_li = []
for tick in tick_li:
    try:
        #Get the ticker_id from the server, if there isn't one create it
        sql = '''
            SELECT ticker_id FROM tickers WHERE ticker = '{}' ORDER BY last_seen_date DESC LIMIT 1;
            '''.format(tick.upper())

        #Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('FETCH_TICKER_ID_ERROR','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
        if cur.fetchwarnings() != None: #If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_TICKER_ID_ERROR','server warning {}'.format(cur.fetchwarnings()))
        #Establish the ticker_id
        if cur.rowcount > 0:
            ticker_id = cur.fetchone()[0]
        else:
            ticker_id = None
        logging.info('TICKER ID FOR {} ESTABLISHED AS -> {}'.format(tick.upper(),ticker_id))

        #Get the last date for which there were entries
        sql = '''
            SELECT price_date FROM historic_prices_w WHERE ticker_id = {} ORDER BY price_date DESC LIMIT 1;
        '''.format(ticker_id)
        #Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('FETCH_LAST_DATE','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
        if cur.fetchwarnings() != None: #If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_LAST_DATE','server warning {}'.format(cur.fetchwarnings()))
        #Establish the last date
        if cur.rowcount > 0:
            last_date = cur.fetchone()[0]
        else:
            last_date = dt.datetime(1970,1,1)
        logging.info('\tLAST_DATE_ESTABLISHED AS -> {}'.format(last_date))

        #Create SQL for inserting data
        rows_sql = ''
        filtered_count = data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date)].shape[0]
        if filtered_count < 1:
            logging.info('\tNO NEW PRICES TO ADD, SKIP TO THE NEXT TICKER')
            continue
        for index,row in data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date),:].iterrows():
            row['ticker_id'] = ticker_id
            data = row.to_dict()
            if rows_sql != '':
                rows_sql += ','
            rows_sql += '''(
                {ticker_id}
                ,'{date}'
                ,{open}
                ,{close}
                ,{high}
                ,{low}
                ,{change}
                ,{volume}
                ,{ema12}
                ,{ema26}
                ,{macd_line}
                ,{signal}
                ,{macd}
            )'''.format(**data)
        #Replace nan with NULL for SQL
        rows_sql = re.sub('nan','NULL',rows_sql)

        #Insert into the database
        sql = '''
        INSERT INTO historic_prices_w(
            ticker_id
            ,price_date
            ,price_open
            ,price_close
            ,price_high
            ,price_low
            ,price_change
            ,volume
            ,ema12
            ,ema26
            ,macd_line
            ,macd_signal
            ,macd_hist
        ) VALUES
        {0}
        '''.format(rows_sql)
        if rows_sql == '':
            logging.info('\trows_sql IS EMPTY, SKIP TO NEXT TICKER')
            continue
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('INSERT_HIST_PRICES_W_ERROR','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
        #Check for results
        if cur.fetchwarnings() != None: #If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('INSERT_HIST_PRICES_W_ERROR','server warning {}'.format(cur.fetchwarnings()))
        if cur.lastrowid < 1:
            raise Exception('INSERT_HIST_PRICES_W_ERROR','no rows inserted')
    except Exception as e:
        logging.error('ERROR -> {}'.format(e))
        err_obj = {
            'error':e
            ,'ticker':tick
        }
        errors_li.append(err_obj)

#Show errors
def show_err(_errors_li):
    logging.error('ERRORS COUNT -> {:,}'.format(len(_errors_li)))
    if len(_errors_li) > 0:
        for e in _errors_li:
            logging.error('\t{}'.format(e))
show_err(errors_li)




#STEP 4 - REPEAT FOR DAILY PRICES
#Read in the file
logging.info('Reading in daily prices file...')
f = h5py.File(path + r'\all_hist_prices_d.h5',mode='r')
grp = f['daily_data'] #Group name given to weekly data in this file
logging.info('Successfully read in data')
#Get the column headers
col_li = list(grp['_i_table'])
#Get the data
data_df = pd.DataFrame(grp['table'][:],columns=col_li)
#Convert date
data_df.date = [conv_msec_to_dt(x/10**6) for x in data_df.date] #Converting microseconds to date (div by 10**6)
#Convert the ticker
data_df.ticker = [str(x,'utf-8') for x in data_df.ticker]
logging.info('\nEXAMPLE OF data_df -> \n{}'.format(data_df.head()))
#Get a list of tickers
tick_li = data_df['ticker'].unique().tolist()
logging.info('\nUNIQUE TICKERS -> \n{}'.format(tick_li))
for tick in tick_li:
    try:
        #Get the ticker_id from the server, if there isn't one create it
        sql = '''
            SELECT ticker_id FROM tickers WHERE ticker = '{}' ORDER BY last_seen_date DESC LIMIT 1;
            '''.format(tick.upper())

        #Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('FETCH_TICKER_ID_ERROR','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
        if cur.fetchwarnings() != None: #If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_TICKER_ID_ERROR','server warning {}'.format(cur.fetchwarnings()))
        #Establish the ticker_id
        if cur.rowcount > 0:
            ticker_id = cur.fetchone()[0]
        else:
            ticker_id = None
        logging.info('TICKER ID FOR {} ESTABLISHED AS -> {}'.format(tick.upper(),ticker_id))

        #Get the last date for which there were entries
        sql = '''
            SELECT price_date FROM historic_prices_d WHERE ticker_id = {} ORDER BY price_date DESC LIMIT 1;
        '''.format(ticker_id)
        #Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('FETCH_LAST_DATE','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
        if cur.fetchwarnings() != None: #If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_LAST_DATE','server warning {}'.format(cur.fetchwarnings()))
        #Establish the last date
        if cur.rowcount > 0:
            last_date = cur.fetchone()[0]
        else:
            last_date = dt.datetime(1970,1,1)
        logging.info('\tLAST_DATE_ESTABLISHED AS -> {}'.format(last_date))

        #Create SQL for inserting data
        rows_sql = ''
        filtered_count = data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date)].shape[0]
        if filtered_count < 1:
            logging.info('\tNO NEW PRICES TO ADD, SKIP TO THE NEXT TICKER')
            continue
        for index,row in data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date),:].iterrows():
            row['ticker_id'] = ticker_id
            data = row.to_dict()
            if rows_sql != '':
                rows_sql += ','
            rows_sql += '''(
                {ticker_id}
                ,'{date}'
                ,{open}
                ,{close}
                ,{high}
                ,{low}
                ,{change}
                ,{volume}
                ,{ema12}
                ,{ema26}
                ,{macd_line}
                ,{signal}
                ,{macd}
            )'''.format(**data)
        #Replace nan with NULL for SQL
        rows_sql = re.sub('nan','NULL',rows_sql)
        if rows_sql == '':
            logging.info('\trows_sql IS EMPTY, SKIP TO NEXT TICKER')
            continue

        #Insert into the database
        sql = '''
        INSERT INTO historic_prices_d(
            ticker_id
            ,price_date
            ,price_open
            ,price_close
            ,price_high
            ,price_low
            ,price_change
            ,volume
            ,ema12
            ,ema26
            ,macd_line
            ,macd_signal
            ,macd_hist
        ) VALUES
        {0}
        '''.format(rows_sql)

        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('INSERT_HIST_PRICES_D_ERROR','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
        #Check for results
        if cur.fetchwarnings() != None: #If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('INSERT_HIST_PRICES_D_ERROR','server warning {}'.format(cur.fetchwarnings()))
        if cur.lastrowid < 1:
            raise Exception('INSERT_HIST_PRICES_D_ERROR','no rows inserted')
    except Exception as e:
        logging.error('ERROR -> {}'.format(e))
        err_obj = {
            'error':e
            ,'ticker':tick
        }
        errors_li.append(err_obj)

#STEP 5 - GET THE LATEST PRICES AND UPDATE CURRENT TABLES WITH THIS
logging.info('\nUPDATE current_prices_w WITH LATEST PRICES')
try:
    #Delete the current table contents
    sql = 'DELETE FROM current_prices_w'
    #Query the database
    try:
        cur.execute(sql)
        logging.info('\nSUCCESSFULLY DELETED {} RECORDS'.format(cur.rowcount))
    except Exception as e:
        raise Exception('DELETE_ALL_ERROR','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
    if cur.fetchwarnings() != None: #If true there was a warning
        logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
        raise Exception('DELETE_ALL_ERROR','server warning {}'.format(cur.fetchwarnings()))
    #Get the sever to create a new set of results form the latest results in the table
    sql = '''
        INSERT INTO current_prices_w (
            price_id
            ,ticker_id
            ,price_date_int
            ,price_date
            ,price_open
            ,price_close
            ,price_high
            ,price_low
            ,price_change
            ,volume
            ,ema12
            ,ema26
            ,macd_line
            ,macd_signal
            ,macd_hist
        ) 
        SELECT
            hpw.price_id
            ,hpw.ticker_id
            ,hpw.price_date_int
            ,hpw.price_date
            ,hpw.price_open
            ,hpw.price_close
            ,hpw.price_high
            ,hpw.price_low
            ,hpw.price_change
            ,hpw.volume
            ,hpw.ema12
            ,hpw.ema26
            ,hpw.macd_line
            ,hpw.macd_signal
            ,hpw.macd_hist
        FROM historic_prices_w AS hpw
        INNER JOIN tickers AS t
            ON t.ticker_id = hpw.ticker_id
        INNER JOIN (
            SELECT
                ticker_id
                ,MAX(price_date_int) AS max_price_date_int
            FROM historic_prices_w
            GROUP BY ticker_id
        ) AS t1
            ON t1.ticker_id = hpw.ticker_id
            AND t1.max_price_date_int = hpw.price_date_int
    '''
    try:
        cur.execute(sql)
        logging.info('\nSUCCESSFULLY INSERTED {} RECORDS'.format(cur.rowcount))
    except Exception as e:
        raise Exception('INSERT_INTO_CUR_PRICES','sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql,e))
except Exception as e:
    logging.error('ERROR -> {}'.format(e))
    err_obj = {
        'error':e
        ,'ticker':tick
    }
    errors_li.append(err_obj)

#Close the connection
db.close()

#Show errors
show_err(errors_li)