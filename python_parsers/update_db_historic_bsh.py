###############################################
#   CODE FOR READING IN ALL RECORDS FROM
#   HISTORIC BSH RECOMMENDATIONS AND
#   UPLOADING TO THE SERVER
###############################################

#STEPS:
#   1.Read in the dataframe being held in an h5. file for weekly prices
#   2.Create an SQL template for adding rows and compile into one query
#   3.Run each ticker individually and upload to the server
#   4.Get the latest figure and update current_bsh_signals with this

#SETUP LOGGING FILE
import logging
log_file = r'C:\xampp\htdocs\freshandeasyfood\trading\python_parsers\update_db_historic_bsh_LOG.log'    
logging.basicConfig(filename=log_file, filemode="w", level=logging.DEBUG)   
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logging.getLogger("").addHandler(console)

logging.info('STARTING update_db_historic_bsh...')

#STEP 1 - READ IN THE DATAFRAME OF PRICES
import pandas as pd
import h5py
import datetime as dt
import mysql.connector as mysql
import re
import tables
logging.info('Libraries loaded')

#Setup the variables
path = r'C:\Users\Robert\Documents\python_scripts\stock_trading_ml_modelling\historical_prices'

#Read in the file
logging.info('Reading in weekly bsh signals file...')
f = h5py.File(path + r'\historic_lgb_bsh_signals.h5',mode='r')
grp = f['bsh_signals'] #Group name given to weekly data in this file
logging.info('Successfully read in data')
#Get the column headers
col_li = list(grp['_i_table'])
#Get the data
data_df = pd.DataFrame(grp['table'][:],columns=col_li) #[:] for get all rows, fails without this
#Close the connection to the .h5 file
f.close()
#Convert date
def conv_msec_to_dt(x):
    base_datetime = dt.datetime( 1970,1,1)
    delta = dt.timedelta(0,0,0, x)
    return base_datetime + delta
data_df.date = [conv_msec_to_dt(x/10**6) for x in data_df.date] #Converting microseconds to date (div by 10**6)
#Convert the ticker
data_df.ticker = [str(x,'utf-8') for x in data_df.ticker]
data_df.signal = [str(x,'utf-8') for x in data_df.signal]
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
        #Get the ticker_id from the server
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
        logging.info('TICKER FOR {} ID ESTABLISHED AS -> {}'.format(tick.upper(),ticker_id))

        #Get the last date for which there were entries
        sql = '''
            SELECT bsh_date FROM historic_bsh_signals WHERE ticker_id = {} ORDER BY bsh_date DESC LIMIT 1;
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
        logging.info('\tLAST DATE ESTABLISHED AS -> {}'.format(last_date))

        #Create SQL for inserting data
        rows_sql = ''
        filtered_count = data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date)].shape[0]
        if filtered_count < 1:
            logging.info('\tNO NEW SIGNALS TO ADD, SKIP TO THE NEXT TICKER')
            continue
        else:
            logging.info('\n{} NEW SIGNALS TO ADD'.format(data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date),:].shape[0]))
        for index,row in data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date),:].iterrows():
            row['ticker_id'] = ticker_id
            data = row.to_dict()
            if rows_sql != '':
                rows_sql += ','
            rows_sql += '''(
                {ticker_id}
                ,'{date}'
                ,'{signal}'
                ,{signal_prob}
            )'''.format(**data)
        #Replace nan with NULL for SQL
        rows_sql = re.sub('nan','NULL',rows_sql)

        #Insert into the database
        sql = '''
        INSERT INTO historic_bsh_signals(
            ticker_id
            ,bsh_date
            ,bsh_signal
            ,bsh_signal_proba
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

#STEP 4 - GET THE LATEST SIGNALS AND UPDATE CURRENT TABLES WITH THIS
logging.info('\nUPDATE current_bsh_signals WITH LATEST RESULTS')
try:
    #Delete the current table contents
    sql = 'DELETE FROM current_bsh_signals'
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
        INSERT INTO current_bsh_signals (
            ticker_id
            ,bsh_signal
            ,bsh_signal_proba
            ,bsh_date
        ) 
        SELECT
            hbs.ticker_id
            ,hbs.bsh_signal
            ,hbs.bsh_signal_proba
            ,hbs.bsh_date
        FROM historic_bsh_signals AS hbs
        INNER JOIN tickers AS t
            ON t.ticker_id = hbs.ticker_id
        INNER JOIN (
            SELECT
                ticker_id
                ,MAX(bsh_date) AS max_bsh_date
            FROM historic_bsh_signals
            GROUP BY ticker_id
        ) AS t1
            ON t1.ticker_id = hbs.ticker_id
            AND t1.max_bsh_date = hbs.bsh_date
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

db.close()

#Show errors
def show_err(_errors_li):
    logging.error('ERRORS COUNT -> {:,}'.format(len(_errors_li)))
    if len(_errors_li) > 0:
        for e in _errors_li:
            logging.error('\t{}'.format(e))
show_err(errors_li)