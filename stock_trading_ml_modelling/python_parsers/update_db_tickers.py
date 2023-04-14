###############################################
#   CODE FOR READING IN ALL RECORDS FROM
#   FTSE TICKERS LIST AND UPDATE THE SERVER
#
###############################################

# STEPS:
#   1.Read in the csv of ticker lists
#   2.Loop through the tickers and:
#       2.1.Check if it exists and is in the right index
#       2.2.If it does not exist create it, if it's in the wrong index correct it, if both are right then update the last seen date

# SETUP LOGGING FILE
from rf_db_conx import db_conx
import re
import datetime as dt
import pandas as pd
import logging
from stock_trading_ml_modelling.config import WS_UPDATE_TICKERS_LOG, \
    TICK_FTSE

log_file = WS_UPDATE_TICKERS_LOG
logging.basicConfig(filename=log_file, filemode="w", level=logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logging.getLogger("").addHandler(console)

logging.info('RUN update_db_tickers.py')

# STEP 1 - READ IN THE DATAFRAME OF PRICES

# Setup the variables

# Read in the file
data_df = pd.read_csv(TICK_FTSE)
logging.info('\nSUCCESSFULLY LOADED tickers')
logging.info('EXAMPLE OF data_df -> \n{}'.format(data_df.head()))

# STEP 2 - CONNECT TO THE DATABASE AND CREATE AN SQL TEMPLATE

# Setup the connection
logging.info('\nCONNECTING TO DATABASE')
db = db_conx()

# you must create a Cursor object. It will let
#  you execute all the queries you need
logging.info('\nLOOPING TICKERS AND UPDATING DATABASE')
cur = db.cursor()
errors_li = []
for index, row in data_df.iterrows():
    try:
        # Get the ticker_id from the server, if there isn't one create it
        sql = '''
            SELECT ticker,latest_index FROM tickers WHERE ticker = '{}' ORDER BY last_seen_date DESC LIMIT 1;
            '''.format(row.ticker.upper())

        # Query the database
        if cur.fetchwarnings() != None:  # If true there was a warning
            # TEMP - come back to this to put in some error handling
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
        cur.execute(sql)
        if cur.rowcount > 0:
            resp = dict(zip(cur.column_names, cur.fetchone()))
        else:
            resp = ()

        # Check if the record exists
        if len(resp) == 0:  # Record does not exist
            logging.info(
                '{} NOT FOUND IN RECORDS - CREATING NEW RECORD'.format(row['ticker'].upper()))
            # Create sql
            sql = '''
                INSERT INTO tickers (
                    ticker
                    ,company_name
                    ,first_seen_date
                    ,last_seen_date
                    ,latest_index
                ) VALUES (
                    '{0}'
                    ,'{1}'
                    ,'{2}'
                    ,'{3}'
                    ,'{4}'
                );
            '''.format(
                row.ticker.upper(), re.sub("\'", "\\'", row.company), dt.datetime.now().strftime(
                    '%Y-%m-%d'), dt.datetime.now().strftime('%Y-%m-%d'), row['index'].upper()
            )
            # Query the daabase
            cur.execute(sql)
            # Check if query was a success
            if cur.fetchwarnings() != None:  # If true there was a warning
                # TEMP - come back to this to put in some error handling
                logging.warning(
                    'SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            elif cur.lastrowid != None:  # Shows a row was created
                logging.info('''\tRECORD CREATION SUCCESS -> 
                    \t\tticker_id:{}
                    \t\tticker:{}
                    \t\tcompany_name:{}
                    \t\tfirst_seen_date:{}
                    \t\tlast_seen_date:{}
                    \t\tlatest_index:{}'''.format(
                    cur.lastrowid, row.ticker.upper(), re.sub("\'", "\\'", row.company), dt.datetime.now().strftime('%Y-%m-%d'), dt.datetime.now().strftime('%Y-%m-%d'), row['index'].upper()))
            else:
                logging.warning(
                    '\tUNKNOWN FAILURE - NEW RECORD WAS NOT CREATED')
        else:  # Update with last seen and index
            logging.info('UPDATING RECORD FOR {}'.format(
                row['ticker'].upper()))
            # Create sql
            sql = '''
                UPDATE tickers SET
                    last_seen_date = '{0}'
                    ,latest_index = '{1}'
                WHERE
                    ticker = '{2}'
                ORDER BY 
                    last_seen_date DESC
                LIMIT 1
            '''.format(
                dt.datetime.now().strftime(
                    '%Y-%m-%d'), row['index'], row['ticker'].upper()
            )
            # Query the daabase
            cur.execute(sql)
            # Check if query was a success
            if cur.fetchwarnings() != None:  # If true there was a warning
                # TEMP - come back to this to put in some error handling
                logging.warning(
                    'SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            elif cur.lastrowid != None:  # Shows a row was updated
                logging.info('''\tRECORD UPDATE SUCCESS -> 
                    \t\tticker_id:{}
                    \t\tticker:{}
                    \t\tlast_seen_date:{}
                    \t\tlatest_index:{}'''.format(
                    cur.lastrowid, row.ticker.upper(), dt.datetime.now().strftime('%Y-%m-%d'), row['index'].upper()))
            else:
                logging.warning('\tUNKNOWN FAILURE - RECORD WAS NOT UPDATED')
    except Exception as e:
        logging.error('ERROR -> {}'.format(e))
        err_obj = {
            'error': e, 'ticker': row['ticker'].upper()
        }
        errors_li.append(err_obj)

db.close()

# Show errors


def show_err(_errors_li):
    logging.error('ERRORS COUNT -> {:,}'.format(len(_errors_li)))
    if len(_errors_li) > 0:
        for e in _errors_li:
            logging.error('\t{}'.format(e))


show_err(errors_li)
