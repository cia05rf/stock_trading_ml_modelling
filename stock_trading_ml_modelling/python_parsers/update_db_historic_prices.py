###############################################
#   CODE FOR READING IN ALL RECORDS FROM
#   PRICE FILE AND TRANSFERING TO SERVER
#
###############################################

# STEPS:
#   1.Read in the dataframe being held in an h5. file for weekly prices
#   2.Create an SQL template for adding rows and compile into one query
#   3.Run each ticker individually and upload to the server
#   4.Repeat steps 1-3 for daily prices

# SETUP LOGGING FILE
from rf_db_conx import db_conx
import re
import datetime as dt
import h5py
import pandas as pd
import logging
from stock_trading_ml_modelling.config import WS_UPDATE_PRICES_LOG, \
    HIST_PRICES_W, DB_UPDATE_PRICES

log_file = WS_UPDATE_PRICES_LOG
logging.basicConfig(filename=log_file, filemode="w", level=logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logging.getLogger("").addHandler(console)

logging.info('OPENING FILE...')

# STEP 1 - READ IN THE DATAFRAME OF PRICES
logging.info('Libraries loaded')

# Setup the variables

# Read in the file
logging.info('Reading in weekly prices file...')
f = h5py.File(HIST_PRICES_W, mode='r')
grp = f['data']  # Group name given to weekly data in this file
logging.info('Successfully read in data')
# Get the column headers
col_li = list(grp['_i_table'])
# Get the data
# [:] for get all rows, fails without this
data_df = pd.DataFrame(grp['table'][:], columns=col_li)
# Convert date


def conv_msec_to_dt(x):
    base_datetime = dt.datetime(1970, 1, 1)
    delta = dt.timedelta(0, 0, 0, x)
    return base_datetime + delta


# Converting microseconds to date (div by 10**6)
data_df.date = [conv_msec_to_dt(x/10**6) for x in data_df.date]
# Convert the ticker
data_df.ticker = [str(x, 'utf-8') for x in data_df.ticker]
logging.info('\nEXAMPLE OF data_df -> \n{}'.format(data_df.head()))
# Get a list of tickers
tick_li = data_df['ticker'].unique().tolist()
logging.info('\nUNIQUE TICKERS -> \n{}'.format(tick_li))

# STEP 2 - CONNECT TO THE DATABASE
# Setup the connection
db = db_conx()

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

# If doing a full upload wipe the tables clean
if DB_UPDATE_PRICES == 'full':
    logging.info('\nWIPING DB PRICING TABLES')
    sql = '''
        DELETE FROM historic_prices_w;
        DELETE FROM historic_prices_d;
    '''
    # Query the database
    try:
        cur.execute(sql)
    except Exception as e:
        raise Exception('WIPE_TABLES_ERROR',
                        'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
    logging.info('\nWIPE COMPLETE')
    # Close and re-create the connection
    cur.close()
    db = db_conx()
    cur = db.cursor()

# STEPS 2 & 3 - LOOP THROUGH THE TICKERS AND CREATE AN SQL TEMPLATE WHICH ADDS TO THE DATABASE
errors_li = []
for tick in tick_li:
    try:
        # Get the ticker_id from the server, if there isn't one create it
        sql = '''
            SELECT ticker_id FROM tickers WHERE ticker = '{}' ORDER BY last_seen_date DESC LIMIT 1;
            '''.format(tick.upper())

        # Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('FETCH_TICKER_ID_ERROR',
                            'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
        if cur.fetchwarnings() != None:  # If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_TICKER_ID_ERROR',
                            'server warning {}'.format(cur.fetchwarnings()))
        # Establish the ticker_id
        if cur.rowcount > 0:
            ticker_id = cur.fetchone()[0]
        else:
            ticker_id = None
        logging.info(
            'TICKER ID FOR {} ESTABLISHED AS -> {}'.format(tick.upper(), ticker_id))

        # Get the last date for which there were entries
        sql = '''
            SELECT price_date FROM historic_prices_w WHERE ticker_id = {} ORDER BY price_date DESC LIMIT 1;
        '''.format(ticker_id)
        # Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception(
                'FETCH_LAST_DATE', 'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
        if cur.fetchwarnings() != None:  # If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_LAST_DATE',
                            'server warning {}'.format(cur.fetchwarnings()))
        # Establish the last date
        if cur.rowcount > 0:
            last_date = cur.fetchone()[0]
        else:
            last_date = dt.datetime(1970, 1, 1)
        logging.info('\tLAST_DATE_ESTABLISHED AS -> {}'.format(last_date))

        # Create SQL for inserting data
        rows_sql = ''
        filtered_count = data_df.loc[(data_df.ticker == tick) & (
            data_df.date > last_date)].shape[0]
        if filtered_count < 1:
            logging.info('\tNO NEW PRICES TO ADD, SKIP TO THE NEXT TICKER')
            continue
        else:
            logging.info('\t{} NEW PRICES TO ADD'.format(
                data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date), :].shape[0]))
        for index, row in data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date), :].iterrows():
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
            )'''.format(**data)
        # Replace nan with NULL for SQL
        rows_sql = re.sub('nan', 'NULL', rows_sql)

        # Insert into the database
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
        ) VALUES
        {0}
        '''.format(rows_sql)
        if rows_sql == '':
            logging.info('\trows_sql IS EMPTY, SKIP TO NEXT TICKER')
            continue
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('INSERT_HIST_PRICES_W_ERROR',
                            'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
        # Check for results
        if cur.fetchwarnings() != None:  # If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('INSERT_HIST_PRICES_W_ERROR',
                            'server warning {}'.format(cur.fetchwarnings()))
        if cur.lastrowid < 1:
            raise Exception('INSERT_HIST_PRICES_W_ERROR', 'no rows inserted')
    except Exception as e:
        logging.error('ERROR -> {}'.format(e))
        err_obj = {
            'error': e, 'ticker': tick
        }
        errors_li.append(err_obj)

# Show errors


def show_err(_errors_li):
    logging.error('ERRORS COUNT -> {:,}'.format(len(_errors_li)))
    if len(_errors_li) > 0:
        for e in _errors_li:
            logging.error('\t{}'.format(e))


show_err(errors_li)


# STEP 4 - REPEAT FOR DAILY PRICES
# Read in the file
logging.info('Reading in daily prices file...')
f = h5py.File(HIST_PRICES_D, mode='r')
grp = f['data']  # Group name given to weekly data in this file
logging.info('Successfully read in data')
# Get the column headers
col_li = list(grp['_i_table'])
# Get the data
data_df = pd.DataFrame(grp['table'][:], columns=col_li)
# Convert date
# Converting microseconds to date (div by 10**6)
data_df.date = [conv_msec_to_dt(x/10**6) for x in data_df.date]
# Convert the ticker
data_df.ticker = [str(x, 'utf-8') for x in data_df.ticker]
logging.info('\nEXAMPLE OF data_df -> \n{}'.format(data_df.head()))
# Get a list of tickers
tick_li = data_df['ticker'].unique().tolist()
logging.info('\nUNIQUE TICKERS -> \n{}'.format(tick_li))
for tick in tick_li:
    try:
        # Get the ticker_id from the server, if there isn't one create it
        sql = '''
            SELECT ticker_id FROM tickers WHERE ticker = '{}' ORDER BY last_seen_date DESC LIMIT 1;
            '''.format(tick.upper())

        # Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('FETCH_TICKER_ID_ERROR',
                            'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
        if cur.fetchwarnings() != None:  # If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_TICKER_ID_ERROR',
                            'server warning {}'.format(cur.fetchwarnings()))
        # Establish the ticker_id
        if cur.rowcount > 0:
            ticker_id = cur.fetchone()[0]
        else:
            ticker_id = None
        logging.info(
            'TICKER ID FOR {} ESTABLISHED AS -> {}'.format(tick.upper(), ticker_id))

        # Get the last date for which there were entries
        sql = '''
            SELECT price_date FROM historic_prices_d WHERE ticker_id = {} ORDER BY price_date DESC LIMIT 1;
        '''.format(ticker_id)
        # Query the database
        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception(
                'FETCH_LAST_DATE', 'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
        if cur.fetchwarnings() != None:  # If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('FETCH_LAST_DATE',
                            'server warning {}'.format(cur.fetchwarnings()))
        # Establish the last date
        if cur.rowcount > 0:
            last_date = cur.fetchone()[0]
        else:
            last_date = dt.datetime(1970, 1, 1)
        logging.info('\tLAST_DATE_ESTABLISHED AS -> {}'.format(last_date))

        # Create SQL for inserting data
        rows_sql = ''
        filtered_count = data_df.loc[(data_df.ticker == tick) & (
            data_df.date > last_date)].shape[0]
        if filtered_count < 1:
            logging.info('\tNO NEW PRICES TO ADD, SKIP TO THE NEXT TICKER')
            continue
        else:
            logging.info('\t{} NEW PRICES TO ADD'.format(
                data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date), :].shape[0]))
        for index, row in data_df.loc[(data_df.ticker == tick) & (data_df.date > last_date), :].iterrows():
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
            )'''.format(**data)
        # Replace nan with NULL for SQL
        rows_sql = re.sub('nan', 'NULL', rows_sql)
        if rows_sql == '':
            logging.info('\trows_sql IS EMPTY, SKIP TO NEXT TICKER')
            continue

        # Insert into the database
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
        ) VALUES
        {0}
        '''.format(rows_sql)

        try:
            cur.execute(sql)
        except Exception as e:
            raise Exception('INSERT_HIST_PRICES_D_ERROR',
                            'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
        # Check for results
        if cur.fetchwarnings() != None:  # If true there was a warning
            logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
            raise Exception('INSERT_HIST_PRICES_D_ERROR',
                            'server warning {}'.format(cur.fetchwarnings()))
        if cur.lastrowid < 1:
            raise Exception('INSERT_HIST_PRICES_D_ERROR', 'no rows inserted')
    except Exception as e:
        logging.error('ERROR -> {}'.format(e))
        err_obj = {
            'error': e, 'ticker': tick
        }
        errors_li.append(err_obj)

# STEP 5 - GET THE LATEST PRICES AND UPDATE CURRENT TABLES WITH THIS
logging.info('\nUPDATE current_prices_w WITH LATEST PRICES')
try:
    # Delete the current table contents
    sql = 'DELETE FROM current_prices_w'
    # Query the database
    try:
        cur.execute(sql)
        logging.info('\nSUCCESSFULLY DELETED {} RECORDS'.format(cur.rowcount))
    except Exception as e:
        raise Exception('DELETE_ALL_ERROR',
                        'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
    if cur.fetchwarnings() != None:  # If true there was a warning
        logging.warning('SERVER WARNING -> {}'.format(cur.fetchwarnings()))
        raise Exception('DELETE_ALL_ERROR',
                        'server warning {}'.format(cur.fetchwarnings()))
    # Get the sever to create a new set of results form the latest results in the table
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
        raise Exception('INSERT_INTO_CUR_PRICES',
                        'sql error \n\tsql -> {}\n\tERROR -> {}'.format(sql, e))
except Exception as e:
    logging.error('ERROR -> {}'.format(e))
    err_obj = {
        'error': e, 'ticker': tick
    }
    errors_li.append(err_obj)

# Close the connection
db.close()

# Show errors
show_err(errors_li)
