"""Master functions"""


from models import engine 
from models import Session as session
from models.prices import create_db
from scrapping import full_scrape
from libs.logs import set_logger, NoLog
from manage_data import remove_duplicate_daily_prices, remove_duplicate_weekly_prices, \
    fill_price_gaps

from config import CONFIG

def create_database():
    create_db(engine)

def run_full_scrape():
    log = set_logger("_run_full_scrape")
    full_scrape(log=log)

def remove_duplicate_prices():
    log = set_logger("_remove_duplicate_prices")
    #Remove daily price duplicates
    remove_duplicate_daily_prices(log=log)
    #Remove weekly price duplicates
    remove_duplicate_weekly_prices(log=log)

def fill_all_price_gaps():
    log = set_logger("_fill_price_gaps")
    fill_price_gaps(log=log)


if __name__ == "__main__":
    # create_database()
    # remove_duplicate_prices()
    # run_full_scrape()
    fill_all_price_gaps()
    pass