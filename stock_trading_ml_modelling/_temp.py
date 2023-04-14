import pandas as pd
from tqdm import tqdm

from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df
from stock_trading_ml_modelling.database import ticker, ticker_market, daily_price, weekly_price
from stock_trading_ml_modelling.data_eng.data import DataSet

from_date=None
to_date=None
#Fetch prices
prices_df = sqlaq_to_df(daily_price.fetch(from_date=from_date, to_date=to_date))
ticker_df = sqlaq_to_df(ticker.fetch()) \
    .rename(columns={"id":"ticker_id"})

#Filter to keep only items which are current
max_date = prices_df.date.max()
ticks = prices_df[prices_df.date == max_date] \
    .ticker_id \
    .drop_duplicates()
ticks = pd.merge(ticks.to_frame(), ticker_df[["ticker_id","ticker"]], on=["ticker_id"])

#Setup variables
buy = []
sell = []

prices_df = prices_df.sort_values(['ticker_id','date']) \
    .reset_index(drop=True)

#Loop ticks and get results
for _,r in tqdm(ticks.iterrows(), total=ticks.shape[0], desc="Loop stock to find buy signals"):
    tick_prices = prices_df[prices_df.ticker_id == r.ticker_id]
    dataset = DataSet()
    dataset.add_dataset(tick_prices.change, "change")
    #Calc consecutive losses
    cons_loses = dataset.change.calc_consec_loss()
    dataset.add_dataset(cons_loses, "cons_loses")
    #Identify if it is a buy signal
    check1 = (dataset.cons_loses.data.iloc[-1] == 0 \
        and dataset.cons_loses.data.iloc[-2] >= 3)
    if check1:
        buy.append({
            "ticker":r.ticker,
            "ticker_id":r.ticker_id,
            "cons_loses":dataset.cons_loses.data.iloc[-2],
        })


#Put into a dataframe
buy_df = pd.DataFrame(buy) \
    .sort_values(["cons_loses"], ascending=[False])


logger.info(f"{buy_df.shape[0]} opportunities found")

