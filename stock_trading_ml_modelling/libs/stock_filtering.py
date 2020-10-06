import pandas as pd
from tqdm import tqdm

from stock_trading_ml_modelling.utils.ft_eng import calc_macd
from stock_trading_ml_modelling.libs.logs import log
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df
from stock_trading_ml_modelling.database import ticker, ticker_market, daily_price, weekly_price


def filter_stocks(from_date=None, to_date=None):
    """Function to search for shares to buy
    
    args:
    ----
    from_date - datetime:None - a bounding minimum date (if neccesary)
    to_date - datetime:None - a bounding maximum date (if neccesary)
    
    returns:
    ----
    pandas dataframe
    """
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
        #Calculate the short macd
        _, _, _, _, tick_prices["macd_short"] = calc_macd(tick_prices.close, ema_lng=26, ema_sht=12, sig_period=9)
        #Calculate the long macd
        _, _, _, _, tick_prices["macd_long"] = calc_macd(tick_prices.close, ema_lng=26*5, ema_sht=12*5, sig_period=9*5)
        #Calc gradients of macds
        tick_prices["macd_short_grad"] = (tick_prices.macd_short - tick_prices.macd_short.shift(1)) / abs(tick_prices.macd_short)
        tick_prices["macd_long_grad"] = (tick_prices.macd_long - tick_prices.macd_long.shift(1)) / abs(tick_prices.macd_long)
        tick_prices["macd_long"] = tick_prices.macd_long / abs(tick_prices.close)
        #Identify if it is a buy signal
        check1 = (tick_prices.iloc[-1].macd_short_grad > 0 \
            and tick_prices.iloc[-2].macd_short_grad < 0
            and tick_prices.iloc[-1].macd_long > 0)
        if check1:
            buy.append({
                "ticker":r.ticker,
                "ticker_id":r.ticker_id,
                "short_grad":tick_prices.iloc[-1].macd_short_grad,
                "long_grad":tick_prices.iloc[-1].macd_long_grad,
                "macd_long":tick_prices.iloc[-1].macd_long
            })
     
    #Put into a dataframe
    buy_df = pd.DataFrame(buy) \
        .sort_values(["long_grad"], ascending=[False])


    log.info(f"{buy_df.shape[0]} opportunities found")

    return buy_df

# import numpy as np
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots
    # for c in ['macd','real_macd_min','prev_min_macd','prev_min_macd_grad','real_macd_max','prev_max_macd','prev_max_macd_grad']:
    #     prices_d_df[c] = np.nan

    # for tick in tqdm(prices_d_df.ticker.unique(),total=prices_d_df.ticker.unique().shape[0]):
    #     tick_df = prices_d_df.loc[prices_d_df.ticker == tick,:]
    #     tick_df = calc_ema_macd(tick_df)
    #     tick_df['real_macd_min'] = flag_mins(tick_df['macd'],_period=1,_cur=False)
    #     tick_df['real_macd_max'] = flag_maxs(tick_df['macd'],_period=1,_cur=False)
    #     ### MINS ###
    #     #Find the last 2 mins
    #     tick_df["prev_min_macd"],tick_df["prev_min_macd_date"],tick_df["prev_min_macd_index"] = prev_max_min(tick_df[["date",'macd',"real_macd_min"]].copy(),'macd',"real_macd_min",1)
    #     tick_df["prev_min_macd_change"] = mk_prev_move_float(tick_df['prev_min_macd'])
    #     tick_df["prev_min_macd_index_change"] = mk_prev_move_float(tick_df['prev_min_macd_index'])
    #     #Calc the gradient
    #     tick_df['prev_min_macd_grad'] = tick_df["prev_min_macd_change"] / tick_df["prev_min_macd_index_change"]
    #     ### MAXS ###
    #     #Find the last 2 maxs
    #     tick_df["prev_max_macd"],tick_df["prev_max_macd_date"],tick_df["prev_max_macd_index"] = prev_max_min(tick_df[["date",'macd',"real_macd_max"]].copy(),'macd',"real_macd_max",1)
    #     tick_df["prev_max_macd_change"] = mk_prev_move_float(tick_df['prev_max_macd'])
    #     tick_df["prev_max_macd_index_change"] = mk_prev_move_float(tick_df['prev_max_macd_index'])
    #     #Calc the gradient
    #     tick_df['prev_max_macd_grad'] = tick_df["prev_max_macd_change"] / tick_df["prev_max_macd_index_change"]
    #     prices_d_df.loc[prices_d_df.ticker == tick,:] = tick_df

    # #Filter to signal items
    # buy_mask = (prices_d_df.date == prices_d_df.date.max()) & (prices_d_df.prev_min_macd_grad > 0) & (prices_d_df.macd > prices_d_df.macd.shift(1)) & (prices_d_df.macd.shift(1) < prices_d_df.macd.shift(2))
    # buy_df = prices_d_df[buy_mask]
    # buy_df['signal'] = 'BUY'

    # sell_mask = (prices_d_df.date == prices_d_df.date.max()) & (prices_d_df.prev_min_macd_grad < 0) & (prices_d_df.macd < prices_d_df.macd.shift(1)) & (prices_d_df.macd.shift(1) > prices_d_df.macd.shift(2))
    # sell_df = prices_d_df[sell_mask]
    # sell_df['signal'] = 'SELL'

    # print(f"COUNT BUY -> {buy_df.shape[0]}")
    # print(f"COUNT SELL -> {sell_df.shape[0]}")
    # display(buy_df)
    # display(sell_df)


# ft_eng_w_df = ft_eng_w_df[['ticker','date','close','macd','prev_min_macd_grad']]
# ft_eng_w_df['open'] = ft_eng_w_df.close
# ft_eng_w_df['high'] = ft_eng_w_df.close
# ft_eng_w_df['low'] = ft_eng_w_df.close

# ft_eng_w_df = ft_eng_w_df.sort_values(['ticker','date']).reset_index(drop=True)


# tick = 'BAB'
# tmp_df = ft_eng_w_df[ft_eng_w_df.ticker == tick]
# # tmp_df = calc_ema_macd(tmp_df)

# fig = make_subplots(rows=2,cols=1,specs=[[{'secondary_y':False}],[{'secondary_y':True}]])
# #Chart 1
# fig.add_trace(
#     go.Ohlc(
#         x=tmp_df.date,
#         open=tmp_df.open,
#         high=tmp_df.high,
#         low=tmp_df.low,
#         close=tmp_df.close,
#         name='OHLC'
#     ),
#     row=1,col=1
# )
# # fig.add_trace(
# #     go.Scatter(
# #         x=tmp_df.date,
# #         y=tmp_df.ema12,
# #         name='ema12'
# #     ),
# #     row=1,col=1
# # )
# # fig.add_trace(
# #     go.Scatter(
# #         x=tmp_df.date,
# #         y=tmp_df.ema26,
# #         name='ema26'
# #     ),
# #     row=1,col=1
# # )

# #Chart 2
# fig.add_trace(
#     go.Bar(
#         x=tmp_df[tmp_df.macd > 0].date,y=tmp_df[tmp_df.macd > 0].macd,
#         marker_color='green'
#     ),
#     row=2,col=1
# )
# fig.add_trace(
#     go.Bar(
#         x=tmp_df[tmp_df.macd < 0].date,y=tmp_df[tmp_df.macd < 0].macd,
#         marker_color='red'
#     ),
#     row=2,col=1
# )
# # fig.add_trace(
# #     go.Scatter(
# #         x=tmp_df.date,
# #         y=tmp_df.macd_line,
# #         name='macd line'
# #     ),
# #     row=2,col=1,secondary_y=True
# # )
# # fig.add_trace(
# #     go.Scatter(
# #         x=tmp_df.date,
# #         y=tmp_df.signal_line,
# #         name='signal line'
# #     ),
# #     row=2,col=1,secondary_y=True
# # )


# #Establish range selector and buttons
# rng_sel_di = dict(
#     buttons=list([
#         dict(count=1,
#              label="1m",
#              step="month",
#              stepmode="backward"),
#         dict(count=6,
#              label="6m",
#              step="month",
#              stepmode="backward"),
#         dict(count=1,
#              label="YTD",
#              step="year",
#              stepmode="todate"),
#         dict(count=1,
#              label="1y",
#              step="year",
#              stepmode="backward"),
#         dict(count=5,
#              label="5y",
#              step="year",
#              stepmode="backward"),
#         dict(count=3,
#              label="3y",
#              step="year",
#              stepmode="backward"),
#         dict(step="all")
#     ])
# )
# for axis in ['xaxis'
#              ,'xaxis2'
#             ]:
#     fig.layout[axis].rangeselector=rng_sel_di
#     fig.layout[axis].rangeslider.visible=False
# # fig.layout.yaxis.domain = [0.7,1.0]
# # fig.layout.yaxis2.domain = [0.0,0.3]
# fig.update_yaxes(automargin=True)
# fig.update_layout(
#     title=f'Charts for {tick}'
# )

# fig.show()
# display(ft_eng_w_df[ft_eng_w_df.ticker == tick][['ticker','date','close','ema26','macd','prev_min_macd_grad']].tail(15))