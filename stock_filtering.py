import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tqdm import tqdm

from config import CONFIG
from libs.sql_funcs import start_engine
from libs.ft_eng_funcs import calc_ema_macd, flag_mins, flag_maxs, prev_max_min, mk_prev_move_float

# prices_w_df = pd.read_hdf(CONFIG['files']['store_path'] + CONFIG['files']['hist_prices_w'])

db_file = CONFIG['files']['store_path'] + CONFIG['files']['prices_db']

engine, session = start_engine(db_file)

sql = """
    SELECT
        t.ticker,
        dp.date,
        dp.open,
        dp.high,
        dp.low,
        dp.close
    FROM daily_price AS dp
    LEFT JOIN ticker AS t
        ON t.id = dp.ticker_id;
"""
prices_w_df = pd.read_sql(sql, engine)

#Keep only relevant columns
prices_w_df = prices_w_df[['ticker','date','open','high','low','close']]

prices_w_df = prices_w_df.sort_values(['ticker','date']).reset_index(drop=True)

for c in ['macd','real_macd_min','prev_min_macd','prev_min_macd_grad','real_macd_max','prev_max_macd','prev_max_macd_grad']:
    prices_w_df[c] = np.nan

for tick in tqdm(prices_w_df.ticker.unique(),total=prices_w_df.ticker.unique().shape[0]):
    tick_df = prices_w_df.loc[prices_w_df.ticker == tick,:]
    tick_df = calc_ema_macd(tick_df)
    tick_df['real_macd_min'] = flag_mins(tick_df['macd'],_period=1,_cur=False)
    tick_df['real_macd_max'] = flag_maxs(tick_df['macd'],_period=1,_cur=False)
    ### MINS ###
    #Find the last 2 mins
    tick_df["prev_min_macd"],tick_df["prev_min_macd_date"],tick_df["prev_min_macd_index"] = prev_max_min(tick_df[["date",'macd',"real_macd_min"]].copy(),'macd',"real_macd_min",1)
    tick_df["prev_min_macd_change"] = mk_prev_move_float(tick_df['prev_min_macd'])
    tick_df["prev_min_macd_index_change"] = mk_prev_move_float(tick_df['prev_min_macd_index'])
    #Calc the gradient
    tick_df['prev_min_macd_grad'] = tick_df["prev_min_macd_change"] / tick_df["prev_min_macd_index_change"]
    ### MAXS ###
    #Find the last 2 maxs
    tick_df["prev_max_macd"],tick_df["prev_max_macd_date"],tick_df["prev_max_macd_index"] = prev_max_min(tick_df[["date",'macd',"real_macd_max"]].copy(),'macd',"real_macd_max",1)
    tick_df["prev_max_macd_change"] = mk_prev_move_float(tick_df['prev_max_macd'])
    tick_df["prev_max_macd_index_change"] = mk_prev_move_float(tick_df['prev_max_macd_index'])
    #Calc the gradient
    tick_df['prev_max_macd_grad'] = tick_df["prev_max_macd_change"] / tick_df["prev_max_macd_index_change"]
    prices_w_df.loc[prices_w_df.ticker == tick,:] = tick_df

#Filter to signal items
buy_mask = (prices_w_df.date == prices_w_df.date.max()) & (prices_w_df.prev_min_macd_grad > 0) & (prices_w_df.macd > prices_w_df.macd.shift(1)) & (prices_w_df.macd.shift(1) < prices_w_df.macd.shift(2))
buy_df = prices_w_df[buy_mask]
buy_df['signal'] = 'BUY'

sell_mask = (prices_w_df.date == prices_w_df.date.max()) & (prices_w_df.prev_min_macd_grad < 0) & (prices_w_df.macd < prices_w_df.macd.shift(1)) & (prices_w_df.macd.shift(1) > prices_w_df.macd.shift(2))
sell_df = prices_w_df[sell_mask]
sell_df['signal'] = 'SELL'

print(f"COUNT BUY -> {buy_df.shape[0]}")
print(f"COUNT SELL -> {sell_df.shape[0]}")
display(buy_df)
display(sell_df)


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