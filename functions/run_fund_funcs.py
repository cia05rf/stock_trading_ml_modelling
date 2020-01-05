"""Functions to run the funds"""

#Import models
import numpy as np
import pandas as pd
import math
import os
import tables
from rf_modules import *

#Create a class for maintaining the value of the fund
class new_fund:
    def __init__(self,init_val):
        self.st_val = init_val #The starting value of the fund
        self.available = init_val #The amount available to invest, initially set to the investment value
        self.invested_value = 0 #The total value of hel stocks
        self.codb = 0 #The sum cost of doing business (stamp duty, trading fees)
        self.ledger = [] #A running total of the trades made in this fund
        self.cur_holdings = {} #A dictionary of the current fund holdings
        print('NEW FUND')
        print('st_val:{:,}'.format(self.st_val))
        print('available:{:,}'.format(self.available))
    @property
    def fund_value(self):
        return self.invested_value + self.available
    #Create a function to buy shares
    def buy(self
            ,ticker:str
            ,trade_date:int
            ,price:float
            ,spread:float
            ,value:float
            ,trade_cost:float
            ,signal_prob:float
            ,sd_rate:float = 0.005
            ,val_inc_tc:bool = True
            ,_verbose:bool = True):
        if _verbose == True:
            print('\nBUY {}'.format(ticker))
        #Check if already bought
        if ticker in self.cur_holdings:
            if _verbose == True:
                print('\t{} is already in the fund, ignore and move to the next row'.format(ticker))
            return
        #Error check
        if spread > 1 or spread < 0:
            raise ValueError('spread should be between 0 and 1, the value expressed was -> {:,.2f}'.format(spread))
        if price < 0:
            raise ValueError('price cannot be a negative, the value expressed was -> {:,.2f}'.format(price))
        #Calculate the trade value
        if val_inc_tc == True: #Does the value include the trade cost
            trade_funds = value - trade_cost
        else:
            trade_funds = value
        #Calculate the ask and bid price of each share
        a_price = round(price * (1+spread),2)
        b_price = round(price * (1-spread),2)
        #Calculate the number of whole shares which can be purchased
        share_vol = int((trade_funds / (1+sd_rate))/a_price)
        trade_value = round(share_vol * a_price,2)
        stamp_duty = round(trade_value * sd_rate,2)
        #Calc the ledger_value
        spread_cost = round(share_vol * price * spread,2)
        ledger_value = -round(trade_value + trade_cost + stamp_duty,2)
        #Check the fund has the money to cover this trade
        if -ledger_value > self.available:
            print('\tnot enough funds')
            raise ValueError('you do not have the funds to make this trade -> this transaction will be cancelled')
        #Create a record for the ledger
        ledge_rec = {
            'trade_type':'buy'
            ,'signal_prob':signal_prob
            ,'ticker':ticker
            ,'trade_date':trade_date
            ,'spread':spread
            ,'price':price
            ,'ask_price':a_price
            ,'bid_price':b_price
            ,'share_vol':share_vol
            ,'trade_value':trade_value
            ,'stamp_duty':stamp_duty
            ,'trade_cost':trade_cost
            ,'spread_cost':spread_cost
            ,'holding_value':share_vol*price
            ,'ledger_value':ledger_value
            ,'invested_pre_trade':self.invested_value
            ,'invested_post_trade':self.invested_value + (share_vol*price)
            ,'available_pre_trade':self.available
            ,'available_post_trade':self.available + ledger_value
        }
        self.ledger.append(ledge_rec)
        if _verbose == True:
            print('\tLEDGER ENTRY -> {}'.format(ledge_rec)) #OPTIONAL - PRINT ENTRY
        #Update the object
        self.available += round(ledger_value,2)
        self.codb += round(trade_cost + spread_cost + stamp_duty,2)
        self.invested_value += round(share_vol * price,2)
        #Add to cur_holdings
        holding_rec = {
           'share_vol':share_vol
            ,'cur_price':price
            ,'value':round(share_vol * price,2)
        }
        #Check if key already in dict
        if ticker in self.cur_holdings: #Should not happen due to earlier exit if key in cur_holdings
            self.cur_holdings[ticker]['share_vol'] += share_vol
            self.cur_holdings[ticker]['cur_price'] = round(price,2)
            self.cur_holdings[ticker]['value'] = round(self.cur_holdings[ticker]['share_vol']*price,2)
        else:
            self.cur_holdings[ticker] = holding_rec
        
    #Create a function to sell shares
    def sell(self
             ,ticker:str
             ,trade_date:int
             ,price:float
             ,spread:float
             ,trade_cost:float
             ,signal_prob:float
             ,sd_rate:float = 0.00
            ,_verbose:bool = True):
        if _verbose == True:
            print('\nSELL {}'.format(ticker))
        #Check if there are holdings to be sold
        if ticker not in self.cur_holdings:
            if _verbose == True:
                print('\t{} is not in the fund, ignore and move to the next row'.format(ticker))
            return
        #Round down the share_vol
        share_vol = int(self.cur_holdings[ticker]['share_vol'])
        #Error check
        if spread > 1 or spread < 0:
            raise ValueError('spread should be between 0 and 1, the value expressed was -> {}'.format(spread))
        if price < 0:
            raise ValueError('price cannot be a negative, the value expressed was -> {}'.format(price))
        #Calculate the ask and bid price of each share
        a_price = round(price * (1+spread),2)
        b_price = round(price * (1-spread),2)
        #Calculate the trade value
        trade_value = round(share_vol*b_price,2)
        stamp_duty = round(trade_value * sd_rate,2)
        value = round(trade_value - trade_cost - stamp_duty,2)
        spread_cost = round(share_vol * price * spread)
        #Calc the ledger_value
        ledger_value = round(trade_value - trade_cost - stamp_duty,2)
        #Create a record for the ledger
        ledge_rec = {
           'trade_type':'sell'
            ,'signal_prob':signal_prob
            ,'ticker':ticker
            ,'trade_date':trade_date
            ,'spread':spread
            ,'price':price
            ,'ask_price':a_price
            ,'bid_price':b_price
            ,'share_vol':share_vol
            ,'trade_value':trade_value
            ,'stamp_duty':stamp_duty
            ,'trade_cost':trade_cost
            ,'spread_cost':spread_cost
            ,'holding_value':share_vol*price
            ,'ledger_value':ledger_value
            ,'invested_pre_trade':self.invested_value
            ,'invested_post_trade':self.invested_value - (share_vol*price)
            ,'available_pre_trade':self.available
            ,'available_post_trade':self.available + ledger_value
        }
        self.ledger.append(ledge_rec)
        if _verbose == True:
            print('\tLEDGER ENTRY -> {}'.format(ledge_rec)) #OPTIONAL - PRINT ENTRY
        #Update the object
        self.available += round(ledger_value,2)
        self.codb += round(trade_cost + spread_cost + stamp_duty,2)
        self.invested_value += -round((share_vol*price),2)
        #Remove from cur_holdings
        #Check if key already in dict
        if ticker in self.cur_holdings:
            if self.cur_holdings[ticker]['share_vol'] > share_vol:
                self.cur_holdings[ticker]['share_vol'] += -share_vol
                self.cur_holdings[ticker]['cur_price'] = price
                self.cur_holdings[ticker]['value'] = round(self.cur_holdings[ticker]['share_vol']*price,2)
            elif self.cur_holdings[ticker]['share_vol'] == share_vol:
                del self.cur_holdings[ticker] #Delete from the dictionary
            else:
                raise ValueError('you do not have enough shares to make this trade. You want to sell {} of {} however you only have {}'.format(share_vol,ticker,self.cur_holdings[ticker]['share_vol']))
        else:
            return
        
    #Create a function to update value after a price change
    def price_change(self,ticker:str,price:float):
        #Check if key already in dict
        if ticker in self.cur_holdings:
            self.invested_value += round((self.cur_holdings[ticker]['share_vol']*price) - self.cur_holdings[ticker]['value'],2)
            self.cur_holdings[ticker]['cur_price'] = price
            self.cur_holdings[ticker]['value'] = round(self.cur_holdings[ticker]['share_vol']*price,2)
        else:
            return

def completed_trades(_df_in,_init_val:int=1,_cur_holdings:dict={}):
    #From the ledger create a dataframe of completed trades
    _completed_trades = {}
    #Format
    #     ABC:{
    #         open_position:True/False #Bool showing if there is currently an open position
    #         ,trades:[ #List showing all trades
    #             { #Each trade has an object
    #                 share_vol:12345 #Volume of shares purchased
    #                 ,buy_spend:12345.67 #Total value spent in buying shares including costs
    #                 ,sell_spend:12345.67 #Total value spent in selling shares including costs
    #                 ,profit_loss:12345.67 #Profit/loss of ths trade
    #                 ,holding_value:12345.67 #Current value of the shares
    #             }
    #         ]
    #     }
    for _index,_row in _df_in.iterrows():
        #Check if there is an open trade for this ticker
        if _row['ticker'] not in _completed_trades:
            _completed_trades[_row['ticker']] = {
                'open_position':False
                ,'trades':[]
            }
        #Deal with buying
        if _row['trade_type'] == 'buy':
            #Create a trade object and add to the trades list in completed_trades
            _completed_trades[_row['ticker']]['trades'].append({
                'ticker':_row['ticker']
                ,'share_vol':_row['share_vol']
                ,'buy_price':_row['price']
                ,'buy_spend':-_row['ledger_value']
                ,'buy_prob':_row['signal_prob']
                ,'buy_date':_row['trade_date']
                ,'sell_price':None
                ,'sell_spend':None
                ,'sell_prob':None
                ,'sell_date':None
                ,'periods_held':None
                ,'profit_loss':None
                ,'holding_value':_row['holding_value']
            })
            #open the trading position
            _completed_trades[_row['ticker']]['open_position'] = True
        #Dealing with selling
        elif _row['trade_type'] == 'sell':
            _shares_to_sell = _row['share_vol']
            #Find open positions and sell until shares al gone
            for _trade in _completed_trades[_row['ticker']]['trades']:
                if _trade['share_vol'] == _row['share_vol']:
                    _trade['sell_price'] = _row['price']
                    _trade['sell_spend'] = _row['ledger_value']
                    _trade['sell_prob'] = _row['signal_prob']
                    _trade['sell_date'] = _row['trade_date']
                    _trade['holding_value'] = _row['holding_value']
    _trades_li = []
    for _tick in _completed_trades:
        for _trade in _completed_trades[_tick]['trades']:
            _trades_li.append(_trade)
    _trades_df = pd.DataFrame(_trades_li,columns=[
                'ticker'
                ,'share_vol'
                ,'buy_price'
                ,'buy_spend'
                ,'buy_prob'
                ,'buy_date'
                ,'sell_price'
                ,'sell_spend'
                ,'sell_prob'
                ,'sell_date'
                ,'periods_held'
                ,'profit_loss'
                ,'holding_value'])
    _trades_df['profit_loss'] = _trades_df['sell_spend'] - _trades_df['buy_spend'] 
    _trades_df['periods_held'] = _trades_df['sell_date'] - _trades_df['buy_date']
    _trades_df['periods_held'] = [_x.days/7 for _x in _trades_df['periods_held']]
    #Dealing with open trade values
    _trades_df.reset_index(inplace=True,drop=True)
    for _index,_row in _trades_df[_trades_df.sell_date.isna()].iterrows():
        _trades_df.iloc[_index,_trades_df.columns.get_loc('holding_value')] = _cur_holdings[_row.ticker]['value']
    #Display top level summary
    print('TRADE COUNT -> {:,}'.format(_trades_df[~_trades_df['profit_loss'].isna()].shape[0]))
    print('TOTAL PROFIT/LOSS -> £{:,.2f}'.format(_trades_df['profit_loss'].sum()/100))
    print('\nOPEN TRADES -> {:,}'.format(_trades_df[_trades_df['profit_loss'].isna()].shape[0]))
    print('OPEN TRADES VALUE -> £{:,.2f}'.format(_trades_df[_trades_df['profit_loss'].isna()]['holding_value'].sum()/100))
    print('\nTOTAL ROI VALUE -> £{:,.2f}'.format((_trades_df['profit_loss'].sum()/100) + (_trades_df[_trades_df['profit_loss'].isna()]['holding_value'].sum()/100)))
    print('TOTAL ROI % -> {:,.2f}%'.format(100 * (_trades_df['profit_loss'].sum() + _trades_df[_trades_df['profit_loss'].isna()]['holding_value'].sum() - _init_val) / _init_val))
    #Calculate ROI
    _trades_df['roi'] = (_trades_df['holding_value'] / _trades_df['buy_spend']) - 1
    print('''\nROI STATS:
        AVERAGE ROI % -> {:.2f}
        AVERAGE POS ROI % -> {:.2f}
        POS COUNT -> {:,.0f}
        AVERAGE NEG ROI % -> {:.2f}
        NEG COUNT -> {:,.0f}
    \nPERIODS HELD STATS:
        AVERAGE PERIODS HELD -> {:.2f}
        AVERAGE POS ROI PERIODS HELD -> {:.2f}
        AVERAGE NEG ROI PERIODS HELD -> {:.2f}
        '''.format(
        _trades_df['roi'].mean()*100
        ,_trades_df[_trades_df['roi'] > 0]['roi'].mean()*100
        ,len(_trades_df[_trades_df['roi'] > 0])
        ,_trades_df[_trades_df['roi'] <= 0]['roi'].mean()*100
        ,len(_trades_df[_trades_df['roi'] <= 0])
        ,_trades_df['periods_held'].mean()
        ,_trades_df[_trades_df['roi'] > 0]['periods_held'].mean()
        ,_trades_df[_trades_df['roi'] <= 0]['periods_held'].mean()
    ))
    
    return _trades_df


def run_fund(
    _df_in
    ,_fund_value_st:float=1000000
    ,_investment_limit_min_val:float=100000
    ,_investment_limit_max_per:float=0.1
    ,_spread:float=0.01
    ,_trade_cost:float=250
    ,_buy_signal='buy'
    ,_sell_signal='sell'):
    #Run through rows and buy and sell according to signals and holdings
    _run_time = process_time()
    _fund = new_fund(_fund_value_st)
    #Error object
    _errors_li = []
    for _index,_row in _df_in.iterrows():
        try:
            #Follow signal
            if _row['signal'] == _buy_signal:
                #Check for funds
                if _fund.available < _investment_limit_min_val:
                    continue
                #Buy shares
                _val_to_invest = _fund.available*_investment_limit_max_per if _fund.available*_investment_limit_max_per > _investment_limit_min_val else _investment_limit_min_val
                _fund.buy(_row['ticker'],_row['date'],_row['open_shift_neg1'],_spread,_val_to_invest,_trade_cost,_row['signal_prob'])
            elif _row['signal'] == _sell_signal:
                #Sell shares
                _fund.sell(_row['ticker'],_row['date'],_row['open_shift_neg1'],_spread,_trade_cost,_row['signal_prob'])
            elif _row['signal'] == 'hold':
                _fund.price_change(_row['ticker'],_row['close'])
        except Exception as e:
            print('ERROR: {}'.format(e))
            _errors_li.append({
                'error':type(e)
                ,'error_desc':e
            })
    _run_time.end()
    #Check for errors
    print('\n\nERROR COUNT -> {}'.format(len(_errors_li)))
    if len(_errors_li) > 0:
        print('\tSHOWING ERRORS')
        for e in _errors_li:
            print('\t{}'.format(e))
    return _fund