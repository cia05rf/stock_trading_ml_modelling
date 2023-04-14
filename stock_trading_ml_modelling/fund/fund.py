"""Functions to run the funds"""

#Import models
import numpy as np
import pandas as pd
import math
import os

#Create a class for a trade
class Trade:
    def __init__(self,
        trade_type:str,
        ticker:str,
        trade_date:int,
        price:float,
        spread:float,
        signal_prob:float,
        value:float=None,
        share_vol:float=None,
        val_inc_tc:bool=True,
        trade_cost:float=0.0,
        sd_rate:float=0.005
        ):
        #Error check
        if spread > 1 or spread < 0:
            raise ValueError('spread should be between 0 and 1, the value expressed was -> {}'.format(spread))
        if price < 0:
            raise ValueError('price cannot be a negative, the value expressed was -> {}'.format(price))
        if value is None and share_vol is None:
            raise ValueError('value or share_vol must be supplied for a trade')
        #Define trade type - can be BUY or SELL
        self.trade_type = trade_type.upper()
        #Define trade attributes
        self.ticker = ticker
        self.trade_date = trade_date
        self.price = price
        self.spread = spread
        self.signal_prob = signal_prob
        self.val_inc_tc = val_inc_tc
        self.trade_cost = trade_cost
        self.sd_rate = sd_rate
        self.ask = self.ask_calc()
        self.bid = self.bid_calc()
        self.trade_price = self.ask if self.trade_type == "BUY" else self.bid
        self.value = value
        self.share_vol = share_vol
        #Calc if value given
        if value is not None:
            #Calc attributes for buy
            self.trade_funds = self.trade_fund_calc()
            self.trade_value = self.trade_value_calc()
            self.share_vol = self.share_vol_calc()
        elif share_vol is not None:
            #Calc attributes for sell
            self.trade_value = self.trade_value_calc()
        #Calc remaining attributes
        self.stamp_duty = self.stamp_duty_calc()
        self.spread_cost = self.spread_cost_calc()
        self.ledger_value = self.ledger_value_calc()

    def trade_fund_calc(self):
        #Calculate the trade value
        if self.val_inc_tc == True: #Does the value include the trade cost
            return self.value - self.trade_cost
        else:
            return self.value

    def ask_calc(self):
        return round(self.price * (1 + self.spread), 2)

    def bid_calc(self):
        return round(self.price * (1 - self.spread), 2)

    def share_vol_calc(self):
        #Calculate the number of whole shares which can be purchased
        return int((self.trade_funds / (1 + self.sd_rate)) / self.trade_price)

    def trade_value_calc(self):
        #Calculate the value of the trade
        return round(self.share_vol * self.trade_price, 2)

    def stamp_duty_calc(self):
        #Calculate the value of the trade
        return round(self.trade_value * self.sd_rate, 2)

    def spread_cost_calc(self):
        #Calc the spread cost
        return round(self.share_vol * self.price * self.spread, 2)

    def ledger_value_calc(self):
        #Calc the ledger value
        if self.trade_type == "BUY":
            return -round(self.trade_value + self.trade_cost + self.stamp_duty, 2)
        else:
            return round(self.trade_value - self.trade_cost - self.stamp_duty, 2)

    def create_df(self,
        trade_id:int,
        invested_value:float,
        available:float
        ):
        #Creates a dataframe for this trade
        return pd.DataFrame([{
                'trade_id':trade_id,
                'trade_type':'buy',
                'signal_prob':self.signal_prob,
                'ticker':self.ticker,
                'trade_date':self.trade_date,
                'spread':self.spread,
                'price':self.price,
                'ask_price':self.ask,
                'bid_price':self.bid,
                'share_vol':self.share_vol,
                'trade_value':self.trade_value,
                'stamp_duty':self.stamp_duty,
                'trade_cost':self.trade_cost,
                'spread_cost':self.spread_cost,
                'ledger_value':self.ledger_value,
                'invested_pre_trade':invested_value,
                'invested_post_trade':invested_value + (self.share_vol * self.price),
                'available_pre_trade':available,
                'available_post_trade':available + self.ledger_value \
                    if self.trade_type == "BUY" \
                    else available - self.ledger_value,
            }])


#Create a class for maintaining the value of the fund
class Fund:
    def __init__(self,
        init_val:float,
        trade_cost:float=0.0,
        sd_rate:float=0.005,
        val_inc_tc:bool=True,
        _verbose:bool=False
        ):
        self.st_val = init_val #The starting value of the fund
        self.available = init_val #The amount available to invest, initially set to the investment value
        self.invested_value = 0 #The total value of hel stocks
        self.codb = 0 #The sum cost of doing business (stamp duty, trading fees)
        self.ledger = pd.DataFrame([]) #A running total of the trades made in this fund
        self.cur_holdings = {} #A record of the current fund holdings
        self.trade_cost = trade_cost
        self.sd_rate = sd_rate
        self.val_inc_tc = val_inc_tc
        self._verbose = _verbose
        print('NEW FUND')
        print('st_val:{:,}'.format(self.st_val))
        print('available:{:,}'.format(self.available))

    @property
    def fund_value(self):
        return self.invested_value + self.available

    def full_update(self):
        """Performs a full update on the fund using all values in the ledger"""
        pass

    def update_post_trade(self, trade):
        """Updates the fund attributes after the latest trade"""
        #Update the object
        if trade.trade_type == "BUY":
            self.available -= trade.ledger_value
            self.codb += round(trade.trade_cost + trade.spread_cost + trade.stamp_duty, 2)
            self.invested_value += trade.ledger_value

    def update_cur_holdings(self, trade):
        #Check if key already in dict
        if trade.ticker in self.cur_holdings: #Should not happen due to earlier exit if key in cur_holdings
            self.cur_holdings[trade.ticker]['share_vol'] += trade.share_vol
            self.cur_holdings[trade.ticker]['cur_price'] = trade.price
            self.cur_holdings[trade.ticker]['value'] = trade.trade_value
            self.cur_holdings[trade.ticker]['trade_ids'].append(trade.trade_id)
        else:
            #Add to cur_holdings
            holding_rec = {
                'share_vol':trade.share_vol
                ,'cur_price':trade.price
                ,'value': trade.trade_value
                ,'trade_ids': [trade.trade_id]
            }
            self.cur_holdings[trade.ticker] = holding_rec
 
    #Create a function to buy shares
    def buy(self
            ,ticker:str
            ,trade_date:int
            ,price:float
            ,spread:float
            ,value:float
            ,signal_prob:float
            ):
        if self._verbose == True:
            print('\nBUY {}'.format(ticker))
        #Check if already bought
        if ticker in self.cur_holdings:
            if self._verbose == True:
                print('\t{} is already in the fund, ignore and move to the next row'.format(ticker))
            return
        #Create the trade
        trade = Trade(
            "BUY",
            ticker,
            trade_date,
            price,
            spread,
            signal_prob,
            value=value,
            val_inc_tc=self.val_inc_tc,
            trade_cost=self.trade_cost,
            sd_rate=self.sd_rate
        )
        #Check the fund has the money to cover this trade
        if abs(trade.ledger_value) > self.available:
            print('\tnot enough funds')
            raise ValueError('you do not have the funds to make this trade -> this transaction will be cancelled')
        #Create a record for the ledger
        self.ledger = pd.concat(
            self.ledger,
            trade.create_df(
                self.ledger.shape[0],
                self.invested_value,
                self.available
            )
        )
        if self._verbose == True:
            print('\tLEDGER ENTRY -> {}'.format(self.ledger.loc[-1])) #OPTIONAL - PRINT ENTRY
        #Update the object
        self.update_post_trade(trade)
        #Update current holdings
        self.update_post_trade(trade)
        
    #Create a function to sell shares
    def sell(self
            ,ticker:str
            ,trade_date:int
            ,price:float
            ,spread:float
            ,signal_prob:float
            ):
        if self._verbose == True:
            print('\nSELL {}'.format(ticker))
        #Check if there are holdings to be sold
        if ticker not in self.cur_holdings:
            if self._verbose == True:
                print('\t{} is not in the fund, ignore and move to the next row'.format(ticker))
            return
        #Round down the share_vol
        share_vol = int(self.cur_holdings[ticker]['share_vol'])
        #Create the trade
        trade = Trade(
            "SELL",
            ticker,
            trade_date,
            price,
            spread,
            signal_prob,
            share_vol=share_vol,
            val_inc_tc=self.val_inc_tc,
            trade_cost=self.trade_cost,
            sd_rate=self.sd_rate
        )
        #Create a record for the ledger
        self.ledger = pd.concat(
            self.ledger,
            trade.create_df(
                self.ledger.shape[0],
                self.invested_value,
                self.available
            )
        )
        if self._verbose == True:
            print('\tLEDGER ENTRY -> {}'.format(self.ledger.loc[-1])) #OPTIONAL - PRINT ENTRY
        #Update the object
        self.update_post_trade(trade)
        #Update current holdings
        self.update_post_trade(trade)
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
    def price_change(self, ticker:str, price:float):
        #Check if key already in dict
        if ticker in self.cur_holdings:
            self.invested_value += round((self.cur_holdings[ticker]['share_vol'] * price) - self.cur_holdings[ticker]['value'], 2)
            self.cur_holdings[ticker]['cur_price'] = price
            self.cur_holdings[ticker]['value'] = round(self.cur_holdings[ticker]['share_vol'] * price, 2)
        else:
            return

    def completed_trades(self, _init_val:int=1, _cur_holdings:dict={}):
        #From the ledger find completed trades
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
        for _index,_row in self.ledger.iterrows():
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
        self,
        _df_in,
        _fund_value_st:float=1000000,
        _investment_limit_min_val:float=100000,
        _investment_limit_max_per:float=0.1,
        _spread:float=0.01,
        _trade_cost:float=250,
        _buy_signal='buy',
        _sell_signal='sell'
        ):
        """Function to run trades through the fund simulator. _df_in must contain columns:
        - signal, ticker, date, open_shift_neg1, signal_prob"""
        #Run through rows and buy and sell according to signals and holdings
        #Error object
        _errors_li = []
        for _index,_row in _df_in.iterrows():
            try:
                #Follow signal
                if _row['signal'] == _buy_signal:
                    #Check for funds
                    if self.available < _investment_limit_min_val:
                        continue
                    #Buy shares
                    _val_to_invest = self.available * _investment_limit_max_per if self.available*_investment_limit_max_per > _investment_limit_min_val else _investment_limit_min_val
                    self.buy(
                        _row['ticker'],
                        _row['date'],
                        _row['open_shift_neg1'],
                        _spread,
                        _val_to_invest,
                        _row['signal_prob']
                        )
                elif _row['signal'] == _sell_signal:
                    #Sell shares
                    self.sell(
                        _row['ticker'],
                        _row['date'],
                        _row['open_shift_neg1'],
                        _spread,
                        _row['signal_prob']
                        )
                elif _row['signal'] == 'hold':
                    self.price_change(
                        _row['ticker'],
                        _row['close']
                        )
            except Exception as e:
                print('ERROR: {}'.format(e))
                _errors_li.append({
                    'error':type(e)
                    ,'error_desc':e
                })
        #Check for errors
        print('\n\nERROR COUNT -> {}'.format(len(_errors_li)))
        if len(_errors_li) > 0:
            print('\tSHOWING ERRORS')
            for e in _errors_li:
                print('\t{}'.format(e))
        return self