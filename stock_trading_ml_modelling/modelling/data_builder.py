
import numpy as np
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from sklearn.model_selection import train_test_split
import json
import gc
from stock_trading_ml_modelling.libs.data import Data

from stock_trading_ml_modelling.modelling.price_data import PriceData


class DataBuilder:
    def __init__(self,
        period=10,
        macd_sht=[12, 26, 9],
        macd_lng=[60, 130, 45],
        ticker_ids=[],
        folder:str="default",
        window:int=256
        ):
        #Turn off annoying warning
        pd.options.mode.chained_assignment = None  # default='warn'
        self.prices = None
        self.period = period
        self.macd_sht = macd_sht
        self.macd_lng = macd_lng
        self.X = None
        self.y = None
        self.prices_id = None
        self.labels = {}
        self.signals = None
        self.X_train = None
        self.y_train = None
        self.prices_id_train = None
        self.X_test = None
        self.y_test = None
        self.prices_id_test = None
        self.folder = folder
        self.window = window
        self.ticker_ids = ticker_ids

    def get_price_data(self, weeks=52*10, force:bool=False):
        if self.prices is None or force:
            price_data = PriceData()
            self.prices = price_data.get_prices(ticker_ids=self.ticker_ids, weeks=weeks)

    def create_data(self, weeks=52*10, force=False):
        self.get_price_data(weeks=weeks, force=force)
        #Create labels
        self.prices["signal"] = self.identify_signals_gain_loss(self.prices.close, self.period)
        self.prices["encoded_signal"], self.labels = self.encode_labels(self.prices.signal)
        #Loop tickers
        ticker_ids = self.prices.ticker_id.unique()
        for tick_id in tqdm(ticker_ids, total=ticker_ids.shape[0], desc="Create data for tickers"):
            #Get all the values from df
            tick_prices = self.prices[self.prices.ticker_id == tick_id]
            prices_id, encoded_signal, tick_data = self.create_ticker_data(tick_prices)
            tick_X = self.zip_data(tick_data)
            #Append data
            self.X = tick_X if self.X is None else np.concatenate((self.X, tick_X), axis=0)
            self.y = encoded_signal if self.y is None else np.concatenate((self.y, encoded_signal), axis=None)
            self.prices_id = prices_id if self.prices_id is None else np.concatenate((self.prices_id, prices_id), axis=None)
        #Split data
        self.test_train_split()
        self.signals, _ = self.decode_labels(self.y)

    def create_ticker_data(self, tick_prices):
        _, close = self.create_data_max_min_norm(tick_prices.close, name="close")
        _, _open = self.create_data_max_min_norm(tick_prices.open, name="_open")
        pos_change, neg_change = self.split_pos_neg(tick_prices.change, fillna=0)
        _, pos_change = self.create_data_max_min_norm(pos_change, name="change")
        _, neg_change = self.create_data_max_min_norm(neg_change, name="change")
        _, tail = self.create_data_intraday(tick_prices, head_tail="tail")
        _, head = self.create_data_intraday(tick_prices, head_tail="head")
        _, encoded_signal = self.fetch_last_from_moving_window(tick_prices.encoded_signal)
        _, prices_id = self.fetch_last_from_moving_window(tick_prices.id)
        macd_sht_pos, macd_sht_neg = self.create_macd(tick_prices, *self.macd_sht)
        macd_lng_pos, macd_lng_neg = self.create_macd(tick_prices, *self.macd_lng)
        rsi = self.create_rsi(tick_prices)
        return prices_id, encoded_signal, [close, _open, tail, head, macd_sht_pos, macd_sht_neg, macd_lng_pos, macd_lng_neg, rsi, pos_change, neg_change]

    def split_pos_neg(self, s, fillna=np.nan):
        """Will take in a series and return a series where values are positive
        and a series where vales are negative"""
        pos_s = s.copy()
        pos_s.loc[pos_s < 0] = fillna
        neg_s = s.copy()
        neg_s.loc[neg_s > 0] = fillna
        return pos_s, neg_s

    def identify_signals_gain_loss(self, s, gain:float=0.05, period1:int=5, period2:int=10):
        """Identify signals based on future performance
        
        buy - 5 days future greater than now (finds if still had further to drop)
            - 10 days future greater than now
            - gain great than x%
        
        sell - 5 days future less than now
            - 10 days future less than now

        hold - all others
        """
        #Create a data object for it
        tick_data = Data(s)
        future1 = tick_data.data.shift(-period1)
        future2 = tick_data.data.shift(-period2)
        signal = pd.Series(["hold"] * tick_data.data.shape[0], name="signal", index=tick_data.data.index)
        buy_mask = (future1 > tick_data.data) \
            & (future2 > (tick_data.data * (gain + 1)))
        sell_mask = (future1 < tick_data.data) & (future2 < tick_data.data)
        signal.loc[buy_mask] = "buy"
        signal.loc[sell_mask] = "sell"
        return signal

    def identify_signals_min_max(self, s, period:int=10):
        """Find the mins in the dataset"""
        #Create a data object for it
        tick_data = Data(s)
        data_df = tick_data.data.to_frame()
        data_df["maxs"] = tick_data.flag_maxs(period)
        data_df["mins"] = tick_data.flag_mins(period)
        #Turn into a single field
        data_df["signal"] = "hold"
        data_df.loc[data_df.maxs, "signal"] = "sell"
        data_df.loc[data_df.mins, "signal"] = "buy"
        data_df = data_df.drop(columns=["maxs","mins"])
        #Shift by 1 to make max and min spotting easier
        data_df["signal"] = data_df.signal.shift(1, fill_value="hold")
        return data_df["signal"]

    def fetch_last_from_moving_window(self, s):
        data = Data(s)
        index, arr = data.fetch_last_from_moving_window(window=self.window)
        return index, arr

    def create_data_max_min_norm(self, s, name:str="UNDEFINED"):
        data = Data(s)
        index, arr = data.build_moving_window_data(window=self.window, bulk_val=0)
        #Normalise the data
        for i,x in enumerate(arr):
            arr[i] = Data(x).norm_data_max_min_value()
        #Fill nan with 0
        arr = np.nan_to_num(arr, nan=0)
        return index, arr

    def create_data_intraday(self, prices, head_tail:str="tail"):
        if head_tail == "tail":
            mask = prices.close < prices.open
        else:
            mask = prices.close > prices.open
        baseline = prices.open
        baseline.loc[mask] = prices[mask].close
        #Normalise to between the high and low for that day
        if head_tail == "tail":
            outlier = (baseline - prices.low) / (prices.high - prices.low)
        else:
            outlier = (prices.high - baseline) / (prices.high - prices.low)
        #Correct for errors
        outlier.loc[outlier > 1] = 1 #Impossible vaues
        outlier.loc[outlier < 1] = 0 #Impossible vaues
        #Create windows
        outlier = Data(outlier)
        index, arr = outlier.build_moving_window_data(window=self.window, bulk_val=0)
        #Fill nan with 0
        arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
        assert not np.any(np.isnan(arr))
        return index, arr

    def create_windows(self, pos_neg, macd_hist, sht, lng, sig):
        macd2 = macd_hist.copy()
        if pos_neg == "pos":
            #Positive
            #Set values to 0
            macd2.loc[macd2 < 0] = 0
        else:
            #Negative
            #Set values to 0
            macd2.loc[macd2 > 0] = 0
            #Reverse sign
            macd2 = macd2.apply(lambda x: abs(x))
        #Create windows
        macd2 = Data(macd2)
        _, macd2 = macd2.build_moving_window_data(window=self.window, bulk_val=0)
        #Normalise the data to max value
        for i,x in enumerate(macd2):
            macd2[i] = Data(x).norm_data_max_value()
        return macd2
        
    def create_macd(self, prices, sht, lng, sig):
        # _, _, _, _, macd_hist = Data(prices.close).calc_macd(sht, lng, sig)
        #Catch if too short
        if prices.shape[0] < (lng + sig):
            macd_hist = pd.Series([0] * prices.shape[0])
        else:
            macd_df = Data(prices.close).calc_macd(sht, lng, sig)
            macd_hist = macd_df[f"MACDh_{sht}_{lng}_{sig}"]
        pos, neg = self.create_windows("pos", macd_hist, sht, lng, sig), self.create_windows("neg", macd_hist, sht, lng, sig)
        pos, neg = np.nan_to_num(pos), np.nan_to_num(neg)
        return pos, neg
        
    def create_rsi(self, prices, length:int=14):
        # _, _, _, _, macd_hist = Data(prices.close).calc_macd(sht, lng, sig)
        rsi_s = Data(prices.close).calc_rsi(length)
        #Normalise
        rsi_s = rsi_s / 100
        _, rsi =  Data(rsi_s).build_moving_window_data(window=self.window, bulk_val=0.5)
        return rsi
    
    def zip_data(self, datasets:list):
        X = np.stack(datasets, axis=1)
        assert not np.any(np.isnan(X))
        return X

    def create_labels(self, s):
        uni_classes = s.unique()
        labels = {k:v for v,k in enumerate(uni_classes)}
        return labels

    def encode_labels(self, s):
        labels = self.create_labels(s)
        encoded_s = np.vectorize(labels.get)(s.values)
        return encoded_s, labels

    def decode_labels(self, np_array=None, key=None):
        if key is not None:
            np_array = getattr(self, key)
        elif np_array is None:
            raise "key and np_array cannot both be None"
        rev_labels = {v:k for k,v in self.labels.items()}
        decoded_s = np.vectorize(rev_labels.get)(np_array)
        return decoded_s, rev_labels

    def test_train_split(self):
        self.X_train, self.X_test, self.y_train, self.y_test, self.prices_id_train, self.prices_id_test = train_test_split(
            self.X, self.y, self.prices_id,
            test_size=0.25, random_state=42
            )

    def save_preds(self, preds):
        path = Path("data", self.folder)
        path.mkdir(parents=True, exist_ok=True)
        np.save(path / "preds.npy", preds)

    def load_preds(self, decode=True):
        path = Path("data", self.folder)
        path.mkdir(parents=True, exist_ok=True)
        preds = np.load(path / "preds.npy")
        if decode:
            with open(path / "labels.txt", "r") as f:
                self.labels = json.loads(f.read())
            preds = self.decode_labels(preds)
        return preds

    def save_data(self, keys=[]):
        path = Path("data", self.folder)
        path.mkdir(parents=True, exist_ok=True)
        if not len(keys):
            keys = ["X", "y", "signals", "prices_id", "prices_id_test", "prices_id_train", "labels"]
        k2 = [k for k in keys if k in ["X", "y"]]
        for k in k2:
            np.save(path / f"{k}.npy", getattr(self, k))
        k2 = [k for k in keys if k in ["signals", "prices_id", "prices_id_test", "prices_id_train"]]
        for k in k2:
            np.save(path / f"{k}.npy", getattr(self, k), allow_pickle=True)
        k2 = [k for k in keys if k in ["labels"]]
        for k in k2:
            with open(path / f"{k}.txt", "w+") as f:
                f.write(json.dumps(getattr(self, k)))

    def load_data(self, keys=[], split_data=True):
        path = Path("data", self.folder)
        if not len(keys):
            keys = ["X", "y", "signals", "prices_id", "prices_id_test", "prices_id_train", "labels"]
        k2 = [k for k in keys if k in ["X", "y"]]
        for k in k2:
            setattr(self, k, np.load(path / f"{k}.npy"))
        k2 = [k for k in keys if k in ["signals", "prices_id", "prices_id_test", "prices_id_train"]]
        for k in k2:
            setattr(self, k, np.load(path / f"{k}.npy", allow_pickle=True))
        k2 = [k for k in keys if k in ["labels"]]
        for k in k2:
            with open(path / f"{k}.txt", "r") as f:
                setattr(self, k, json.loads(f.read()))
        if split_data and "X" in keys and "y" in keys:
            self.test_train_split()

    def gc(self,
        prices:bool=True,
        X:bool=True,
        y:bool=True,
        prices_id:bool=True,
        signals:bool=True,
        X_train:bool=True,
        y_train:bool=True,
        prices_id_train:bool=True,
        X_test:bool=True,
        y_test:bool=True,
        prices_id_test:bool=True,
        ):
        """garbage collect to free memory
        
        Specify the datasets to keep by changing to false
        """
        if prices:
            self.prices = None
        if X:
            self.X = None
        if y:
            self.y = None
        if prices_id:
            self.prices_id = None
        if signals:
            self.signals = None
        if X_train:
            self.X_train = None
        if y_train:
            self.y_train = None
        if prices_id_train:
            self.prices_id_train = None
        if X_test:
            self.X_test = None
        if y_test:
            self.y_test = None
        if prices_id_test:
            self.prices_id_test = None
        gc.collect()