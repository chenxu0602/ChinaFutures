
import os, sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
from continuousFuturesContracts import loadDailyContinuous

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA, FactorAnalysis


liquid_universe = {
    "A", "AG", "AL", "AU", "BU", "C", "CF", "CS", "CU", "FG", "HC", "I",
    "IC", "IF", "IH", "J", "JD", "JM", "L", "M", "MA", "NI", "P", "PB",
    "PM", "PP", "RB", "RM", "RU", "SF", "SM", "SN", "SR", "T", "TF", "V",
    "Y", "ZC", "ZN" 
}

black_universe = {
    "I", "J", "JM", "RB", "ZC", "HC", "SF", "SM"
}

chemical_universe = {
    "FG", "BU", "PP", "RU", "TA", "V", "L", "MA"
}

metal_universe = {
    "AL", "CU", "PB", "ZN", "NI", "SN", "AG", "AU"
}

def TSRank(array):
    return pd.Series(array).rank(ascending=True, pct=True).iloc[-1]

class Port:
    def __init__(self, name, sig, ret, w, slip):
        self.name   = name
        self.signal = sig
        self.ret    = ret
        self.weight = w.fillna(method="bfill")
        self.position = self.signal.shift(1)
        self.position.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.position.fillna(0, inplace=True)
        self.tvr = self.position.diff().abs().mean()
        self.nPosition = self.position.count(axis=1)
        self.nLongPosition  = self.position[self.position > 0].count(axis=1)
        self.nShortPosition = self.position[self.position < 0].count(axis=1)

        self.rawPnL = self.ret.mul(self.position.shift(1))
        self.slip   = self.position.diff().abs() * slip / 1e4
        self.PnL    = self.rawPnL - self.slip
        self.maxdd  = (self.PnL.cumsum().cummax() - self.PnL.cumsum()).max()

        self.wPosition = self.weight.mul(self.position)
        self.wPosition.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.wPosition.fillna(0, inplace=True)

        self.wRawPnL   = self.ret.mul(self.wPosition)
        self.wSlip     = self.wPosition.diff().abs() * slip / 1e4
        self.wPnL      = self.wRawPnL - self.wSlip

        self.annualizedRet = self.PnL.mean() * 252.
        self.annualizedStd = self.PnL.std() * 16.
        self.annualizedSharpe = self.annualizedRet.div(self.annualizedStd)

        self.portPosition = self.wPosition.sum(axis=1)
        self.portRawPnL   = self.wRawPnL.sum(axis=1)
        self.portSlip     = self.wSlip.sum(axis=1)
        self.portPnL      = self.wPnL.sum(axis=1)
        self.portTVR      = self.wPosition.diff().abs().sum(axis=1).mean()
        self.portMaxDD    = (self.portPnL.cumsum().cummax() - self.portPnL.cumsum()).max()

        self.portLongPosition  = self.wPosition[self.wPosition > 0].sum(axis=1)
        self.portShortPosition = self.wPosition[self.wPosition < 0].sum(axis=1)

        self.annualizedPortRet  = self.portPnL.mean() * 252.
        self.annualizedPortSlip = self.portSlip.mean() * 252.
        self.annualizedPortStd  = self.portPnL.std() * 16.
        self.annualizedPortSharpe = self.annualizedPortRet / self.annualizedPortStd

    def plot(self):
        self.portPnL.cumsum().plot(grid=True)
        plt.show()

    def __str__(self):
        s = "\n############################################## Strategy: {0} #####################################".format(self.name)
        s += "\nAnnualized Portfolio Return:         {0:.2f}%".format(self.annualizedPortRet * 100)
        s += "\nAnnualized Portfolio Slip:           {0:.2f}%".format(self.annualizedPortSlip * 100)
        s += "\nAnnualized Portfolio Std:            {0:.2f}%".format(self.annualizedPortStd * 100)
        s += "\nAnnualized Portfolio Sharpe ratio:   {0:.2f}".format(self.annualizedPortSharpe)
        s += "\nAverage number of long  positions:   {0:.2f}".format(self.nLongPosition.mean())
        s += "\nAverage number of short positions:   {0:.2f}".format(self.nShortPosition.mean())
        s += "\nAverage long  position:              {0:.2f}%".format(self.portLongPosition.mean() * 100)
        s += "\nAverage short position:              {0:.2f}%".format(self.portShortPosition.mean() * 100)
        s += "\nPortfolio Daily Turnover:            {0:.2f}%".format(self.portTVR * 100)
        s += "\nPortfolio Max Drawdown:              {0:.2f}%".format(self.portMaxDD * 100)

        return s

class IntraPort(Port):
    def __init__(self, name, sig, pri, w, slip):
        super().__init__(name, sig, pri, w, slip)

        self.dailyRawPnL = self.rawPnL.resample("1D", label="right", closed="right").sum()
        self.dailySlip   = self.slip.resample("1D", label="right", closed="right").sum()
        self.dailyPnL    = self.PnL.resample("1D", label="right", closed="right").sum()
        self.dailyTVR    = self.position.diff().abs().resample("1D", label="right", closed="right").sum()

        self.dailyWRawPnL = self.wRawPnL.resample("1D", label="right", closed="right").sum()
        self.dailyWSlip   = self.wSlip.resample("1D", label="right", closed="right").sum()
        self.dailyWPnL    = self.wPnL.resample("1D", label="right", closed="right").sum()

        self.annualizedRet = self.dailyPnL.mean() * 252.
        self.annualizedStd = self.dailyPnL.std() * 16.
        self.annualizedSharpe = self.annualizedRet.div(self.annualizedStd)

        self.portRawPnL = self.portRawPnL.resample("1D", label="right", closed="right").sum()
        self.portSlip   = self.portSlip.resample("1D", label="right", closed="right").sum()
        self.portPnL    = self.portPnL.resample("1D", label="right", closed="right").sum()
        self.portTVR    = self.wPosition.diff().abs().resample("1D", label="right", closed="right").sum().sum(axis=1).mean()

        self.annualizedPortRet  = self.portPnL.mean() * 252.
        self.annualizedPortSlip = self.portSlip.mean() * 252.
        self.annualizedPortStd  = self.portPnL.std() * 16.
        self.annualizedPortSharpe = self.annualizedPortRet / self.annualizedPortStd

def my_pca(N, data, products):
    pnls = defaultdict(pd.Series)
    for sym in sorted(products):
        if not sym in data.keys():
            print("{0} doesn't exist in data!".format(sym))
            sys.exit(1)
        pnls[sym] = data[sym].PnL
    pnls = pd.DataFrame(pnls)
    pnls = pnls.iloc[-N:]
    pnls.fillna(0, inplace=True)

    symbols = sorted(pnls.columns)

    X = pnls.loc[:, symbols].values
    X_standardized = StandardScaler().fit_transform(X)

    pca = PCA(n_components=8)
    res = pca.fit(X_standardized)
    print(res.explained_variance_ratio_)
    print(pd.DataFrame(res.components_, columns=symbols))

    X = pca.transform(X)
    df = pd.DataFrame(index=pnls.index, columns=["F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"])
    for i in range(0, len(X)):
        idx = pnls.index[i]
        df.loc[idx, "F1"] = X[i][0]
        df.loc[idx, "F2"] = X[i][1]
        df.loc[idx, "F3"] = X[i][2]
        df.loc[idx, "F4"] = X[i][3]
        df.loc[idx, "F5"] = X[i][4]
        df.loc[idx, "F6"] = X[i][5]
        df.loc[idx, "F7"] = X[i][6]
        df.loc[idx, "F8"] = X[i][7]

    return df

    

def daily_momentum(N, data, products, start="2017-01-01", delay=0, weight="Equal"):

    pnls = defaultdict(pd.Series)
    vols = defaultdict(pd.Series)
    ois  = defaultdict(pd.Series)

    for sym in sorted(products):
        if not sym in data.keys():
            print("{0} doesn't exist in data!".format(sym))
            sys.exit(1)
        
        df = data[sym]
        df = df[df.index >= start]

        pnls[sym] = df.PnL
        vols[sym] = df.Volume
        ois[sym]  = df.OI

    pnls = pd.DataFrame(pnls)
    vols = pd.DataFrame(vols)
    ois = pd.DataFrame(ois)

    cumPnLs = pnls.rolling(window=N, min_periods=1).sum()

    sVol = vols.rolling(window=5, min_periods=1).mean()
    lVol = vols.rolling(window=50, min_periods=1).mean()

    sOI  = ois.rolling(window=5, min_periods=1).mean()
    lOI  = ois.rolling(window=50, min_periods=1).mean()

    volratio = sVol.div(lVol)
    oiratio  = sOI.div(lOI)

    volRank = volratio.rolling(window=250, min_periods=1).apply(TSRank)
    oiRank  = oiratio.rolling(window=250, min_periods=1).apply(TSRank)

    signals = pd.DataFrame(index=cumPnLs.index, columns=cumPnLs.columns)

    signals[(cumPnLs > 0) & (oiRank < 0.7)] = 1.0
    signals[(cumPnLs < 0) & (volRank > 0.3)] = -1.0

    """
    signals[(cumPnLs > 0)] = 1.0
    signals[(cumPnLs < 0)] = -1.0
    """

    signals.fillna(0, inplace=True)

    signals = signals.rolling(window=5, min_periods=1).mean()

    weights = pd.DataFrame(index=signals.index, columns=signals.columns)
    weights.fillna(1.0/len(products), inplace=True)

    if weight == "ATR":
        atr = defaultdict(pd.Series)
        for sym in sorted(products):
            atr[sym] = data[sym].ATR

        atr = pd.DataFrame(atr)
        atr[~(atr > 0.01)] = 0.01
        weights = 0.01 / atr / len(products)

    if weight == "STD":
        std = defaultdict(pd.Series)
        for sym in sorted(products):
            std[sym] = data[sym].PnL.rolling(window=100, min_periods=1).std()

        std = pd.DataFrame(std)
        std[~(std > 0.005)] = 0.005
        weights = 0.01 / std / len(products)

    weights = weights[weights.index >= start]

    return signals, pnls, weights

    


if __name__ == "__main__":

    pd.set_option('display.expand_frame_repr', False)


    products = ["A", "M", "Y"]
    products = black_universe
#    products = chemical_universe
    continuous = loadDailyContinuous("/Users/chenxu/Work/ChinaFutures", products)
    signals, returns, weights = daily_momentum(15, continuous, products, "2007-01-01", 0, "ATR")

    x = my_pca(1500, continuous, products)

    st = Port("Daily-Momentum", signals, returns, weights, 10)
    print(st)
    st.plot()


    """
    fig, ax = plt.subplots()
    pd.DataFrame({"Cum PnL (BTC-LTC)": intraStats2.portPnL.cumsum()}).plot(ax=ax, grid=True)
    plt.legend(loc="best")

    import matplotlib.dates as mdates
    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    """

    """
    pd.DataFrame({"Cum PnL (3-Day Reversion)": stats.portPnL.cumsum()}).plot(grid=True)
    plt.legend(loc="best")
    """
