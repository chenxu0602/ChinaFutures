
import os, sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict


class Port:
    def __init__(self, name, sig, pri, w, slip):
        self.name   = name
        self.signal = sig
        self.price  = pri
        self.ret    = np.log(self.price).diff(1)
        self.weight = w
        self.position = self.signal.fillna(0).shift(1)
        self.tvr = self.position.diff().abs().mean()
        self.nPosition = self.position.count(axis=1)
        self.nLongPosition  = self.position[self.position > 0].count(axis=1)
        self.nShortPosition = self.position[self.position < 0].count(axis=1)

        self.rawPnL = self.ret.mul(self.position.shift(1))
        self.slip   = self.position.diff().abs() * slip / 1e4
        self.PnL    = self.rawPnL - self.slip

        self.wPosition = self.weight.mul(self.position)
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

        self.portLongPosition  = self.wPosition[self.wPosition > 0].sum(axis=1)
        self.portShortPosition = self.wPosition[self.wPosition < 0].sum(axis=1)

        self.annualizedPortRet  = self.portPnL.mean() * 252.
        self.annualizedPortSlip = self.portSlip.mean() * 252.
        self.annualizedPortStd  = self.portPnL.std() * 16.
        self.annualizedPortSharpe = self.annualizedPortRet / self.annualizedPortStd

    def __str__(self):
        s = "\n############################################## Strategy: {0} #####################################".format(self.name)
        s += "\nAnnualized Portfolio Return:         {0:.2f}%".format(self.annualizedPortRet * 100)
        s += "\nAnnualized Portfolio Slip:           {0:.2f}%".format(self.annualizedPortSlip * 100)
        s += "\nAnnualized Portfolio Std:            {0:.2f}%".format(self.annualizedPortStd * 100)
        s += "\nAnnualized Portfolio Sharpe ratio:   {0:.2f}".format(self.annualizedPortSharpe)
        s += "\nAverage number of long  positions:   {0}".format(int(self.nLongPosition.mean()))
        s += "\nAverage number of short positions:   {0}".format(int(self.nShortPosition.mean()))
        s += "\nAverage long  position:              {0:.2f}%".format(self.portLongPosition.mean() * 100)
        s += "\nAverage short position:              {0:.2f}%".format(self.portShortPosition.mean() * 100)
        s += "\nPortfolio Daily Turnover:            {0:.2f}%".format(self.portTVR * 100)

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

if __name__ == "__main__":
    pass


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