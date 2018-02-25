#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from matplotlib import pyplot as plt
import os, sys, datetime, argparse, pytz, glob, logging, re, csv, operator, math, logging.handlers
from dy8_signals import *

class Stats:
	def __init__(self, signal, data, start, end, slip, weight="Equal", logger=None):
		self.signal = signal[(signal.index >= start) & (signal.index < end)]
		self.weight = weight_equal(self.signal, data, logger)

		if weight == "ATR":
			self.weight = weight_atr(self.signal, data, start, end, logger)

		self.position = self.signal.fillna(0).mul(self.weight)
		self.symbols = self.position.columns
		
		""" Get the return data frame """
		return_dict = defaultdict(pd.DataFrame)
		for sym in self.symbols:
			if not sym in data.keys():
				logger.critical("ERROR: %s appears in the signal but not in the return data!", sym)
				continue
			ret = data[sym]["PnL"]
			ret = ret[(ret.index >= start) & (ret.index < end)]
			return_dict[sym] = ret
		self.ret = pd.DataFrame(return_dict)
		self.ret.fillna(0, inplace=True)

		""" Calculate PnL """
		self.rawPnL = self.ret.mul(self.position.shift(1), fill_value=0)

		""" Calculate Slippage """
		self.slippage = self.position.diff(1).abs() * slip * 1e-4
		self.PnL = self.rawPnL - self.slippage

		""" Calculate Annuazlied Return """
		self.annReturn = self.PnL.mean() * 252.
		
		""" Calculate Volatility """
		self.annVolatility = self.PnL.std() * np.sqrt(252.)

		""" Calculate Sharpe Ratio """
		self.sharpe = self.annReturn / self.annVolatility

		""" Calculate Portfolio Stats """
		self.portfolioPnL = self.PnL.sum(axis=1)
		self.portfolioAnnReturn = self.portfolioPnL.mean() * 252.
		self.portfolioAnnVolatility = self.portfolioPnL.std() * np.sqrt(252.)
		self.portfolioSharpe = self.portfolioAnnReturn / self.portfolioAnnVolatility

		logger.info("Products: %s", ','.join(self.symbols))
		logger.info("Portfolio Ann Return: %.2f%%", self.portfolioAnnReturn * 100)
		logger.info("Portfolio Ann Volatility: %.2f%%", self.portfolioAnnVolatility * 100)
		logger.info("Portfolio Sharpe Ratio: %.2f", self.portfolioSharpe)


if __name__ == "__main__":
	today = datetime.date.today()

	parser = argparse.ArgumentParser(prog="Generate DY8 Signals", description="", 
			formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--datadir", nargs="?", type=str, 
			default="/Users/chenxu/Work/ChinaFutures/data", dest="datadir", help="data dir")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="products to process")
	parser.add_argument("--debug", action="store_true", default=False, dest="debug", help="debug logging mode")

	args = parser.parse_args()

	FORMAT = "%(asctime)s %(levelname)s %(message)s"
	logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
	logger = logging.getLogger("DY8 Processing Logger")
	logger.setLevel(logging.INFO)

	if args.debug: logger.setLevel(logging.DEBUG)

	logger.info("Today is %s", today.strftime("%Y-%m-%d"))

	maindir  = os.path.join(args.datadir, "main")

	data = load_continuous_dailydata(maindir, args.products, logger)
	signal = daily_return_mom(data, 15, 1, 10, logger)

#	signal = daily_regression(data, args.products, 3, 10, 5, logger)

	start = datetime.datetime(2007, 1, 1)
	end = datetime.datetime(2018, 1, 1)
	stats = Stats(signal, data, start, end, 0, "ATR", logger)
