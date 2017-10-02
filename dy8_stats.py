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
	def __init__(self, signal, data, slip, logger):
		self.position = signal.fillna(0)
		self.symbols = self.position.columns
		
		""" Get the return data frame """
		return_dict = defaultdict(pd.DataFrame)
		for sym in self.symbols:
			if not sym in data.keys():
				logger.critical("ERROR: %s appears in the signal but not in the return data!", sym)
				continue
			return_dict[sym] = data[sym]["PnL"]
		self.ret = pd.DataFrame(return_dict)
		self.ret.fillna(0, inplace=True)

		""" Calculate PnL """
		self.rawPnL = self.ret.mul(self.position, fill_value=0)

		""" Calculate Slippage """
		self.slippage = self.position.diff(1).abs() * slip * 1e-4
		self.PnL = self.rawPnL - self.slippage

		""" Calculate Annuazlied Return """
		self.annualizedReturn = self.PnL.mean() * 252.
		
		""" Calculate Volatility """
		self.annualizedVolatility = self.PnL.std() * np.sqrt(252.)

		""" Calculate Sharpe Ratio """
		self.sharpe = self.annualizedReturn / self.annualizedVolatility

		""" Calculate Portfolio Stats """
		self.portfolioPnL = self.PnL.sum(axis=1)
		self.portfolioAnnualizedReturn = self.portfolioPnL.mean() * 252.
		self.portfolioAnnualizedVolatility = self.portfolioPnL.std() * np.sqrt(252.)
		self.portfolioSharpe = self.portfolioAnnualizedReturn / self.portfolioAnnualizedVolatility

		logger.info("Products: %s", ','.join(self.symbols))
		logger.info("Portfolio Annualized Return: %.2f%%", self.portfolioAnnualizedReturn * 100)
		logger.info("Portfolio Annualized Volatility: %.2f%%", self.portfolioAnnualizedVolatility * 100)
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
	frontdir = os.path.join(args.datadir, "front")

	mains  = load_continuous_dailydata(maindir, args.products, logger)
	fronts = load_continuous_dailydata(frontdir, args.products, logger)

	signal_ortus_main  = ortus(mains, 1, 10, logger)
	signal_ortus_front = ortus(fronts, 1, 10, logger)

	weight_main  = weight_atr(signal_ortus_main, mains, logger)
	weight_front = weight_atr(signal_ortus_front, fronts, logger)

	stats_main  = Stats(signal_ortus_main * weight_main, mains, 5, logger)
	stats_front = Stats(signal_ortus_front * weight_front, fronts, 10, logger)

