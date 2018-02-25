#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from matplotlib import pyplot as plt
import os, sys, datetime, argparse, pytz, glob, logging, re, csv, operator, math, logging.handlers

def load_continuous_dailydata(datadir, products, logger=None):
	if not os.path.exists(datadir):
		logger.infor("Directory %s doesn't exist!", datadir)
		return None

	logger.info("Loading data from directory %s ...", datadir)
	data = defaultdict(pd.DataFrame)

	files = glob.iglob("{0}/*.csv".format(datadir))
	for fl in files:
		base = os.path.basename(fl)
		sym, ext = base.split('.')
		if len(products) > 0 and not sym in products: continue
		logger.debug("Loading %s data from file %s", sym, fl)
		df = pd.read_csv(fl, delimiter=',', parse_dates=[0], index_col=[0])
		data[sym] = df

	return data

def daily_return_mom(data, N, delay=0, smooth=10, logger=None):
	if data == None: return None

	signal_dict = defaultdict(pd.Series)
	for sym in data.keys():
		df = data[sym]
		raw_sig = df["PnL"].rolling(window=N, min_periods=1).sum()
		sig = pd.Series(index=raw_sig.index)

		sig[raw_sig > 0] = 1.0
		sig[raw_sig < 0] = -1.0

		signal_dict[sym] = sig.shift(delay)

	signal = pd.DataFrame(signal_dict)
	signal.fillna(method="ffill", inplace=True)

	if smooth > 0:
		signal = signal.rolling(window=10, min_periods=1).mean()

	return signal

def daily_regression(data, products, L, N, delay=0, logger=None):
	if data == None: return None
	signal_dict = defaultdict(pd.Series)
	for sym in data.keys():
		if len(products) > 0 and not sym in products: continue
		df = data[sym]
		ret = df["PnL"].copy(deep=True)
		ret  = ret.resample("5D", label="right", closed="right").sum()
		sig = pd.Series(index=ret.index)
		retN = ret.shift(1).rolling(window=L, min_periods=1).sum()

		for i in range(L+N, len(ret)):
			end = ret.index[i]
			start = ret.index[i-N]

			df1 = ret[(ret.index > start) & (ret.index <= end)]
			df2 = retN[(retN.index > start) & (retN.index <= end)]

			cor = df1.corr(df2)
			logger.debug("%s %s   corr: %.2f", end, sym, cor)

			if cor > -0.0:
				indicator = df2[end]
				if indicator > 0:
					sig.loc[end] = 1.0
				else:
					sig.loc[end] = -1.0
			else:
				sig.loc[end] = 0.0
				
		sig2 = pd.DataFrame(index=df.index, columns=["Daily"])
		sig3 = pd.merge(sig2, sig.to_frame(name="Weekly"), left_index=True, right_index=True, how="outer")

		signal_dict[sym] = sig3["Weekly"].fillna(method="ffill").shift(delay)

	signal = pd.DataFrame(signal_dict)
	signal.fillna(method="ffill", inplace=True)
	return signal


def combined_daily_mom(signals, logger=None):
	if not len(signals) > 0:
		logger.critical("Combining signals with empty signal vector ...")
		return None

	signal = pd.DataFrame()
	for sig in signals:
		signal = signal.add(sig, fill_value=0)

	signal = signal / len(signals)
	signal.fillna(0, inplace=True)
	return signal

def ortus(data, delay, smooth, logger=None):
	signals = []
	sig1 = daily_return_mom(data, 10, delay, 0, logger)
	sig2 = daily_return_mom(data, 22, delay, 0, logger)
	sig3 = daily_return_mom(data, 66, delay, 0, logger)
	sig4 = daily_return_mom(data, 132, delay, 0, logger)

	signals.append(sig1)
	signals.append(sig2)
	signals.append(sig3)
	signals.append(sig4)

	signal = combined_daily_mom(signals, logger)

	if smooth > 0:
		signal = signal.rolling(window=10, min_periods=1).mean()

	return signal

def weight_equal(signal, data, logger=None):
	logger.info("Calculating weighting equally ...")
	N = len(signal.columns)
	weight = pd.DataFrame(index=signal.index, columns=signal.columns)
	weight.fillna(1.0 / N, inplace=True)

	return weight

def weight_atr(signal, data, start, end, logger=None):
	logger.info("Calculating weighting based on ATR ...")

	weight_dict = defaultdict(pd.DataFrame)
	for sym in sorted(data.keys()):
		atr = data[sym]["ATR"]
		settle = data[sym]["Settle"]

		df = atr.div(settle) * np.sqrt(252.)
		weight_dict[sym] = df[(df.index >= start) & (df.index < end)]

	weight = 0.05 / pd.DataFrame(weight_dict)
	""" Remove inf and -inf points """
	weight.replace([np.inf, -np.inf], np.nan, inplace=True)
	weight.fillna(method="ffill", inplace=True)

	return weight 

def weight_fisher(signal, data, alpha, logger=None):
	logger.info("Calculating weighting based on fisher score ...")
	w = weight_atr(signal, data, logger)
	weight = np.exp(-(1.0/w)* alpha)

	return weight 

def weight_std(signal, data, N=100, logger=None):
	logger.info("Calculating weighting based on std ...")

	weight_dict = defaultdict(pd.DataFrame)
	for sym in sorted(data.keys()):
		ret = data[sym]["PnL"]
		df = ret.rolling(window=N, min_periods=1).std() * np.sqrt(252.)
		weight_dict[sym] = df

	weight = 1.0 / pd.DataFrame(weight_dict)
	""" Remove inf and -inf points """
	weight.replace([np.inf, -np.inf], np.nan, inplace=True)
	weight.fillna(method="ffill", inplace=True)

	return weight 



if __name__ == "__main__":
	today = datetime.date.today()

	parser = argparse.ArgumentParser(prog="Generate DY8 Signals", description="", 
			formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--datadir", nargs="?", type=str, 
			default="/Users/chenxu/Work/ChinaFutures/data", dest="datadir", help="data dir")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="products to process")
	parser.add_argument("--debug", action="store_true", default=False, dest="debug", help="debug logging mode")
#	parser.add_argument("--main", action="store_true", default=False, dest="main", help="get main contracts")
#	parser.add_argument("--front", action="store_true", default=False, dest="front", help="get front contracts")

	args = parser.parse_args()

	FORMAT = "%(asctime)s %(levelname)s %(message)s"
	logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
	logger = logging.getLogger("DY8 Processing Logger")
	logger.setLevel(logging.INFO)

	if args.debug: logger.setLevel(logging.DEBUG)

	logger.info("Today is %s", today.strftime("%Y-%m-%d"))

	maindir  = os.path.join(args.datadir, "main")
	frontdir = os.path.join(args.datadir, "front")

	data = load_continuous_dailydata(maindir, args.products, logger)

	signal = daily_regression(data, args.products, 1, 30, 1, logger)

