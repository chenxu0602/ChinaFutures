#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from matplotlib import pyplot as plt
import os, sys, datetime, argparse, pytz, glob, logging, re, csv, operator, math, logging.handlers

months = {
	"01" : "F",
	"02" : "G",
	"03" : "H",
	"04" : "J",
	"05" : "K",
	"06" : "M",
	"07" : "N",
	"08" : "Q",
	"09" : "U",
	"10" : "V",
	"11" : "X",
	"12" : "Z",
}

def get_contracts(dy8_file, pnl, threshold=0, symbols=[], logger=None):
	logger.info("Reading DY8 file %s ...", dy8_file)
	dy8 = pd.read_csv(dy8_file, delimiter=',', parse_dates=[1], index_col=[0, 1], header=None, names=fields)

	wrongs = dict()

	frames = defaultdict(lambda: defaultdict(pd.DataFrame))

	for contract, df in dy8.groupby(level="Contract"):
		df2 = df.xs(contract, level="Contract")

		match = re.search("^([A-Z]{1,2})(\d{3,4})_(\d{4})(\d{2})$", contract)

		if not match:
			logger.critical("Wrong contract symbol: %s", contract)
			wrongs[contract] = True
			continue

		root = match.group(1)
		year = match.group(3)
		expM = match.group(4)
		expiration = "{0}{1}".format(year, months[expM])

		if len(symbols) > 0 and not root in symbols: 
			"""
			logger.debug("Skipping symbols %s", root)
			"""
			continue

		logger.debug("%s    %s     %s         %s", root, year, expM, expiration)

		df3 = df2[df2["Volume"] > 0]

		i = 0
		j = len(df3.index) - 1
		while i < len(df3.index):
			ind = df3.index[i]
			vol = df3.loc[ind, "Volume"]
			if vol >= threshold: break
			i += 1

		while j >= 0:
			ind = df3.index[j]
			vol = df3.loc[ind, "Volume"]
			if vol >= threshold: break
			j -= 1

		if i > j:
			logger.info("%s %s doesn't have enough volumes!", root, expiration)
		else:
			start = df3.index[i]
			end   = df3.index[j]

			logger.debug("%s %s starts from %s and ends at %s", root, expiration, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

			df4 = df3[(df3.index >= start) & (df3.index <= end)]

			df4["PnL"] = df4[pnl].pct_change().round(6)
			if args.logr:
				df4["PnL"] = np.log(df4[pnl]).diff().round(6)

			cl = df4["Close"]
			hi = df4["High"]
			lo = df4["Low"]

			df5 = pd.DataFrame({"high-low":hi-lo, "high-close":hi-cl.shift(1), "low-close":lo-cl.shift(1)})
			tr = df5.abs().max(axis=1)
			atr = tr.ewm(span=14, min_periods=1, adjust=False, ignore_na=True).mean()
			df4["TR"]  = tr.round(2)
			df4["ATR"] = atr.round(2)
			df4["Contract"] = expiration

			frames[root][expiration] = df4

	if len(wrongs) > 0:
		logger.info("Unregistered symbols: %s", ",".join(wrongs))

	return frames

def get_front_continuous(contracts, logger=None):
	logger.info("Generating front continuous contracts ...")
	front_contracts = defaultdict(pd.DataFrame)

	for sym in contracts.keys():
		frames = []
		cons = sorted(contracts[sym].keys())

		start = None
		for i in range(0, len(cons)):
			con = cons[i]
			df = contracts[sym][con]
			if not len(df) > 0: continue
			if not start == None: 
				df2 = df[df.index > start]	
				if not len(df2) > 10: continue
				start = df2.index[-1]
			else:
				df2 = df
				start = df2.index[-1]
			frames.append(df2)

		front_contracts[sym] = pd.concat(frames)

	return front_contracts

def get_main_continuous(contracts, flag="Main", logger=None):
	logger.info("Generating main continuous contracts ...")
	main_contracts = defaultdict(pd.DataFrame)
	
	for sym in contracts.keys():
		cons = contracts[sym]
		frames = []
		for exp in cons:
			df = cons[exp]
			df2 = df[df[flag] == 1]
			frames.append(df2)
		main_contracts[sym] = pd.concat(frames)

	return main_contracts

def dump_continuous_contracts(mains, outdir, logger=None):	
	if not os.path.exists(outdir):
		logger.info("%s doesn't exist, creating it ...", outdir)
		os.system("mkdir -p {0}".format(outdir))

	logger.info("Dumping continuous contracts to dir %s ...", outdir)
	for sym in mains.keys():
		out = os.path.join(outdir, "{0}.csv".format(sym))
		df = mains[sym]
		df.to_csv(out, columns=["Contract", "Open", "High", "Low", "Close", "Settle", "TR", "ATR", "Volume", "Value", "OPI", "PnL"], \
				na_rep="NA", sep=",", index=True, header=True, mode="w")


if __name__ == "__main__":
	today = datetime.date.today()

	parser = argparse.ArgumentParser(prog="Process DY8 Data", description="", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--datadir", nargs="?", type=str, default="/Users/chenxu/Work/ChinaFutures", dest="datadir", help="data dir")
	parser.add_argument("--outdir", nargs="?", type=str, default="/Users/chenxu/Work/ChinaFutures/data", dest="outdir", help="out dir")
	parser.add_argument("--file", nargs="?", type=str, default="DY8_20171018.csv", dest="file", help="DY8 file name")
	parser.add_argument("--pnl", nargs="?", type=str, default="Settle", dest="pnl", help="price field for PnL calculation")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="products to process")
	parser.add_argument("--debug", action="store_true", default=False, dest="debug", help="debug logging mode")
	parser.add_argument("--logr", action="store_true", default=False, dest="logr", help="use log return")
	parser.add_argument("--main", action="store_true", default=False, dest="main", help="get main contracts")
	parser.add_argument("--front", action="store_true", default=False, dest="front", help="get front contracts")
	parser.add_argument("--volumeThreshold", type=int, default=0, dest="volumeThreshold", help="remove data with volumes <= this thr")

	args = parser.parse_args()

	FORMAT = "%(asctime)s %(levelname)s %(message)s"
	logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
	logger = logging.getLogger("DY8 Processing Logger")
	logger.setLevel(logging.INFO)

	if args.debug: logger.setLevel(logging.DEBUG)

	logger.info("Today is %s", today.strftime("%Y-%m-%d"))

	fields = ["Contract", "Date", "Pre_Settle", "Open", "High", "Low", "Settle", "Close", "Volume", "Value", "OPI", \
				"Change1", "Change2", "Main", "Sec_Main"] 

	dy8_file = os.path.join(args.datadir, args.file)
	contracts = get_contracts(dy8_file, args.pnl, args.volumeThreshold, args.products, logger)

	if args.main:
		main_dir = os.path.join(args.outdir, "main")
		mains = get_main_continuous(contracts, "Main", logger)
		dump_continuous_contracts(mains, main_dir, logger)

	if args.front:
		front_dir = os.path.join(args.outdir, "front")
		fronts = get_front_continuous(contracts, logger)
		dump_continuous_contracts(fronts, front_dir, logger)
