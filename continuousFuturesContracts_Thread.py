#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import sys, os, re
from collections import defaultdict 
import argparse, datetime, quandl 
import matplotlib.pyplot as plt
import datetime
import concurrent.futures

mon2char = {
	'F' : 1,
	'G' : 2,
	'H' : 3,
	'J' : 4,
	'K' : 5,
	'M' : 6,
	'N' : 7,
	'Q' : 8,
	'U' : 9,
	'V' : 10,
	'X' : 11,
	'Z' : 12
}

def month_diff(mon1, mon2):
	if not len(mon1) == 5:
		print("Wrong month {0}".format(mon1))
		return

	if not len(mon2) == 5:
		print("Wrong month {0}".format(mon2))
		return

	x1 = int(mon1[:4]) * 12 + mon2char[mon1[4]]
	x2 = int(mon2[:4]) * 12 + mon2char[mon2[4]]

	return x1 - x2

def dumpOne(outdir, sym, data):
	filename = os.path.join(outdir, f"{sym}.csv")
	try:
		data.to_csv(filename)
	except Exception as exc:
		print(f"Dumping to {filename} failed!")
	else:
		return True	

def dump(outdir, data):
	with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
		futures = {executor.submit(dumpOne, outdir, sym, data[sym]) : sym for sym in data}

		for future in concurrent.futures.as_completed(futures):
			sym = futures[future]
			try:
				success = future.result()
				if success:
					print(f"Dumped continuous {sym} to {outdir}.")
			except Exception as exc:
				print(f"{sym} generated an exception: {exc}")
			else:
				print(f"Loaded {sym} data.")

def loadRawData(rawdir, dirs, products):
	symbols = []
	for sym in dirs:
		if not products or sym in products:
			symbols.append(sym)

	results = {}

	with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
		futures = {executor.submit(loadRawDataSymbol, rawdir, sym) : sym for sym in symbols}

		for future in concurrent.futures.as_completed(futures):
			sym = futures[future]
			try:
				data = future.result()
				results[sym] = data
			except Exception as exc:
				print(f"{sym} generated an exception: {exc}")
			else:
				print(f"Loaded {sym} data from {rawdir}.")

	return results

def loadRawDataSymbol(rawdir, sym):
	productdir = os.path.join(rawdir, sym)
	if not os.path.exists(productdir):
		print(f"ERROR: directory {productdir} doesn't exist!")
		sys.exit(1)

	results = defaultdict(pd.DataFrame)

	for con in os.listdir(productdir):
		contract, _ = re.split('\.', con)
		filename = os.path.join(productdir, con)
		df = pd.read_csv(filename, parse_dates=[0], index_col=[0])
		df.fillna(method="bfill", inplace=True)
		df["Contract"] = contract
		df["Close"].fillna(df["Settle"], inplace=True)
		df["PnL"] = np.log(df["Settle"]).diff()
		df["Liquidity"] = 0.7 * df["Volume"].rolling(window=2, min_periods=1).mean() \
			+ 0.3 * df["OI"].rolling(window=2, min_periods=1).mean()

		high_minus_low   = df["High"] - df["Low"]
		high_minus_close = df["High"] - df["Close"].shift(1)
		low_minus_close  = df["Low"] - df["Close"].shift(1)

		df_minus = pd.DataFrame({"HL":high_minus_low, "HC":high_minus_close.abs(), \
				"LC":low_minus_close.abs()})

		tr  = df_minus.max(axis=1)
		atr = tr.ewm(span=14, min_periods=1, adjust=False).mean()
		df["TR"] = tr
		df["ATR"] = atr.div(df["Settle"].rolling(window=5, min_periods=1).mean())
		df["ATR"].fillna(method="bfill")

		results[contract] = df

	return results

def chain(cons, fld="Liquidity", frontThreshold=100):
	results = {}

	with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
		futures = {executor.submit(singleChain, sym, contracts[sym], fld) : sym for sym in contracts}

		for future in concurrent.futures.as_completed(futures):
			sym = futures[future]
			try:
				data = future.result()
				results[sym] = data
			except Exception as exc:
				print(f"{sym} generated an exception: {exc}")
			else:
				print(f"Generated {sym} continous.")

	return results

def carryChain(sym, cons, fld, frontThreshold):
	print("Chaining {0} for carry ...".format(sym))
	results = defaultdict(lambda: pd.Series)
	for contract in sorted(cons.keys()):
		results[contract] = cons[contract][fld]

	df = pd.DataFrame(results)
	rank = df.rank(axis=1, pct=False, ascending=False)
	rank.dropna(axis=0, how="all", inplace=True)

	res = pd.DataFrame(index=df.index)

	for i in range(0, len(rank)):
		date = rank.index[i]

		""" Find the front contract """
		for front in df.columns:
			val = df.loc[date, front]
			if val > frontThreshold:
				break
		else:
			print(f"Can't find a front contract with liquidity greater than {frontThreshold} for {sym} on date {date}!")
			break

		mains = rank.columns[(rank == 1).iloc[i]]
		sym_main = mains[0]

		for r_main in range(1, len(rank.columns)):
			mains = rank.columns[(rank == r_main).iloc[i]]

			if not len(mains) > 0: 
				break

			sym_main = mains[0]

			"""
			if sym_main >= front:
				break
			"""

			if month_diff(sym_main, front) >= 0:
				break

		sec_mains = rank.columns[(rank == 1).iloc[i]]
		sym_sec_main = sec_mains[0]

		for r_sec in range(r_main, len(rank.columns)):
			sec_mains = rank.columns[(rank == r_sec).iloc[i]]

			if not len(sec_mains) > 0: 
				break

			sym_sec_main = sec_mains[0]
			if month_diff(sym_sec_main, sym_main) >= 3:
				break


		res.loc[date, "FrontContract"] = front
		res.loc[date, "MainContract"]  = sym_main
		res.loc[date, "SecContract"]   = sym_sec_main

		res.loc[date, "FrontSettle"] = cons[front].loc[date, "Settle"]
		res.loc[date, "MainSettle"]  = cons[sym_main].loc[date, "Settle"]
		res.loc[date, "SecSettle"]   = cons[sym_sec_main].loc[date, "Settle"]

		res.loc[date, "FrontPnL"]  = cons[front].loc[date, "PnL"]
		res.loc[date, "MainPnL"]   = cons[sym_main].loc[date, "PnL"]
		res.loc[date, "SecPnL"]    = cons[sym_sec_main].loc[date, "PnL"]
		res.loc[date, "SpreadPnL"] = res.loc[date, "MainPnL"] - res.loc[date, "SecPnL"]

		res.loc[date, "Volume"] = cons[front].loc[date, "Volume"] \
										+ cons[sym_main].loc[date, "Volume"] \
										+ cons[sym_sec_main].loc[date, "Volume"] 

		res.loc[date, "OI"] 		= cons[front].loc[date, "OI"] \
										+ cons[sym_main].loc[date, "OI"] \
										+ cons[sym_sec_main].loc[date, "OI"] 

	return res

def singleChain(sym, cons, fld):
	print("Chaining {0} ...".format(sym))
	results = defaultdict(lambda: pd.Series)
	oi = defaultdict(lambda: pd.Series)
	volume = defaultdict(lambda: pd.Series)
	for contract in sorted(cons.keys()):
		results[contract] = cons[contract][fld]
		oi[contract] = cons[contract]["OI"]
		volume[contract] = cons[contract]["Volume"]

	df = pd.DataFrame(results)
	df_oi = pd.DataFrame(oi)
	df_volume = pd.DataFrame(volume)
	rank = df.rank(axis=1, pct=False, ascending=False)
	rank.dropna(axis=0, how="all", inplace=True)

	res = pd.DataFrame(index=df.index)

	if not rank.empty:
		for i in range(0, len(rank)):
			date = rank.index[i]
			candidates = rank.columns[(rank == 1).iloc[i]]
			if len(candidates) > 0:
					symbol = candidates[0]
					data   = cons[symbol]
					res.loc[date, "Contract"]  = data.loc[date, "Contract"]
					res.loc[date, "Open"]      = data.loc[date, "Open"].round(1)
					res.loc[date, "High"]      = data.loc[date, "High"].round(1)
					res.loc[date, "Low"]       = data.loc[date, "Low"].round(1)
					res.loc[date, "Close"]     = data.loc[date, "Close"].round(1)
					res.loc[date, "Settle"]    = data.loc[date, "Settle"].round(1)
					res.loc[date, "Volume"]    = data.loc[date, "Volume"].round(0)
					res.loc[date, "OI"]        = data.loc[date, "OI"].round(0)
					res.loc[date, "PnL"]       = data.loc[date, "PnL"].round(6)
					res.loc[date, "TR"]        = data.loc[date, "TR"].round(1)
					res.loc[date, "ATR"]       = data.loc[date, "ATR"].round(4)
					res.loc[date, "TotalVolume"]   = df_volume.iloc[i].sum()
					res.loc[date, "TotalOI"]   = df_oi.iloc[i].sum()
			else:
					print("{0} doesn't have any contracts on date {1}!".format(sym, date))
					continue
	else:
		print("{0} doesn't have data!".format(sym))

	return res

def loadDailyContinuous(rootdir, products=[]):
	datadir = os.path.join(rootdir, "continuous")
	if not os.path.exists(datadir):
		print("Data directory {0} doesn't exist!".format(datadir))
		sys.exit(1)

	csvfiles = os.listdir(datadir)
	results = defaultdict(pd.DataFrame)

	for f in sorted(csvfiles):
		sym, ext = re.split('\.', f)
		if products and not sym in products: 
			continue

		datafile = os.path.join(datadir, f)

		print("Loading data {0} ...".format(datafile))
		df = pd.read_csv(datafile, parse_dates=[0], index_col=[0])
		results[sym] = df

	return results

if __name__ == "__main__":

	parser = argparse.ArgumentParser(prog="Generate Continuous Futures Contracts", description="",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--rootdir", nargs="?", type=str, default="/Users/chenxu/Work/ChinaFutures", 
		dest="rootdir", help="root directory")
	parser.add_argument("--rawdatadir", nargs="?", type=str, default="rawdata.2018-04-26", dest="rawdatadir", help="raw data dir")
	parser.add_argument("--outdir", nargs="?", type=str, default="continuous", dest="outdir", help="output dir")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="product list")
	parser.add_argument("--fields", nargs="*", type=str, default=["Open", "High", "Low", "Close", "Settle", "Volume", "OI"], \
			dest="fields", help="fields")

	args = parser.parse_args()

	rootdir = os.path.expandvars(args.rootdir)

	rawdatadir = os.path.join(rootdir, args.rawdatadir)
	if not os.path.exists(rawdatadir):
		print("Raw data {0} doesn't exist!".format(rawdatadir))
		sys.exit(1)

	subdirs = os.listdir(rawdatadir)
	contracts = loadRawData(rawdatadir, subdirs, args.products)

	continuous = chain(contracts)

	outdir = os.path.join(rootdir, args.outdir)
	if not os.path.exists(outdir):
		print(f"Creating output directory {outdir}")
		os.mkdir(outdir)

	dump(outdir, continuous)

	print("Continuous done.")
	sys.exit(0)
	
