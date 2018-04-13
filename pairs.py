#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import sys, os, re
from collections import defaultdict 
import argparse, datetime, quandl 
import matplotlib.pyplot as plt
import datetime

from continuousFuturesContracts import mon2char, month_diff, loadRawData, loadDailyContinuous

def add_next(tickers):
	additional = []
	last = tickers[-1]
	for t in tickers:
		t2 = "{0}{1}".format(int(t[:4])+1, t[-1])
		if t2 > last:
			additional.append(t2)

	if len(additional) > 0:
		tickers.append(sorted(additional)[0])

def calc_start_end(frame, threshold):
	left, right = frame.LVolume, frame.RVolume
	left[left < threshold]   = np.nan
	left[left < left.max() / 4.0] = np.nan
	right[right < threshold] = np.nan
	left.dropna(inplace=True)
	right.dropna(inplace=True)

	if left.empty or right.empty:
		return None, None
	else:
		return max(left.index[0], right.index[0]), min(left.index[-1], right.index[-1])
		

def calc_pair(sym, left, right, contracts, multi, field="Settle", volumeThreshold=3000):
	name = f"{left}-{right}"; df = pd.DataFrame()
	print(f"Constructing pair {sym} {name} ...")

	if not left in contracts:
		print(f"{sym} {left} doesn't exist in raw contracts!")
	elif not right in contracts:
		print(f"{sym} {right} doesn't exist in raw contracts!")
	else:
		df_left  = contracts[left]
		df_right = contracts[right]

		df = pd.DataFrame({
			f"L{field}" : df_left[field],
			f"R{field}" : df_right[field],
			f"LVolume"  : df_left.Volume,
			f"RVolume"  : df_right.Volume,
			f"LOI"      : df_left.OI,
			f"ROI"      : df_right.OI,
			f"Spread"   : df_left[field] - df_right[field],
			f"Value"    : (df_left[field] - df_right[field]) * multi
		})

		start, end = calc_start_end(df, volumeThreshold)

		if start and end:
			df = df[(df.index >= start) & (df.index <= end)]

	return name, df[[f"L{field}", f"R{field}", "LVolume", "RVolume", "LOI", "ROI", "Spread", "Value"]]


if __name__ == "__main__":

	parser = argparse.ArgumentParser(prog="Generate Continuous Futures Contracts", description="",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--rootdir", nargs="?", type=str, default="/Users/chenxu/Work/ChinaFutures", 
		dest="rootdir", help="root directory")
	parser.add_argument("--rawdatadir", nargs="?", type=str, default="rawdata.2018-04-12", dest="rawdatadir", help="raw data dir")
	parser.add_argument("--multiplierfile", nargs="?", type=str, default="products.csv", dest="multiplierfile", help="multiplier file")
	parser.add_argument("--outdir", nargs="?", type=str, default="pairs", dest="outdir", help="output dir")
	parser.add_argument("--spreadfield", nargs="?", type=str, default="Settle", dest="spreadfield", help="the field to use for the spread calculation")
	parser.add_argument("--volumeThreshold", nargs="?", type=int, default=3000, dest="volumeThreshold", help="minimum volume requirement")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="product list")
	parser.add_argument("--fields", nargs="*", type=str, default=["Open", "High", "Low", "Close", "Settle", "Volume", "OI"], \
			dest="fields", help="fields")

	args = parser.parse_args()

	products = args.products

	multiplierfile = os.path.join(args.rootdir, args.multiplierfile)
	if not os.path.exists(multiplierfile):
		print(f"The multiplier file {multiplierfile} doesn't exist!")
		sys.exit(1)

	multipliers = pd.read_csv(multiplierfile, index_col=[0])

	continuous = loadDailyContinuous(args.rootdir, products)

	rawdatadir = os.path.join(args.rootdir, args.rawdatadir)
	if not os.path.exists(rawdatadir):
		print(f"Raw data {rawdatadir} doesn't exist!")
		sys.exit(1)

	subdirs = os.listdir(rawdatadir)
	contracts = loadRawData(rawdatadir, subdirs, products)

	pairs = defaultdict(lambda: defaultdict(pd.DataFrame))

	for sym in sorted(continuous.keys()):
		cons = sorted(list(set(continuous[sym].Contract.dropna())))

		if not len(cons) > 1: 
			print(f"Not enough contracts for {sym}, skip ...")
			continue

		if not sym in contracts:
			print(f"{sym} raw contracts don't exist!")
			continue

		if not sym in multipliers.index:
			print(f"{sym} doesn't have a multiplier!")
			continue

		add_next(cons)

		multi = multipliers.loc[sym, "Multiplier"]

		for i in range(1, len(cons)):
			name, data = calc_pair(sym, cons[i-1], cons[i], contracts[sym], multi, args.spreadfield, args.volumeThreshold)
			pairs[sym][name] = data

	outdir = os.path.expandvars(args.outdir)
	for sym in sorted(pairs.keys()):
		out = os.path.join(outdir, sym)
		if not os.path.exists(out):
			print(f"Creating directory {out} ...")
			os.system(f"mkdir -p {out}")

		for name in sorted(pairs[sym].keys()):
			fileName = os.path.join(out, f"{name}.csv")
			print(f"Saving data to {fileName} ...")
			pairs[sym][name].round(1).to_csv(fileName)


