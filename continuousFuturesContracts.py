#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import sys, os, re
from collections import defaultdict 
import argparse, datetime, quandl 
import matplotlib.pyplot as plt
import datetime

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

def loadRawData(rawdir, dirs, products):
    results = defaultdict(lambda: defaultdict(pd.DataFrame))

    for sub in dirs:
        if products and not sub in products:
            continue

        productdir = os.path.join(rawdir, sub)
        print("Loading data from {0} ...".format(productdir))

        for con in os.listdir(productdir):
            contract, ext = re.split('\.', con)
            filename = os.path.join(productdir, con)
            df = pd.read_csv(filename, parse_dates=[0], index_col=[0])
            df["Contract"] = contract
            df["Close"].fillna(df["Settle"], inplace=True)
            df["PnL"] = np.log(df["Settle"]).diff()
            df["Liquidity"] = 0.7 * df["Volume"].rolling(window=3, min_periods=1).mean() \
					+ 0.3 * df["OI"].rolling(window=3, min_periods=1).mean()
            results[sub][contract] = df

    return results

def chain(cons, fld="Liquidity"):
    results = defaultdict(lambda: pd.DataFrame)
    for sym in sorted(cons.keys()):
        ch = signalChain(sym, cons[sym], fld)
        results[sym] = ch

    return results

def signalChain(sym, cons, fld):
    print("Chaining {0} ...".format(sym))
    results = defaultdict(lambda: pd.Series)
    for contract in sorted(cons.keys()):
        results[contract] = cons[contract][fld]

    df = pd.DataFrame(results)
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
            else:
                print("{0} doesn't have any contracts on date {1}!".format(sym, date))
                continue
    else:
        print("{0} doesn't have data!".format(sym))

    return res

def loadDailyContinuous(rootdir):
	datadir = os.path.join(rootdir, "continuous")
	if not os.path.exists(datadir):
		print("Data directory {0} doesn't exist!".format(datadir))
		sys.exit(1)

	csvfiles = os.listdir(datadir)
	results = defaultdict(pd.DataFrame)

	for f in sorted(csvfiles):
		sym, ext = re.split('\.', f)
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
    parser.add_argument("--rawdatadir", nargs="?", type=str, default="rawdata.2018-01-27", dest="rawdatadir", help="raw data dir")
    parser.add_argument("--outdir", nargs="?", type=str, default="continuous", dest="outdir", help="output dir")
    parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="product list")
    parser.add_argument("--fields", nargs="*", type=str, default=["Open", "High", "Low", "Close", "Settle", "Volume", "OI"], dest="fields", help="fields")

    args = parser.parse_args()

    rawdatadir = os.path.join(args.rootdir, args.rawdatadir)
    if not os.path.exists(rawdatadir):
        print("Raw data {0} doesn't exist!".format(rawdatadir))
        sys.exit(1)

    subdirs = os.listdir(rawdatadir)
    contracts = loadRawData(rawdatadir, subdirs, args.products)

    continuous = chain(contracts)

    outdir = os.path.expandvars(args.outdir)
    if not os.path.exists(outdir):
        print("Creating directory {0} ...".format(outdir))
        os.system("mkdir -p {0}".format(outdir))

    for sym in sorted(continuous.keys()):
        filename = os.path.join(outdir, sym + ".csv")
        print("Dumping to {0} ...".format(filename))
        continuous[sym].to_csv(filename)

    print("Done.")
    sys.exit(0)
    
