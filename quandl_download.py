#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import sys, os, re
from collections import defaultdict 
import argparse, datetime, quandl 
import matplotlib.pyplot as plt
import datetime

from quandl.errors.quandl_error import NotFoundError

quandl.ApiConfig.api_key = "d5KcMbVrv2GRC2H9Qrn4"

monthSymbols = {
	1 : 'F',
	2 : 'G',
	3 : 'H',
	4 : 'J',
	5 : 'K',
	6 : 'M',
	7 : 'N',
	8 : 'Q',
	9 : 'U',
	10 : 'V',
	11 : 'X',
	12 : 'Z'
}

def next_contract(startYr, curYr, curMon, bYr, months):
	for yr in range(startYr, curYr+bYr+1):
		for m in list(months):
			if yr - curYr >= bYr and m > curMon:
				raise StopIteration
			else:
				yield "{0}{1}".format(m, yr)

if __name__ == "__main__":

	parser = argparse.ArgumentParser(prog="Quandl China Futures", description="",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--rootdir", nargs="?", type=str, default="/Users/chenxu/Work/ChinaFutures", 
		dest="rootdir", help="root directory")
	parser.add_argument("--startyear", nargs="?", type=int, default=2015, dest="startyear", help="start year")
	parser.add_argument("--yearbeyond", nargs="?", type=int, default=1, dest="yearbeyond", help="how many years further")
	parser.add_argument("--productfile", nargs="?", type=str, default="products.csv", dest="productfile", 
		help="product file")
	parser.add_argument("--outputdir", nargs="?", type=str, default="rawdata", dest="outputdir", help="output dir")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="product list")
	parser.add_argument("--fields", nargs="*", type=str, 
		default=["Open", "High", "Low", "Close", "Settle", "PreSettle", "Volume", "OI"], dest="fields", help="fields")

	args = parser.parse_args()


	productfile = os.path.join(args.rootdir, args.productfile)
	if not os.path.exists(productfile):
		print("{0} Can't find file {1}".format(EnvironmentError, productfile))
		sys.exit(1)

	products = pd.read_csv(productfile, index_col=[0])

	startdate = datetime.date(args.startyear, 1, 1)
	curdate = datetime.datetime.now().date()


	print("Start from {0} to {1}".format(startdate, curdate))


	for sym in sorted(products.index):
		if not args.products or sym in args.products:
			exch, mult, months = products.loc[sym, ["Exchange", "Multiplier", "Expirations"]]

			results = defaultdict(pd.DataFrame)
			for contract in next_contract(args.startyear, curdate.year, 
				monthSymbols[curdate.month], args.yearbeyond, months):
				ticker = "{0}/{1}{2}".format(exch, sym, contract)
			
				try:
					data = quandl.get(ticker, start_date=startdate, end_date=curdate)
					data.sort_index(inplace=True)

					""" Change headers """
					for fld in args.fields:
						if not fld in data.columns:
							if fld == "PreSettle":
								if "Pre Settle" in data.columns:
									data.rename(columns={"Pre Settle" : fld}, inplace=True)
								else:
									print("{0} doesn't exist!".format(fld))
							elif fld == "OI":
								if "O.I." in data.columns:
									data.rename(columns={"O.I." : fld}, inplace=True)
								elif "Open Interest" in data.columns:
									data.rename(columns={"Open Interest" : fld}, inplace=True)
								elif "Prev. Day Open Interest" in data.columns:
									data.rename(columns={"Prev. Day Open Interest" : fld}, inplace=True)
								else:
									print("{0} doesn't exist!".format(fld))
							else:
								print("{0} doesn't exist!".format(fld))

					results[contract] = data.loc[:, args.fields]
				except NotFoundError:
					print("{0} unable to get {1}".format(NameError, ticker))
				finally:
					print("Done ticker {0}".format(ticker))

			outputdir = os.path.join(args.rootdir, "{0}.{1}".format(args.outputdir, curdate), sym)
			if not os.path.exists(outputdir):
				os.system("mkdir -p {0}".format(outputdir))
			print("Dump to {0}".format(outputdir))

			for con in sorted(results.keys()):
				df = results[con]
				fname = os.path.join(outputdir, "{0}{1}.csv".format(con[1:5], con[0]))
				df.to_csv(fname)
