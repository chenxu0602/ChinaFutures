#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import sys, os, re
from collections import defaultdict 
import argparse, datetime, quandl 
import matplotlib.pyplot as plt
import datetime

from queue import Queue
from threading import Thread
import concurrent.futures

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

def download(sym, outputdir, exch, months, startyear, curyear, curmonth, yearby):
	folder = os.path.join(outputdir, sym)
	if not os.path.exists(folder):
		print(f"Creating directory {folder} ...")
		os.mkdir(folder)

	for contract in next_contract(startyear, curyear, monthSymbols[curmonth], yearby, months):
		ticker = f"{exch}/{sym}{contract}"
	
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

			df = data.loc[:, args.fields]
			filename = os.path.join(folder, f"{contract}.csv")
			df.to_csv(filename)

		except NotFoundError:
			print(f"{NameError} unable to get {ticker}")
		finally:
			print(f"Done ticker {ticker}")

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

	rootdir = os.path.expandvars(args.rootdir)
	if not os.path.exists(rootdir):
		print(f"ERROR: root dir {rootdir} doesn't exist!")
		sys.exit(1)

	productfile = os.path.join(args.rootdir, args.productfile)
	if not os.path.exists(productfile):
		print(f"{EnvironmentError} Can't find file {productfile}")
		sys.exit(1)

	products = pd.read_csv(productfile, index_col=[0])

	startdate = datetime.date(args.startyear, 1, 1)
	curdate = datetime.datetime.now().date()
	print(f"Start from {startdate} to {curdate}")

	outputdir = os.path.join(rootdir, f"{args.outputdir}.{curdate}")
	if not os.path.exists(outputdir):
		print(f"Output dir {outputdir} doesn't exist, creating it ...")
		os.mkdir(outputdir)

	""" Conventional Multi-Threading Way """
	"""
	threads = defaultdict(Thread)

	for sym in sorted(products.index):
		if not args.products or sym in args.products:
			exch, mult, months = products.loc[sym, ["Exchange", "Multiplier", "Expirations"]]

			th = Thread(target=download, \
				args=(sym, exch, months, startdate.year, curdate.year, curdate.month, args.yearbeyond,))
			threads[sym] = th


	for sym, th in threads.items():
		th.start()

	for sym, th in threads.items():
		th.join()
	"""

	with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:	
		futures = []
		for sym in sorted(products.index):
			if not args.products or sym in args.products:
				exch, mult, months = products.loc[sym, ["Exchange", "Multiplier", "Expirations"]]
				futures.append(executor.submit(\
					download, sym, outputdir, exch, months, startdate.year, curdate.year, curdate.month, args.yearbeyond))

		concurrent.futures.wait(futures)

	print("Downloading done.")
	sys.exit(0)
