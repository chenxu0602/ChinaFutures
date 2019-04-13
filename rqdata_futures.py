
import rqdatac
from rqdatac import *
import pandas as pd

import os, sys
import argparse

import asyncio

async def download_contract(sym, ticker, start, end, period, outputdir):
   try:
      print(f"Downloading {period} data for {ticker} from {start} to {end} ...")
      df = get_price(ticker, start, end, period)
   except Exception as e:
      print(e.message)
      sys.exit(1)
   else:
      symdir = os.path.join(outputdir, sym)
      if not os.path.exists(symdir):
         print(f"Creating output dir {symdir} ...")
         os.mkdir(symdir)

      filename = os.path.join(symdir, f"{ticker}.csv")
      print(f"Saving to {filename} ...")
      df.to_csv(filename, index=True)

async def download_contracts(univ, period, outputdir):
   tasks = []
   for i in range(len(univ)):
      con = univ.iloc[i]
      sym = con["underlying_symbol"]
      ticker = con["order_book_id"]
      start = con["listed_date"]
      end = con["de_listed_date"]

      task = asyncio.ensure_future(download_contract(sym, ticker, start, end, period, outputdir))
      tasks.append(task)
   await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
   parser = argparse.ArgumentParser(prog="RQ Futures Data", description="", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
   parser.add_argument("--outputdir", nargs="?", type=str, default="RQ_Futures", dest="outputdir", help="output dir")
   parser.add_argument("--period", nargs="?", type=str, default="tick", dest="period", help="period")
   parser.add_argument("--exchanges", nargs="*", type=str, default=["CFFEX", "DCE", "CZCE", "INE", "SHFE"], dest="exchanges", help="exchanges")
   parser.add_argument("--symbols", nargs="*", type=str, default=[], dest="symbols", help="symbols")
   parser.add_argument("--startyear", nargs="?", type=int, default=1701, dest="startyear", help="start year")

   args = parser.parse_args()

   print(f"Initializing RQ Data ...")
   rqdatac.init()

   inst = pd.read_csv("futures_contracts.csv")
   inst = inst[["exchange", "underlying_symbol", "order_book_id", "listed_date", "de_listed_date"]]
   inst["year"] = inst["order_book_id"].str.extract("(\d+)").astype(int)

   univ = inst.loc[inst.year >= args.startyear]

   if args.exchanges:
      univ = univ.loc[univ.exchange.isin(args.exchanges)]

   if args.symbols:
      univ = univ.loc[univ.underlying_symbol.isin(args.symbols)]

   print("Created universe.")
   print(univ)

   outputdir = os.path.expandvars(args.outputdir + f"_{args.period}")
   if not os.path.exists(outputdir):
      print(f"Creating output dir {outputdir} ...")
      os.mkdir(outputdir)

   asyncio.run(download_contracts(univ, args.period, outputdir))
   print("Downloading finished.")