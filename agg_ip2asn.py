#!/usr/bin/env python3

import sys
import csv
import logging
import argparse
import multiprocessing as mp

import util

csv.field_size_limit(sys.maxsize)

if __name__ == "__main__":
  # Configure logging module
  logging.basicConfig(#filename="aggregate_scans.log", 
    format=util.LOG_FMT, level=util.LOG_LEVEL)

  parser = argparse.ArgumentParser()
  parser.add_argument("infiles", nargs="+", type=argparse.FileType("r"))
  parser.add_argument("--delimiter", "-d", default="\t",
    help="Input and output field delimiter (tab by default)")
  parser.add_argument("--inner-delimiter", "-id", default=";", 
    help="Delimiter to use for lists within a field (; by default)")
  parser.add_argument("--unique", "-u", action="store_true",
    help="If specified, remove duplicate values before processing each input row.")
    
  parser.add_argument("--concurrency", "-j", type=int, default=util.DEFAULT_CONCURRENCY,
    help="Number of MP workers to use for reading scanfiles concurrently."
    " (default={})".format(util.DEFAULT_CONCURRENCY))

  ARGS = parser.parse_args()

  def process_row(row, keyfunc=lambda r: r[0], valuefunc=lambda r: r[1].strip()):
    key = keyfunc(row)
    logging.info("Processing row key %s", key)
    values = valuefunc(row)
    valuelist = values.strip(ARGS.inner_delimiter).split(ARGS.inner_delimiter)
    if ARGS.unique:
      valuelist = set(valuelist)

    # transform IP addresses using the selected transformation and remove any
    # that transform to a None value (e.g. un-announced IPs)
    # e.g. IP -> ASN or IP -> /24 prefix etc
    valuelist = list(map(lambda v: str(util.ip2asn(v, key)), valuelist))
    return key, valuelist

  writer = csv.writer(sys.stdout, delimiter=ARGS.delimiter, 
      lineterminator="\n")

  # Read input files into data structure
  for infile in ARGS.infiles:
    logging.info("Reading input file %s", infile.name)
    with infile as inf:
      reader = csv.reader(inf, delimiter=ARGS.delimiter)
      
      with mp.Pool(ARGS.concurrency) as p:
        rows = p.map(process_row, reader)

      for (date, nodelist) in rows:
        nodelist = sorted(nodelist)
        nodelist = ARGS.inner_delimiter.join(nodelist)
        writer.writerow((date, nodelist,))
