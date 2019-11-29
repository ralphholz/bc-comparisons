#!/usr/bin/env python3

import re
import sys
import csv
import lzma
import glob
import logging
import datetime
import argparse
import functools
import collections
import multiprocessing as mp

from datetime import datetime
from os import path

import util
import load_scan

# Takes a list of dates and scans on stdin or from a file, outputs scans that
# failed the integrity check and the reason why

if __name__ == "__main__":
    # Configure logging module
    logging.basicConfig(format=util.LOG_FMT, level=util.LOG_LEVEL)

    parser = argparse.ArgumentParser()

    # Optional args
    parser.add_argument("--delimiter", "-d", default="\t",
      help="Input and output field delimiter (tab by default)")
    parser.add_argument("--inner-delimiter", "-id", default=";", 
      help="Delimiter to use for lists within a field (; by default)")

    parser.add_argument("--concurrency", "-j", type=int, default=util.DEFAULT_CONCURRENCY,
      help="Number of MP workers to use for reading scanfiles concurrently."
      " (default={})".format(util.DEFAULT_CONCURRENCY))

    # Required args
    parser.add_argument("--format", "-f", choices=list(load_scan.FORMAT_LOADERS.keys()), 
      help="Format of scan file.", required=True)
    parser.add_argument("infile", nargs="*", type=argparse.FileType("r"),
                        default=sys.stdin,
                        help="File containing a list of scan file paths, all of the same format.")

    logging.debug("===STARTUP===")

    ARGS = parser.parse_args()
    logging.debug("Parsed args: %s", str(ARGS))

    # Read all scan file paths from all input files
    date_scanfiles = collections.defaultdict(set)
    for inf in ARGS.infile:
        reader = csv.reader(inf, delimiter=ARGS.delimiter)
        # Each row is in format: date,list_of_scanfiles
        for (date, scanfiles) in reader:
            scanfiles = set(scanfiles.split(ARGS.inner_delimiter))
            date_scanfiles[date] = date_scanfiles[date].union(scanfiles)

    # Initialize TSV output writer
    writer = csv.writer(sys.stdout, delimiter=ARGS.delimiter,
        lineterminator="\n")
    
    # Get correct loader for selected scanfile type
    loader_cls = load_scan.FORMAT_LOADERS[ARGS.format]

    # Mutex for file writing
    lock = mp.Lock()

    def writerow(row):
        with lock:
          writer.writerow(row)

    def integrity_check(date_scanfiles: tuple):
        date, scanfiles = date_scanfiles

        for sf in scanfiles:
            l = loader_cls(sf)
            res, err = l.integrity_pass, l.integrity_err
            if not res:
                writerow(("FAIL", l.filedt(l.scanpath), sf, err,))
            else:
                writerow(("PASS", l.filedt(l.scanpath), sf,))

    # Load scans for each date
    with mp.Pool(ARGS.concurrency) as p:
        p.map(integrity_check, sorted(date_scanfiles.items()))

    logging.debug("===FINISH===")
