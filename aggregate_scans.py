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

# Takes a list of scans on stdin or from a file, outputs in TSV format:
# date,nodes

if __name__ == "__main__":
    # Configure logging module
    logging.basicConfig(#filename="aggregate_scans.log", 
        format=util.LOG_FMT, level=util.LOG_LEVEL)

    parser = argparse.ArgumentParser()

    # Optional args
    parser.add_argument("--delimiter", "-d", default="\t",
      help="Input and output field delimiter (tab by default)")
    parser.add_argument("--inner-delimiter", "-id", default=";", 
      help="Delimiter to use for lists within a field (; by default)")
    parser.add_argument("--keep-ipv6", "-k6", action="store_true",
      help="If specified, node IPv6 addresses will be kept in output.")
    parser.add_argument("--only-ipv6", "-v6", action="store_true",
      help="If specified, only IPv6 addresses will be included (provided "
           "that --omit-ip is not specified).")

    parser.add_argument("--concurrency", "-j", type=int, default=util.DEFAULT_CONCURRENCY,
      help="Number of MP workers to use for reading scanfiles concurrently."
      " (default={})".format(util.DEFAULT_CONCURRENCY))

    # Output options
    parser.add_argument("--omit-ip", "-oip", action="store_true", 
      help="If specified, output will exclude IP addresses.")
    parser.add_argument("--omit-port", "-oport", action="store_true", 
      help="If specified, output will exclude port numbers.")
    parser.add_argument("--omit-nodeid", "-onodeid", action="store_true", 
      help="If specified, output will exclude node IDs.")

    parser.add_argument("--dedupe-output-nodes", "-dd", action="store_true", 
      help="If specified, output nodes will appear uniquely in each aggregation.")

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

    # Loads confirmed nodes from a given scanfile using a given Loader class
    # NOTE: function is defined here because it wraps ARGS and loader_cls
    # local vars
    def load(scanfile: str):
        loader = loader_cls(scanfile)
        if not ARGS.keep_ipv6 and not ARGS.only_ipv6:
            loader.drop_ipv6()
        if ARGS.only_ipv6:
            loader.drop_ipv4()
        return loader.nodes

    def writerow(row):
        with lock:
          writer.writerow(row)

    def build_nodelist_for_date(date_scanfiles: tuple):
        date, scanfiles = date_scanfiles

        nodeset_for_date = set()

        for sf in scanfiles:
          nodes = load(sf)
          nodeset_for_date = nodeset_for_date.union(set(nodes))
        
        nodelist_for_date = [
            loader_cls.format_node(n, 
                                   omit_nodeid=ARGS.omit_nodeid, 
                                   omit_ip=ARGS.omit_ip, 
                                   omit_port=ARGS.omit_port)
            for n in nodeset_for_date
        ]
        if ARGS.dedupe_output_nodes:
            nodelist_for_date = set(nodelist_for_date)
        # Sort to return a deterministic ordering of nodes
        nodelist_for_date = sorted(nodelist_for_date)
        nodelist_for_date = ARGS.inner_delimiter.join(nodelist_for_date)
        writerow((date, nodelist_for_date,))

    # Load scans for each date
    with mp.Pool(ARGS.concurrency) as p:
        p.map(build_nodelist_for_date, sorted(date_scanfiles.items()))

    logging.debug("===FINISH===")
