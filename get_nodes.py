#!/usr/bin/env python3

import re
import sys
import csv
import lzma
import glob
import logging
import datetime
import argparse
import collections

from datetime import datetime
from os import path

sys.path.insert(0, "../btccrawlgo-processing")
from processing.dataset import Dataset

import util

class LoadScan:
    def __init__(self, scan_path):
        self.scanpath = scan_path
        logging.info("Loading nodes from %s", self.scanpath)
        self.nodes = self._read_nodes()

    def dedupe(self):
        """Removes duplicate nodes (non-order-preserving)"""
        self.nodes = list(set(self.nodes))
    
    def filedt(self, scanfile):
        """Extract UTC datetime from given scan file path"""
        raise NotImplementedError

    def _read_nodes(self):
        raise NotImplementedError


class LoadYethiScan(LoadScan):
    def filedt(self, scanfile):
        return util.yethi_scanfile_dt(scanfile)

    def _read_nodes(self):
        nodes = []
        with lzma.open(self.scanpath, "rt") as f:
            for l in f:
                values = l.strip().replace(":", ";").split(";")
                nodes.append(tuple(values))
        return nodes

class LoadBtcScan(LoadScan):
    def filedt(self, scanfile):
        return util.btc_scanfile_dt(scanfile)

    def _read_nodes(self):
        ds = Dataset()
        ds.load(self.scanpath.strip("/"))
        return list(map(util.parse_ip_port_pair,
                        ds.address_ipinfos["address"].tolist()))

class LoadLtcScan(LoadBtcScan):
    pass

FORMAT_LOADERS = {
    "Yethi": LoadYethiScan,
    "BTC": LoadBtcScan,
    "LTC": LoadLtcScan,
}

if __name__ == "__main__":
    # Configure logging module
    logging.basicConfig(filename="get_nodes.log", 
        format=util.LOG_FMT, level=logging.DEBUG)

    parser = argparse.ArgumentParser()

    # Optional args
    parser.add_argument("--delimiter", "-d", default="\t",
      help="Output field delimiter (tab by default)")

    # Required args
    parser.add_argument("--format", "-f", choices=list(FORMAT_LOADERS.keys()), 
      help="Format of scan file.", required=True)
    parser.add_argument("scan_path",
      help="Full path to one scan.")

    logging.debug("===STARTUP===")

    ARGS = parser.parse_args()
    logging.debug("Parsed args: %s", str(ARGS))

    # Initialize TSV output writer
    writer = csv.writer(sys.stdout, delimiter=ARGS.delimiter)

    # Initialize correct loader for selected scanfile type
    loader_cls = FORMAT_LOADERS[ARGS.format]
    loader = loader_cls(ARGS.scan_path)

    for n in loader.nodes:
      writer.writerow(n)

    logging.debug("===FINISH===")
