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
    NODE_PART_SEP = ":"

    def __init__(self, scan_path):
        self.scanpath = scan_path
        logging.info("Loading nodes from %s", self.scanpath)
        self.nodes = self._read_nodes()

    def dedupe(self):
        """Removes duplicate nodes (non-order-preserving)"""
        self.nodes = list(set(self.nodes))

    @classmethod
    def format_node(cls, node, omit_nodeid=False, omit_ip=False, omit_port=False):
        """Formats node tuple into string"""
        raise NotImplementedError

    def drop_ipv6(self):
        """Removes any node with a non-IPv4 address"""
        raise NotImplementedError
    
    def filedt(self, scanfile):
        """Extract UTC datetime from given scan file path"""
        raise NotImplementedError

    def _read_nodes(self):
        raise NotImplementedError


class LoadYethiScan(LoadScan):
    def filedt(self, scanfile):
        return util.yethi_scanfile_dt(scanfile)

    @classmethod
    def format_node(cls, node, omit_nodeid=False, omit_ip=False, omit_port=False):
        """Formats node tuple into string"""
        # We must have at least one component to identify a node
        assert not (omit_nodeid and omit_ip and omit_port)
        fieldmap = {
            0: omit_nodeid,
            1: omit_ip,
            2: omit_port,
        }
        # This code assumes Yethi nodes are always a thruple (node_id, ip, port)
        assert len(node) == 3
        newnode = tuple(nodepart for i, nodepart in enumerate(node) if not fieldmap[i])
        return cls.NODE_PART_SEP.join(newnode)
    
    def drop_ipv6(self):
        """Removes any node with a non-IPv4 address"""
        self.nodes = list(filter(lambda n: util.is_ipv4(n[1]), self.nodes))

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
    
    @classmethod
    def format_node(cls, node, omit_nodeid=False, omit_ip=False, omit_port=False):
        """Formats node tuple into string"""
        # We must have at least one component to identify a node
        assert not (omit_nodeid and omit_ip and omit_port)
        fieldmap = {
            0: omit_ip,
            1: omit_port,
        }
        # This code assumes BTC nodes are always a thruple (node_id, ip, port)
        assert len(node) == 2
        newnode = tuple(nodepart for i, nodepart in enumerate(node) if not fieldmap[i])
        return cls.NODE_PART_SEP.join(newnode)

    def drop_ipv6(self):
        """Removes any node with a non-IPv4 address"""
        self.nodes = list(filter(lambda n: util.is_ipv4(n[0]), self.nodes))

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
    logging.basicConfig(filename="load_scan.log", 
        format=util.LOG_FMT, level=util.LOG_LEVEL)

    parser = argparse.ArgumentParser()

    # Optional args
    parser.add_argument("--delimiter", "-d", default="\t",
      help="Output field delimiter (tab by default)")
    parser.add_argument("--keep-ipv6", "-k6", action="store_true",
      help="If specified, node IPv6 addresses will be kept in output.")

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

    # Remove non-IPv4 if required
    if not ARGS.keep_ipv6:
        loader.drop_ipv6()
    
    # Write out nodes
    for n in loader.nodes:
      writer.writerow(n)

    logging.debug("===FINISH===")
