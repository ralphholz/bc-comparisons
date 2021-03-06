#!/usr/bin/env python3

import os
import re
import sys
import csv
import gzip
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
        self.integrity_pass, self.integrity_err = self._integrity_check(preload=True)

        if not self.integrity_pass:
            logging.warning("Scan %s failed pre-load integrity check! Reason: %s",
                    self.scanpath, self.integrity_err)
            self.nodes = []
            self.uncontactable_nodes = None
        else:
            logging.info("Loading nodes from %s", self.scanpath)
            # Immediately load confirmed nodes, but don't load uncontactable
            # until someone calls .load_uncontactable()
            try:
                self.nodes = self._read_nodes()
            except:
                self.nodes = []
            self.uncontactable_nodes = None
            self.integrity_pass, self.integrity_err = self._integrity_check(preload=False)
            if not self.integrity_pass:
                logging.warning("Scan %s failed post-load integrity check! Reason: %s",
                        self.scanpath, self.integrity_err)

    def load_uncontactable(self):
        """
        Loads uncontactable nodes from the scan.
        """
        if not self.uncontactable_nodes:
            self.uncontactable_nodes = self._read_uncontactable_nodes()

    def dedupe(self):
        """
        Removes duplicate nodes (non-order-preserving)
        Only operates on loaded nodes. If uncontactable nodes are not loaded,
        it will only dedupe contactable nodes.
        """
        self.nodes = sorted(set(self.nodes))
        if self.uncontactable_nodes is not None:
            self.uncontactable_nodes = sorted(set(self.uncontactable_nodes))

    @classmethod
    def format_node(cls, node, omit_nodeid=False, omit_ip=False, omit_port=False):
        """Formats node tuple into string"""
        raise NotImplementedError

    def node_ip(self, node):
        """Returns the IP address of a given node tuple."""
        raise NotImplementedError

    def drop_ipv6(self):
        """Removes any node with a non-IPv4 address"""
        self.nodes = list(filter(lambda n: util.is_ipv4(self.node_ip(n)),
                                 self.nodes))
        if self.uncontactable_nodes:
            self.uncontactable_nodes = list(filter(lambda n: util.is_ipv4(self.node_ip(n)),
                                                   self.uncontactable_nodes))
    
    def drop_ipv4(self):
        """Removes any node with an IPv4 address"""
        self.nodes = list(filter(lambda n: not util.is_ipv4(self.node_ip(n)),
                                 self.nodes))
        if self.uncontactable_nodes:
            self.uncontactable_nodes = list(filter(lambda n: not util.is_ipv4(self.node_ip(n)),
                                                   self.uncontactable_nodes))
    
    def filedt(self, scanfile):
        """Extract UTC datetime from given scan file path"""
        raise NotImplementedError

    def _read_nodes(self):
        raise NotImplementedError

    def _read_uncontactable_nodes(self):
        raise NotImplementedError

    def _integrity_check(self, preload=False):
        """
        preload: should be False if calling integrity_check before _read_nodes

        Returns (True, None) if scan appears to be intact, otherwise 
        returns (False, str) with an error string.
        
        """
        raise NotImplementedError


class LoadYethiScan(LoadScan):
    def __init__(self, scan_path):
        scan_path = util.yethi_scanpath(scan_path)
        super().__init__(scan_path)

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
    
    def node_ip(self, node):
        return node[1]

    def _integrity_check(self, preload=True):
        NB_MIN_EXPECTED_FILES = 11
        NB_MIN_NODES = 5
        # scan directory must exist and be a directory
        if not path.isdir(self.scanpath):
            return False, "Scanpath not a dir"
        # scan must contain at least NB_MIN_EXPECTED_FILES
        if len(os.listdir(self.scanpath)) < NB_MIN_EXPECTED_FILES:
            return False, "Scan missing files"
        # should be able to read every xz file
        try:
            for xz in os.walk(path.join(self.scanpath, "*.xz")):
                with lzma.open(xz) as xzf:
                    xzf.read()
        except:
            return False, "Couldn't read every xz"
        # scan must contain more than MIN_NODES confirmed nodes
        if not preload and len(self.nodes) < NB_MIN_NODES:
            return False, "Less than {} contactable nodes".format(NB_MIN_NODES)
        # all checks passed
        return True, None

    def _read_nodes(self):
        """Reads contactable nodes from the Yethi scan data"""
        nodes = []
        with lzma.open(path.join(self.scanpath, "confirmed.csv.xz"), "rt") as f:
            for l in f:
                values = l.strip().replace(":", ";").split(";")
                nodes.append(tuple(values))
        return nodes

    def _read_uncontactable_nodes(self):
        """Reads uncontactable nodes from the Yethi scan data"""
        nodes = []
        with lzma.open(path.join(self.scanpath, "events.csv.xz"), "rt") as f:
            for l in f:
                values = l.strip().split(",")
                # We want only the uncontactable nodes
                if values[0] == "UNCONTACTABLE" and values[-1] == "BOND":
                    nodes.append(tuple(values[1:4]))
        return nodes

class LoadBtcScan(LoadScan):
    def __init__(self, scan_path):
        # use this property to flag that after loading the dataset, it
        # contained no nodes (to avoid loading the dataset multiple times)
        self.__empty = False
        # cache the dataframe after loading the scan, so that when we want to
        # do more with it later (e.g. load uncontactable_nodes, we still have
        # it)
        self.__df = None
        super().__init__(scan_path)

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

    def node_ip(self, node):
        return node[0]

    def __load_df(self):
        if self.__df is not None or self.__empty:
            return
        ds = Dataset()
        ds.load(self.scanpath.rstrip("/"))
        if ds.address_ipinfos is None:
            self.__empty = True
        self.__df = ds.address_ipinfos

    def _integrity_check(self, preload=True):
        NB_MIN_EXPECTED_FILES = 5
        NB_MIN_NODES = 5
        # scan directory must exist and be a directory
        if not path.isdir(self.scanpath):
            return False, "Scanpath not a dir"
        # scan must contain at least NB_MIN_EXPECTED_FILES
        if len(os.listdir(self.scanpath)) < NB_MIN_EXPECTED_FILES:
            return False, "Scan missing files"
        # scan must contain "done" file
        if not path.isfile(path.join(self.scanpath, "done")):
            return False, "Missing 'done' file"
        # should be able to read at least the first line of every gz file
        try:
            for gz in glob.glob(path.join(self.scanpath, "*.gz")):
                with gzip.open(gz) as gzf:
                    gzf.read()
        except:
            return False, "Couldn't read every gz"
        if not preload and len(self.nodes) < NB_MIN_NODES:
            return False, "Less than {} contactable nodes".format(NB_MIN_NODES)
        # all checks passed
        return True, None
    
    def _read_nodes(self):
        self.__load_df()
        df = self.__df
        if df is None:
            logging.warning("Empty nodeset for scan %s %s",
                    self.filedt(self.scanpath),
                    self.scanpath)
            return []
        reachable = df[df["proto_reachable"] == True]["address"]
        if reachable is None or len(reachable) == 0:
            logging.warning("Empty nodeset for scan %s %s",
                    self.filedt(self.scanpath),
                    self.scanpath)
            return []
        return sorted(util.parse_ip_port_pair(n) for n in reachable.tolist())

    def _read_uncontactable_nodes(self):
        self.__load_df()
        df = self.__df
        if df is None:
            logging.warning("Empty uncontactable nodeset for scan %s %s",
                    self.filedt(self.scanpath),
                    self.scanpath)
            return []
        unreachable = df[df["proto_reachable"] == False]["address"]
        if unreachable is None or len(unreachable) == 0:
            logging.warning("Empty uncontactable nodeset for scan %s %s",
                    self.filedt(self.scanpath),
                    self.scanpath)
            return []
        return sorted(util.parse_ip_port_pair(n) for n in unreachable.tolist())

# LTC and Dash use same scan file format as Bitcoin
class LoadLtcScan(LoadBtcScan):
    pass

class LoadDashScan(LoadBtcScan):
    pass

class LoadZecScan(LoadBtcScan):
    pass

FORMAT_LOADERS = {
    "Yethi": LoadYethiScan,
    "BTC": LoadBtcScan,
    "LTC": LoadLtcScan,
    "Dash": LoadDashScan,
    "ZEC": LoadZecScan,
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
    parser.add_argument("--dedupe", "-dd", action="store_true",
      help="If specified, nodes will appear uniquely.")
    parser.add_argument("--uncontactable", "-uc", action="store_true",
      help="If specified, load uncontactable nodes instead.")
    parser.add_argument("--integrity", "-i", action="store_true",
      help="If specified, just test integrity of the scan.")

    # Required args
    parser.add_argument("--format", "-f", choices=list(FORMAT_LOADERS.keys()), 
      help="Format of scan file.", required=True)
    parser.add_argument("scan_path",
      help="Full path to one scan.")

    logging.debug("===STARTUP===")

    ARGS = parser.parse_args()
    logging.debug("Parsed args: %s", str(ARGS))

    # Initialize TSV output writer
    writer = csv.writer(sys.stdout, delimiter=ARGS.delimiter,
        lineterminator="\n")

    # Initialize correct loader for selected scanfile type
    loader_cls = FORMAT_LOADERS[ARGS.format]

    # If we're doing an integrity check only, then do that now
    if ARGS.integrity:
        loader = loader_cls(ARGS.scan_path, )
        result, err = loader.integrity_pass, loader.integrity_err
        if not result:
            writer.writerow(("FAIL", err,))
            sys.exit(1)
        else:
            writer.writerow(("PASS",))
            sys.exit(0)

    loader = loader_cls(ARGS.scan_path)
    
    # Load uncontactable nodes if we're doing that
    if ARGS.uncontactable:
        loader.load_uncontactable()
    
    # Remove non-IPv4 if required
    if not ARGS.keep_ipv6:
        loader.drop_ipv6()

    # Dedupe
    if ARGS.dedupe:
        loader.dedupe()
    
    # Write out nodes
    # Uncontactable nodes if selected:
    if ARGS.uncontactable:
        for n in loader.uncontactable_nodes:
            writer.writerow(n)
    # Only confirmed nodes:
    else:
        for n in loader.nodes:
            writer.writerow(n)

    logging.debug("===FINISH===")
