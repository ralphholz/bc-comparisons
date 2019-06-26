#!/usr/bin/env python3

import sys
import csv
import bisect
import logging
import argparse
import collections

import util

csv.field_size_limit(sys.maxsize)

class CoreNodes:
  def __init__(self, date_nodes: dict, backcheck_t: int):
    if len(date_nodes) == 0:
      raise ValueError("date_nodes must be non-empty")
    self.data = {k: collections.Counter(set(v)) for k, v in date_nodes.items()}
    self.scandates = sorted(self.data.keys())
    # self.__scan_ids = {date: i for i, date in enumerate(self.scandates)}
    # self.backcheck_t = backcheck_t
    # # first date in our rolling range will be 
    # self.start_rolling_date = util.str2dt(self.scandates[0]) + datetime.timedelta(days=backcheck_t-1)
    # self._build_node_scanmap()

  def core(self, start_date, end_date, percentile = 0.9):
    # Find scan range for dates
    scans = util.values_in_range(self.scandates, start_date, end_date)
    # Count occurrences of each node in each scan in the range
    totals = collections.Counter()
    for scan in scans:
      totals += self.data[scan]
    return list(filter(lambda n: totals[n]/len(scans) >= percentile, totals))

  # def _build_node_scanmap(self):
    # logging.info("Building node -> scans map")
    # node_scanmap = collections.defaultdict(list)
    # for date, nodes in self.data.items():
    #   logging.debug("Building node -> scans map for key={}".format(date))
    #   for node in nodes:
    #     bisect.insort(node_scanmap[node], seld.__scan_ids[date])


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
  parser.add_argument("--ignore-missing-keys", "-imk", action="store_true",
    help="If set, missing keys will be ignored instead of causing an exception.")
  parser.add_argument("--compare", "-c", choices=sorted(IP_TRANSFORMS.keys()),
      default="ip", help="What to compare.")
  parser.add_argument("--explore", "-e", default=None,
    help="Explore one intersection of a specific date/key. Format: key=combo "
    "where combo is an --inner-delimiter separated list of input filenames.")
  parser.add_argument("--no-grouping", "-ng", action="store_true",
    help="If specified, counts are shown for each set in output of --explore.")
  parser.add_argument("--unique", "-u", action="store_true",
    help="If specified, remove duplicate values before processing each input row.")
    
  parser.add_argument("--concurrency", "-j", type=int, default=util.DEFAULT_CONCURRENCY,
    help="Number of MP workers to use for reading scanfiles concurrently."
    " (default={})".format(util.DEFAULT_CONCURRENCY))

  ARGS = parser.parse_args()
  
  # Read input files into data structure
  for infile in filter(infile_filter, ARGS.infiles):
    logging.info("Reading input file %s", infile.name)
    with infile as inf:
      reader = csv.reader(inf, delimiter=ARGS.delimiter)
      table = {}
      
      with mp.Pool(ARGS.concurrency) as p:
        rows = p.map(process_row, filter(row_filter, reader))

      for (key, valueset) in rows:
        if key not in keys_seen:
          keys_seen.add(key)
          keys.append(key)
        table[key] = valueset

      groups[infile.name] = table
    
  # Generate all possible combinations of the input files
  combos = util.all_combinations(groups.keys())

  if ARGS.ignore_missing_keys:
    logging.warning("Dropping keys that don't appear in all inputs...")
    # Remove keys that are not in all input files
    missing_keys = set()
    for fname, sets in groups.items():
      if sets.keys() != keys_seen:
        missing_keys = missing_keys.union(keys_seen - sets.keys())
    for mk in missing_keys:
      for fname, sets in groups.items():
        if mk in sets:
          logging.warning("Ignoring extraneous key %s in %s", mk, fname)
          del sets[mk]
      keys.remove(mk)
  else:
    # Sanity check: we should never have a key (e.g. date) that appears in one
    # input file but not another
    logging.debug("Starting sanity check: no missing dates")
    all_ok = True
    for fname, sets in groups.items():
      if sets.keys() != keys_seen:
        all_ok = False
        missing_keys = keys_seen - sets.keys()
        for k in sorted(missing_keys):
          logging.fatal("%s missing key %s", fname, k)
    if not all_ok:
      raise Exception("Key mismatch in input files")

