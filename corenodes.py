#!/usr/bin/env python3

import sys
import csv
import bisect
import logging
import datetime
import argparse
import collections

import util

csv.field_size_limit(sys.maxsize)

class CoreNodes:
  def __init__(self, date_nodes: dict):
    if len(date_nodes) == 0:
      raise ValueError("date_nodes must be non-empty")
    self.data = {k: collections.Counter(set(v)) for k, v in date_nodes.items()}
    self.scandates = sorted(self.data.keys())
    self.__nodecount_range_cache = {}
    # self.__scan_ids = {date: i for i, date in enumerate(self.scandates)}
    # self.backcheck_t = backcheck_t
    # # first date in our rolling range will be 
    # self.start_rolling_date = util.str2dt(self.scandates[0]) + datetime.timedelta(days=backcheck_t-1)
    # self._build_node_scanmap()

  def scans_in_range(self, start_date, end_date):
    """
    Return a list of scans in the given date range.
    """
    return util.values_in_range(self.scandates, start_date, end_date)

  def _range_totals(self, start_date, end_date):
    """
    Returns counts of how many nodes appear across all scans in the given
    range. e.g. Counter({'node1': 1, 'node2': 4, ...})
    """
    start_dt = util.str2dt(start_date)
    end_dt = util.str2dt(end_date)
    # Cache results for efficiency
    if (start_date, end_date) not in self.__nodecount_range_cache:
      # Find scan range for dates
      scans = self.scans_in_range(start_date, end_date)

      # Check if one day back is in the cache (useful because we use +1day sliding windows)
      prev_start = (start_dt - datetime.timedelta(days=1)).date().isoformat()
      prev_end = (end_dt - datetime.timedelta(days=1)).date().isoformat()

      if (prev_start, prev_end) in self.__nodecount_range_cache:
        scans_prev = self.scans_in_range(prev_start, prev_end)
        prev_totals = self.__nodecount_range_cache[(prev_start, prev_end)]
        to_subtract = set(scans_prev) - set(scans)
        to_add = set(scans) - set(scans_prev)

        new_totals = collections.Counter() + prev_totals
        for scan in to_subtract:
          new_totals -= self.data[scan]
        for scan in to_add:
          new_totals += self.data[scan]
        self.__nodecount_range_cache[(start_date, end_date)] = new_totals

      # If not, we really do need to compute it from scratch, unfortunately
      else:
        # Count occurrences of each node in each scan in the range
        totals = collections.Counter()
        for scan in scans:
          totals += self.data[scan]
        self.__nodecount_range_cache[(start_date, end_date)] = totals
    return self.__nodecount_range_cache[(start_date, end_date)]

  def core(self, start_date, end_date, percentile = 0.9, invert: bool = False):
    """
    Return the list of nodes which appear in percentile% of all scans in the
    given date interval [start_date, end_date], and the number of total nodes.
    If invert is True, the set is inverted and the method returns NON-CORE
    nodes instead.
    Returns a tuple of the format:
    (total_number_of_nodes_in_window:int, sorted_list_of_nodes:list)
    """
    # print(start_date, end_date, percentile)
    # Find scan range for dates
    scans = self.scans_in_range(start_date, end_date)
    # Count occurrences of each node in each scan in the range
    totals = self._range_totals(start_date, end_date)
    # sort for determinism
    if invert:
      return len(totals), sorted(filter(lambda n: not(totals[n]/len(scans) >= percentile), totals))
    else:
      return len(totals), sorted(filter(lambda n: totals[n]/len(scans) >= percentile, totals))

  def rolling_core(self, backcheck: int, percentile: float = 0.9, 
      invert: bool = False):
    """
    Generator that returns daily core nodes based on the previous backcheck
    days, where a core node is one which has appeared in percentile% of scans
    in the rolling backcheck period.
    Yields tuples of the format:
    (start_date:datetime, end_date:datetime, 
     total_number_of_nodes_in_window:int, sorted_list_of_nodes:list)
    """
    start = self.scandates[0]
    end = self.scandates[-1]
    start_dt = util.str2dt(start)
    end_dt = util.str2dt(end)
    if (end_dt - start_dt).days < backcheck:
      raise ValueError("backcheck must not exceed total length of campaign")
    # establish the initial sliding window
    core_start = start_dt
    core_end = start_dt + datetime.timedelta(days=backcheck-1)
    # print(core_start, core_end)
    # slide the window along
    while core_end <= end_dt:
      totalnodes, core = self.core(core_start.date().isoformat(), 
        core_end.date().isoformat(), percentile=percentile, invert = invert)
      yield (core_start, core_end, totalnodes, core)
      core_start += datetime.timedelta(days=1)
      core_end += datetime.timedelta(days=1)

  # def _build_node_scanmap(self):
    # logging.info("Building node -> scans map")
    # node_scanmap = collections.defaultdict(list)
    # for date, nodes in self.data.items():
    #   logging.debug("Building node -> scans map for key={}".format(date))
    #   for node in nodes:
    #     bisect.insort(node_scanmap[node], seld.__scan_ids[date])

# if __name__ == "__main__":
#   # Configure logging module
#   logging.basicConfig(#filename="aggregate_scans.log", 
#     format=util.LOG_FMT, level=util.LOG_LEVEL)

#   parser = argparse.ArgumentParser()
#   parser.add_argument("infiles", nargs="+", type=argparse.FileType("r"))
#   parser.add_argument("--delimiter", "-d", default="\t",
#     help="Input and output field delimiter (tab by default)")
#   parser.add_argument("--inner-delimiter", "-id", default=";", 
#     help="Delimiter to use for lists within a field (; by default)")
#   parser.add_argument("--ignore-missing-keys", "-imk", action="store_true",
#     help="If set, missing keys will be ignored instead of causing an exception.")
#   parser.add_argument("--compare", "-c", choices=sorted(IP_TRANSFORMS.keys()),
#       default="ip", help="What to compare.")
#   parser.add_argument("--explore", "-e", default=None,
#     help="Explore one intersection of a specific date/key. Format: key=combo "
#     "where combo is an --inner-delimiter separated list of input filenames.")
#   parser.add_argument("--no-grouping", "-ng", action="store_true",
#     help="If specified, counts are shown for each set in output of --explore.")
#   parser.add_argument("--unique", "-u", action="store_true",
#     help="If specified, remove duplicate values before processing each input row.")
    
#   parser.add_argument("--concurrency", "-j", type=int, default=util.DEFAULT_CONCURRENCY,
#     help="Number of MP workers to use for reading scanfiles concurrently."
#     " (default={})".format(util.DEFAULT_CONCURRENCY))

#   ARGS = parser.parse_args()
  
#   # Read input files into data structure
#   for infile in filter(infile_filter, ARGS.infiles):
#     logging.info("Reading input file %s", infile.name)
#     with infile as inf:
#       reader = csv.reader(inf, delimiter=ARGS.delimiter)
#       table = {}
      
#       with mp.Pool(ARGS.concurrency) as p:
#         rows = p.map(process_row, filter(row_filter, reader))

#       for (key, valueset) in rows:
#         if key not in keys_seen:
#           keys_seen.add(key)
#           keys.append(key)
#         table[key] = valueset

#       groups[infile.name] = table
    
#   # Generate all possible combinations of the input files
#   combos = util.all_combinations(groups.keys())

#   if ARGS.ignore_missing_keys:
#     logging.warning("Dropping keys that don't appear in all inputs...")
#     # Remove keys that are not in all input files
#     missing_keys = set()
#     for fname, sets in groups.items():
#       if sets.keys() != keys_seen:
#         missing_keys = missing_keys.union(keys_seen - sets.keys())
#     for mk in missing_keys:
#       for fname, sets in groups.items():
#         if mk in sets:
#           logging.warning("Ignoring extraneous key %s in %s", mk, fname)
#           del sets[mk]
#       keys.remove(mk)
#   else:
#     # Sanity check: we should never have a key (e.g. date) that appears in one
#     # input file but not another
#     logging.debug("Starting sanity check: no missing dates")
#     all_ok = True
#     for fname, sets in groups.items():
#       if sets.keys() != keys_seen:
#         all_ok = False
#         missing_keys = keys_seen - sets.keys()
#         for k in sorted(missing_keys):
#           logging.fatal("%s missing key %s", fname, k)
#     if not all_ok:
#       raise Exception("Key mismatch in input files")

