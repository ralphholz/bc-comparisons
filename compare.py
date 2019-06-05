#!/usr/bin/env python3

import sys
import csv
import logging
import argparse
import ipaddress
import itertools
import collections
import multiprocessing as mp

import util

csv.field_size_limit(sys.maxsize)

# This script computes, row-wise based on a join key in field 0, the
# intersections of all possible combinations of the input files
# Input format: TSV e.g.
#     2019-05-14	8.8.8.8;8.8.4.4;8.8.8.8
# Output format: TSV e.g.
#     key	sample1.tsv	sample2.tsv	sample1.tsv;sample2.tsv
#     2019-05-14	2	3	1

# Functions that transform IP addresses to other info
IP_TRANSFORMS = {
  "ip": lambda x: x,  # do nothing
  "asn": util.ip2asn, # map IP to ASN
  "geo": util.geoip,  # map IP to geo
  "24prefix": lambda ip: util.ip_prefix(ip, 24), # map IP to /24 prefix
  "16prefix": lambda ip: util.ip_prefix(ip, 16), # map IP to /16 prefix
}

if __name__ == "__main__":
  # Configure logging module
  logging.basicConfig(#filename="aggregate_scans.log", 
    format=util.LOG_FMT, level=util.LOG_LEVEL)

  parser = argparse.ArgumentParser()
  parser.add_argument("infiles", nargs="*", type=argparse.FileType("r"))
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
    
  parser.add_argument("--concurrency", "-j", type=int, default=util.DEFAULT_CONCURRENCY,
    help="Number of MP workers to use for reading scanfiles concurrently."
    " (default={})".format(util.DEFAULT_CONCURRENCY))

  ARGS = parser.parse_args()
  
  # Function to transform input IP addresses to comparable format
  transform = IP_TRANSFORMS[ARGS.compare]

  # NOTE: pre-load ASN DB
  # This may not be necessary, but with mp forking, it save some time
  util.asn_db()

  def process_row(row, keyfunc=lambda r: r[0], valuefunc=lambda r: r[1].strip()):
    key = keyfunc(row)
    logging.info("Processing row key %s", key)
    values = valuefunc(row)
    valuelist = values.strip(ARGS.inner_delimiter).split(ARGS.inner_delimiter)

    # transform IP addresses using the selected transformation and remove any
    # that transform to a None value (e.g. un-announced IPs)
    # e.g. IP -> ASN or IP -> /24 prefix etc
    valuelist = filter(lambda v: v is not None, map(transform, valuelist))
    return key, collections.Counter(valuelist)

  # A mapping of {input-filename -> {date -> counter of identifiers}}
  groups = {}

  # Keep track of all seen keys (first value in each row, e.g. scan date)
  # Also keep track of their order, so we output in the same order as keys were
  # seen, in order of the list of input files
  keys_seen = set()
  keys = []

  # Read input files into data structure
  with mp.Pool(ARGS.concurrency) as p:
    for infile in ARGS.infiles:
      logging.info("Reading input file %s", infile.name)
      with infile as inf:
        reader = csv.reader(inf, delimiter=ARGS.delimiter)
        table = {}
        for (key, valueset) in p.map(process_row, reader):
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

  def make_cardinality_outputrow(key):
    # Produce row-wise intersections for the same key for all possible
    # combinations of input files
    rowvalues = {'key': key}
    logging.debug("make_cardinality_outputrow: computing intersections "
                  "for key = %s", key)
    for combo in combos:
      isect = None
      for fname in combo:
        if isect is None:
          isect = groups[fname][key].keys()
        else:
          isect &= groups[fname][key].keys()
      rowvalues[ARGS.inner_delimiter.join(combo)] = len(isect)
    return rowvalues
  
  def make_intersection_outputrows(key, combo, group=True):
    isect = None
    logging.debug("make_intersection_outputrows: computing intersections "
                  "for %s, %s, group=%s", key, combo, group)
    for fname in combo:
      nodes = groups[fname][key]
      if isect is None:
        isect = nodes
      else:
        isect = util.counter_isect(isect, nodes)

    # if we're grouping by value, yield groups
    if group:
      logging.debug("make_intersection_outputrows: computing grouped output")
      # generate output rows: key,node,count
      for (k, v) in sorted(isect.items(), key=lambda t: -t[1]):
        yield key, k, v
    # if we're not grouping then we want to see values for each set in the intersection
    else:
      logging.debug("make_intersection_outputrows: computing ungrouped output")
      res = []
      for fname in combo:
        nodes = groups[fname][key] & isect
        res += [(key, fname, k, v) for (k, v) in nodes.items()]
      # sort output rows by (date, reverse:intersectioncardinality, fname)
      for r in sorted(res, key=lambda t: (t[0], -isect[t[2]], t[2], t[1])):
        yield r

  # Write exploration data for specific key
  if ARGS.explore:
    writer = csv.writer(sys.stdout, delimiter=ARGS.delimiter,
        lineterminator="\n")
    key, combo = ARGS.explore.split("=")
    combo = sorted(combo.split(ARGS.inner_delimiter))
    logging.info("Writing intersection data for key=%s combo=%s", key, combo)
    for row in make_intersection_outputrows(key, combo, group=(not ARGS.no_grouping)):
      writer.writerow(row)

  # Write intersection cardinalities
  else:
    outfields = ["key"]+[ARGS.inner_delimiter.join(combo) for combo in combos]
    writer = csv.DictWriter(sys.stdout, fieldnames=outfields,
        delimiter=ARGS.delimiter,  lineterminator="\n")
    writer.writeheader()

    # Loop over keys (e.g. dates) in order 
    for key in sorted(keys):
      rowvalues = make_cardinality_outputrow(key)
      writer.writerow(rowvalues)
