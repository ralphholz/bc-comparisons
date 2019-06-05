#!/usr/bin/env python3

import sys
import csv
import pyasn
import logging
import argparse
import ipaddress
import itertools
import collections

import util

csv.field_size_limit(sys.maxsize)

# This script computes, row-wise based on a join key in field 0, the
# intersections of all possible combinations of the input files
# Input format: TSV e.g.
#     2019-05-14	8.8.8.8;8.8.4.4;8.8.8.8
# Output format: TSV e.g.
#     key	sample1.tsv	sample2.tsv	sample1.tsv;sample2.tsv
#     2019-05-14	2	3	1

asndb = None

def ip2asn(ip):
  global asndb
  try:
    if asndb is None:
      asndb = pyasn.pyasn('ipasn.dat')
    asn = asndb.lookup(ip)
    if asn[0] is None:
      return -1
    return asn[0]
  except:
    return -1
  return -1

def geoip(ip):
  # TODO
  raise NotImplementedError

def ip_prefix(ip, prefix):
  """
  Returns the IPv4 supernet with the specified prefix of the specified /32
  address.
  >>> ip_prefix('8.8.8.8', 24)
  '8.8.8.0/24'
  >>> ip_prefix('8.8.8.8', 16)
  '8.8.0.0/16'
  """
  assert prefix <= 32
  ipnet = ipaddress.ip_network(ip)
  return str(ipnet.supernet(new_prefix=prefix))

IP_TRANSFORMS = {
  "ip": lambda x: x,  # do nothing
  "asn": ip2asn, # map IP to ASN
  "geo": geoip,  # map IP to geo
  "24prefix": lambda ip: ip_prefix(ip, 24), # map IP to /24 prefix
  "16prefix": lambda ip: ip_prefix(ip, 16), # map IP to /16 prefix
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
  parser.add_argument("--explore" "-e", default=None,
    help="Explore full intersections of a specific date/key.")

  ARGS = parser.parse_args()
  
  # Function to transform input IP addresses to comparable format
  transform = IP_TRANSFORMS[ARGS.compare]

  # Produce all possible combinations of elements of iterable
  def all_combinations(iterable):
    combos = []
    for i in range(1, len(iterable)+1):
      combos += list(itertools.combinations(iterable, i))
    return combos

  def process_row(row, keyfunc=lambda r: r[0], valuefunc=lambda r: r[1].strip()):
    key = keyfunc(row)
    values = valuefunc(row)
    valuelist = values.strip(ARGS.inner_delimiter).split(ARGS.inner_delimiter)
    valuelist = map(transform, valuelist)
    return key, collections.Counter(valuelist)

  # A mapping of {input-filename -> {date -> counter of identifiers}}
  groups = {}

  # Keep track of all seen keys (first value in each row, e.g. scan date)
  # Also keep track of their order, so we output in the same order as keys were
  # seen, in order of the list of input files
  keys_seen = set()
  keys = []

  # Read input files into data structure
  for infile in ARGS.infiles:
    with infile as inf:
      reader = csv.reader(inf, delimiter=ARGS.delimiter)
      table = {}
      for row in reader:
        key, valueset = process_row(row)
        if key not in keys_seen:
          keys_seen.add(key)
          keys.append(key)
        table[key] = valueset
      groups[infile.name] = table
    
  # Generate all possible combinations of the input files
  combos = all_combinations(groups.keys())

  # Output writer
  outfields = ["key"]+[ARGS.inner_delimiter.join(combo) for combo in combos]
  writer = csv.DictWriter(sys.stdout, fieldnames=outfields,
      delimiter=ARGS.delimiter,  lineterminator="\n")
  writer.writeheader()

  if ARGS.ignore_missing_keys:
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
    all_ok = True
    for fname, sets in groups.items():
      if sets.keys() != keys_seen:
        all_ok = False
        missing_keys = keys_seen - sets.keys()
        for k in sorted(missing_keys):
          logging.fatal("%s missing key %s", fname, k)
    if not all_ok:
      raise Exception("Key mismatch in input files")

  # Loop over keys (e.g. dates) in order 
  for key in sorted(keys):
    # Produce row-wise intersections for the same key for all possible
    # combinations of input files
    rowvalues = {'key': key}
    for combo in combos:
      isect = None
      for fname in combo:
        if isect is None:
          isect = groups[fname][key].keys()
        else:
          isect &= groups[fname][key].keys()
      rowvalues[ARGS.inner_delimiter.join(combo)] = len(isect)
    writer.writerow(rowvalues)
