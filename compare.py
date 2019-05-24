#!/usr/bin/env python3

import sys
import csv
import argparse
import itertools
import collections

# This script computes, row-wise based on a join key in field 0, the
# intersections of all possible combinations of the input files
# Input format: TSV e.g.
#     2019-05-14	8.8.8.8;8.8.4.4;8.8.8.8
# Output format: TSV e.g.
#     key	sample1.tsv	sample2.tsv	sample1.tsv;sample2.tsv
#     2019-05-14	2	3	1

parser = argparse.ArgumentParser()
parser.add_argument("infiles", nargs="*", type=argparse.FileType("r"))
parser.add_argument("--delimiter", "-d", default="\t",
  help="Input and output field delimiter (tab by default)")
parser.add_argument("--inner-delimiter", "-id", default=";", 
  help="Delimiter to use for lists within a field (; by default)")

ARGS = parser.parse_args()

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
outfields = ['key']+[ARGS.inner_delimiter.join(combo) for combo in combos]
writer = csv.DictWriter(sys.stdout, fieldnames=outfields,
    delimiter=ARGS.delimiter, dialect=csv.unix_dialect)
writer.writeheader()

# Sanity check: we should never have a key (e.g. date) that appears in one
# input file but not another
for fname, sets in groups.items():
  assert key in sets

# Loop over keys (e.g. dates) in order 
for key in keys:
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
