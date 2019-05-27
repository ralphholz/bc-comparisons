#!/usr/bin/env python3

import csv
import glob
import logging
import datetime
import argparse

from datetime import datetime
from os import path

class ScansLoader():
  def __init__(self, scans_dir):
    self.scans_dir = scans_dir
    self.scanfiles = self.__list_scanfiles()

  def __list_scanfiles(self):
    raise NotImplementedError
  
  def filedt(self, scanfile):
    """Extract UTC datetime from given scan file path"""
    raise NotImplementedError

  def filter(self, not_before:str=None, not_after:str=None, custom=None):
    """Remove scan files not meeting time restrictions or custom condition.
    not_before and not_after should be type datetime in UTC."""
    if not_before is None and not_after is None and custom is None:
      return
    filtered = self.scanfiles
    if not_before is not None:
      filtered = filter(lambda sf: filedt(sf) >= not_before, filtered)
    if not_after is not None:
      filtered = filter(lambda sf: filedt(sf) <= not_after, filtered)
    if custom is not None:
      filtered = filter(custom, filtered)
    self.scanfiles = list(filtered)

  def downsample(self, targets=("12:00:00")):
    """Downsample scan files by taking nearest scan to each target time in
    24-hour HH:MM:SS format in each UTC day. This is NOT order-preserving."""
    def find_closest(scans, day, target):
        target_dt = util.time2dt(target)
        return min(scans, key=lambda s: abs(self.filedt(sf) - target_dt))
    downsampled = []
    sf_by_date = self.scanfiles_by_date()
    for day, scans in sf_by_date.items():
      closest = {find_closest(scans, day, target) for target in targets}
      if len(closest) != len(targets):
        logging.warning(day, "has only", len(closest), "of", len(targets),
            "scans after downsampling!")
      for sf in closest:
        downsampled.append(sf)
    self.scanfiles = downsampled()

  def __sort_scanfiles(self, scanfiles, reverse=False):
    return sorted(self.scanfiles, key=lambda sf: filedt(sf), reverse=reverse)

  def sort(self, reverse=False):
    """Sort the list of scan files by date"""
    self.scanfiles = self.__sort_scanfiles()

  def scanfiles_by_date(self):
    """Group scan files by ISO date string (YYYY-MM-DD), return a dict 
    {date -> list of scanfiles}. This method is order-preserving."""
    sf_by_date = collections.defaultdict(list)
    for sf in self.scanfiles:
      day = filedt(sf).isoformat().split("T")[0]
      sf_by_date[day].append(sf)
    return sf_by_date

class YethiScansLoader(ScansLoader):
  FILE_GLOB = "/*/confirmed.csv.xz"

  def filedt(self, scanfile):
    return datetime.utcfromtimestamp(int(scanfile.split('/')[-2]))

  def __list_scanfiles(self):
    return glob.glob(path.join(self.scans_dir, FILE_GLOB))

FORMAT_LOADERS = {
    "Yethi": YethiScansLoader,
}

parser = argparse.ArgumentParser()

# Optional args
parser.add_argument("--delimiter", "-d", default="\t",
  help="Input and output field delimiter (tab by default)")
parser.add_argument("--inner-delimiter", "-id", default=";", 
  help="Delimiter to use for lists within a field (; by default)")
parser.add_argument("--not-before", "-nb", default=None,
  help="Don't include scan files before the given UTC ISO date/time string")
parser.add_argument("--not-after", "-na", default=None,
  help="Don't include scan files after the given UTC ISO date/time string")
parser.add_argument("--downsample", "-ds", default="10:00:00",
  help="Comma-separated list of 24-hour times in HH:MM:SS format. "
       "Downsample scans by selecting closest scans to each of these times each day.")

# Required args
parser.add_argument("--format", "-f", choices=list(FORMAT_LOADERS.keys()), 
  help="Format of scan files.", required=True)
parser.add_argument("scan_dir",
  help="Full path to directory containing scan files")

ARGS = parser.parse_args()

loader = FORAMT_LOADERS[ARGS.format]
print(loader.scanfiles_by_date)
