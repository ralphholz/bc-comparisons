#!/usr/bin/env python3

import re
import sys
import csv
import glob
import logging
import datetime
import argparse
import collections

from datetime import datetime
from os import path

import util

# Class that handles all logic of enumerating, downsampling, and filtering
# files/directories from the results of one scanner.
class ScansLoader:
    def __init__(self, scans_dir):
        self.scans_dir = scans_dir
        self.scanfiles = self._list_scanfiles()
        logging.debug("Loaded scanfiles: %s", ";".join(self.scanfiles))

    def _list_scanfiles(self):
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
            filtered = filter(lambda sf: self.filedt(sf) >= not_before, filtered)
            logging.info("Filtering out scans before %s", not_before.isoformat())
        if not_after is not None:
            filtered = filter(lambda sf: self.filedt(sf) <= not_after, filtered)
            logging.info("Filtering out scans after %s", not_after.isoformat())
        if custom is not None:
            logging.info("Running custom filter")
            filtered = filter(custom, filtered)
        # Run the filters and return a new list
        self.scanfiles = list(filtered)

    def downsample(self, targets=("12:00:00")):
        """Downsample scan files by taking nearest scan to each target time in
        24-hour HH:MM:SS format in each UTC day. This is NOT order-preserving."""
        def find_closest(scans, day, target):
            target_dt = util.time2dt(target, day)
            return min(scans, key=lambda sf: abs(self.filedt(sf) - target_dt))
        downsampled = []
        sf_by_date = self.scanfiles_by_date()
        for day, scans in sf_by_date.items():
            closest = {find_closest(scans, day, target) for target in targets}
            if len(closest) != len(targets):
                logging.warning("%s has only %s of %s scans after downsampling!", 
                                day, len(closest), len(targets))
            for sf in closest:
                downsampled.append(sf)
        self.scanfiles = downsampled

    def __sort_scanfiles(self, scanfiles, reverse=False):
        return sorted(self.scanfiles, key=lambda sf: self.filedt(sf), reverse=reverse)

    def sort(self, reverse=False):
        """Sort the list of scan files by date"""
        logging.info("Sorting scan files (reverse={})".format(str(reverse)))
        self.scanfiles = self.__sort_scanfiles(self.scanfiles)

    def scanfiles_by_date(self):
        """Group scan files by ISO date string (YYYY-MM-DD), return a dict 
        {date -> list of scanfiles}. This method is order-preserving."""
        sf_by_date = collections.defaultdict(list)
        for sf in self.scanfiles:
            day = self.filedt(sf).isoformat().split("T")[0]
            sf_by_date[day].append(sf)
        return sf_by_date

    def scanfiles_and_isotimes(self):
        for sf in self.scanfiles:
            yield (self.filedt(sf).isoformat(), sf)


class YethiScansLoader(ScansLoader):
    FILE_GLOB = "*/confirmed.csv.xz"

    def filedt(self, scanfile):
        return datetime.utcfromtimestamp(int(scanfile.split("/")[-2].strip()))

    def _list_scanfiles(self):
        return glob.glob(path.join(self.scans_dir, self.FILE_GLOB))


class BtcScansLoader(ScansLoader):
    FILE_GLOB = "log-*/"

    def filedt(self, scanfile):
        dirname = scanfile.strip("/").split("/")[-1].strip()
        # Remove the "log-" prefix from dirname
        dt_components = dirname.replace("log-", "").split("T")
        # If this assertion fails, the glob is matching something that isn't a
        # scan dir, or a scan directory name is not formatted correctly
        assert len(dt_components) == 2
        # Replace the dashes with colons in time part of dirname
        dt_components[1] = dt_components[1].replace("-", ":")
        isofmt = "T".join(dt_components)
        return util.str2dt(isofmt)

    def _list_scanfiles(self):
        return list(map(lambda f: f.strip('/'),
            glob.glob(path.join(self.scans_dir, self.FILE_GLOB))))


class LtcScansLoader(BtcScansLoader):
    pass

FORMAT_LOADERS = {
    "Yethi": YethiScansLoader,
    "BTC": BtcScansLoader,
    "LTC": LtcScansLoader,
}

if __name__ == "__main__":
    # Configure logging module
    logging.basicConfig(filename="select_scans.log", 
        format=util.LOG_FMT, level=logging.DEBUG)

    parser = argparse.ArgumentParser()

    # Optional args
    parser.add_argument("--delimiter", "-d", default="\t",
      help="Output field delimiter (tab by default)")
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

    logging.debug("===STARTUP===")

    ARGS = parser.parse_args()

    # Initialize TSV output writer
    writer = csv.writer(sys.stdout, delimiter=ARGS.delimiter)

    # Initialize correct loader for selected scanfile type
    loader_cls = FORMAT_LOADERS[ARGS.format]
    loader = loader_cls(ARGS.scan_dir)

    # Filter
    not_before_dt = util.str2dt(ARGS.not_before) if ARGS.not_before is not None else None
    not_after_dt = util.str2dt(ARGS.not_after) if ARGS.not_after is not None else None
    loader.filter(not_before_dt, not_after_dt)

    # Downsample
    TIME_RE = re.compile('[0-9]{2}:[0-9]{2}:[0-9]{2}(,[0-9]{2}:[0-9]{2}:[0-9]{2})*')
    if TIME_RE.fullmatch(ARGS.downsample.strip()):
        downsample_targets = ARGS.downsample.strip().split(',')
        logging.info("Downsampling to times: %s", ", ".join(downsample_targets))
        loader.downsample(targets=downsample_targets)

    # Sort by time
    loader.sort()

    # Produce output
    for iso_sf in loader.scanfiles_and_isotimes():
        writer.writerow(iso_sf)

    logging.debug("===FINISH===")
