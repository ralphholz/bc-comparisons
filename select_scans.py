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

    def campaigns(self, max_allowed_dist_days=4):
        """
        Returns a list of campaigns in the form (start_date, end_date)
        where a gap of longer than max_allowed_dist_days days causes the
        following scan to become part of the next campaign.
        """
        campaigns = []
        prev = None
        campaign_start = None
        
        for dt in map(self.filedt, self.scanfiles):
            # edge case: first campaign
            if campaign_start is None:
                campaign_start = dt
            else:
                if (dt.date() - prev.date()).days > max_allowed_dist_days:
                    camp = (campaign_start.isoformat(), prev.isoformat())
                    campaign_start = dt
                    campaigns.append(camp)
            prev = dt

        # edge case: final campaign
        campaigns.append((campaign_start.isoformat(), prev.isoformat(),))
        return campaigns

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
        prevday = None
        for sf in self.scanfiles:
            day = self.filedt(sf).isoformat().split("T")[0]
            # Check for missing days
            if prevday is not None:
                prevday_dt = util.str2dt(prevday)
                day_dt = util.str2dt(day)
                if (day_dt.date() - prevday_dt.date()).days > 1:
                    logging.warning("Missing data between dates %s and %s", prevday, day)
            sf_by_date[day].append(sf)
            prevday = day
        return sf_by_date

    def scanfiles_and_isotimes(self):
        for sf in self.scanfiles:
            yield (self.filedt(sf).isoformat(), sf)


class YethiScansLoader(ScansLoader):
    FILE_GLOB = "*/confirmed.csv.xz"

    def filedt(self, scanfile):
        return util.yethi_scanfile_dt(scanfile)

    def _list_scanfiles(self):
        return list(map(util.yethi_scanpath,
                        glob.glob(path.join(self.scans_dir,
                                            self.FILE_GLOB))))


class BtcScansLoader(ScansLoader):
    FILE_GLOB = "log-*/"

    def filedt(self, scanfile):
        return util.btc_scanfile_dt(scanfile)

    def _list_scanfiles(self):
        return list(map(lambda f: f.rstrip("/"),
            glob.glob(path.join(self.scans_dir, self.FILE_GLOB))))


# LTC and Dash use same format as Bitcoin
class LtcScansLoader(BtcScansLoader):
    pass

class DashScansLoader(BtcScansLoader):
    pass

class ZecScansLoader(BtcScansLoader):
    pass

FORMAT_LOADERS = {
    "Yethi": YethiScansLoader,
    "BTC": BtcScansLoader,
    "LTC": LtcScansLoader,
    "Dash":  DashScansLoader,
    "ZEC": ZecScansLoader,
}

if __name__ == "__main__":
    # Configure logging module
    logging.basicConfig(#filename="select_scans.log", 
        format=util.LOG_FMT, level=util.LOG_LEVEL)

    parser = argparse.ArgumentParser()

    # Optional args
    parser.add_argument("--delimiter", "-d", default="\t",
      help="Output field delimiter (tab by default)")
    parser.add_argument("--inner-delimiter", "-id", default=";", 
      help="Delimiter to use for lists within a field (; by default)")
    parser.add_argument("--not-before", "-nb", default=None,
      help="Don't include scan files before the given UTC ISO date/time string")
    parser.add_argument("--not-after", "-na", default=None,
      help="Don't include scan files after the given UTC ISO date/time string")
    parser.add_argument("--downsample", "-ds", default="12:00:00",
      help="Comma-separated list of 24-hour times in HH:MM:SS format. "
           "Downsample scans by selecting closest scans to each of these times each day. "
           "default=12:00:00. Set to False to disable.")
    parser.add_argument("--each-scan", "-e", action="store_true",
      help="If given, outputs one scanfile per output row instead of using date buckets.")
    parser.add_argument("--campaigns", "-c", action="store_true",
      help="If given, normal output is suppressed. Instead, the script outputs "
           "start and end dates for each campaign.")
    parser.add_argument("--campaign-dist", "-cd", default=4,
      help="Scans more than this number of days apart will be considered "
           "to be separate campaigns. Default value is 4.")

    # Required args
    parser.add_argument("--format", "-f", choices=list(FORMAT_LOADERS.keys()), 
      help="Format of scan files.", required=True)
    parser.add_argument("scan_dir",
      help="Full path to directory containing scan files")

    logging.debug("===STARTUP===")

    ARGS = parser.parse_args()

    # Initialize TSV output writer
    writer = csv.writer(sys.stdout, delimiter=ARGS.delimiter,
        lineterminator="\n")

    # Initialize correct loader for selected scanfile type
    loader_cls = FORMAT_LOADERS[ARGS.format]
    loader = loader_cls(ARGS.scan_dir)

    # Filter
    not_before_dt = util.str2dt(ARGS.not_before) if ARGS.not_before is not None else None
    not_after_dt = util.str2dt(ARGS.not_after) if ARGS.not_after is not None else None
    loader.filter(not_before_dt, not_after_dt)

    # Downsample
    if not ARGS.downsample.strip().upper().startswith("F"):
        TIME_RE = re.compile('[0-9]{2}:[0-9]{2}:[0-9]{2}(,[0-9]{2}:[0-9]{2}:[0-9]{2})*')
        if TIME_RE.fullmatch(ARGS.downsample.strip()):
            downsample_targets = ARGS.downsample.strip().split(',')
            logging.info("Downsampling to times: %s", ", ".join(downsample_targets))
            loader.downsample(targets=downsample_targets)

    # Sort by time
    loader.sort()

    # Produce output
    if ARGS.campaigns:
        camp = loader.campaigns(ARGS.campaign_dist)
        for c in camp:
            writer.writerow(c)
    elif ARGS.each_scan:
        for iso_sf in loader.scanfiles_and_isotimes():
            writer.writerow(iso_sf)
    else:
        sf_by_date = loader.scanfiles_by_date()
        # Ensure output rows are ordered by ISO date
        for date in sorted(sf_by_date.keys()):
            scanfiles = sf_by_date[date]
            scanfiles_data = ARGS.inner_delimiter.join(scanfiles)
            writer.writerow((date, scanfiles_data,))

    logging.debug("===FINISH===")
