#!/usr/bin/env python3

import argparse
import datetime

def daterange(fromdate, todate):
  if type(fromdate) is not datetime.datetime:
    fromdate = datetime.datetime.strptime(fromdate, "%Y-%m-%d")
  if type(todate) is not datetime.datetime:
    todate = datetime.datetime.strptime(todate, "%Y-%m-%d")
  yield fromdate
  while (todate - fromdate).days > 0:
    fromdate += datetime.timedelta(days=1)
    yield fromdate

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--not-before", "-nb", required=True,
  help="Don't include dates before the given UTC ISO date/time string")
  parser.add_argument("--not-after", "-na", default=datetime.datetime.today(),
  help="Don't include dates after the given UTC ISO date/time string")
  ARGS = parser.parse_args()

  for dt in daterange(ARGS.not_before, ARGS.not_after):
    print(dt.strftime("%Y%m%d"))
