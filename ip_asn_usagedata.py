import csv
import sys
import json

import util

from ip2location_db import lookup

as_names = json.loads(open("asnames.dat").read())

def as_name(asnum):
  if asnum is None:
    return None
  elif str(asnum) in as_names:
    return as_names[str(asnum)]
  return "Unknown"

iploc = lookup.IP2Loc("./ip2location_db/ip2location.sqlite")

ASNDATE = sys.argv[1]

outwriter = csv.writer(sys.stdout, lineterminator="\n")

for ip in sys.stdin:
  ip = ip.strip()
  asn = util.ip2asn(ip, ASNDATE)
  iploc = iploc.lookupv4(ip)
  outwriter.writerow((ip, asn, as_name(asn), 
    iploc["country_name"], iploc["isp"], iploc["domain"], iploc["usage_type"],
    ))
