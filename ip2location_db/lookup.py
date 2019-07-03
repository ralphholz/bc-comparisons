#!/usr/bin/env python3

import re
import sys
import sqlite3
import ipaddress

# Field names
FIELD_NAMES = (
  "ip_from",
  "ip_to",
  "country_code",
  "country_name",
  "region_name",
  "city_name",
  "latitude",
  "longitude",
  "isp",
  "domain",
  "mcc",
  "mnc",
  "mobile_brand",
  "usage_type",
)

# Usage type strings from IP2Location docs
USAGE_TYPE = {
  "COM": "Commercial",
  "ORG": "Organization",
  "GOV": "Government",
  "MIL": "Military",
  "EDU": "University/College/School",
  "LIB": "Library",
  "CDN": "Content Delivery Network",
  "ISP": "Fixed Line ISP",
  "MOB": "Mobile ISP",
  "ISP/MOB": "Mobile ISP",
  "DCH": "Data Center/Web Hosting/Transit",
  "SES": "Search Engine Spider",
  "RSV": "Reserve",
   "-" : "Unknown",
}

# Regexp matching an IPv4 address in IPv6 format
IPV4_AS_IPV6 = re.compile("::ffff:([0-9]{1,3}\.){3}[0-9]{1,3}")

# int_to_ip regexps
REG1 = re.compile("(.{4})")
REG2 = re.compile(":$")

def ipv4_to_int(ipv4:str):
  return int(ipaddress.IPv6Address("::ffff:"+ipv4))

def int_to_ip(number:int):
  retval = format(number, 'x')
  retval = retval.zfill(32)
  retval = REG1.sub(r"\1:", retval)
  retval = REG2.sub("", retval)
  ipv6 = ipaddress.IPv6Address(retval)
  if ipv6.ipv4_mapped:
    return str(ipv6.ipv4_mapped)
  return str(retval)

# Transformations to make field values human-readbale
FIELD_TRANSFORMS = {
  "ip_from": int_to_ip,
  "ip_to": int_to_ip,
  "usage_type": lambda u: USAGE_TYPE[u],
}

class IP2Loc:
  LOOKUP_QUERY = "SELECT * FROM {} WHERE ? <= ip_to LIMIT 1"

  def __init__(self, dbfile, table="ip2location_db23"):
    self.conn = sqlite3.connect(dbfile)
    self.table = table

  def _query(self, ip:int):
    c = self.conn.cursor()
    c.execute(self.LOOKUP_QUERY.format(self.table), (ip,))
    res = c.fetchone()
    c.close()
    if not res or len(res) == 0:
      return None
    return res

  def lookupv4(self, ipv4:str, transform:bool=True):
    ipnum = int(ipaddress.IPv6Address("::ffff:"+ipv4))
    res = dict(zip(FIELD_NAMES, self._query(ipnum)))
    if transform:
      for fname, t in FIELD_TRANSFORMS.items():
        res[fname] = t(res[fname])
    return res

  def close(self):
    self.conn.close()

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("Usage: lookup.py IPV4_ADDRESS")
    sys.exit(1)
  import json
  db = IP2Loc("./ip2location.sqlite")
  res = db.lookupv4(sys.argv[1])
  print(json.dumps(res, indent=2))
