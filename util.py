#!/usr/bin/env python3

import re
import pyasn
import logging
import doctest
import itertools
import ipaddress

from datetime import datetime
from collections import Counter

ASN_DB = "ipasn.dat"

LOG_FMT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
LOG_LEVEL = logging.DEBUG

def time2dt(timestr:str, daystr:str):
    """timestr should be 24-hour time string in format HH:MM:SS
    daystr should be in format YYYY-MM-DD."""
    isoutc = "{}T{}Z".format(daystr, timestr)
    return datetime.strptime(isoutc, "%Y-%m-%dT%H:%M:%SZ")

def str2dt(dtstr:str):
    just_date = re.compile("[0-9]{4}(-[0-9]{2}){2}")
    date_and_time = re.compile("[0-9]{4}(-[0-9]{2}){2}T[0-9]{2}(:[0-9]{2}){2}Z?")

    if just_date.fullmatch(dtstr):
        fstr = "{}T{}Z".format(dtstr, "00:00:00")
        return datetime.strptime(fstr, "%Y-%m-%dT%H:%M:%SZ")

    elif date_and_time.fullmatch(dtstr):
        fstr = dtstr
        if not fstr.endswith("Z"):
            fstr += "Z"
        return datetime.strptime(fstr, "%Y-%m-%dT%H:%M:%SZ")

    logging.fatal("time2dt: Invalid date/time string %s", str(dtstr))
    raise ValueError("Invalid date/time string: " + str(dtstr))

def parse_ip_port_pair(pair: str):
    """
    Parse a string containing an IPv4 or v6 address and port, separated by
    a colon (:) into a tuple containing (IP, port)
    >>> parse_ip_port_pair("1.1.1.1:8080")
    ('1.1.1.1', '8080')
    >>> parse_ip_port_pair("[2001:56b:dda9:4b00:49f9:121b:aa9e:de30]:1199")
    ('[2001:56b:dda9:4b00:49f9:121b:aa9e:de30]', '1199')
    """
    components = pair.split(":")
    ip = ":".join(components[:-1])
    port = components[-1]
    return (ip, port,)

def is_ipv4(ip: str):
    """
    >>> is_ipv4("8.8.8.8")
    True
    >>> is_ipv4("[2001:56b:dda9:4b00:49f9:121b:aa9e:de30]")
    False
    >>> is_ipv4("foo")
    False
    """
    try:
        return type(ipaddress.ip_address(ip)) is ipaddress.IPv4Address
    except:
        return False

def yethi_scanfile_dt(scanfile: str):
    return datetime.utcfromtimestamp(int(scanfile.split("/")[-2].strip()))

def btc_scanfile_dt(scanfile: str):
    dirname = scanfile.rstrip("/").split("/")[-1].strip()
    # Remove the "log-" prefix from dirname
    dt_components = dirname.replace("log-", "").split("T")
    # If this assertion fails, the glob is matching something that isn't a
    # scan dir, or a scan directory name is not formatted correctly
    assert len(dt_components) == 2
    # Replace the dashes with colons in time part of dirname
    dt_components[1] = dt_components[1].replace("-", ":")
    isofmt = "T".join(dt_components)
    return str2dt(isofmt)

def counter_isect(*args):
  """
  Accepts some collections.Counter objects and returns the intersection of
  their keys, with the counts being the total count within the intersection.
  >>> c1 = Counter([1,1,1,2,2,3,4,5])
  >>> c2 = Counter([1,1,2,7,8,9])
  >>> c3 = Counter([10,11,12])
  >>> sorted(counter_isect(c1, c2, c3).items())
  []
  >>> sorted(counter_isect(c1, c2).items())
  [(1, 5), (2, 3)]
  """
  assert len(args) >= 1

  # Compute intersection of counter keys
  isect = args[0].keys()
  for c in args[1:]:
    isect &= c.keys()

  # Remove keys that aren't in the intersection
  filtered = [Counter({k: v for k, v in c.items() if k in isect}) for c in args]

  # Add all the filtered counters
  res = filtered[0]
  for fc in filtered[1:]:
    res += fc
  return res

def ip2asn(ip, asndb=[]):
  try:
    if len(asndb) == 0:
      logging.info("Loading IPASN database %s", ASN_DB)
      asndb.append(pyasn.pyasn(ASN_DB))
    asndb = asndb[0]
    asn = asndb.lookup(ip)
    if asn[0] is None:
      logging.fatal("util.ip2asn: unknown ASN for IP %s", ip)
      raise KeyError("Unknown ASN for IP {}".format(ip))
    return asn[0]
  except:
    logging.fatal("util.ip2asn: unknown ASN for IP %s", ip)
    raise KeyError("Unknown ASN for IP {}".format(ip))
  logging.fatal("util.ip2asn: unknown ASN for IP %s", ip)
  raise KeyError("Unknown ASN for IP {}".format(ip))

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

# Produce all possible combinations of elements of iterable
def all_combinations(iterable):
  """
  Produce all (|i| C 1) + (|i| C 2) + ... + (|i| C |i|) selections of elements
  from a given iterable i with length |i|.
  >>> all_combinations([1,2,3])
  [(1,), (2,), (3,), (1, 2), (1, 3), (2, 3), (1, 2, 3)]
  """
  combos = []
  for i in range(1, len(iterable)+1):
    combos += list(itertools.combinations(iterable, i))
  return combos

if __name__ == "__main__":
    doctest.testmod()
