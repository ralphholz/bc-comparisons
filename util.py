#!/usr/bin/env python3

import re
import logging
import doctest
import ipaddress

from datetime import datetime

LOG_FMT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
LOG_LEVEL = logging.INFO

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

if __name__ == "__main__":
    doctest.testmod()
