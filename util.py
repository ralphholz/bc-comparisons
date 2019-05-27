import re
from datetime import datetime

LOG_FMT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"

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
