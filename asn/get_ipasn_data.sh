#!/bin/bash

NOT_BEFORE="2019-06-15"
NOT_AFTER="2019-06-17"

./generate_pyasn_dates.py --not-before $NOT_BEFORE --not-after $NOT_AFTER > dates_to_download
pyasn_util_download.py --dates-from-file dates_to_download
pyasn_util_convert.py --bulk $NOT_BEFORE $NOT_AFTER
