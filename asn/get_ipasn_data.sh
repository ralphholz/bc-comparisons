#!/bin/bash

set -e

NOT_BEFORE="2019-01-21"
NOT_AFTER=`date --rfc-3339=date`

python3 generate_pyasn_dates.py --not-before $NOT_BEFORE --not-after $NOT_AFTER > dates_to_download
pyasn_util_download.py --dates-from-file dates_to_download
pyasn_util_convert.py --bulk $NOT_BEFORE $NOT_AFTER
