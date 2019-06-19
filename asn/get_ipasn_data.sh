#!/bin/bash

set -e

NOT_BEFORE="2019-01-21"
NOT_AFTER=`date --date=yesterday --rfc-3339=date`

DATES_TO_DOWNLOAD_FNAME="dates_to_download"

# Generate dates to download (only dates that haven't already been downloaded)
python3 generate_pyasn_dates.py --not-before $NOT_BEFORE --not-after $NOT_AFTER > $DATES_TO_DOWNLOAD_FNAME

# Download RIB files
pyasn_util_download.py --dates-from-file $DATES_TO_DOWNLOAD_FNAME

# Convert RIB files to IPASN files
while read date; do
  RIBFILE=`find . -name "rib.$date.*.bz2" | head -1`
  if [[ -f "$RIBFILE" ]]; then
    pyasn_util_convert.py --single $RIBFILE ipasn_$date.dat
  fi
done < $DATES_TO_DOWNLOAD_FNAME
