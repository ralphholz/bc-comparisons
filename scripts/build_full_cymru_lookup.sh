#!/bin/bash

# Run this script from the parent directory

set -e

NOT_BEFORE="2019-01-21"
NOT_AFTER="2019-05-31"
DOWNSAMPLE="12:00:00"

BTC_PATH="/srv/hdd/autodownloads/blockchain-observatory/digitalocean-btc-measurements/logs"
LTC_PATH="/srv/hdd/autodownloads/blockchain-observatory/digitalocean-ltc-measurements/logs"
YETHI_PATH="/srv/hdd/autodownloads/blockchain-observatory/yethi-measurements/results"

# Generate cymru bulk lookup format from scans

./select_scans.py \
  --format Yethi \
  --not-before $NOT_BEFORE \
  --not-after $NOT_AFTER \
  --downsample $DOWNSAMPLE \
  $YETHI_PATH |
./aggregate_scans.py --format Yethi -onode -oport - | 
./scripts/make_cymru_lookup.py > ./scripts/cymru.dat

./select_scans.py \
  --format BTC \
  --not-before $NOT_BEFORE \
  --not-after $NOT_AFTER \
  --downsample $DOWNSAMPLE \
  $BTC_PATH |
./aggregate_scans.py --format BTC -onode -oport - | 
./scripts/make_cymru_lookup.py >> ./scripts/cymru.dat

./select_scans.py \
  --format LTC \
  --not-before $NOT_BEFORE \
  --not-after $NOT_AFTER \
  --downsample $DOWNSAMPLE \
  $LTC_PATH |
./aggregate_scans.py --format LTC -onode -oport - | 
./scripts/make_cymru_lookup.py >> ./scripts/cymru.dat

# De-duplicate
sort -u ./scripts/cymru.dat > ./scripts/cymru.dat.unique

# Create final lookup file
echo "begin" > ./scripts/cymru.dat
echo "verbose" >> ./scripts/cymru.dat
cat ./scripts/cymru.dat.unique >> ./scripts/cymru.dat
echo "end" >> ./scripts/cymru.dat
