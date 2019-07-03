#!/bin/bash

set -e
set -x

DBNAME="ip2location.sqlite"
SCHEMA="schema.sql"
FULL_CSV="IPV6-COUNTRY-REGION-CITY-LATITUDE-LONGITUDE-ISP-DOMAIN-MOBILE-USAGETYPE.CSV"
DATA_CSV="ipv4only.csv"
TABLE_NAME="ip2location_db23"

# # First create a version of the DB containing only IPv4 addresses, since IPv6
# # addresses are too long to store in a SQLite integer field
# sed -r 's/^"([^"]+)","([^"]+)"/\1,\2/' $FULL_CSV | 
#   awk -F, '$2 <= 281474976710655' > $DATA_CSV

# Next, create the database (delete if already exists)
rm -rf $DBNAME
sqlite3 $DBNAME < $SCHEMA

# Now, load data
sqlite3 $DBNAME <<- EOF 
.separator ,
.import ${DATA_CSV} ${TABLE_NAME}
EOF
