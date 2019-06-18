#!/usr/bin/env python3

import sys
import csv

csv.field_size_limit(sys.maxsize)

reader = csv.reader(sys.stdin, delimiter="\t")
writer = csv.writer(sys.stdout, delimiter=" ")

# writer.writerow(("begin",))
# writer.writerow(("verbose",))
for (date, ips) in reader:
  for ip in ips.split(';'):
    writer.writerow((ip, date,))
# writer.writerow(("end",))
