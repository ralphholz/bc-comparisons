Tools for processing raw data from our research blockchain scanners/crawlers.

# The Idea

- Select the scans you want to operate on using `select_scans.py`.
- Use the output of `select_scans.py` as the input for the next step.
- Manually inspect contents of a scan (i.e., confirmed nodes) using `./load_scan.py`
- Compute, compare and inspect date-wise set intersections using `./compare.py`

For example, `aggregate_scans.py` expects a TSV input containing the fields:
1. date
2. semicolon-separated list of scan files

# Dependencies

Run `make deps`.

# Usage Examples

## select_scans.py

A tool for enumerating raw scan data. Works with scan file paths/names, but
doesn't read the actual scan data.

### Example: List Campaigns

```
./select_scans.py --format Yethi --campaigns --downsample=F /srv/hdd/autodownloads/blockchain-observatory/yethi-measurements/results
```

```
./select_scans.py --format BTC --campaigns --downsample=F /srv/hdd/autodownloads/blockchain-observatory/digitalocean-btc-measurements/logs
```

```
./select_scans.py --format LTC --campaigns --downsample=F /srv/hdd/autodownloads/blockchain-observatory/digitalocean-ltc-measurements/logs
```

### Example: List Scans in Date Range

```
./select_scans.py --format Yethi /srv/hdd/autodownloads/blockchain-observatory/yethi-measurements/results --not-before 2019-01-01 --not-after 2019-01-31 --each-scan --downsample=F
```

### Example: Downsample and Group Scans by Date for Input to aggregate_scans.py

```
./select_scans.py --format Yethi /srv/hdd/autodownloads/blockchain-observatory/yethi-measurements/results --not-before 2019-02-01 --not-after 2019-05-31 --downsample "12:00:00"
```

## aggregate_scans.py

Loads nodes from scan files and produces TSV output containing, for each date,
a list of confirmed nodes.

### Example: list unique node IP addresses for each day in Feb 2019 from Yethi scans

```
./select_scans.py --format Yethi /srv/hdd/autodownloads/blockchain-observatory/yethi-measurements/results --not-before 2019-02-01 --not-after 2019-02-28 --downsample "12:00:00" | ./aggregate_scans.py --format Yethi --omit-nodeid --omit-port --dedupe-output-nodes -
```
