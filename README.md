WIP

```
# output is map of selected file to date
./select_scanfiles.py --format=LTC --downsample="12:00:00" --not-before= --not-after= /srv/hdd/ltc

# input is map of file to date
# output is CSV of date, list of node identifiers
./scanfiles_to_nodes_grouped_by_date.py --format=LTC # stdin is list of file paths, stdout is CSV of date

# input is output of process_scanfiles
# output is node intersection sizes for each date
./compare.py 
```
