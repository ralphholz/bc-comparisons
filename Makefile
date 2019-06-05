.PHONY: deps clean pip ipasn-rib

deps: pip ipasn.dat

pip:
	pip install -r requirements.txt
	
ipasn-rib:
	find . -name 'rib.*.bz2' -exec rm -f {} +
	pyasn_util_download.py --latest

ipasn.dat: ipasn-rib
	$(eval RIB_DATE = $(shell find . -name 'rib.*.bz2' | head -1 | sed -r 's/.+([0-9]{8})\..*/\1/'))
	pyasn_util_convert.py --single rib.*.bz2 ipasn-$(RIB_DATE).dat
	find . -name 'ipasn.dat' -exec rm -f {} +
	ln -s ./ipasn-*.dat ./ipasn.dat
	find . -name 'rib.*.bz2' -exec rm -f {} +

clean:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name 'rib.*.bz2' -exec rm -f {} +
	find . -name 'ipasn*.dat' -exec rm -f {} +
