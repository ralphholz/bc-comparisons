.PHONY: deps clean pip

deps: pip

# TODO: task to clone btccrawlgo-processing into parent dir and pip install 

pip:
	pip install -r requirements.txt
	
clean:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name 'rib.*.bz2' -exec rm -f {} +
