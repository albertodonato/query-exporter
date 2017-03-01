PYTHON = python3
SETUP = $(PYTHON) setup.py
LINT = $(PYTHON) -m flake8


all: build

build:
	$(SETUP) build

devel:
	$(SETUP) develop

clean:
	rm -rf build *.egg-info
	find . -type d -name __pycache__ | xargs rm -rf

test:
	@$(PYTHON) -m unittest

coverage:
	@coverage run -m unittest
	@coverage report --show-missing --skip-covered --fail-under=100 \
		--include=query_exporter/\* --omit=\*\*/tests/\*

lint:
	@$(LINT) setup.py query_exporter

.PHONY: build devel clean test coverage lint
