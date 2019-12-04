.PHONY: help clean dev docs package test

help:
	@echo "This project assumes that an active Python virtualenv is present."
	@echo "The following make targets are available:"
	@echo "	 dev 	install all deps for dev env"
	@echo "  docs	create documentation"
	@echo "	 test	run all tests with coverage"

clean:
	rm -rf dist/*

dev:
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pip install -e .

docs:
	$(MAKE) -C docs html

package:
	python setup.py sdist

test:
	@echo "No test to run"
	python setup.py test
	coverage run -m unittest discover
#	coverage html