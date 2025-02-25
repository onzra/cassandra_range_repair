PACKAGE := rangerepair
.PHONY: clean-pyc clean-build

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "test - run tests quickly with the default Python"
	@echo "release - package and upload a release"

clean: clean-build clean-pyc clean-pkg

clean-build:
	rm -rf .tox/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf *.egg

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +

clean-pkg:
	rm -rf debian/$(PACKAGE)
	rm -rf debian/$(PACKAGE).debhelper.log
	rm -rf debian/$(PACKAGE).substvars
	rm -rf debian/$(PACKAGE).postinst.debhelper
	find ../ -maxdepth 1 -iname '$(PACKAGE)_*_amd64.changes' -exec rm -f {} +
	find ../ -maxdepth 1 -iname '$(PACKAGE)_*_amd64.deb' -exec rm -f {} +
	find ../ -maxdepth 1 -iname '$(PACKAGE)_*.dsc' -exec rm -f {} +
	find ../ -maxdepth 1 -iname '$(PACKAGE)_*.tar.gz' -exec rm -f {} +

build: clean
	python3 setup.py build

test:
	python3 setup.py test
	rm -f logfile.count

release: clean
	python3 setup.py sdist

debian: clean-pkg
	sh make_deb.sh

