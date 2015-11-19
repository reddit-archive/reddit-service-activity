all: thrift

.PHONY: thrift
thrift:
	python setup.py build
