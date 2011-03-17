#!/bin/bash

# Just pass all arguments straight through
python -m ion.test.load_runner -s -c ion.test.loadtests.ccbrokerload.CCBrokerTest $@