#!/bin/sh
set -e  
test -f attachEIPAndCreate53Record.zip && rm -f attachEIPAndCreate53Record.zip
zip -j attachEIPAndCreate53Record.zip attachEIPAndCreate53Record.py
