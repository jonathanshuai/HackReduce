#!/usr/bin/env python

from operator import itemgetter
import sys

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    data = line.split('\t')
    name, population, latitude, longitude, cityID = data[1], data[-5], data[4], data[5], data[0]
    if name.count(' ') <= 1:
        print '%s\t%s\t%s\t%s\t%s'%(name, population, latitude, longitude, cityID)
