#!/usr/bin/env python

from operator import itemgetter
import sys

citypopulation = {}

def parseLine(line):
    # remove leading and trailing whitespace
    line = line.strip()
    data = line.split('\t')
    name, population, latitude, longitude, cityID = data[0], int(data[1]), data[2], data[3], data[4]
    return name, population, latitude, longitude, cityID
    

for line in sys.stdin:
    name, population, latitude, longitude, cityID = parseLine(line);
    p = citypopulation.get(name, (0, None, None, None, None))[0]
    if population > p:
        citypopulation[name] = (population, latitude, longitude, cityID)

sorted_citypop = sorted(citypopulation.items(), key=itemgetter(0))

for k, v in sorted_citypop:
    print "%s\t%d\t%s\t%s\t%s"%(k, v[0], v[1], v[2], v[3])
