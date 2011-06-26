import io
import random
import json

INPUT = "../dummy.output"
OUTPUT = "../JSON.real"
data = {}
f = file(INPUT)

data = {}

currentYear = 0
lines = f.readlines()
for l in lines:
    
    
    entries = l.split('\t')
    year = entries[0]
   
    
    jsonstring = entries[1]
    if year in data.keys():
        data[year].append(jsonstring)
    else:
        data[year] = [jsonstring]
   
        
f.close()

for year in data.keys():
    g = file(OUTPUT + "." + year, 'w')
    g.write("parseData([")
    for jsonstring in data[year]:
        g.write(jsonstring)    
        g.flush()

    g.write("])\n")
    g.flush()
    g.close()

