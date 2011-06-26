import io
import random
import json

INPUT = "../datasets/geonames/cities1000.txt"
OUTPUT = "../datasets/geonames/fake.JSON"
data = []
f = file(INPUT)
g = file(OUTPUT,'w')

g.write("parsedata(")

lines = f.readlines()
for l in lines[0:2]:
    
 
    
    entries = l.split('\t')
    ident = entries[0]
    name = entries[2]
    latitude = entries[-15]
    longitude = entries[-14]
    for year in xrange(1990,2000):
        count = random.randint(100,10000)
        smallEntry = {"id":ident,"city":name,"long":longitude,"lat":latitude,"year":year,"count":count}
        data.append(smallEntry)
    
    
    
dump = json.dumps(data)
g.write(dump)
g.write(")\n")
g.flush()
g.close()
f.close()

