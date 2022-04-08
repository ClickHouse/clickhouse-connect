
import array
from datetime import datetime

b = bytearray()

start = datetime.now()
for x in range(10000):
    b = bytearray()
    a = array.array('H', list(range(5000)))
    b += a
print (str(len(b)) + ' ' + str(datetime.now() - start))


start = datetime.now()

for x in range(10000):
    b = bytearray()
    for y in range(5000):
        b.extend(y.to_bytes(2, 'little'))
print (str(len(b)) + ' ' + str(datetime.now() - start))
