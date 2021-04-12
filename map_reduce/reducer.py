#!/usr/bin/python
"""reducer.py"""

import sys
import random


current_count = random.randint(1, 5)
data = []
for line in sys.stdin:
    _, id = line.strip().split('\t', 1)
    data.append(id)
    current_count -= 1
    if current_count == 0:
        print(','.join(data))
        data = []
        current_count = random.randint(1, 5)

if data:
    print(','.join(data))
