#!/usr/bin/python
"""mapper.py"""
import sys
import random


for line in sys.stdin:
    prefix = random.randint(0, 9)
    print('%s\t%s' % (str(prefix), line.strip()))
