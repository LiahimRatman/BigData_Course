#!/usr/bin/env python3
import sys


for line in sys.stdin:
    year, tag, count = line.strip().split('\t')
    print(year, count, tag, 1, sep='\t')
