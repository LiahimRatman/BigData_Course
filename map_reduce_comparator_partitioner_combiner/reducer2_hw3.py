#!/usr/bin/env python3
import sys


current_year = None

for line in sys.stdin:
    year, counts, tag, _ = line.strip().split("\t")
    if year == current_year:
        if tag_count < 10:
            tag_count += 1
            print(year, tag, counts, sep='\t')
        else:
            pass
    else:
        print(year, tag, counts, sep='\t')
        tag_count = 1
        current_year = year
