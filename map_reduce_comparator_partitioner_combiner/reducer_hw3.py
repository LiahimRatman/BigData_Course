#!/usr/bin/env python3
import sys

current_word = None
current_year = None
word_count = 0

for line in sys.stdin:
    year, tag, counts = line.split("\t")
    counts = int(counts)
    if tag == current_word and year == current_year:
        tag_count += counts
    else:
        if current_word:
            print(current_year, current_word, tag_count, sep="\t")
        current_word = tag
        current_year = year
        tag_count = counts

if current_word:
    print(current_year, current_word, tag_count, sep="\t")
