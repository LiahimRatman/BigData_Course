#!/usr/bin/env python3
import sys
import lxml.etree


for line in sys.stdin:
    try:
        root = lxml.etree.fromstring(line)
        if root.tag == 'row':
            if 'Tags' in root.attrib.keys() and 'CreationDate' in root.attrib.keys():
                tmp = root.attrib['CreationDate'][:4]
                if tmp in ['2010', '2016']:
                    for tag in root.attrib['Tags'][1:-1].split('><'):
                        print(tmp, tag, 1, sep='\t')
    except:
        pass
