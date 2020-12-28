#!/usr/bin/env python

import sys

for line in sys.stdin:
	line = line.strip()
	components=line.split(',')

    output = ''
    for c in range(len(components)):
        output += components[c]
        if (c!=len(components)-1):
            output += ','
    print '%s'%(output)
