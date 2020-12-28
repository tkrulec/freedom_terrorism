#!/usr/bin/env python

import sys

for line in sys.stdin:
	line.strip()
	words=line.split()
	if len(line.split())!= 0:
		print '%s\t%s' % ('line', 1)