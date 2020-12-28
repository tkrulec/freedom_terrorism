#!/usr/bin/env python

import sys
import csv

for line in sys.stdin:
	line = line.strip()
	words=line.split(';')
	x = len(words)
	count=0
	list=''
	for a in words:
		count+=1
		if a!='-':
			list+=a
		else:
			list+='-1'

		if count!=x:
			list+=';'

	print '%s'%(list)
	#print '\n'
