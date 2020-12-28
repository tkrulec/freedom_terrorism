#!/usr/bin/python

import sys
import re

words = {}

for line in sys.stdin:
	line = line.strip()
	word,count = line.split('\t')

	if word in words.keys():
		words[word]=int(words[word])+1
	else:
		words[word]=1

for a in words:
	print ('%s\t%s' % (a, words[a]))