#!/usr/bin/python
import sys
dict={}
for line in sys.stdin:
    (word,count) = line.split()
    count = int(count)
    dict[word]=dict.get(word,0)+count
for key in dict.keys():
    sys.stdout.write("%s\t%d\n"%(key,dict[key]))
