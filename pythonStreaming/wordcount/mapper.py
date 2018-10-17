#!/usr/bin/python
import sys

for line in sys.stdin:
    for word in line.split():
        sys.stdout.write("%s\t%d\n"%(word,1))
