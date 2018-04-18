#from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
import itertools
import os

if __name__ == "__main__":
    sc = SparkContext(appName="inf553")
    partnum = 2
    lines = sc.textFile(sys.argv[1], partnum)
    s = float(sys.argv[2])
    globalsup = int(s * lines.count())
    localsup = int(s * lines.count() / partnum)
    def printf(iterator):
            par  = list(iterator)
            print "partition:" + par
    def frequentitems(iterator):
        singleton = {}
        single = []
        pairton = {}
        pair = []
        pairs = []
        cpairs = []
        tripleton = {}
        triple = []
        ctriples = []
        quadarton = {}
        quadar = []
        cquadars = []
        iterator = list(iterator)
        for l in iterator:
            #print "l in single", l
            for x in l:
                if x in singleton:
                    singleton[x] += 1
                else:
                    singleton[x] = 1
        #print singleton
        for k in singleton:
            #print singleton[k]
            if singleton[k] >= localsup:
                single.append(k)
        for l in iterator:
            pairs = itertools.combinations(l,2)
            cpairs += pairs
        #print pairs
        for x in cpairs:
            if x[0] in single and x[1] in single:
                if x in pairton:
                    pairton[x] += 1
                else:
                    pairton[x] = 1
        #print "pairton: ",pairton
        for k in pairton:
            if pairton[k] >= localsup:
                pair.append(k)

        for l in iterator:
            triples = itertools.combinations(l,3)
            ctriples += triples
        for x in ctriples:
            if (x[0],x[1]) in pair and (x[0],x[2]) in pair and (x[1],x[2]) in pair:
                if x in tripleton:
                    tripleton[x] += 1
                else:
                    tripleton[x] = 1
        for k in tripleton:
            if tripleton[k] >= localsup:
                triple.append(k)

        for l in iterator:
            quadars = itertools.combinations(l,4)
            cquadars += quadars
        for x in cquadars:
            if (x[0],x[1],x[2]) in triple and (x[0],x[1],x[3]) in triple and (x[0],x[2],x[3]) in triple and (x[1],x[2],x[3]) in triple:
                if x in quadarton:
                    quadarton[x] += 1
                else:
                    quadarton[x] = 1
        for k in quadarton:
            if quadarton[k] >= localsup:
                quadar.append(k)
        #pentas = 
        concatlist = single+pair+triple+quadar
        for x in range(len(concatlist)):
            concatlist[x] = (concatlist[x], 1)
        return concatlist
    def countfrequent(iterator):
        singleton = {}
        single = []
        pairton = {}
        pair = []
        pairs = []
        cpairs = []
        tripleton = {}
        triple = []
        ctriples = []
        quadarton = {}
        cquadars = []
        iterator = list(iterator)
        for l in iterator:
            #print "l in single", l
            for x in l:
                if x in candidates:
                    if x in singleton:
                        singleton[x] += 1
                    else:
                        singleton[x] = 1
            #print "l in pair",l
        
        for l in iterator:
            pairs = itertools.combinations(l,2)
            cpairs += pairs
        #print pairs
        for x in cpairs:
            if x in candidates:
                if x in pairton:
                    pairton[x] += 1
                else:
                    pairton[x] = 1
        
        for l in iterator:
            triples = itertools.combinations(l,3)
            ctriples += triples
        for x in ctriples:
            if x in candidates:
                if x in tripleton:
                    tripleton[x] += 1
                else:
                    tripleton[x] = 1

        for l in iterator:
            quadars = itertools.combinations(l,4)
            cquadars += quadars
        for x in cquadars:
            if x in candidates:
                if x in quadarton:
                    quadarton[x] += 1
                else:
                    quadarton[x] = 1
        #pentas = 
        concatlist = []
        for k in singleton:
            concatlist.append((k, singleton[k]))
        for k in pairton:
            concatlist.append((k, pairton[k]))
        for k in tripleton:
            concatlist.append((k, tripleton[k]))
        for k in quadarton:
            concatlist.append((k, quadarton[k]))
        return concatlist
    candidates = lines.map(lambda x: x.split(',')).mapPartitions(frequentitems).reduceByKey(lambda x,y: 1).map(lambda x:x[0]).collect() #Phase 1
    candidates.sort()
    result = lines.map(lambda x: x.split(',')).mapPartitions(countfrequent).reduceByKey(add).filter(lambda x: x[1]>=globalsup).map(lambda x:x[0]).collect() #Phase 2
    print candidates
    print result
    with open('%s' %sys.argv[3], 'w') as F:
        for v in result:
            F.write(str(v) + "\n")
        #F.write(s, globalsup, localsup)
