import sys
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import itertools
import os
"""
By ynzhu
Just a naive implementation
Just notes
No guarantee it will work
"""
if __name__ == "__main__":
    conf = SparkConf().setAppName("inf553").set("spark.driver.memory","2g").set("spark.executor.cores",'5')
    sc = SparkContext(conf = conf)
    partnum = 2
    lines = sc.textFile(sys.argv[1], partnum)
    s = float(sys.argv[2])
    globalsup, localsup  = int(s * lines.count()), int(s * lines.count() / partnum)
    def frequentitems(iterator):
        single = []
        concatlist = []
        iterator = list(iterator)
        k = 1
        while True:
            dic = {}
            i = set([])
            if k == 1:
                [dic.update({x:dic[x]+1}) if x in dic else dic.update({x:1}) for l in iterator for x in l]
                [(concatlist.append((m,1)),single.append(m),i.add(1)) for m in dic if (dic[m] >= localsup)]
            else:
                for l in iterator:
                    [l.remove(x) for x in l if x not in single]
                    tempcomb = list(itertools.combinations(l,k))
                    [dic.update({x:dic[x]+1}) if x in dic else dic.update({x:1}) for x in tempcomb]
                [(concatlist.append((m,1)),i.add(1)) for m in dic if (dic[m] >= localsup)]                
            if len(i) == 0:
                break
            k += 1
        return concatlist
    def countfrequent(iterator):
        concatlist = []
        single = [x for x in candidates if len(x)==1 or len(x) == 2]
        iterator = list(iterator)
        k = 1
        while True:
            dic = {}
            i = set([])
            if k == 1:
                i.add(1)
                [[dic.update({x:dic[x]+1}) if x in dic else dic.update({x:1})] for l in iterator for x in l if x in single]
                [concatlist.append((m, dic[m])) for m in dic]
            else:
                kcan = [x for x in candidates if len(x) == k]
                for l in iterator:
                    [l.remove(x) for x in l if x not in single]
                    tempcomb = list(itertools.combinations(l,k))
                    [[dic.update({x:dic[x]+1}) if x in dic else dic.update({x:1})] for x in tempcomb if x in kcan]
                [(concatlist.append((m, dic[m])),i.add(1)) for m in dic]
            if len(i) == 0:
                break
            k += 1
        return concatlist
    candidates = lines.map(lambda x:x.encode('utf-8')).map(lambda x: x.split(',')).mapPartitions(frequentitems).reduceByKey(lambda x,y: 1).map(lambda x:x[0]).collect() #Phase 1
    candidates.sort()
    result = lines.map(lambda x:x.encode('utf-8')).map(lambda x: x.split(',')).mapPartitions(countfrequent).reduceByKey(add).filter(lambda x: x[1]>=globalsup).map(lambda x:x[0]).collect() #Phase 2
    newresult = []
    for x in result:
        if type(x) == tuple:
            newresult.append(tuple(map(int,x)))
        else:
            newresult.append(int(x))
    newresult.sort()
    newresult.sort(key = lambda s: len(s) if type(s) == tuple else 1)

    with open('%s' %sys.argv[3], 'w') as F:
        for v in newresult:
            F.write(str(repr(v).replace(' ','')))
            F.write('\n')
