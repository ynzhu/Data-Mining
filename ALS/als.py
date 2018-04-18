#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
by ynzhu
This is an customed version of example ALS code
Please be careful if you want to use it
No guarantee it will work
Just notes
"""
from __future__ import print_function

import sys

import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession
from itertools import islice  

LAMBDA = 0.01   # regularization
np.random.seed(33)


def rmse(R, ms, us):
    diff = R - ms * us.T
    return np.sqrt(np.sum(np.power(diff, 2)) / (M * U))


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]

    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T

    for j in range(ff):
        XtX[j, j] += LAMBDA * uu

    return np.linalg.solve(XtX, Xty)


if __name__ == "__main__":

    """
    Usage: als [M] [U] [F] [iterations] [partitions]"
    """

    #print("""WARN: This is a naive implementation of ALS and is given as an
    #  example. Please use pyspark.ml.recommendation.ALS for more
    #  conventional use.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("DATAMININGALS")\
        .getOrCreate()
    spark.conf.set("spark.exeutor.memory", "4g")
    spark.conf.set("spark.executor.cores",'5')
    spark.conf.set("spark.driver.memory","4g")

    sc = spark.sparkContext

    M = int(sys.argv[2]) 
    U = int(sys.argv[3]) 
    F = int(sys.argv[4]) 
    ITERATIONS = int(sys.argv[5]) 
    partitions = int(sys.argv[6]) 
    
    f = sys.argv[1]
    with open(sys.argv[1]) as f:
        lines = f.readlines()
    realM = np.zeros([M,U])
    preM = []
    with open('%s' %sys.argv[1], 'r') as f:
        for l in islice(f,1,None):
            basket = []
            basket.append(int(l.split(',')[0]))
            basket.append(int(l.split(',')[1]))
            basket.append(float(l.split(',')[2]))
            preM.append(basket)
    def Update(x,y,z): #Update Matrix realM
        if y<=U and x<=M:
            realM[x-1][y-1] = z
            return 0    
    [Update(x[0],x[1],x[2]) for x in preM]

    #print (R)
    #print("Running ALS with M=%d, U=%d, F=%d, iters=%d, partitions=%d\n" %
    #      (M, U, F, ITERATIONS, partitions))
    file2 = open(sys.argv[7],'w')

    R = matrix(realM)
    ms = matrix(np.ones([M, F]))
    us = matrix(np.ones([U, F]))

    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)

    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), partitions) \
               .map(lambda x: update(x, usb.value, Rb.value)) \
               .collect()
        # collect() returns a list, so array ends up being
        # a 3-d array, we take the first 2 dims for the matrix
        ms = matrix(np.array(ms)[:, :, 0])
        msb = sc.broadcast(ms)

        us = sc.parallelize(range(U), partitions) \
               .map(lambda x: update(x, msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)

        error = rmse(R, ms, us)
        #print("Iteration %d:" % i)
        #print("\nRMSE: %5.4f\n" % error)
        file2.write("%.4f\n" %error)
        file2.close()
    spark.stop()
