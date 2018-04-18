import sys
import numpy as np
from itertools import islice  
from sympy import *

"""
Wrong Code
It won't work, really
"""
def Update(x,y,z): #Update Matrix realM
    if y<=m and x<=n:
        realM[x-1][y-1] = z
        return 0

def RMSE(M,U,V): #Calculate rmse
    diff = 0
    MM = U.dot(V)
    for i in range(n):
        for j in range(m):
            if M[i][j] > 0:
                diff += np.power((M[i][j] - MM[i][j]),2)
    return np.sqrt(diff / float(nonz))

if __name__ == "__main__":
    M = []
    with open('%s' %sys.argv[1], 'r') as f:
        for l in islice(f,1,None):
            basket = []
            basket.append(int(l.split(',')[0]))
            basket.append(int(l.split(',')[1]))
            basket.append(float(l.split(',')[2]))
            M.append(basket)
    n = int(sys.argv[2]) #if len(sys.argv) > 2 else 100 #Number of Rows
    m = int(sys.argv[3]) #if len(sys.argv) > 3 else 500 #Number of Columns
    f = int(sys.argv[4]) #if len(sys.argv) > 4 else 10 #Number of dimensions in the factor model, U n-by-f, V f-by-m
    ITERATIONS = int(sys.argv[5]) #if len(sys.argv) > 5 else 5 #Number of Iterations
    U = np.ones([n,f])
    V = np.ones([f,m])

    realM = np.zeros([n,m])

    [Update(x[0],x[1],x[2]) for x in M]
    nonz = len(realM.nonzero()[0])
    for num in range(ITERATIONS):
        #Adjust U
        for o in range(n): # o represents row of U
            for p in range(f): # p represents column of U
                tempS = 0
                tempF = 0
        #o = 0
        #[minimizeU(o,p) for p in range(f) for o in range(n)]
                for j in range(m):
                    tempSS = 0
                    if realM[o][j]>0:
                        fil = filter(lambda x: x != p, range(f))
                        newl = map(lambda x:U[o][x]*V[x][j] , fil)
                        tempSS = reduce(lambda x,y:x+y,newl)
                        tempS += V[p][j] * (realM[o][j] - tempSS)
                        tempF += V[p][j] * V[p][j]
                U[o][p] = (float(tempS) / tempF) if tempF else 0
                
        for o in range(m):
            for p in range(f):
                tempS = 0
                tempF = 0
                for i in range(n):
                    tempSS = 0
                    if realM[i][o]>0:
                        fil = filter(lambda x: x != p, range(f))
                        newl = map(lambda x:U[i][x]*V[x][o], fil)
                        tempSS = reduce(lambda x,y:x+y,newl)
                        tempS += U[i][p] * (realM[i][o] - tempSS)
                        tempF += U[i][p] * U[i][p]
                V[p][o] = (float(tempS) / tempF) if tempF else 0
        rmse = RMSE(realM,U,V)
        print '%.4f' %rmse
