import sys
import numpy as np
from itertools import combinations
"""
by ynzhu
Just a frame you can refer
Contain naive implementation of Hierechical Clustering
"""
def eu_dis(p1, p2, p_cluster): 
    """
    Calculate euclidian distance between 2 closest points each from a cluster.
    p1, p2 is the centroid of each cluster
    p_cluster is {centroid:[points],...}
    return distance between two clusters
    """
    "CODE"

def p_cen(list_of_points): 
    """
    Calculate centroids of a cluster
    Input: list of points [[x,y],[x,y],[x,y],...]
    Return: (x,y)
    """

def if_ele_int(old_c_p):
    return [type(x)==float for x in old_c_p][0]
def if_ele_list(old_c_p):
    return [type(x)==list for x in old_c_p][0]
def hierarchical(sample, k): # Class centroids, and points in each cluster
    n_clusters = len(sample) # Input: Sample Data [[x,y],[x,y],[x,y],...], k number of clusters
    p_cluster = {}
    for p in sample:
        p_cluster[tuple(p)] = p #list can't be key
    while n_clusters>k:
        disdict = {}
        new_cluster_points = []
        pcombination = combinations(p_cluster.keys(),2)
        for p1p2 in pcombination:
            #print p1p2
            disdict[p1p2] = eu_dis(p1p2[0],p1p2[1],p_cluster) # Compute distance between each 2 points
        minp1p2 = min(disdict, key=disdict.get) # 2 points with minimal distance

        if if_ele_int(p_cluster[minp1p2[0]]) and if_ele_int(p_cluster[minp1p2[1]]):
            new_cluster_points = [p_cluster[minp1p2[0]], p_cluster[minp1p2[1]]]
        elif if_ele_int(p_cluster[minp1p2[0]]) and if_ele_list(p_cluster[minp1p2[1]]):
            new_cluster_points = [p_cluster[minp1p2[0]]] + p_cluster[minp1p2[1]]
        elif if_ele_list(p_cluster[minp1p2[0]]) and if_ele_int(p_cluster[minp1p2[1]]):
            new_cluster_points = p_cluster[minp1p2[0]] + [p_cluster[minp1p2[1]]]
        elif if_ele_list(p_cluster[minp1p2[0]]) and if_ele_list(p_cluster[minp1p2[1]]):
            new_cluster_points = p_cluster[minp1p2[0]] + p_cluster[minp1p2[1]]
        new_centroids = p_cen(new_cluster_points)
        
        p_cluster[new_centroids] = new_cluster_points
        del p_cluster[minp1p2[0]]
        del p_cluster[minp1p2[1]]
        n_clusters -= 1
    return p_cluster

def dist(p1, p2):
    pass

def find_represents(n, cent, p_cluster): 
    represents = []
    points_list = sorted(p_cluster[cent])
    if n > len(points_list):
        raise Exception("Too Big \"N\"")
    represents.append(points_list[0]) #1st point: Find smallest X, then smallest Y
    points_list.remove(represents[0])
    for choosing in range(n-1):
        distance_dict = {}
        for points in points_list: 
            distance_of_each = []
            for pp in represents:
                distance_of_each.append(dist(points,pp))
            distance_dict[tuple(points)] = min(distance_of_each)
        point_chosen = max(distance_dict, key=distance_dict.get)
        represents.append(list(point_chosen))
        points_list.remove(list(point_chosen))
    return represents
def move(p, center, move_percentage): # Computer moved points
    p = np.array(p) # Input: p [x, y], center [x, y], move percentage of move
    center = np.array(center)
    return ((center-p) * move_percentage + p).tolist()
def assign_cluster_number(point, somedict):
    dis_dict = {}
    point = np.array(point)
    for kk in somedict:
        temp_dis = np.infty
        for ppp in somedict[kk]:
            ppp = np.array(ppp)
            dis = np.sqrt(np.sum(np.square(ppp-point)))
            if dis < temp_dis:
                temp_dis = dis
        dis_dict[kk] = temp_dis
    return min(dis_dict, key=dis_dict.get)

if __name__ == '__main__':
    sample = []# sample data
    with open('%s' %sys.argv[1], 'r') as f: 
        for line in f:
            if '\r\n' in line:
                sample.append(map(float, line.replace('\r\n', '').strip().split(',')))
    full = [] # full data
    with open('%s' %sys.argv[2], 'r') as f:
        for line in f:
            if '\r\n' in line:
                full.append(map(float, line.replace('\r\n', '').strip().split(',')))
    k = int(sys.argv[3]) # k: number of clusters
    n = int(sys.argv[4]) # n: number of representative points
    move_percentage = float(sys.argv[5]) # p: percentage of move to center
    desired_clusters = hierarchical(sample, k)

    some_dict = {} # {center:representatives}
    for cent in desired_clusters: 
        some_dict[cent] = find_represents(n,cent,desired_clusters)
    for key in some_dict:
        print some_dict[key]
    number_reps = {} # {cluster_number: representatives}
    cluster_number = 0
    for cent in some_dict:
        new_reps = []
        for p in some_dict[cent]:
            new_reps.append(move(p, cent, move_percentage))
        number_reps[cluster_number] = new_reps
        cluster_number += 1
    for ppp in full:
        ppp.append(assign_cluster_number(ppp, number_reps))

    output = open(sys.argv[6],'w') # output_file # Write every point and their cluster numbers
    for ppp in full:
        output_form = "%f,%f,%i\n" %(ppp[0],ppp[1],ppp[2])
        output.write(output_form)
    output.close()
