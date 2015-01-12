# -*- coding: utf-8 -*-
"""
Created on Thu Dec  4 08:50:13 2014

@author: nicholac

UNCLASSIFIED VERSION

Supports:
- PG tables only with timezone (converts all to UTC for vectorisation)


"""

import numpy as np
import psycopg2
from sklearn.cluster import KMeans
from datetime import datetime, timedelta
import scipy.spatial.distance as dist
from random import random, randint
import pytz, operator


def coordRange(xMin, xMax, yMin, yMax):
    '''Calculates Coordinate Range
    '''
    xRange = abs(xMin-xMax)
    yRange = abs(yMin-yMax)
    return xRange, yRange

def normCoords(inX, inY, xRange, yRange, xMin, yMin):
    '''Normalise input coordinates 0-1 based on the given bounds
    '''
    normX = abs(inX - xMin)/xRange
    normY = abs(yMin - inY)/yRange
    return normX, normY

def unNormCoords(inX, inY, xMin, yMin, xRange, yRange):
    '''Un normalises coordinates from 0-1 to geo
    '''
    outX = (inX * xRange)+xMin
    outY = (inY * yRange)+yMin
    return outX, outY

def unixDtgBounds(dtgFrom, dtgTo):
    '''Converts input DTG's from datetime to unix stamp (secs since UTC)
    '''
    dtgFromUnix = float(dtgFrom.strftime('%s'))
    dtgToUnix = float(dtgTo.strftime('%s'))
    return dtgFromUnix, dtgToUnix

def normDtg(inDtg, dtgFromUnix, dtgToUnix):
    '''Normalise the date (0-1) based on the sets total range
    '''
    #Make them all unix timestamps
    inDtgUnix = float(inDtg.strftime('%s'))
    totSecs= dtgToUnix - dtgFromUnix
    #Normalise
    normDtg = (dtgFromUnix - inDtgUnix)/totSecs
    return normDtg

def unNormDtg(inDtgUnix, dtgFromUnix, dtgToUnix):
    '''Convert a normalsied date back to datetime
    '''
    totSecs= dtgToUnix - dtgFromUnix
    unNormDtg = datetime.fromtimestamp(((inDtgUnix*totSecs)-dtgFromUnix)*-1)
    return unNormDtg


def cluster():
    #KMeans for 3 Datasets
    #Load geos and dtg's from PG
    pgConn = psycopg2.connect(database='', user='', host='', password='')
    cur = pgConn.cursor()

#TODO: use this on the select for normalisation of TZ from PG: time_stamp AT TIME ZONE \'UTC\'

    #Get the data - First
    sql = 'select st_astext(the_geom), to_timestamp from TABLE1 where st_contains(st_geomfromtext(\'POLYGON((Bounding Box))\',4326), the_geom)'
    cur.execute(sql)
    resOne = cur.fetchall()
    dtgList = []
    for r in resOne:
        dtgList.append(r[1])

    #Get the data - Second
    sql = 'select st_astext(the_geom), time from TABLE2 where st_contains(st_geomfromtext(\'POLYGON((Bounding Box))\',4326), the_geom)'
    cur.execute(sql)
    resTwo = cur.fetchall()
    #dtgList = []
    for r in resTwo:
        dtgList.append(r[1])


    #Get the data - Third
    sql = 'select st_astext(st_centroid(the_geom)), time_stamp from TABLE3 where st_contains(st_geomfromtext(\'POLYGON((Bounding Box))\',4326), the_geom)'
    cur.execute(sql)
    resThree = cur.fetchall()
    for r in resThree:
        dtgList.append(r[1])

    #Get the date range for later normalisation
    dtgFrom, dtgTo = getDtgRange(dtgList)


     #Get the geometry range - both datasets
    longMinOne, latMinOne, longMaxOne, latMaxOne = getGeomRange('TABLE1', cur)
    longMinTwo, latMinTwo, longMaxTwo, latMaxTwo = getGeomRange('TABLE2', cur)
    longMinThree, latMinThree, longMaxThree, latMaxThree = getGeomRange('TABLE3', cur)

    #Overall boundary
    longMin = min([longMinTwo, longMinOne, longMinThree])
    latMin = min([latMinTwo, latMinOne, latMinThree])
    longMax = max([longMaxTwo, longMaxOne, longMaxThree])
    latMax = max([latMaxTwo, latMaxOne, latMaxThree])

    vects = []
    infoStore = []
    #Normalise all to 0-1 - each dataset
    for r in resOne:
        pt = r[0].split(' ')
        vecLong = float(pt[0].lstrip('POINT('))
        vecLat = float(pt[1].rstrip(')'))
        dtg = r[1]
        vecLongNorm, vecLatNorm = normCoords(vecLong, vecLat, longMin, latMin, longMax, latMax)
        dtgNorm = normDtg(dtg, dtgFrom, dtgTo)
        arr = np.array((vecLongNorm, vecLatNorm, dtgNorm))
        vects.append(arr)
        #Checking norming
        #unNormLong, unNormLat = unNormCoords(vecLongNorm, vecLatNorm, longMin, latMin, longMax, latMax)
        #print vecLong, vecLat, unNormLong, unNormLat, vecLongNorm, vecLatNorm

    #remember where One stoppped
    infoStore.append(len(vects))

    for r in resTwo:
        pt = r[0].split(' ')
        vecLong = float(pt[0].lstrip('POINT('))
        vecLat = float(pt[1].rstrip(')'))
        dtg = r[1]
        vecLongNorm, vecLatNorm = normCoords(vecLong, vecLat, longMin, latMin, longMax, latMax)
        dtgNorm = normDtg(dtg, dtgFrom, dtgTo)
        arr = np.array((vecLongNorm, vecLatNorm, dtgNorm))
        vects.append(arr)
        #Checking norming
        #unNormLong, unNormLat = unNormCoords(vecLongNorm, vecLatNorm, longMin, latMin, longMax, latMax)
        #print vecLong, vecLat, unNormLong, unNormLat, vecLongNorm, vecLatNorm

    infoStore.append(len(vects))

    for r in resThree:
        pt = r[0].split(' ')
        vecLong = float(pt[0].lstrip('POINT('))
        vecLat = float(pt[1].rstrip(')'))
        dtg = r[1]
        vecLongNorm, vecLatNorm = normCoords(vecLong, vecLat, longMin, latMin, longMax, latMax)
        dtgNorm = normDtg(dtg, dtgFrom, dtgTo)
        arr = np.array((vecLongNorm, vecLatNorm, dtgNorm))
        vects.append(arr)
        #Checking norming
        #unNormLong, unNormLat = unNormCoords(vecLongNorm, vecLatNorm, longMin, latMin, longMax, latMax)
        #print vecLong, vecLat, unNormLong, unNormLat, vecLongNorm, vecLatNorm

    infoStore.append(len(vects))


    #Run through kmeans
    inVecs = np.array(vects)
    #print inVecs[0:5]
    #print vects[0:5]
    stop_it = False
    n_clusters = 5
    firstone = True
    while stop_it == False:
        initKmeans = KMeans(init='k-means++', max_iter=1000, n_clusters=n_clusters, n_init=5)
        #Do the clustering
        kmClust = initKmeans.fit(inVecs)
        if firstone == True:
            cluster_inertia = np.array([[n_clusters,kmClust.inertia_]])
            firstone = False
        else:
            cluster_inertia=np.vstack((cluster_inertia,[n_clusters,kmClust.inertia_]))
        if len(cluster_inertia) > 5:
            gradient = (cluster_inertia[-2:,1][0]-cluster_inertia[-1:,1])[0]
            print gradient
            if gradient < 0.002:
                stop_it = True
        n_clusters += 1
    print 'it used this many clusters:',n_clusters

    #Dump out clusters
    outCenters = kmClust.cluster_centers_
    #print outCenters
    kmLabels = kmClust.labels_
    #Zip up the cluster labels and vectors
    zipped = zip(kmLabels, inVecs)
    #print zipped[0:10]

    clustPts = []
    avgs = []
    avgDict = {}
    d = 0.0
    dSpat = 0.0
    #Work out how good the clusters are
    for idx, cent in enumerate(outCenters):
        #Get the points in this cluster
        for pt in zipped:
            if pt[0] == idx:
                #Its in the class - store
                clustPts.append(pt[1])
        #Now we have a list with all points for this cluster
        #Calc centroid to pt distances
        for c in clustPts:
            #Whole vector
            d = d+dist.pdist([c,cent])
            #Geospatial only
            dSpat = dSpat+dist.pdist([c[0:1],cent[0:1]])
        #Avg it - maybe working
        avgDist = d/len(clustPts)
        avgSpatDist = dSpat/len(clustPts)
        avgs.append([idx, avgDist])
        avgDict[idx]=[avgDist, avgSpatDist]
        #Total Dist
        #avgs.append([idx, d])
        #avgDict[idx]=[d, dSpat]
        d = 0.0
        dSpat = 0.0
        clustPts = []

    #Sort by cluster distance and pick
    #Do it the list way
    goodClusts = []
    print 'Clusters and dists follow'
    sAvgs = sorted(avgs, key=lambda clust:clust[1])
    for i in sAvgs:
        print i
        goodClusts.append(i[0])
    goodClusts = goodClusts[0:20]
    print goodClusts


    #Dump features to an outclass to see clusters
    pgConn2 = psycopg2.connect(database='', user='', host='', password='')
    cur2 = pgConn2.cursor()
    for idx, pt in enumerate(zipped):
        #Only take the ones we care about
        if int(pt[0]) in goodClusts:
            #unNorm coords
            unNormLong, unNormLat = unNormCoords(pt[1][0], pt[1][1], longMin, latMin, longMax, latMax)
            #print pt[1][0], pt[1][1], inVecs[idx]
            sql = 'select st_geomfromtext(\'POINT(%s %s)\',4326)'
            cur2.execute(sql, [unNormLong, unNormLat])
            res = cur2.fetchall()
            geom = res[0][0]
            #Un-normalise the dtg vector
            dtgFromUnix = float(dtgFrom.strftime('%s'))
            dtgToUnix = float(dtgTo.strftime('%s'))
            totSecs= dtgToUnix - dtgFromUnix
            unNormDtg = datetime.fromtimestamp(((pt[1][2]*totSecs)-dtgFromUnix)*-1)
            #Get the original feature info
            if idx <= infoStore[0]:
                dataset = 'TABLE1'
            elif idx > infoStore[0] and idx <= infoStore[1]:
                dataset = 'TABLE2'
            else:
                dataset = 'TABLE3'
            sql = 'INSERT INTO OUTCLUSTERSTABLE(cluster_num, cluster_dist, geo_dist, dataset, time_stamp, the_geom) values (%s, %s, %s, %s, %s, %s)'
            cur2.execute(sql, [int(pt[0]), float(avgDict[int(pt[0])][0]),float(avgDict[int(pt[0])][1]), dataset, unNormDtg, geom])

    pgConn2.commit()
    pgConn2.close()
    pgConn.close()
    return


#Run it

#threeDatasets()
#twoDatasets()















