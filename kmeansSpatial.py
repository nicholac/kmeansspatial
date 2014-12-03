'''
Created on 2 Dec 2014

@author: dusted-ipro
'''
import numpy as np
import psycopg2
from sklearn.cluster import KMeans
from datetime import datetime, timedelta
from sklearn.metrics import silhouette_score
import scipy.spatial.distance as dist
from random import random, randint

def genRandPt():
    '''Just generates a random point
    '''
    lng = randint(-180,179)+random()
    lat = randint(-90,89)+random()
    dtg = datetime.now()+timedelta(days=randint(-640, 640))
    return [lng,lat,dtg]


NEXT: Normalise each feature to between 0 and 1:
y = np.array((np.array([ 8.66025404]), np.array([ 8.66025404]), np.array([ 8.66025404]), np.array([ 2.66025404]), np.array([ 179.66025404])))
outY = y/180.0

Just divide all the points by the maximum they can be on the scale (180 and 360)


#load in points from pg
pgConn = psycopg2.connect(database='dusted', user='dusted', host='localhost')
cur = pgConn.cursor()
sql = 'select ident, st_astext(the_geom), dtg from tstpts'
cur.execute(sql)
res = cur.fetchall()

#Vectorize spatial and temporal parts
vects = []
for r in res:
    pt = r[1].split(' ')
    vecLat = pt[0].lstrip('POINT(')
    vecLong = pt[1].rstrip(')')
    vecDtg = float(r[2].strftime('%s'))
    arr = np.array((vecLong, vecLat, vecDtg))
    vects.append(arr)


#Generate a load of random points as noise
for i in range(0,100):
    randPt = genRandPt()
    print randPt
    randDtg = float(randPt[2].strftime('%s'))
    arr = np.array((randPt[0], randPt[1], randDtg))
    vects.append(arr)
    i+=1

#run through sklearn k means
inVecs = np.array(vects)
#Init kmeans
initKmeans = KMeans(init='k-means++', max_iter=1000, n_clusters=15, n_init=1)

#Do the clustering
kmClust = initKmeans.fit(inVecs)

comment = '5 clust, 100 iter, init=1, km++'

#Dump cluster centers into database
outCenters =  kmClust.cluster_centers_
print outCenters
#Calculate the global silouhette matrix
kmLabels = kmClust.labels_
print kmLabels
zipped = zip(kmLabels, inVecs)
#print zipped
'''
#Working - dumps the centers to PG
for cent in outCenters:
    #print cent[0], cent[1], datetime.fromtimestamp(cent[2])
    #Make a proper geom
    sql = 'select st_geomfromtext(\'POINT(%s %s)\',4326)'
    cur.execute(sql, [cent[0], cent[1]])
    res = cur.fetchall()
    geom = res[0]
    sql = 'insert into tstptsclust(dtg, comms, the_geom) values(%s, %s, %s)'
    cur.execute(sql, [datetime.fromtimestamp(cent[2]), comment, geom])
'''

#Append the points to an out featureclasss with cluster numbers
for pt in zipped:
    sql = 'select st_geomfromtext(\'POINT(%s %s)\',4326)'
    cur.execute(sql, [float(pt[1][0]), float(pt[1][1])])
    res = cur.fetchall()
    geom = res[0][0]
    #print geom
    sql = 'insert into tstptsclust(dtg, comms, the_geom) values(%s, %s, %s)'
    cur.execute(sql, [datetime.fromtimestamp(float(pt[1][2])), str(pt[0]), geom])


#Calculate some metrics on the features
#Here its average distance from centroid to each class member
clustPts = []
avgs = []
d = 0.0
for idx, cent in enumerate(outCenters):
    #Get the points in this cluster
    for pt in zipped:
        if pt[0] == idx:
            #its in this class - store
            clustPts.append(pt[1])
    #Now we have list with all points for this cluster
    #Calc some distances
    for c in clustPts:
        #Gotta be a better way of doing this
        d = d+dist.pdist([c,cent])
    #Avg it
    avgDist = d/len(clustPts)
    avgs.append([idx, avgDist])
    d = 0.0
    clustPts = []
print avgs

print 'Done'

pgConn.commit()
del kmClust, initKmeans
pgConn.close()


#Find centroids and select points in those sets















