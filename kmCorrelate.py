'''
Created on 12 Jan 2015

@author: dusted

Supports:
- PG tables only with timezone (converts all to UTC for vectorisation)
- Points only at present

'''

import numpy as np
import psycopg2
from sklearn.cluster import KMeans
from datetime import datetime, timedelta
import scipy.spatial.distance as dist
from random import random, randint


class kmCorrelate(object):
    '''
    Kmeans Clustering for Spatio Temporal Data stored in PostGIS tables
    Inputs: Geo Bounds (decimal degrees), Temporal Bounds (UTC timezone), PG Tables, acceptable distance measures
    Outputs: Table with convex hull of cluster and date range (creates table)
    '''

    def __init__(self, params):
        '''
        Init params:
        {'xMin':-9.4921875, 'yMin':38.272688, 'xMax':14.150390, 'yMax':50.73645,
         'dtgFrom':'2012-01-12T11:58:26', 'dtgTo':'2016-02-12T11:58:26',
         'pgTablesIn':[{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots2tz', "tsField":'time_stamp', "geomField":'the_geom'},
                       {"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots3tz', "tsField":'time_stamp', "geomField":'the_geom'}
                       ],
         'maxClustArea':10.0, 'maxClustTime':3600.0,
         'pgTableOut':{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                       "tableName":'testClass', "tsField":'time_stamp', "geomField":'the_geom'},
         'runType': 'setupcheck'}
        '''
        #Checking for run type
        if params['runType'] == 'unittest':
            pass
        else:
            self.pgTablesIn = params['pgTablesIn']
            try:
                self.xMin = float(params['xMin'])
                self.xMax = float(params['xMax'])
                self.yMin = float(params['yMin'])
                self.yMax = float(params['yMax'])
            except:
                print 'Fatal: Bad coords: ' + params
                raise ValueError
            #Work out Coord ranges
            self.xRange, self.yRange = self.coordRange(self.xMin, self.xMax, self.yMin, self.yMax)

            #Check sanity of distance inputs
            try:
                self.maxClusterArea = float(params['maxClustArea'])
            except:
                print 'Fatal: Bad Max Cluster Area Number: ' + params['maxClusterArea']
                raise ValueError
            try:
                self.maxClusterTime = float(params['maxClustTime'])
            except:
                print 'Fatal: Bad Max Cluster Time Number: ' + params['maxClusterTime']
                raise ValueError

            #Work out Date ranges
            try:
                self.dtgFrom = datetime.strptime(params['dtgFrom'], "%Y-%m-%dT%H:%M:%S")
            except:
                print 'Fatal: Date parsing failed with from input date: ' + params['dtgFrom']
                raise ValueError
            try:
                self.dtgTo = datetime.strptime(params['dtgTo'], "%Y-%m-%dT%H:%M:%S")
            except:
                print 'Fatal: Date parsing failed with To input date: ' + params['dtgTo']
                raise ValueError

            #Get the UNIX UTC versions
            self.unixDtgFrom, self.unixDtgTo = self.unixDtgBounds(self.dtgFrom, self.dtgTo)

            #Check the PG Input Tables
            if self.testPGInput(params['pgTablesIn']) == False:
                print 'Fatal: PG Input table check failed'
                raise ValueError

            #If we are just checking setup we've passed init - return
            if params['runType'] == 'setupcheck':
                print 'Success: Params look good: '+ str(params)
                return None

            elif params['runType'] == 'full':
                #Now run the algo
                vects, infoStore = self.vectorise()
                clustVecs, clustCenters = self.cluster(vects, infoStore)
                self.reduceClusters(clustVecs, clustCenters)

                return None
            else:
                #bad run type
                print 'Fatal: Unknown runtype (use \'unittest\', \'setupcheck\' or \'full\': ' + params['runType']
                raise ValueError



    def testPGOutput(self, pgTableOut):
        '''Checks for privs to create an output table
        '''
        pass

    def testPGInput(self, pgTables):
        '''Tests all the given pg tables for appropriateness for running
        Returns False if setup is bad
        '''
        f = True
        #Check the PG Input tables
        for table in pgTables:
            pgC = self.pgConn(table['host'], table['db'], table['user'], table['passwd'])
            if not pgC:
                print 'Fatal: Connection failed to input table: ' + str(table)
                f = False
                continue
            else:
                #Run a test select
                sql = 'SELECT ' + table['tsField'] +', ' + table['geomField'] + ' from '+ table['tableName'] +' limit 1'
                cur = pgC.cursor()
                cur.execute(sql)
                res = cur.fetchall()
                if not res:
                    print 'Fatal: No data found with given field names in table: ' + str(table)
                    f = False
                    pgC.close()
                    continue
                #Check geom type
                sql = 'SELECT GeometryType('+ table['geomField'] +') as result FROM (SELECT ' + table['geomField'] + ' from '+ table['tableName'] +' limit 1) as g;'
                cur = pgC.cursor()
                cur.execute(sql)
                res = cur.fetchall()
                if res[0][0] != 'POINT':
                    print 'Fatal: Table has wrong geometry type: ' + str(table)
                    f = False
                    pgC.close()
                    continue
                else:
                    #Good, just close conn
                    pgC.close()

        #Success if here:
        return f

    def pgConn(self, host, db, user, passwd):
        '''Connect to a PG database
        '''
        pgConn = psycopg2.connect(database=db,
                                  user=user,
                                  host=host,
                                  password=passwd)
        return pgConn

    def coordRange(self, xMin, xMax, yMin, yMax):
        '''Calculates Coordinate Range
        '''
        xRange = abs(xMin-xMax)
        yRange = abs(yMin-yMax)
        return xRange, yRange

    def normCoords(self, inX, inY, xRange, yRange, xMin, yMin):
        '''Normalise input coordinates 0-1 based on the given bounds
        '''
        normX = abs(inX - xMin)/xRange
        normY = abs(yMin - inY)/yRange
        return normX, normY

    def unNormCoords(self, inX, inY, xMin, yMin, xRange, yRange):
        '''Un normalises coordinates from 0-1 to geo
        '''
        outX = (inX * xRange)+xMin
        outY = (inY * yRange)+yMin
        return outX, outY

    def unixDtgBounds(self, dtgFrom, dtgTo):
        '''Converts input DTG's from datetime to unix stamp (secs since UTC)
        '''
        dtgFromUnix = float(dtgFrom.strftime('%s'))
        dtgToUnix = float(dtgTo.strftime('%s'))
        return dtgFromUnix, dtgToUnix

    def normDtg(self, inDtg, dtgFromUnix, dtgToUnix):
        '''Normalise the date (0-1) based on the sets total range
        '''
        #Make them all unix timestamps
        inDtgUnix = float(inDtg.strftime('%s'))
        totSecs= dtgToUnix - dtgFromUnix
        #Normalise
        normDtg = (dtgFromUnix - inDtgUnix)/totSecs
        return normDtg

    def unNormDtg(self, inDtgUnix, dtgFromUnix, dtgToUnix):
        '''Convert a normalsied date back to datetime
        '''
        totSecs= dtgToUnix - dtgFromUnix
        unNormDtg = datetime.fromtimestamp(((inDtgUnix*totSecs)-dtgFromUnix)*-1)
        return unNormDtg

    def vectorise(self):
        '''Creates an array of normalised vectors from the input tables
        '''
        #TODO: Use Numpy array throughout!
        #Storage for all the vectors
        print 'Running: Vectorising data...'
        vects = []
        infoStore = []
        #Vectorise all the data from the input tables
        for table in self.pgTablesIn:
            pgC = self.pgConn(table['host'], table['db'], table['user'], table['passwd'])
            if not pgC:
                #Init should have already caught this...
                print 'Warning: Connection failed to input table: ' + str(table)
                continue
            else:
                cur = pgC.cursor()
                #Get all the data over bounding box and time - converting to UTC
                sql = 'select st_astext(' + table['geomField'] + '), '+ table['tsField'] +' AT TIME ZONE \'UTC\' from '+ table['tableName'] +' where st_contains(st_geomfromtext(\'POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))\',4326), the_geom) AND '+ table['tsField'] +' AT TIME ZONE \'UTC\' > %s AND '+ table['tsField'] +' AT TIME ZONE \'UTC\' < %s'
                data = (self.xMin, self.yMin, self.xMin, self.yMax, self.xMax,
                        self.yMax, self.xMax, self.yMin, self.xMin, self.yMin,
                        self.dtgFrom, self.dtgTo)
                cur.execute(sql, data)
                res = cur.fetchall()

                #Normalise all data to vectors: 0-1
                for r in res:
                    pt = r[0].split(' ')
                    vecX = float(pt[0].lstrip('POINT('))
                    vecY = float(pt[1].rstrip(')'))
                    dtg = r[1]
                    vecXNorm, vecYNorm = self.normCoords(vecX, vecY, self.xRange, self.yRange, self.xMin, self.yMin)
                    dtgNorm = self.normDtg(dtg, self.unixDtgFrom, self.unixDtgTo)
                    arr = np.array((vecXNorm, vecYNorm, dtgNorm))
                    vects.append(arr)

                #Store the endpoint for this table
                infoStore.append(len(vects))
            pgC.close()

        #Send back the data
        return vects, infoStore

    def cluster(self, vects, infoStore):
        '''Runs the KM Clustering Algorithm
        '''
        print 'Running: Running KMeans...'
        #Run through kmeans
        inVecs = np.array(vects)
        del vects
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
                #Tunable gradient for cluster fitting
                if gradient < 0.002:
                    stop_it = True
            n_clusters += 1
        print 'Running: Completed KMeans with # clusters: '+ str(n_clusters)

        print 'Running: Dumping Clusters...'
        #Dump out clusters
        outCenters = kmClust.cluster_centers_
        kmLabels = kmClust.labels_
        #Zip up the cluster labels and vectors
        zipped = zip(kmLabels, inVecs)

        return zipped, outCenters

    def reduceClusters(self, clustVecs, clustCenters):
        '''Take the KMeans clustering output and picks the clusters for export
        '''
        print 'Running: Reducing Clusters...'
        clustPts = []
        avgs = []
        avgDict = {}
        d = 0.0
        dSpat = 0.0
        #Work out how good the clusters are
        for idx, cent in enumerate(clustCenters):
            #Get the points in this cluster
            for pt in clustVecs:
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
        print 'Done'


if __name__ == "__main__":

    params = {'xMin':-9.4921875, 'yMin':38.272688, 'xMax':14.150390, 'yMax':50.73645,
         'dtgFrom':'2012-01-12T11:58:26', 'dtgTo':'2016-02-12T11:58:26',
         'pgTablesIn':[{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots2tz', "tsField":'time_stamp', "geomField":'the_geom'},
                       {"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots3tz', "tsField":'time_stamp', "geomField":'the_geom'}
                       ],
         'maxClustArea':10.0, 'maxClustTime':3600.0,
         'pgTableOut':{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                       "tableName":'testClass', "tsField":'time_stamp', "geomField":'the_geom'},
         'runType': 'full'}
    #Run
    kmCorrelate(params)





















