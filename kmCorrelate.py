'''
Created on 12 Jan 2015

@author: dusted

Supports:
- PG tables only with timezone (converts all to UTC for vectorisation)
- Points only at present

TODOS:
- Speed up the cluster reduction method:
    - reduce by time bounds first (do all vecs)
    - Estimate area from normed vecs and bounding box

- Output more metrics about the clusters - using centroid etc

'''

import numpy as np
import psycopg2
import sys
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
         'pgTableOut':{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted', "schema":'public',
                       "tableName":'testClass1'},
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

            #check PG Output table privileges
            if self.testPGOutput(params['pgTableOut']) == False:
                print 'Fatal: PG Output table check failed'
                raise ValueError

            #If we are just checking setup we've passed init - return
            if params['runType'] == 'setupcheck':
                print 'Success: Params look good: '+ str(params)
                return None

            elif params['runType'] == 'full':
                #Record Starttime
                startDtg = datetime.now()
                #Now run the algo
                vects, infoStore = self.vectorise()
                clustLabels, clustVecs, clustCents = self.cluster(vects, infoStore)
                goodClusts = self.reduceClusters(clustLabels, clustVecs, clustCents, params['maxClustTime'],
                                                 params['maxClustArea'], params['pgTableOut'], self.xMin, self.yMin,
                                                 self.xRange, self.yRange)
                #print goodClusts
                output = self.saveClusterHulls(goodClusts, params['pgTableOut'])
                if output == True:
                    #Tell run time
                    runTime =  datetime.now() - startDtg
                    'Complete, Runtime (mins): '+str(runTime.total_seconds()/60.0)
                else:
                    print 'Fatal: Cluster Hull Output Failed'
                    raise ValueError
            else:
                #bad run type
                print 'Fatal: Unknown runtype (use \'unittest\', \'setupcheck\' or \'full\': ' + params['runType']
                raise ValueError



    def testPGOutput(self, pgTableOut):
        '''Checks for privs and existance to create an output table
        '''
        try:
            user = pgTableOut['user']
            db = pgTableOut['db']
            host = pgTableOut['host']
            passwd = pgTableOut['passwd']
            schema = pgTableOut['schema']
        except KeyError as e:
            err = str(e)
            print 'Fatal: Missing DB Connection info for Output table: ' + err
            return False
        try:
            pgC = self.pgConn(pgTableOut['host'], pgTableOut['db'],
                          pgTableOut['user'], pgTableOut['passwd'])
        except:
            print 'Fatal: Connection to output table failed: ' + str(pgTableOut)
            return False

        cur = pgC.cursor()
        #Check output exists
        sql = 'SELECT \''+pgTableOut['schema']+'.'+pgTableOut['tableName']+'\'::regclass'
        try:
            cur.execute(sql)
            print 'Fatal: Output table exists'
            pgC.close()
            return False
        except:
            #Table doesnt exist - continue
            pgC.close()
            pass
        #Re-open connection because th exception kills it...
        pgC = self.pgConn(pgTableOut['host'], pgTableOut['db'],
                          pgTableOut['user'], pgTableOut['passwd'])
        cur = pgC.cursor()

        #Check the Schema Privs for table creation

        sql = 'SELECT * FROM has_schema_privilege(%s, %s, %s)'
        data = (pgTableOut['user'],pgTableOut['schema'], 'CREATE')
        cur.execute(sql, data)
        r = cur.fetchall()
        if r[0][0] != True:
            print 'Fatal: User does not have create privileges for output DB: ' + str(pgTableOut)
            pgC.close()
            return False
        else:
            return True

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
        normDtg = (inDtgUnix - dtgFromUnix)/totSecs
        return normDtg

    def unNormDtg(self, inDtgVec, dtgFromUnix, dtgToUnix):
        '''Convert a normalsied date back to datetime
        '''
        totSecs= dtgToUnix - dtgFromUnix
        unNormDtg = datetime.fromtimestamp((dtgFromUnix+(inDtgVec*totSecs)))
        return unNormDtg

    def chkDtgRange(self, clustDtgs, dtgFromUnix, dtgToUnix):
        '''Takes in array of normalised dtg's, finds min - max values and returns range
        dtgfrom and dtg are date ranges of input data in UTC(secs)
        '''
        fromNorm = min(clustDtgs)
        toNorm = max(clustDtgs)
        #UnNormalise
        fromUnNorm = self.unNormDtg(fromNorm, dtgFromUnix, dtgToUnix)
        toUnNorm = self.unNormDtg(toNorm, dtgFromUnix, dtgToUnix)
        #Get Range
        dtgRnge = toUnNorm - fromUnNorm
        return dtgRnge.total_seconds(), fromUnNorm, toUnNorm

    def geoArea(self, inGeomStr, pgParams):
        '''Calculates area of input geometry string
        Uses PostGIS
        '''
        #PG Conn
        try:
            pgC = self.pgConn(pgParams['host'], pgParams['db'],
                          pgParams['user'], pgParams['passwd'])
        except:
            print 'Fatal: Connection to output table failed: ' + str(pgParams)
            raise ValueError

        #Build sql (casting to geography) & execute
        sql = 'SELECT st_area(geography(%s))'
        data = (inGeomStr,)
        cur = pgC.cursor()
        cur.execute(sql, data)
        res = cur.fetchall()
        pgC.close()
        #Result in sq meters
        return res[0][0]

    def convexHullCluster(self, clusterVecs, pgParams, xMin, yMin, xRange, yRange):
        '''Creates a convex hull from the input single cluster (np array of vects with cluster idx)
        Input clusters are normalised on input
        Outputs a PG Geometry string
        Uses PostGIS
        '''
        polySql = 'SELECT ST_ConvexHull(st_GeomFromText(\'POLYGON(('
        #un normalise the coords
        for vec in clusterVecs:
            geoX, geoY = self.unNormCoords(vec[0], vec[1], xMin, yMin, xRange, yRange)
            polySql = polySql + str(geoX) +' '+ str(geoY)+','

        #Close the polygon
        geoX, geoY = self.unNormCoords(clusterVecs[0][0], clusterVecs[0][1], xMin, yMin, xRange, yRange)
        polySql = polySql+str(geoX)+' '+str(geoY)+'))\',4326))'
        #PG connection
        try:
            pgC = self.pgConn(pgParams['host'], pgParams['db'],
                          pgParams['user'], pgParams['passwd'])
        except:
            print 'Fatal: Connection to output table failed: ' + str(pgParams)
            raise ValueError
        cur = pgC.cursor()
        cur.execute(polySql)
        res = cur.fetchall()
        pgC.close()
        return res[0][0]

    def saveClusterHulls(self, clusters, pgTableOut):
        '''Dumps reduced clusters to database as convex hulls
        Input: Cluster array: [geom, area, dtgfrom, dtgto]
        Output: True / False on save to DB
        'pgTableOut':{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted', "schema":'public',
                       "tableName":'testClass'},
        '''
        print 'Running: Dumping Cluster Hulls to DB...'
        insCnt = 0
        #PG connection - already checked for table creation...
        try:
            pgC = self.pgConn(pgTableOut['host'], pgTableOut['db'],
                          pgTableOut['user'], pgTableOut['passwd'])
        except:
            print 'Fatal: Connection to output table failed: ' + str(pgTableOut)
            raise ValueError
        #Create the output table
        sql = 'CREATE table '+pgTableOut['schema']+'.'+pgTableOut['tableName']+' (oid serial, area double precision, dtgfrom timestamp with time zone, dtgto timestamp with time zone)'
        cur = pgC.cursor()
        cur.execute(sql)
        pgC.commit()
        #Add geometry field
        #sql = 'select addgeometrycolumn('+pgTableOut['schema']+','+pgTableOut['tableName']+',\'the_geom\',4326,\'POLYGON\',2)'
        sql = 'select addgeometrycolumn(%s,%s,\'the_geom\',4326,\'POLYGON\',2)'
        data = (pgTableOut['schema'], pgTableOut['tableName'])
        cur.execute(sql, data)

        #Insert the cluster hulls
        #TODO: catches here for invalid geoms
        for hull in clusters:
            #Build sql
            sql = 'INSERT INTO '+pgTableOut['schema']+'.'+pgTableOut['tableName']+'(the_geom, area, dtgfrom, dtgto) values (%s, %s, %s, %s)'
            data = (hull[0], hull[1], hull[2], hull[3])
            cur.execute(sql, data)
            insCnt+=1

        #Flush
        pgC.commit()
        pgC.close()
        print 'Running: Done Dumping Cluster Hulls: '+str(insCnt)
        return True

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
                #sql = 'select st_astext(' + table['geomField'] + '), '+ table['tsField'] +' from '+ table['tableName'] +' where st_contains(st_geomfromtext(\'POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))\',4326), the_geom) AND '+ table['tsField'] +' > %s AND '+ table['tsField'] +' < %s'
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
        print 'Running: # Vectors: '+str(len(vects))
        #Send back the data
        return tuple(vects), tuple(infoStore)

    def cluster(self, vects, infoStore):
        '''Runs the KM Clustering Algorithm
        Input: vects is python array of [x,y,z] triples
        Infostore is python array with ints showing location where datasets stop within array
        '''
        try:
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
                    if gradient < 0.00002:
                        stop_it = True
                n_clusters += 1
            print 'Running: Completed KMeans with # clusters: '+ str(n_clusters)

            print 'Running: Dumping Clusters...'
            #Dump out clusters
            outCenters = kmClust.cluster_centers_
            kmLabels = kmClust.labels_
            return kmLabels, inVecs, outCenters

        #Some Kmeans failure
        except Exception, err:
            print 'Fatal: KMeans Failed: '+sys.exc_info()[0]
            raise ValueError

    def clusterDists(self, clusterVecs, clustCent):
        '''Calculates the average distance between centroid and clusters vectors
        both for spatial distance and whole vector
        NOT IMPLEMENTED
        '''
        d = 0.0
        dSpat = 0.0
        #Calc centroid to pt distances
        for c in clusterVecs:
            #Whole vector
            d = d+dist.pdist([c,clustCent])
            #Geospatial only
            dSpat = dSpat+dist.pdist([c[0:1],clustCent[0:1]])
        #Avg it - maybe working
        avgDist = d/len(clusterVecs)
        avgSpatDist = dSpat/len(clusterVecs)
        d = 0.0
        dSpat = 0.0
        return avgDist, avgSpatDist

    def reduceClusters(self, clustLabels, clustVecs, clustCents, maxClustTime,
                        maxClustArea, pgParams, xMin, yMin, xRange, yRange):
        '''Take the KMeans clustering output and picks the clusters for export
        Output array containing arrays for each good cluster: x,y,z, spatDist, timeDist:
        [[x,y,z], [x,y,z], ...],spatDist, timeDist]
        '''
        print 'Running: Reducing Clusters...'
        clustPts = []
        clustDtgs = []
        outClusts = []
        #Zip lists together
        print set(clustLabels)
        vecZip = zip(clustVecs, clustLabels)
        #Loop through clusters - as a set of unique cluster labels
        i = 0
        for lbl in set(clustLabels):
            i+=1
            #Build Cluster array
            clustPts = [(vec[0][0], vec[0][1]) for vec in vecZip if vec[1]==lbl]
            clustDtgs = [vec[0][2] for vec in vecZip if vec[1]==lbl]
            #Check Length
            if len(clustPts) < 2:
                #Clean up and move on
                print 'Skipped Cluster: '+str(lbl)
                clustPts = []
                clustDtgs = []
                continue
            #Check DTG first - total range in seconds
            clustSecs, clustDtgfrom, clustDtgTo = self.chkDtgRange(clustDtgs, self.unixDtgFrom, self.unixDtgTo)

            ##Testing Delete--------------------------
            #Calculate area of this cluster
            geomStr = self.convexHullCluster(clustPts, pgParams, xMin, yMin, xRange, yRange)
            #Area in Sq Metres
            geoArea = self.geoArea(geomStr, pgParams)
            outClusts.append((geomStr, geoArea, clustDtgfrom, clustDtgTo))
            ##Testing Delete--------------------------
            '''
            if clustSecs <= maxClustTime:
                #Calculate area of this cluster
                geomStr = self.convexHullCluster(clustPts, pgParams, xMin, yMin, xRange, yRange)
                #Area in Sq Metres
                geoArea = self.geoArea(geomStr, pgParams)
                print 'Area:'+str(geoArea)
                #Check if its within input area tolerance
                if geoArea <= maxClustArea:
                    #Good cluster - store
                    #TODO: Output the points here aswell if extended in future
                    outClusts.append((geomStr, geoArea, clustDtgfrom, clustDtgTo))
                else:
                    #Clean up and move on
                    clustPts = []
                    clustDtgs = []
            else:
                #Clean up and move on
                clustPts = []
                clustDtgs = []
            '''
        #Output the good clusters
        print 'Number clusters: '+str(i)
        return outClusts


if __name__ == "__main__":

    params = {'xMin':40.0, 'yMin':40.0, 'xMax':70.0, 'yMax':70.0,
         'dtgFrom':'2013-01-12T11:58:26', 'dtgTo':'2021-02-12T11:58:26',
         'pgTablesIn':[{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots3tz', "tsField":'time_stamp', "geomField":'the_geom'},
                       {"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots2tz', "tsField":'time_stamp', "geomField":'the_geom'}
                       ],
         'maxClustArea':10000.0, 'maxClustTime':36000.0,
         'pgTableOut':{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted', "schema":'public',
                       "tableName":'testclass12'},
         'runType': 'full'}
    #Run
    kmCorrelate(params)





















