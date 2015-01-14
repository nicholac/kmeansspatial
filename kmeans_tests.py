'''
Created on 12 Jan 2015

@author: dusted

TODO: Make portability easier with conenction params for PG

'''
import unittest
from datetime import datetime
#from kmeans import normCoords, unNormCoords, coordRange, unixDtgBounds, normDtg, unNormDtg
from kmCorrelate import kmCorrelate

class Test(unittest.TestCase):

    def setUp(self):
        self.kmCorr = kmCorrelate({'runType':'unittest'})
        self.pgParams = {"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots2tz', "tsField":'time_stamp', "geomField":'the_geom'}

    def tearDown(self):
        pass

    def testInit(self):
        '''Tests Initialisation of the class - Environment dependent
        '''
        params = {'xMin':0.0, 'yMin':0.0, 'xMax':0.0, 'yMax':0.0,
         'dtgFrom':'2015-01-12T11:58:26', 'dtgTo':'2015-02-12T11:58:26',
         'pgTablesIn':[{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots2tz', "tsField":'time_stamp', "geomField":'the_geom'},
                       {"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
                        "tableName":'testdots3tz', "tsField":'time_stamp', "geomField":'the_geom'}
                       ],
         'maxClustArea':10.0, 'maxClustTime':3600.0,
         'pgTableOut':{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted', 'schema':'public',
                       "tableName":'testClass', "tsField":'time_stamp', "geomField":'the_geom'},
         'runType': 'setupcheck'}

        kmCorrInit = kmCorrelate(params)
        self.assertNotEqual(kmCorrInit, None)
        del kmCorrInit

    def testNormUnnormDates(self):
        unixFrom, unixTo = self.kmCorr.unixDtgBounds(datetime(2013, 1, 1, 1, 1, 1, 0), datetime(2013, 1, 1, 1, 3, 1, 0))
        unixDtg = self.kmCorr.normDtg(datetime(2013, 1, 1, 1, 1, 2, 0), unixFrom, unixTo)
        outUnNorm = self.kmCorr.unNormDtg(unixDtg, unixFrom, unixTo)
        self.assertEqual(outUnNorm, datetime(2013, 1, 1, 1, 1, 2, 0))

    def testNormCoords(self):
        #Crossing all meridians
        xRange, yRange = self.kmCorr.coordRange(-30.0, 30.0, -50.0, 50.0)
        self.assertEqual(self.kmCorr.normCoords(0.0, 0.0, xRange, yRange, -30.0, -50.0), (0.5, 0.5))
        #Crossing no meridians - positive range
        xRange, yRange = self.kmCorr.coordRange(10.0, 30.0, 10.0, 30.0)
        self.assertEqual(self.kmCorr.normCoords(20.0, 20.0, xRange, yRange, 10.0, 10.0), (0.5, 0.5))
        #Crossing no meridians - negative range
        xRange, yRange = self.kmCorr.coordRange(-30.0, -10.0, -30.0, -10.0)
        self.assertEqual(self.kmCorr.normCoords(-20.0, -20.0, xRange, yRange, -30.0, -30.0), (0.5, 0.5))
        #Crossing Meridians - just equator
        xRange, yRange = self.kmCorr.coordRange(-30.0, 30.0, -30.0, -10.0)
        self.assertEqual(self.kmCorr.normCoords(0.0, -20.0, xRange, yRange, -30.0, -30.0), (0.5, 0.5))

    def testUnnormCoords(self):
        #Crossing all meridians
        xRange, yRange = self.kmCorr.coordRange(-30.0, 30.0, -50.0, 50.0)
        self.assertEqual(self.kmCorr.unNormCoords(0.5, 0.5, -30.0, -50.0, xRange, yRange), (0.0, 0.0))
        #Crossing no meridians - positive range
        xRange, yRange = self.kmCorr.coordRange(10.0, 30.0, 10.0, 30.0)
        self.assertEqual(self.kmCorr.unNormCoords(0.5, 0.5, 10.0, 10.0, xRange, yRange), (20.0, 20.0))
        #Crossing no meridians - negative range
        xRange, yRange = self.kmCorr.coordRange(-30.0, -10.0, -30.0, -10.0)
        self.assertEqual(self.kmCorr.unNormCoords(0.5, 0.5, -30.0, -30.0, xRange, yRange), (-20.0, -20.0))
        #Crossing Meridians - just equator
        xRange, yRange = self.kmCorr.coordRange(-30.0, 30.0, -30.0, -10.0)
        self.assertEqual(self.kmCorr.unNormCoords(0.5, 0.5, -30.0, -30.0, xRange, yRange), (0.0, -20.0))

    def testCoordRange(self):
        #Crossing all meridians
        self.assertEqual(self.kmCorr.coordRange(-30.0, 30.0, -50.0, 50.0), (60.0, 100.0))
        #Crossing no meridians
        self.assertEqual(self.kmCorr.coordRange(30.0, 50.0, 50.0, 170.0), (20.0, 120.0))

    def testConvexHull(self):
        '''Testing output geom from convex hull
        '''
        #Test Range
        xMin = -30.0
        yMin = -50.0
        xMax = 30.0
        yMax = 50.0
        xRange, yRange = self.kmCorr.coordRange(xMin, xMax, yMin, yMax)
        #Some test vectors - normalised coords
        geoCoords = [[0.0, 0.0], [10.0, 10.0], [10.0, 0.0]]
        #Norm coords
        normVecs = []
        for coord in geoCoords:
            normVecs.append(self.kmCorr.normCoords(coord[0], coord[1], xRange, yRange, xMin, yMin))
        outGeom = "0103000020E6100000010000000400000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000000000000000000000000000000000000000"
        pgParams = self.pgParams
        self.assertEqual(self.kmCorr.convexHullCluster(normVecs, pgParams,
                                                       xMin, yMin, xRange, yRange), outGeom)

    def testChkDtgRange(self):
        '''Test for date range function from normalised array of dates
        '''
        dtgFrom = datetime(2014, 1, 14, 12, 14, 31, 442428)
        dtgTo = datetime(2015, 1, 14, 12, 14, 31, 442428)
        #Normalise
        unixDtgFrom, unixDtgTo = self.kmCorr.unixDtgBounds(dtgFrom, dtgTo)
        inDtg = [datetime(2014, 10, 14, 12, 14, 31), datetime(2014, 10, 14, 12, 15, 31),
                 datetime(2014, 10, 14, 12, 16, 31), datetime(2014, 10, 14, 12, 17, 31)]
        normDtg = []
        for dtg in inDtg:
            normDtg.append(self.kmCorr.normDtg(dtg, unixDtgFrom, unixDtgTo))
        self.assertEqual(self.kmCorr.chkDtgRange(normDtg, unixDtgFrom, unixDtgTo),
                         (180.0, datetime(2014, 10, 14, 12, 17, 31), datetime(2014, 10, 14, 12, 14, 31)))

    def testReduceClusters(self):
        '''Testing cluster reduction based on input and output params
        '''
        pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()














