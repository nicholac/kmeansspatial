'''
Created on 12 Jan 2015

@author: dusted
'''
import unittest
from datetime import datetime
#from kmeans import normCoords, unNormCoords, coordRange, unixDtgBounds, normDtg, unNormDtg
from kmCorrelate import kmCorrelate

class Test(unittest.TestCase):

    def setUp(self):
        self.kmCorr = kmCorrelate({'runType':'unittest'})

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
         'pgTableOut':{"host":'localhost', "user":'dusted', "db":'dusted', "passwd":'dusted',
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

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()














