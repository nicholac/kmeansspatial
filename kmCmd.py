#!/usr/local/bin/python2.7
# encoding: utf-8
'''
KMeans Correlate Command-Line Class

@author:     Nicholas C

@copyright:  2015 Dstl. All rights reserved.

@license:    TBD

@contact:    <Email>
@deffield    updated: 21-01-2015
'''
import sys
import os
from datetime import datetime
from kmCorrelate import kmCorrelate
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

__all__ = []
__version__ = 0.1
__date__ = '2015-01-16'
__updated__ = '2015-01-16'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)
    try:

        program_name = os.path.basename(sys.argv[0])
        program_version = "v%s" % __version__
        program_build_date = str(__updated__)
        program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
        program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
        program_license = '''%s

      Created by Dstl on %s.
      Copyright 2015 organization_name. All rights reserved.

      Licensed under the Dstl License

      Distributed on an "AS IS" basis without warranties
      or conditions of any kind, either express or implied.

    USAGE
    ''' % (program_shortdesc, str(__date__))

        #try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-bb", "--boundingbox", required=True, help="Bounding box of area to process (minx, minY, maxx, maxy): \"0.0, 0.0, 10.0, 10.0\"")
        parser.add_argument("-d1", "--dtgFrom", required=True, help="Start date for processing: \"YYYY-MM-DDTHH:MM:SS\"")
        parser.add_argument("-d2", "--dtgTo", required=True, help="Stop date for processing: \"YYYY-MM-DDTHH:MM:SS\"")
        parser.add_argument("-ti", "--tablesIn", required=True, help="Input Tables in Postgres: \"[host, db, user, passwd, tableName, timestampcolumn, geomcolumn] + [...]\"")
        parser.add_argument("-to", "--tableOut", required=True, help="Output Table in Postgres: \"host, db, user, passwd, schema, tableName\"")
        parser.add_argument("-ma", "--maxClusterArea", required=True, type=float, help="Maximum cluster area for storage (m2) - enter 0 to dump all clusters")
        parser.add_argument("-mt", "--maxClusterTime",  required=True, type=float, help="Maximum cluster timespan for storage (secs) - enter 0 to dump all clusters")
        parser.add_argument("-r", "--runType", required=True, type=str, choices=['unittest', 'full', 'setupcheck'], help="Program run type: unittest=check environment, setupcheck=parse run args without running, full=run with args")

        # Process arguments
        args = parser.parse_args()
        try:
            #Parse dates
            dtgFrom = datetime.strptime(args.dtgFrom, "%Y-%m-%dT%H:%M:%S")
            dtgTo = datetime.strptime(args.dtgTo, "%Y-%m-%dT%H:%M:%S")
        except:
            parser.error('Invalid Dates - check syntax with --help')
        try:
            bbox = args.boundingbox.split(',')
            xMin = float(bbox[0])
            yMin = float(bbox[1])
            xMax = float(bbox[2])
            yMax = float(bbox[3])
        except:
            parser.error('Invalid Bounding Box Arguments - check syntax with --help')
        tablesIn = args.tablesIn.split("+")
        tabsInParse = []
        #In Tables
        try:
            for tab in tablesIn:
                ti = tab.strip().rstrip("]").lstrip("[").split(",")
                tabsInParse.append({"host":ti[0].strip(), "db":ti[1].strip(),
                                    "user":ti[2].strip(), "passwd":ti[3].strip(),
                                    "tableName":ti[4].strip(), "tsField":ti[5].strip(),
                                    "geomField": ti[6].strip()})
        except:
            parser.error('Invalid In-Table Arguments - check syntax with --help')
        try:
            #Out table
            to = args.tableOut.split(",")
            tabOutParse = {"host":to[0].strip(), "db":to[1].strip() ,"user": to[2].strip(),
                           "passwd": to[3].strip(), "schema": to[4].strip(), "tableName":to[5].strip()}
        except:
            parser.error('Invalid Out-Table Arguments - check syntax with --help')

        try:
            #Build the in dict for class
            kmInit = {'xMin':xMin, 'xMax':xMax, 'yMin':yMin, 'yMax':yMax,
                      'dtgFrom':dtgFrom.isoformat(), 'dtgTo':dtgTo.isoformat(),
                      'pgTablesIn':tabsInParse, 'pgTableOut': tabOutParse,
                      'maxClustArea':args.maxClusterArea, 'maxClustTime':args.maxClusterTime,
                      'runType': args.runType}
        except:
            parser.error('Failed to build KmInit - Invalid Arguments - check syntax with --help')
        print 'Running: Ctrl-C aborts...'
        try:
            #Run Kmeans - not handled exceptions as in class
            kmCorrelate(kmInit)
            print 'Running: Done'
        except ValueError:
            print 'Processing Failed'

    #User abort
    except KeyboardInterrupt:
        print 'Aborted by user'
    finally:
        pass


if __name__ == "__main__":
    '''
    Example Cmd Line:
    python kmCmd.py -bb "40.0, 40.0, 70.0, 70.0" -d1 "2013-01-12T11:58:26" -d2 "2021-02-12T11:58:26" -ti "[localhost, db, user, passwd, tableName, time_stamp, the_geom]" -to "host, db, user, password, public, cmdtstout" -ma 100000.0 -mt 360000.0 -r "setupcheck"
    '''
    main()




