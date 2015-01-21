#!/usr/local/bin/python2.7
# encoding: utf-8
'''
cmdlintst -- shortdesc

cmdlintst is a description

It defines classes_and_methods

@author:     user_name

@copyright:  2015 organization_name. All rights reserved.

@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import sys
import os
from datetime import datetime

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

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by user_name on %s.
  Copyright 2015 organization_name. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))
    '''
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
    '''

    #try:
    # Setup argument parser
    parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-bb", "--boundingbox", help="Bounding box of area to process: \"'minX':0.0, 'minY':0.0, 'maxX':10.0, 'maxY':10.0\"")
    parser.add_argument("-d1", "--dtgFrom", help="Start date for processing: \"YYYY-MM-DDTHH:MM:SS\"")
    parser.add_argument("-d2", "--dtgTo", help="Stop date for processing: \"YYYY-MM-DDTHH:MM:SS\"")
    parser.add_argument("-ti", "--tablesIn", help="Input Tables in Postgres: \"[host, db, user, passwd, tableName, timestampcolumn, geomcolumn] + [...]\"")
    parser.add_argument("-to", "--tableOut", help="Output Table in Postgres: \"host, db, user, passwd, schema, tableName\"")
    parser.add_argument("-ma", "--maxClusterArea", type=float, help="Maximum cluster area for storage (m2)")
    parser.add_argument("-mt", "--maxClusterTime",  type=float, help="Maximum cluster timespan for storage (secs)")
    parser.add_argument("-r", "--runType", type=str, choices=['unittest', 'full', 'setupcheck'], help="Program run type: unittest=check environment, setupcheck=parse run args without running, full=run with args")

    # Process arguments
    args = parser.parse_args()
    #Parse dates
    dtgFrom = datetime.strptime(args.dtgFrom, "%Y-%m-%dT%H:%M:%S")
    dtgTo = datetime.strptime(args.dtgTo, "%Y-%m-%dT%H:%M:%S")
    print args.boundingbox
    bbox = dict(args.boundingbox)
    tablesIn = args.tablesIn.split("+")
    tabsInParse = []
    for tab in tablesIn:
        for param in tab.strip().rstrip("]").lstrip("[").split(","):
            print param
            tabsInParse.append({"host":param[0], "db":param[1],
                                "user":param[2], "passwd":param[3],
                                "tableName":param[4], "tsField":param[5],
                                "geomField": param[6]})
    for param in args.tableOut.split(","):
        print param
        tabOutParse = {"host":param[0], "db":param[1] ,"user": param[2],
                       "passwd": param[3], "schema": param[4], "tableName":param[5]}
    #Build the in dict for class
    kmInit = {}
    print args.maxClusterArea
    print args.maxClusterTime
    print args.runType
    while 1>0:
        continue

#     except KeyboardInterrupt:
#         ### handle keyboard interrupt ###
#         return 0
#     except Exception, e:
#         raise(e)


#     except Exception, e:
#         #raise(e)
#         indent = len(program_name) * " "
#         sys.stderr.write(program_name + ": " + repr(e) + "\n")
#         sys.stderr.write(indent + "  for help use --help")
#         return 2

if __name__ == "__main__":
    main()




