# BSD Licensed, Copyright (c) 2006-2010 TileCache Contributors
# BrewMap code by Graham Jones, 2011.

import sys

from TileCache.Layer import MetaLayer
import psycopg2 as psycopg2
import psycopg2.extras
import json
from pprint import pprint

class BrewMap(MetaLayer):
    
    config_properties = [
      {'name':'name', 'description': 'Name of Layer'}, 
      {'name':'mapfile', 'description': 'Location of BrewMap configuration file'},
      {'name':'projection', 'description': 'Target map projection.'},
      {'name':'dbname', 'description': 'database name.'},
      {'name':'uname', 'description': 'database user name.'},
      {'name':'passwd', 'description': 'database password.'},
      {'name':'debug', 'description': 'switch debug output on or off'},
      {'name':'verbose', 'description': 'switch verbose output on or off'}
    ] + MetaLayer.config_properties 
    
    def __init__ (self, name, mapfile = None, projection = None, 
                  dbname='osm_gb', uname='graham', passwd='1234',
                  debug=False, verbose=False, **kwargs):
        MetaLayer.__init__(self, name, **kwargs) 
        self.mapfile = mapfile
        self.projection = projection
        self.dbname = dbname
        self.uname=uname
        self.passwd=passwd
        self.debug = debug

    def query2obj(self,sqlstr):
        """
        Takes an sql string to use as a query and executes it, 
        returning the result.
        The results are returned as an object.  It expectes the query to return
        a field called 'way' which is a text formatted postgis geometry 
        (ie POINT( longitude , latitude)).  This is parsed to create an object
        called 'point' with 'lat' and 'lng' entries in it.
        """
        connStr = 'dbname=%s user=%s' % (self.dbname,self.uname)
        if self.debug: print "connStr=%s" % (connStr)
        connection = psycopg2.connect(connStr)
        mark = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        mark.execute(sqlstr)
        records = mark.fetchall()

        if self.debug:
            if len(records)!=0: print "records=",records
        microbreweries={}
        if len(records)!=0:
            for record in records:
                mb = {}
                point = {}
                point['lng'] = record['way'].split("POINT(")[1].split(" ")[0]
                point['lat'] = record['way'].split("POINT(")[1].split(" ")[1]
                mb['point'] = point
                for key in record:
                    if key != "way":
                        mb[key] = record[key]
                microbreweries[record['osm_id']] = mb
        
        return microbreweries


    def deleteNullEntries(self,obj):
        """Delete entries from an object obj that are 'None'
        Recursively scans through obj for objects with obj.
        Non-object entities are checked and if they are not None, they are
        written to the output object, which is returned.
        NOTE:  This will probably not work problem for lists, but works for 
        objects within objects.
    
        HIST:
        16nov2011 GJ  ORIGINAL VERSION
        """
        op = {}
        for nameStr in obj:
            if type(obj[nameStr]).__name__=='dict':
                op[nameStr] = self.deleteNullEntries(obj[nameStr])
            elif  obj[nameStr]!=None:
                op[nameStr]=obj[nameStr]
        return op
    

    def make_json(self,seto,bbox):
        """
        makes the json files specified in the settings object seto.
        """
        if self.debug: print "make_json"
        # Loop through each layer group
        retObj = {}
        for lg in seto['layerGroups']:
            layerGroup = seto['layerGroups'][lg]
            sqlSelectCol = layerGroup['sqlSelectCol']
            sqlSelectPoint = layerGroup['sqlSelectPoint']
            sqlSelectPolygon = layerGroup['sqlSelectPolygon']
            sqlTagQueries = layerGroup['sqlTagQueries']
            tagQueriesDataFile = layerGroup['tagQueriesDataFile']
            # Loop through each layer within the group
            for layerStr in layerGroup['layers']:
                if self.debug: print "Layer = %s:" % layerStr
                layer = layerGroup['layers'][layerStr]
                if self.debug: pprint(layer)
                sqlWhere = layer['sqlWhere']
                dataFile = layer['dataFile']
                # Extract data from the points table
                sqlStr = "%s, %s %s" % \
                    (sqlSelectCol, sqlSelectPoint,sqlWhere)
                if self.debug: print sqlStr
                pointObj = self.query2obj(sqlStr)
                # Extract data from the polygons table
                sqlStr = "%s, %s %s" % \
                    (sqlSelectCol, sqlSelectPolygon,sqlWhere)
                if self.debug: print sqlStr
                polyObj = self.query2obj(sqlStr)
                # Merge the point and polygon data
                retObj.update(pointObj)
                retObj.update(polyObj)
            retObj = self.deleteNullEntries(retObj)

            ##########################################################
            # Now calculate the tagQueries.json file.
            sqlStr = "%s, %s %s" % \
                (sqlSelectCol, sqlSelectPoint,sqlTagQueries)
            tagQuery_point = self.query2obj(sqlStr)
            sqlStr = "%s, %s %s" % \
                (sqlSelectCol, sqlSelectPolygon,sqlTagQueries)
            tagQuery_poly = self.query2obj(sqlStr)

            tagQuery = {}
            tagQuery.update(tagQuery_poly)
            tagQuery.update(tagQuery_point)
            tagQuery = self.deleteNullEntries(tagQuery)


        return retObj



    def renderTile(self, tile):
        #m.width = tile.size()[0]
        #m.height = tile.size()[1]
        
        try:
            settingsFile=open(self.mapfile)
            settingsJSON = settingsFile.read()
        except:
            print "Error Reading Configuration File: %s.\n" % self.mapfile

        try:
            seto = json.loads(settingsJSON)
        except:
            print "oh no - there is an error in the configuration file: %s.\n" %\
                self.mapfile
            if self.debug:
                print sys.exc_info()[0]
                raise

        bbox = tile.bounds()

        if self.debug: print "Content-type: text/html\n\n";
        retObj = self.make_json(seto,bbox)

        #retStr = "mapfile=%s, tile bounds = (%f,%f),(%f,%f)\n" %\
        #    (self.mapfile,bbox[0],bbox[1],bbox[2],bbox[3])
        tile.data = json.dumps(retObj)
        if self.debug: print tile.data
        return tile.data







# INIT ----------------------------------------------------------
if __name__ == "__main__":
    from optparse import OptionParser

    usage = "Usage %prog [options] "
    version = "SVN Revision $Rev: 177 $"
    parser = OptionParser(usage=usage,version=version)
    parser.add_option("-f", "--file", dest="outfile",
                      help="filename to use for output",
                      metavar="FILE")
    parser.add_option("-c", "--config", dest="configFile",
                      help="Configuration File Name",
                      metavar="FILE")
    parser.add_option("-n", "--dbname", dest="dbname",
                      help="database name")
    parser.add_option("-u", "--uname", dest="dbuname",
                      help="database user name")
    parser.add_option("-p", "--dbpass", dest="dbpass",
                      help="database password")
    parser.add_option("-v", "--verbose", action="store_true",dest="verbose",
                      help="Include verbose output")
    parser.add_option("-d", "--debug", action="store_true",dest="debug",
                      help="Include debug output")
    parser.set_defaults(
        configFile = "BrewMap.cfg",
        outfile = "brewmap",
        dbname = "osm_gb",
        dbuname = "graham",
        dbpass = "1234",
        debug = False,
        verbose = False)
    (options,args)=parser.parse_args()
    
    if (debug):
        options.verbose = True
        print "options   = %s" % options
        print "arguments = %s" % args

    try:
        settingsFile=open(options.configFile)
        settingsJSON = settingsFile.read()
    except:
        print "Error Reading Configuration File: %s.\n" % options.configFile

    try:
        seto = json.loads(settingsJSON)
    except:
        print "oh no - there is an error in the configuration file: %s.\n" %\
            options.configFile
        if debug:
            print sys.exc_info()[0]
            raise

    #make_brew_json(options)
    make_json(options,seto)











