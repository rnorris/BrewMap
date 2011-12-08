# BSD Licensed, Copyright (c) 2006-2010 TileCache Contributors
# BrewMap code by Graham Jones, 2011.

import sys

from TileCache.Layer import MetaLayer
import psycopg2 as psycopg2
import psycopg2.extras
import json
import string
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
                  debug='False', verbose=False, **kwargs):
        MetaLayer.__init__(self, name, **kwargs) 
        self.mapfile = mapfile
        self.projection = projection
        self.dbname = dbname
        self.uname=uname
        self.passwd=passwd

        if (string.lower(debug)=='true'):
            self.debug = True
        else:
            self.debug = False
        if self.debug:
            print "Content: text/html\n\n"

    def query2obj(self,sqlstr):
        """
        Takes an sql string to use as a query and executes it, 
        returning the result.
        The results are returned as an object.  It expectes the query to return
        a field called 'wayStr' which is a geoJSON geometry object.
        It returns an object that is a GeoJSON FeatureCollection Object.
        """
        connStr = 'dbname=%s user=%s' % (self.dbname,self.uname)
        #if self.debug: print "connStr=%s" % (connStr)
        connection = psycopg2.connect(connStr)
        mark = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        mark.execute(sqlstr)
        records = mark.fetchall()

        if self.debug:
            if len(records)!=0: print "records=",records

        retObj = {'type':'FeatureCollection'}
        retObj['Features']=[]
        if len(records)!=0:
            for record in records:
                featureObj={'type':'Feature'}
                geomObj = json.loads(record['waystr'])
                if self.debug: 
                    print "geomObj = "
                    pprint(geomObj)
                featureObj['geometry']=geomObj
                featureObj['properties']={}

                for key in record:
                    if key != "waystr":
                        featureObj['properties'][key] = record[key]
                retObj['Features'].append(featureObj)
        return retObj


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
        if self.debug: 
            print "make_json"
	    print bbox

        sqlBbox = " and st_intersects(way,ST_SetSRID(ST_MakeBox2D(ST_MakePoint(%f,%f), ST_MakePoint(%f,%f)),900913)) " % (bbox)
        if self.debug: print "sqlBbox=%s" % sqlBbox

        # Loop through each layer group
        retObj = []
        for lg in seto['layerGroups']:
            if self.debug: print "layerGroup=%s" % lg
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
                #if self.debug: pprint(layer)
                sqlWhere = layer['sqlWhere']
                dataFile = layer['dataFile']
                # Extract data from the points table
                sqlStr = "%s, %s %s %s" % \
                    (sqlSelectCol, sqlSelectPoint,sqlWhere,sqlBbox)
                #if self.debug: print sqlStr
                pointObj = self.query2obj(sqlStr)
                # Extract data from the polygons table
                sqlStr = "%s, %s %s %s" % \
                    (sqlSelectCol, sqlSelectPolygon,sqlWhere,sqlBbox)
                #if self.debug: print sqlStr
                polyObj = self.query2obj(sqlStr)
                # Merge the point and polygon data
        
                layerObj = {'properties':{
                        'name':layerStr
                        }
                            }
                layerObj.update(pointObj)
                layerObj['Features'].append(polyObj['Features'])
                if self.debug: print layerObj
                retObj.append(layerObj)

            #retObj = self.deleteNullEntries(retObj)

            ##########################################################
            # Now calculate the tagQueries.json file.
            sqlStr = "%s, %s %s" % \
                (sqlSelectCol, sqlSelectPoint,sqlTagQueries)
            tagQuery_point = self.query2obj(sqlStr)
            sqlStr = "%s, %s %s %s" % \
                (sqlSelectCol, sqlSelectPolygon,sqlTagQueries,sqlBbox)
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

        if self.debug: print "Content-type: text/html\n\n";
        bbox = tile.bounds()

        retObj = self.make_json(seto,bbox)

        #retStr = "mapfile=%s, tile bounds = (%f,%f),(%f,%f)\n" %\
        #    (self.mapfile,bbox[0],bbox[1],bbox[2],bbox[3])
        tile.data = json.dumps(retObj,sort_keys=True, indent=4)
        if self.debug: print tile.data
        return tile.data







# INIT ----------------------------------------------------------
if __name__ == "__main__":
    bm = BrewMap(debug=True,mapFile='/home/OSM/code/BrewMap/server/BrewMap.cfg')
    renderTile({bounds:None})











