# BSD Licensed, Copyright (c) 2006-2010 TileCache Contributors

import sys

from TileCache.Layer import MetaLayer

class BrewMap(MetaLayer):
    
    config_properties = [
      {'name':'name', 'description': 'Name of Layer'}, 
      {'name':'mapfile', 'description': 'Location of BrewMap configuration file'},
      {'name':'projection', 'description': 'Target map projection.'},
    ] + MetaLayer.config_properties 
    
    def __init__ (self, name, mapfile = None, projection = None, fonts = None, **kwargs):
        MetaLayer.__init__(self, name, **kwargs) 
        self.mapfile = mapfile
        self.projection = projection
            
    def renderTile(self, tile):
        #m.width = tile.size()[0]
        #m.height = tile.size()[1]
        
        bbox = tile.bounds()
        #bbox = mapnik.Envelope(bbox[0], bbox[1], bbox[2], bbox[3])
                    
        retStr = "mapfile=%s, tile bounds = (%f,%f),(%f,%f)\n" %\
            (self.mapfile,bbox[0],bbox[1],bbox[2],bbox[3])
        tile.data = retStr
        return tile.data
