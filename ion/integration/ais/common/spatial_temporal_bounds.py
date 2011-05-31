#!/usr/bin/env python

"""
@file ion/integration/ais/common/spatial_temporal_bounds.py
@author David Everett
@brief Class to determine whether a given set of metadata is within a given
set of temporal/spatial bounds.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import time, datetime
from decimal import Decimal


class SpatialTemporalBounds(object):

    #
    # Constants for "vertical positive" parameter, which is either "up"
    # or "down" and denotes depth or altitude:
    # "down" means depth, and a positive number means below the surface,
    # sealevel being 0, and a negative number means above the surface.
    # "up" means altitude, and a positive number means above the surface,
    # sealeve being 0, and a negative number means below the surface.
    #
    UP = 0
    DOWN = 1
    
    bounds = {}
    filterByLatitude  = True
    filterByLongitude = True
    filterByVertical  = True
    filterByTime      = True
    bIsInAreaBounds      = True
    bIsInVerticalBounds  = True
    bIsInTimeBounds      = True


    def loadBounds(self, bounds):
        """
        Load up the bounds dictionary object with the given spatial and temporal
        parameters.
        """
        log.debug('__loadBounds')

        #
        # Set these flags; they're used for further tests
        #
        self.bIsMinLatitudeSet =  bounds.IsFieldSet('minLatitude')
        self.bIsMaxLatitudeSet =  bounds.IsFieldSet('maxLatitude')
        self.bIsMinLongitudeSet =  bounds.IsFieldSet('minLongitude')
        self.bIsMaxLongitudeSet =  bounds.IsFieldSet('maxLongitude')
        self.bIsMinVerticalSet  =   bounds.IsFieldSet('minVertical')
        self.bIsMaxVerticalSet  =   bounds.IsFieldSet('maxVertical')
        self.bIsVerticalPositiveSet = bounds.IsFieldSet('posVertical')        

        if self.bIsMinLatitudeSet:
            self.bounds['minLat'] = Decimal(str(bounds.minLatitude))
        if self.bIsMaxLatitudeSet:
            self.bounds['maxLat'] = Decimal(str(bounds.maxLatitude))
        if self.bIsMinLongitudeSet:
            self.bounds['minLon'] = Decimal(str(bounds.minLongitude))
        if self.bIsMaxLongitudeSet:
            self.bounds['maxLon'] = Decimal(str(bounds.maxLongitude))
        
        #
        # If both minLat and maxLat are NOT set, we need to set the default.
        # If none are set we won't filter by area.
        #
        if not (self.bIsMinLatitudeSet and self.bIsMaxLatitudeSet): 
            if self.bIsMinLatitudeSet:
                # 
                # maximum latitude possible
                #
                self.bounds['maxLat'] = Decimal('90')
            elif self.bIsMaxLatitudeSet:
                # 
                # minimum latitude possible
                #
                self.bounds['minLat'] = Decimal('-90')
            else:
                self.filterByLatitude = False

        #
        # If both minLon and maxLon are NOT set, we need to set the default.
        # If none are set we won't filter by area.
        #
        if not (self.bIsMinLongitudeSet and self.bIsMaxLongitudeSet): 
            if self.bIsMinLongitudeSet:
                # 
                # maximum longitude possible
                #
                if self.bounds['minLon'] >= 0:
                    self.bounds['maxLon'] = Decimal('180')
            elif self.bIsMaxLongitudeSet:
                # 
                # minimum longitude possible
                #
                if self.bounds['maxLon'] >= 0:
                    self.bounds['minLon'] = Decimal('-180')
            else:
                self.filterByLongitude = False

        #
        # All three of the vertical parameters must be set in order to filter
        # by vertical
        #
        if self.bIsVerticalPositiveSet and self.bIsMinVerticalSet and self.bIsMaxVerticalSet:
            self.bounds['minVert'] = Decimal(str(bounds.minVertical))
            self.bounds['maxVert'] = Decimal(str(bounds.maxVertical))
            #
            # If posVertical has not been set correctly, don't filter vertically
            #
            if "up" == bounds.posVertical:
                self.bounds['posVert'] = self.UP
            elif "down" == bounds.posVertical:
                self.bounds['posVert'] = self.DOWN
            else:
                self.filterByVertical = False
        else:
            self.filterByVertical = False

        #
        # Load up the time bounds
        #
        if bounds.IsFieldSet('minTime'):
            self.minTimeBound = bounds.minTime
            tmpTime = datetime.datetime.strptime(bounds.minTime, \
                                                           '%Y-%m-%dT%H:%M:%SZ')
            self.bounds['minTime'] = time.mktime(tmpTime.timetuple())
        else:
            self.filterByTime = False

        if bounds.IsFieldSet('maxTime'):
            self.maxTimeBound = bounds.maxTime
            tmpTime = datetime.datetime.strptime(bounds.maxTime, \
                                                           '%Y-%m-%dT%H:%M:%SZ')
            self.bounds['maxTime'] = time.mktime(tmpTime.timetuple())
        else:
            self.filterByTime = False


    def isInBounds(self, dSetMetadata):
        """
        Determine if dataset resource is in bounds.
        Input:
          - dataset metadata
          - bounds
        """
        log.debug('__isInBounds()')

        self.bIsInAreaBounds = True
        self.bIsInVerticalBounds= True
        self.bIsInTimeBounds = True

        if self.filterByLatitude:
            #log.debug("----------------------------- 1 ------------------------------")
            self.bIsInAreaBounds = self.__isInLatitudeBounds(dSetMetadata, self.bounds)

        if self.bIsInAreaBounds and self.filterByLongitude:
            #log.debug("----------------------------- 2 ------------------------------")
            self.bIsInAreaBounds = self.__isInLongitudeBounds(dSetMetadata, self.bounds)

        if self.bIsInAreaBounds and self.filterByVertical:
            #log.debug("----------------------------- 3 ------------------------------")
            self.bIsInVerticalBounds = self.__isInVerticalBounds(dSetMetadata, self.bounds)
                                
        if self.bIsInAreaBounds and self.bIsInVerticalBounds and self.filterByTime:
            #log.debug("----------------------------- 4 ------------------------------")
            self.bIsInTimeBounds = self.__isInTimeBounds(dSetMetadata, self.bounds)
            
        if self.bIsInAreaBounds and self.bIsInTimeBounds and self.bIsInVerticalBounds:
            return True
        else:
            return False


    def __isInLatitudeBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in latitude bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInLatitudeBounds()')

        if self.bIsMinLatitudeSet:
            #
            # If bounds max is less that metadata min, bounds must be completely
            # less than data; return False
            #
            if  bounds['maxLat'] < minMetaData['ion_geospatial_lat_min']:
                log.debug(' bounds max %f is < metadata min %f' % (bounds['minLat'], minMetaData['ion_geospatial_lat_min']))
                return False

            #if minMetaData['ion_geospatial_lat_min'] < bounds['minLat']:
            #    log.error(' metadata min %f is < bounds %f' % (minMetaData['ion_geospatial_lat_min'], bounds['minLat']))
            #    return False
            
        if self.bIsMaxLatitudeSet:
            #
            # If bounds min is greater that metadata max, bounds must be completely
            # above the data; return False
            #
            if  bounds['minLat'] > minMetaData['ion_geospatial_lat_max']:
                log.debug('bounds min %s is > metadata max %s' % (bounds['maxLat'], minMetaData['ion_geospatial_lat_max']))
                return False
            
            #if minMetaData['ion_geospatial_lat_max'] > bounds['maxLat']:
            #    log.error('metadata max %s is > bounds %s' % (minMetaData['ion_geospatial_lat_max'], bounds['maxLat']))
            #    return False
            
        return True


    def __isInLongitudeBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in longitude bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInLongitudeBounds()')

        #
        # If bounds min is greater than metadata max, bounds must be completely
        # outside data; return False
        #
        if self.bIsMinLongitudeSet:
            if  bounds['minLon'] > minMetaData['ion_geospatial_lon_max']:
                log.debug('bounds min %s is > metadata max %s' % (bounds['minLon'], minMetaData['ion_geospatial_lon_max']))
                return False
        
        #
        # If bounds max is less than metadata min, bounds must be comletely
        # outside data; return False
        #
        if self.bIsMaxLongitudeSet:
            if  bounds['maxLon'] < minMetaData['ion_geospatial_lon_min']:
                log.debug('bounds max %s is < metadata min %s' % (bounds['maxLon'], minMetaData['ion_geospatial_lon_min']))
                return False
        
        return True


    def __isInVerticalBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in vertical bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInVerticalBounds()')

        #
        # This needs to adjust for the verical positive parameter
        #
        if self.DOWN == self.bounds['posVert']:
            log.debug('testing bounds by depth')
            #
            # If the minDepth for the data is greater than the max of the bounds,
            # return false
            #
            if self.bIsMinVerticalSet:
                if minMetaData['ion_geospatial_vertical_min'] > bounds['maxVert']:
                    log.debug('min depth for data: %s is > bounds max depth: %s' % (minMetaData['ion_geospatial_vertical_min'], bounds['maxVert']))
                    return False
            #
            # If the maxDepth for the data is less than the min of the bounds,
            # return false
            if self.bIsMaxVerticalSet:
                if minMetaData['ion_geospatial_vertical_max'] < bounds['minVert']:
                    log.debug('max depth for data: %s is < bounds min depth: %s' % (minMetaData['ion_geospatial_vertical_max'], bounds['minVert']))
                    return False
                
            #
            # If the data covers the bounds (bounds depth is contained by the
            # data depth)
            #

        elif self.UP == self.bounds['posVert']:
            log.debug('testing bounds by altitude')
        
        return True

        
    def __isInTimeBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in time bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInTimeBounds()')
        
        try:
            tmpTime = datetime.datetime.strptime(minMetaData['ion_time_coverage_start'], '%Y-%m-%dT%H:%M:%SZ')
            dataMinTime = time.mktime(tmpTime.timetuple())
    
            tmpTime = datetime.datetime.strptime(minMetaData['ion_time_coverage_end'], '%Y-%m-%dT%H:%M:%SZ')
            dataMaxTime = time.mktime(tmpTime.timetuple())
        except ValueError:
            log.error('Error converting bounds time to datatime format')
            #
            # Currently returning true in the spirit of returning more data than
            # less
            #
            return True

        #
        # If data start time is < bounds min time and data max time > bounds min time, return true
        #
        if dataMinTime < bounds['minTime'] and dataMaxTime > bounds['minTime']:
            #log.debug('%s is > bounds %s' % (dataMaxTime, bounds['maxTime']))
            log.debug('DATA TIME COVERS BOUNDS MIN TIME')
            log.debug(' %s is < bounds %s and...' % (minMetaData['ion_time_coverage_start'], self.minTimeBound))
            log.debug(' %s is > bounds %s' % (minMetaData['ion_time_coverage_end'], self.minTimeBound))
            return True
            
        #
        # If data start time is < bounds max time and < data max time > bounds max time, return true
        #
        if dataMinTime < bounds['maxTime'] and dataMaxTime > bounds['maxTime']:
            #log.debug('%s is > bounds %s' % (dataMaxTime, bounds['maxTime']))
            log.debug('DATA TIME COVERS BOUNDS MAX TIME')
            log.debug(' %s is < bounds %s and...' % (minMetaData['ion_time_coverage_start'], self.maxTimeBound))
            log.debug(' %s is > bounds %s' % (minMetaData['ion_time_coverage_end'], self.maxTimeBound))
            return True

        #
        # If data min time > bounds min time and data max time < bounds max time
        #
        if dataMinTime > bounds['minTime'] and dataMaxTime < bounds['maxTime']:
            #log.debug('%s is > bounds %s' % (dataMaxTime, bounds['maxTime']))
            log.debug('BOUNDS TIME COVERS DATA')
            log.debug(' %s is > bounds %s and...' % (minMetaData['ion_time_coverage_start'], self.minTimeBound))
            log.debug(' %s is < bounds %s' % (minMetaData['ion_time_coverage_end'], self.maxTimeBound))
            return True

        log.debug('DATA OUTSIDE TEMPORAL BOUNDS')
        log.debug(' %s , %s' % (self.minTimeBound, self.maxTimeBound))
        log.debug(' %s , %s' % (minMetaData['ion_time_coverage_start'],  minMetaData['ion_time_coverage_end']))

        return False

        
    def __printBounds(self, bounds):
        boundNames = list(bounds)
        log.debug('Spatial and Temporal Bounds: ')
        for boundName in boundNames:
            log.debug('   %s = %s'  % (boundName, bounds[boundName]))

