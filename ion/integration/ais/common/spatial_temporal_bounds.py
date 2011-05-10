#!/usr/bin/env python

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import time, datetime
from decimal import Decimal


class SpatialTemporalBounds(object):

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
        # Determine if only one of the latitude bounds have been set; if so,
        # use the other to determine whether which hemisphere.  If no latitude
        # bounds have been given, set filterByLatitude to False.
        #
        
        self.bIsMinLatitudeSet =  bounds.IsFieldSet('minLatitude')
        self.bIsMaxLatitudeSet =  bounds.IsFieldSet('maxLatitude')
        self.bIsMinLongitudeSet =  bounds.IsFieldSet('minLongitude')
        self.bIsMaxLongitudeSet =  bounds.IsFieldSet('maxLongitude')
        self.bIsMinVerticalSet  =   bounds.IsFieldSet('minVertical')
        self.bIsMaxVerticalSet  =   bounds.IsFieldSet('maxVertical')
        self.bIsVerticalPositiveSet =   bounds.IsFieldSet('posVertical')        

        if self.bIsMinLatitudeSet:
            self.bounds['minLat'] = Decimal(str(bounds.minLatitude))
        if self.bIsMaxLatitudeSet:
            self.bounds['maxLat'] = Decimal(str(bounds.maxLatitude))
        if self.bIsMinLongitudeSet:
            self.bounds['minLon'] = Decimal(str(bounds.minLongitude))
        if self.bIsMaxLongitudeSet:
            self.bounds['maxLon'] = Decimal(str(bounds.maxLongitude))
        if self.bIsMinVerticalSet:
            self.bounds['minVert'] = Decimal(str(bounds.minVertical))
        if self.bIsMaxVerticalSet:
            self.bounds['maxVert'] = Decimal(str(bounds.maxVertical))


        #
        # If both minLat and maxLat are not set, we need to determine which
        # hemisphere the other is in (if it's set), so that we can know how
        # to default the one that isn't set.  If none are set we won't filter
        # by area.  Same with minLon and maxLon.
        #
        if not (self.bIsMinLatitudeSet and self.bIsMaxLatitudeSet): 
            if self.bIsMinLatitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                # Don't have to check which hemisphere anymore; set to
                # maximum latitude possible
                #
                self.bounds['maxLat'] = Decimal('90')
            elif self.bIsMaxLatitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                # Don't have to check which hemisphere anymore; set to
                # minimum latitude possible
                #
                self.bounds['minLat'] = Decimal('-90')
            else:
                self.filterByLatitude = False

        if not (self.bIsMinLongitudeSet and self.bIsMaxLongitudeSet): 
            if self.bIsMinLongitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                # Don't have to check which hemisphere anymore; set to
                # maximum longitude possible
                #
                if self.bounds['minLon'] >= 0:
                    self.bounds['maxLon'] = Decimal('180')
            elif self.bIsMaxLongitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                # Don't have to check which hemisphere anymore; set to
                # minimum longitude possible
                #
                if self.bounds['maxLon'] >= 0:
                    self.bounds['minLon'] = Decimal('-180')
            else:
                self.filterByLongitude = False

        #
        # If posVertical has not been set, don't filter vertically
        #
        if self.bIsVerticalPositiveSet:
            self.bounds['vertPos'] = bounds.posVertical
        else:
            self.filterByVertical = False

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
            if minMetaData['ion_geospatial_lat_min'] < bounds['minLat']:
                log.debug(' %f is < bounds %f' % (minMetaData['ion_geospatial_lat_min'], bounds['minLat']))
                return False
            
        if self.bIsMaxLatitudeSet:
            if minMetaData['ion_geospatial_lat_max'] > bounds['maxLat']:
                log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_lat_max'], bounds['maxLat']))
                return False
            
        return True


    def __isInLongitudeBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in longitude bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInLongitudeBounds()')

        if self.bIsMinLongitudeSet:
            if minMetaData['ion_geospatial_lon_min'] < bounds['minLon']:
                log.debug('%s is < bounds %s' % (minMetaData['ion_geospatial_lon_min'], bounds['minLon']))
                return False
            
        if self.bIsMaxLongitudeSet:
            if minMetaData['ion_geospatial_lon_max'] > bounds['maxLon']:
                log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_lon_max'], bounds['maxLon']))
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
        if self.bIsMinVerticalSet:
            if minMetaData['ion_geospatial_vertical_min'] > bounds['minVert']:
                log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_vertical_min'], bounds['minVert']))
                return False
            
        if self.bIsMaxVerticalSet:
            if minMetaData['ion_geospatial_vertical_max'] > bounds['maxVert']:
                log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_vertical_max'], bounds['maxVert']))
                return False
        
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

