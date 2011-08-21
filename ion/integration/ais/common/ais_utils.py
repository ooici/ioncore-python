#!/usr/bin/env python

import time

from twisted.internet import defer

from ion.services.dm.inventory.association_service import AssociationServiceError, ASSOCIATION_GET_STAR_MSG_TYPE
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, \
    SUBJECT_PREDICATE_QUERY_TYPE, IDREF_TYPE, PREDICATE_REFERENCE_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, \
    DATASET_RESOURCE_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID, HAS_A_ID, OWNED_BY_ID



import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


class AIS_Mixin(object):


    # set to None to turn off timing logging, set to anything else to turn on timing logging
    AnalyzeTiming = None

    class TimeStampsClass(object):
        pass

    TimeStamps = TimeStampsClass()


    @defer.inlineCallbacks
    def findResourcesOfType(self, resourceType):
        """
        A utility method to find all resources of the given type (resourceType).
        """

        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        #
        # Set up a resource type search term using:
        # - TYPE_OF_ID as predicate
        # - object of type: resourceType parameter as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # ..(object)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = resourceType

        pair.object = type_ref

        try:
            result = yield self.asc.get_subjects(request)

        except AssociationServiceError:
            log.error('__findResourcesOfType: association error!')
            defer.returnValue(None)

        defer.returnValue(result)


    @defer.inlineCallbacks
    def getAssociatedOwner(self, dsID):
        """
        Worker class method to find the owner associated with a data set.
        This is a public method because it can be called from the
        findDataResourceDetail worker class.
        """
        log.debug('getAssociatedOwner() entry')

        request = yield self.mc.create_instance(SUBJECT_PREDICATE_QUERY_TYPE)

        #
        # Set up an owned_by_id search term using:
        # - OWNED_BY_ID as predicate
        # - LCS_REFERENCE_TYPE object set to ACTIVE as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # ..(subject)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = dsID

        pair.subject = type_ref

        log.info('Calling get_objects with dsID: ' + dsID)

        try:
            result = yield self.asc.get_objects(request)

        except AssociationServiceError:
            log.error('getAssociatedOwner: association error!')
            defer.returnValue(None)

        if len(result.idrefs) == 0:
            log.error('Owner not found!')
            defer.returnValue('OWNER NOT FOUND!')
        elif len(result.idrefs) == 1:
            log.debug('getAssociatedOwner() exit')
            defer.returnValue(result.idrefs[0].key)
        else:
            log.error('More than 1 owner found!')
            defer.returnValue('MULTIPLE OWNERS!')



    @defer.inlineCallbacks
    def getAssociatedSource(self, dSetID):
        """
        Worker class private method to get the data source that associated
        with a given data set.
        """

        if dSetID is None:
            log.error('getAssociatedSource: dSetID is None')
            defer.returnValue(None)

        log.debug('getAssociatedSource for dSetID %s' %(dSetID))

        qmsg = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
        pair = qmsg.pairs.add()
        pair.object = qmsg.CreateObject(IDREF_TYPE)
        pair.object.key = dSetID
        pair.predicate = qmsg.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = HAS_A_ID

        pair = qmsg.pairs.add()
        pair.object = qmsg.CreateObject(IDREF_TYPE)
        pair.object.key = DATASOURCE_RESOURCE_TYPE_ID
        pair.predicate = qmsg.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = TYPE_OF_ID
        try:
            results = yield self.asc.get_subjects(qmsg)
        except:
            log.exception('Error getting associated data source for Dataset: %s' % dSetID)
            defer.returnValue(None)

        dsrcs = [str(x.key) for x in results.idrefs]

        # we expect one:
        if len(dsrcs) != 1:
            log.error('Expected 1 datasource, got %d' % len(dsrcs))
            defer.returnValue(None)

        log.debug('getAssociatedSource() exit: returning: %s' % dsrcs[0])

        defer.returnValue(dsrcs[0])

    @defer.inlineCallbacks
    def getAssociatedDatasets(self, dSource):
        """
        Worker class private method to get the data sets that associated
        with a given data source.
        """
        log.debug('getAssociatedDatasets() entry')

        dSetList = []

        if dSource is None:
            log.error('getAssociatedDatasets: dSource parameter is None')
            defer.returnValue(dSetList)

        qmsg = yield self.mc.create_instance(ASSOCIATION_GET_STAR_MSG_TYPE)
        pair = qmsg.subject_pairs.add()
        pair.subject = qmsg.CreateObject(IDREF_TYPE)
        pair.subject.key = dSource.ResourceIdentity
        pair.predicate = qmsg.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = HAS_A_ID

        pair = qmsg.object_pairs.add()
        pair.object = qmsg.CreateObject(IDREF_TYPE)
        pair.object.key = DATASET_RESOURCE_TYPE_ID
        pair.predicate = qmsg.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = TYPE_OF_ID
        try:
            results = yield self.asc.get_star(qmsg)
        except:
            log.error('Error getting associated data sets for Datasource: ' + \
                      dSource.ResourceIdentity)
            defer.returnValue([])

        dsets = [str(x.key) for x in results.idrefs]

        log.info('Datasource %s has %d associated datasets.' %(dSource.ResourceIdentity, len(dsets)))
        log.debug('getAssociatedDatasets) exit: returning: %s' %(str(dsets)))

        defer.returnValue(dsets)


    def TimeStamp (self):
        TimeNow = time.time()
        TimeStampStr = "(wall time = " + str (TimeNow) + \
                       ", elapse time = " + str(TimeNow - self.TimeStamps.StartTime) + \
                       ", delta time = " + str(TimeNow - self.TimeStamps.LastTime) + \
                       ")"
        self.TimeStamps.LastTime = TimeNow
        return TimeStampStr

    def getMetadataCache(self):
        if hasattr(self, 'metadataCache'):
            return self.metadataCache
        else:
            return None
