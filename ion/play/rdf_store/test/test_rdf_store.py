#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_blob_store.py
@author Paul Hubbard
@author Michael Meisinger
@author David Stuebe
@brief test class for blob store
"""

import logging
from uuid import uuid4

from twisted.internet import defer

from ion.play.rdf_store.rdf_store import RdfStore
from ion.test.iontest import IonTestCase

from ion.play.rdf_store.rdf_base import RdfBlob, RdfAssociation, RdfEntity, RdfMixin, RdfState, WorkSpace, RdfDefs


class RdfStoreTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.rdfs=RdfStore()
        yield self.rdfs.init()
        
        self.key = str(uuid4())
        self.blob=RdfBlob.create('fancy value!')


    #@defer.inlineCallbacks
    #def test_get_404(self):
    #    # Make sure we can't read the not-written
    #    rc = yield self.rdfs.blobs.get_blobs(self.key)
    #    self.failUnlessEqual(rc, [])
    #    
    #    rc = yield self.rdfs.associations.get_associations(self.key)
    #    self.failUnlessEqual(rc, [])
    #    
    #    rc = yield self.rdfs.states.get_key(self.key)
    #    self.failUnlessEqual(rc, None)
    #    
    #    rc = yield self.rdfs.a_refs.get_references(self.key)
    #    self.failUnlessEqual(rc, set())

    def _compare_ws(self,ws1,ws2):
        self.assertEqual(ws1.get_associations().sort(),ws2.get_associations().sort())
        self.assertEqual(ws1.get_blobs().sort(),ws2.get_blobs().sort())
        #ents = ws1.get_entities()
        #if ents:
        #    print 'ws1 entity!'
        #    for ent in ents:
        #        print ent 
        #ents = ws2.get_entities()
        #if ents:
        #    print 'ws2 entity!'
        #    for ent in ents:
        #        print ent 
        self.assertEqual(ws1.get_entities().sort(),ws2.get_entities().sort())
        
        #ents = ws1.get_states()
        #if ents:
        #    print 'ws1 state!'
        #    for ent in ents:
        #        print ent 
        #ents = ws2.get_states()
        #if ents:
        #    print 'ws2 state!'
        #    for ent in ents:
        #        print ent 
        
        
        self.assertEqual(ws1.get_states().sort(),ws2.get_states().sort())
        self.assertEqual(ws1.key,ws2.key)
        # compare the keys in the references
        self.assertEqual(set(ws1.get_references().keys()),set(ws2.get_references().keys()))
        
        # Compare the full dictionary of references
        self.assertEqual(ws1.get_references(),ws2.get_references())

#        self.assertEqual(set(ws1.get_references().values()),set(ws2.get_references().values()))
        self.assertEqual(ws1.commitRefs,ws2.commitRefs)

    #@defer.inlineCallbacks
    #def test_commit_checkout_diff(self):
    #    
    #    print '=================================='
    #    print '=================================='
    #    print '========= a simple case! ========='
    #    print '=================================='
    #    print '=================================='
    #    
    #    triple1 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('trunk!'))
    #    assoc1=RdfAssociation.create(triple1[0],triple1[1],triple1[2])
    #
    #    ws1_in = WorkSpace.create([(assoc1,triple1)],self.key)
    #    
    #    print '===== Initial status ========'
    #    ws1_in.print_status()
    #    
    #    yield self.rdfs.commit(ws1_in)
    #    
    #    print '===== Committed ws1 status ========'
    #    ws1_in.print_status()
    #
    #    ws1_out = yield self.rdfs.checkout(self.key)
    #    
    #    self._compare_ws(ws1_out,ws1_in)
    #
    #    print '===== Checkout latest ========'
    #    ws1_out.print_status()
    #    
    #
    #    ws2_in = ws1_out.copy()
    #    # Add another associtation
    #    triple2 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('my trunk!'))
    #    ws2_in.add_triple(triple2)
    #
    #    print '===== Added new association status ========'
    #    ws2_in.print_status()
    #    
    #    
    #    print '===== Print the Diff ========'
    #    ws_diff = yield self.rdfs.diff_commit(ws2_in)
    #    ws_diff.print_status()
    #    
    #    
    #    yield self.rdfs.commit(ws2_in)
    #    print '===== Committed ws2 status ========'
    #    ws2_in.print_status()
    #
    #    ws2_out = yield self.rdfs.checkout(self.key)
    #
    #    self._compare_ws(ws2_out,ws2_in)
    #
    #    print '===== Checkout ws1 ========'
    #    ws1_a = yield self.rdfs.checkout(ws1_in.key,ws1_in.commitRefs)
    #    ws1_a.print_status()
    #
    #    self._compare_ws(ws1_a,ws1_out)
    #
    #    print '===== Print the Diff against ws1 commit ========'
    #    ws_diff = yield self.rdfs.diff_commit(ws2_in, ws1_in.commitRefs)
    #    ws_diff.print_status()
    #    
    #    print '===== Complete ========'
    #    
    #    
        
    @defer.inlineCallbacks
    def test_blobs_states_associations(self):
        
        print '=================================='
        print '=================================='
        print '====== a more complex case! ======'
        print '=================================='
        print '=================================='
        
        triple1 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('trunk!'))
        assoc1=RdfAssociation.create(triple1[0],triple1[1],triple1[2])
        
        triple2 = (RdfBlob.create('junk'),assoc1,RdfBlob.create('trunk!'))
        assoc2=RdfAssociation.create(triple2[0],triple2[1],triple2[2])
        
        triple3 = (RdfBlob.create(1),RdfBlob.create(2),RdfBlob.create(3))
        assoc3=RdfAssociation.create(triple3[0],triple3[1],triple3[2])

        ws1_in = WorkSpace.create([(assoc1,triple1),(assoc2,triple2),(assoc3,triple3)],self.key)
        
        print '===== Initial status ========'
        ws1_in.print_status()
        
        yield self.rdfs.commit(ws1_in)
        
        print '===== Committed ws1 status ========'
        ws1_in.print_status()

        ws1_out = yield self.rdfs.checkout(self.key)
        
        self._compare_ws(ws1_out,ws1_in)

        print '===== Checkout latest ========'
        ws1_out.print_status()
        

        ws2_in = ws1_out.copy()
        
        
        # Add another associtation
        triple4 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('my trunk!'))
        assoc4=RdfAssociation.create(triple4[0],triple4[1],triple4[2])
        
        ws2_in.add_association(assoc4,triple4)

        # add an association to ws1 as an entity.
        #Add a reference to a particular state         
        triple5 = (RdfBlob.create('junk'),ws2_in.make_rdf_reference(head=True),RdfBlob.create('my trunk!'))
        triple6 = (RdfBlob.create('junk'),RdfBlob.create('my trunk!'),ws2_in.make_rdf_reference())
        ws2_in.add_triple(triple5)
        ws2_in.add_triple(triple6)
        

        print '===== Added new association status ========'
        ws2_in.print_status()
        
        
        print '===== Print the Diff ========'
        ws_diff = yield self.rdfs.diff_commit(ws2_in)
        ws_diff.print_status()
        
        
        yield self.rdfs.commit(ws2_in)
        print '===== Committed ws2_in status ========'
        ws2_in.print_status()

        ws2_out = yield self.rdfs.checkout(self.key)

        print '===== Checkout ws2_out status ========'
        ws2_out.print_status()
        
        self._compare_ws(ws2_out,ws2_in)

        print '===== Checkout ws1 ========'
        ws1_a = yield self.rdfs.checkout(ws1_in.key,ws1_in.commitRefs)
        ws1_a.print_status()

        self._compare_ws(ws1_a,ws1_out)

        print '===== Print the Diff against ws1 commit ========'
        ws_diff1 = yield self.rdfs.diff_commit(ws2_in, ws1_in.commitRefs)
        ws_diff1.print_status()
        
        ws_diff2 = yield self.rdfs.diff_commit(ws2_out, ws1_in.commitRefs)
        ws_diff2.print_status()
        
        self._compare_ws(ws_diff1,ws_diff2)
        
        print '===== Complete ========'
        
        
        
    @defer.inlineCallbacks
    def test_resource_walking(self):
        
        # Some tricky stuff to setup a first container...
        resources=WorkSpace()
        resources.add_triple((RdfDefs.ROOT,RdfDefs.CLASS,RdfDefs.RESOURCES ))        
        yield self.rdfs.commit(resources)

        resources_ref=resources.make_rdf_reference(head=True)
        resources.add_triple((resources_ref,RdfDefs.INSTANCEOF, RdfDefs.ROOT))
                
        # A user
        props={
            'user name':'David Stuebe',
            'id':'12390usdkln23lys7',
            'group':'OOI:Developers',
            'email':'stu3b3@gmail.com',
        }
        associations=[('this',RdfDefs.MEMBER,RdfDefs.IDENTITY_RESOURCES)]
        ds_id=WorkSpace.resource_properties(RdfDefs.IDENTITY.object,props,associations)
        yield self.rdfs.commit(ds_id)
        ds_ref=ds_id.make_rdf_reference(head=True)
        
        # Another user
        props={
            'user name':'Michael Meisinger',
            'id':'asdlkmxluewku920yusoiy2',
            'email':'mmeisinger@ucsd.edu',
            'group':'OOI:Developers',
        }
        associations=[('this',RdfDefs.MEMBER,RdfDefs.IDENTITY_RESOURCES)]
        mm_id=WorkSpace.resource_properties(RdfDefs.IDENTITY.object,props,associations)
        yield self.rdfs.commit(mm_id)
        mm_ref=mm_id.make_rdf_reference(head=True)
        
        props={}
        
        associations=[
            ('this',RdfDefs.CLASS,RdfDefs.IDENTITY_RESOURCES),
            ('this',RdfDefs.MEMBER,mm_ref),
            ('this',RdfDefs.MEMBER,ds_ref)
        ]
        
        ids=WorkSpace.resource_properties('OOI:Identities',props,associations)
        yield self.rdfs.commit(ids)
        ids_ref=ids.make_rdf_reference(head=True)
        
        
        resources.add_triple((RdfDefs.RESOURCES,RdfDefs.MEMBER,ids_ref))
        yield self.rdfs.commit(resources)
        
        
        search = yield self.rdfs.walk((RdfDefs.CLASS,'*',RdfDefs.IDENTITY_RESOURCES))
        
        print '=== Printing Search Results==='
        search.print_workspace()
        
        