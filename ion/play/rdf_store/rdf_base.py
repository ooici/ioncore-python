#!/usr/bin/env python

"""
@file ion/play/rdf_store/rdf_service.py
@author David Stuebe
@brief  RDF Base class for the objects used in the rdf store.
"""

import logging

# Use the Json serialization and hashing
from ion.data.objstore import ValueRef

from uuid import uuid4

class RdfBase(object):
    
    ENTITY='E'
    STATE='S'
    ASSOCIATION='A'
    BLOB='B'
    
    SUBJECT='s'
    PREDICATE='p'
    OBJECT='o'
    
    def __init__(self,object,type,key=None, commitRefs=[]):
        self.object=object
        self.type=type

        # The Key for the RDF object
        if key:
            self.key=key
        else:
            self.key=ValueRef._secure_value_hash(object)

        # The commit reference for a particular state
        self.commitRefs=commitRefs



    def __hash__(self):
        '''
        @ Note This should not be a hashable object - it can not be used as a dict key
        http://docs.python.org/reference/datamodel.html?highlight=__cmp__#object.__hash__
        '''
        return None
        
    def __eq__(self,other):
        """
        @brief Object Comparison
        @param DataObject Instance
        @retval True or False
        """
        if not isinstance(other, RdfBase):
            return False
        if not self.key == other.key:
            return False
        if not self.type == other.type:
            return False
        if not self.commitRefs == other.commitRefs:
            return False
        if not self.object == other.object:
            return False        
        return True
    
    def __ne__(self,other):
        """
        @brief Object Comparison
        @param other DataObject Instance
        @retval Bool 
        """
        return not self.__eq__(other)

    def __str__(self):
        
        strng ='''\012===== Rdf Type: ''' + str(self.type) +''' =======\012'''
        strng+='''= Key: "''' + str(self.key) + '''"\012'''
        strng+='''= Object Content: "''' + str(self.object) + '''"\012'''
        strng+='''= Commit References: "''' + str(self.commitRefs) + '''"\012'''
        strng+='''============================================\012'''
        return strng

class RdfBlob(RdfBase):
    
    def __init__(self):
        pass

    @classmethod
    def create(cls, value):
        inst=cls()
        RdfBase.__init__(inst,value,RdfBase.BLOB)
        return inst
    
    @classmethod
    def load(cls, key, blob):
        inst=cls()
        RdfBase.__init__(inst,blob,RdfBase.BLOB,key=key)
        return inst



class RdfAssociation(RdfBase):
    
    def __init__(self):
        pass

    def get_dictionary(self):
        return self.object

    @classmethod
    def create(cls, subject, predicate, object):
        assert isinstance(subject, RdfBase)
        assert isinstance(predicate, RdfBase)
        assert isinstance(object, RdfBase)
        
        s=(subject.type,(subject.key, subject.commitRefs))
        p=(predicate.type,(predicate.key, predicate.commitRefs))
        o=(object.type,(object.key, object.commitRefs))
        
        a={ RdfBase.SUBJECT:s,
            RdfBase.PREDICATE:p,
            RdfBase.OBJECT:o }

        inst = cls()
        RdfBase.__init__(inst,a,RdfBase.ASSOCIATION)
        return inst

    @classmethod
    def sort_keys(cls, alist):
        
        # put it in a tuple if we got only one
        if not getattr(alist, '__iter__', False):
            alist = (alist,)
        
        types={
            RdfBase.ASSOCIATION:[],
            RdfBase.BLOB:[],
            RdfBase.ENTITY:[],
            RdfBase.STATE:[]
        }
        for a in alist:
            # make sure we got associations
            assert isinstance(a,RdfAssociation)
            
            for item in a.object:
                type_keycommit=a.object[item]
                
                key = type_keycommit[1][0]
                commit = type_keycommit[1][1]
                
                if not (key, commit) in types[type_keycommit[0]]:
                    types[type_keycommit[0]].append((key, commit))
        return types # This is an ugly data structure!

    @classmethod
    def load(cls, key, association):
        inst=cls()
        RdfBase.__init__(inst,association, RdfBase.ASSOCIATION, key=key)
        return inst
        

#@Todo - change to mixin class
class RdfMixin(object):
    '''
    Make a common class for Entity and State to inherit from
    '''
    def add(self, association):
        assert isinstance(association,RdfAssociation)
        self.object.add(association.key)
        
        
    def remove(self, association):
        assert isinstance(association,RdfAssociation)
        self.object.discard(association.key)

    def __len__(self):
        return len(self.object)

class RdfEntity(RdfBase,RdfMixin):
    '''
    An RdfEntity can only be created - Never returned from the State Service
    The State service only returns States!
    '''
    
    def __init__(self):
        pass    

    @classmethod
    def create(cls,associations, key=None):
        if not key:
            key=str(uuid4())
        s=set()
        if associations:
            if not getattr(associations, '__iter__', False):
                associations = (associations,)
            assert hasattr(associations, '__iter__')
            
            for a in associations:
                assert isinstance(a, RdfAssociation)
                s.add(a.key)
            
        inst=cls()
        RdfBase.__init__(inst,s,RdfBase.ENTITY,key)
        return inst

    @classmethod
    def load(cls, key, entity,commitRefs=[]):
        inst=cls()
        if not key:
            key=str(uuid4())
            
        RdfBase.__init__(inst,entity,RdfBase.ENTITY,key=key,commitRefs=commitRefs)
        return inst
    
    @classmethod
    def reference(cls, key):
        inst=cls()
        RdfBase.__init__(inst,set(),RdfBase.ENTITY,key=key)
        return inst
    

        
class RdfState(RdfBase, RdfMixin):
    '''
    The state of an RDF Resource returned from the State Service
    '''
    def __init__(self):
        pass


    @classmethod
    def create(cls,key,associations,commitRefs):

        if not commitRefs:
            logging.info('RdfState.create: commitRefs argument is None!')
            assert commitRefs != None
            
        if not getattr(commitRefs, '__iter__', False):
            commitRefs = [commitRefs]
        assert len(commitRefs)>0

        s=set()
        if associations:
            if not getattr(associations, '__iter__', False):
                associations = (associations,)
            assert hasattr(associations, '__iter__')
            for a in associations:
                assert isinstance(a, RdfAssociation)
                s.add(a.key)        

        inst=cls()
        RdfBase.__init__(inst,s,RdfBase.STATE,key=key,commitRefs=commitRefs)
        return inst

    @classmethod
    def load(cls, key, state, commitRefs):
        inst=cls()
        RdfBase.__init__(inst, state, RdfBase.STATE, key=key, commitRefs=commitRefs)
        return inst

    @classmethod
    def reference(cls, key, commitRefs):
        inst=cls()
        RdfBase.__init__(inst, set(), RdfBase.STATE, key=key, commitRefs=commitRefs)
        return inst


    
class WorkSpace(object):
    '''
    Things in the RdfWorkSpace sets are like things in your working directory in git.
    '''

    def __init__(self):
        
        self.commitRefs=[]
        self.key=None
        
        self.modified=None
        
        self.workspace={
            RdfBase.ASSOCIATION:{},
            RdfBase.BLOB:{},
            RdfBase.ENTITY:{},
            RdfBase.STATE:{}
        }

        self.references={}

    def __eq__(self, other):
        
        if not isinstance(other, WorkSpace):
            return False

        if not ws1.commitRefs == ws2.commitRefs:
            return False        

        if not ws1.key == ws2.key:
            return False
        
        if not ws1.get_associations() == ws2.get_associations():
            return False
        
        if not ws1.get_blobs() == ws2.get_blobs():
            return False
        
        if not ws1.get_entities() == ws2.get_entities():
            return False
        
        if not ws1.get_states() == ws2.get_states():
            return False
        
        if not ws1.get_references() == ws2.get_references():
            return False
        return True
        
    def __hash__(self):
        '''
        @ Note This should not be a hashable object - it can not be used as a dict key
        http://docs.python.org/reference/datamodel.html?highlight=__cmp__#object.__hash__
        '''
        return None
        

    def __ne__(self,other):
        """
        @brief Object Comparison
        @param other DataObject Instance
        @retval Bool 
        """
        return not self.__eq__(other)


    def add_triple(self,triple,*args):
        
        if isinstance(triple,RdfBase):
            triple=(args[0],triple,args[1])
        
        association = RdfAssociation.create(triple[0],triple[1],triple[2])
        self.add_association(association,triple)
    
    def add_association(self,association,triple):        
        
        # Note that the workspace is modified
        self.modified=True
        
        assert type(triple) is tuple
        assert len(triple) ==3

        # Check to make sure the association and the tuples match! This is a bit exessive?
        if not association.object[RdfBase.SUBJECT][0] == triple[0].type:
            raise RuntimeError('Association Subject does not match triple subject type')
        if not association.object[RdfBase.PREDICATE][0] == triple[1].type:
            raise RuntimeError('Association predicate does not match triple predicate type')
        if not association.object[RdfBase.OBJECT][0] == triple[2].type:
            raise RuntimeError('Association object does not match triple object type')

        if not association.object[RdfBase.SUBJECT][1] == (triple[0].key,triple[0].commitRefs):
            raise RuntimeError('Association Subject does not match triple subject identity')
        if not association.object[RdfBase.PREDICATE][1] == (triple[1].key,triple[1].commitRefs):
            raise RuntimeError('Association predicate does not match triple predicate identity')
        if not association.object[RdfBase.OBJECT][1] == (triple[2].key,triple[2].commitRefs):
            raise RuntimeError('Association object does not match triple object identity')

        # Add the association to the workspace's list of associations
        self.workspace[association.type][association.key]=association
        # Make a reference counter for this association
        if not self.references.has_key(association.key):
            self.references[association.key]=set(['self'])
            
            
        for item in triple:
            if not item.type == RdfBase.STATE:
                # Use the Key
                ref  = item.key
            else:
                # Use the commit
                ref =  item.commitRefs[0]
                if len(item.commitRefs) >1:
                    logging.info('WorkSpace:add_association Illegal attempt to reference a merge state!')
                    assert len(item.commitRefs) ==1
            
            self.workspace[item.type][ref]=item
            
            # add a reference to the thing!
            if ref in self.references:
                self.references[ref].add(association.key)
            else:
                self.references[ref]=set([association.key])    
    
    def remove_association(self, association):
        
        assert isinstance(association, RdfAssociation)
         
        if len(self.references[association.key]) > 0:
            self.references[association.key].discard('self')
            return
            
        # Note that the workspace is modified
        self.modified=True    
        
        del self.workspace[RdfBase.ASSOCIATION][association.key]
        
        for item in association.object:
            type_keycommit = association.object[item]
           
            type = type_keycommit[0]
            if not type == RdfBase.STATE:
                # Use the Key
                ref  = type_keycommit[1][0]
            else:
                # Use the commit
                ref =  type_keycommit[1][1]
                # We can safely delete the state from the workspace... in the store is another matter!
                
            self.references[ref].discard(association.key)
            
            if len(self.references[ref]) == 0:
                
                if type == RdfBase.ASSOCIATION:
                    assoc = self.get_associations(keys=ref)
                    self.remove_association(assoc[0])
                else: 
                    del self.workspace[type][ref]
            
    def copy(self):
        ws=self.load(RdfState.load(self.key,self.get_association_list(),self.commitRefs),
                                  self.get_associations(),
                                  self.get_entities(),
                                  self.get_states(),
                                  self.get_blobs())
        
        return ws
            
    @classmethod
    def load(cls,rdf, associations, entityRefs, stateRefs, blobs):
        '''
        @param rdf An RdfState or RdfEntity object retrieved from the store
        @param associations A set of associations retrieved from the store
        @param entityRefs A set of entity references (a Key)
        @param stateRefs A set of state references (a Key and commit Ref)

        '''
        inst=cls()
        
        # Set the key and commitRefs from state or entity
        inst.key=rdf.key
        if rdf.commitRefs:
            inst.commitRefs=rdf.commitRefs
        
        inst.modified=False
        
        for association in associations:
            inst.workspace[RdfBase.ASSOCIATION][association.key]=association
            
        for blob in blobs:
            inst.workspace[RdfBase.BLOB][blob.key]=blob
        
        for entityRef in entityRefs:
            inst.workspace[RdfBase.ENTITY][entityRef.key]=entityRef
        
        for stateRef in stateRefs:
            inst.workspace[RdfBase.STATE][stateRef.key]=stateRef
            

        # Set the reference count for each item based on the associations
        inst._set_references()
            
            
        return inst


    def _set_references(self):
        associations = self.get_associations()
        
        for a in associations:
            
            if not self.references.has_key(a.key):
                self.references[a.key]=set(['self'])
            else: 
                self.references[a.key].add('self')
                
            for item in a.object:
                type_keycommit=a.object[item]
                
                type = type_keycommit[0]
                key = type_keycommit[1][0]
                commit = tuple(type_keycommit[1][1])

                if type == RdfBase.STATE:
                    if not len(commit) ==1:
                        raise RuntimeError('Can not reference a merged state - it must be committed first')
                    ref = commit[0]
                else:
                    ref =key
                
                if ref in self.references:
                    self.references[ref].add(a.key)
                else:
                    self.references[ref]=set([a.key])  
        



    @classmethod
    def create(cls, associations_triples, key=None):
        '''
        @param associations_triples a list of tuples. Each tuple is an assocation and a triple of RdfBase instances
        '''
        inst=cls()
        inst.key=key
        
        if not getattr(associations_triples, '__iter__', False):
            associations_triples = (associations_triples,)
        
        for pair in associations_triples:
            assert type(pair) is tuple
            association=pair[0]
            triple=pair[1]
            assert type(triple) is tuple
            
            inst.add_association(association,triple)
        
        return inst


    # @Notes this method is the basis for a RDF Store client method to create resources
    @classmethod
    def resource_properties(cls, resource_name, property_dictionary, association_dictionary, key=None, commitRef=[]):
        '''
        @brief A method to create or update a resource description with properties and values.
        @brief The association_ditionary provides a way to specify associations to other existing resources.
        '''
        inst=cls()

        # Set keys and state...
        if key and commitRefs:
            res_instance=RdfState.reference(key,commitRefs)
        elif key:
            res_instance=RdfEntity.reference(key)
        else:
            res_instance=RdfEntity.create(None)
        
        inst.key=res_instance.key
        inst.commitRefs=res_instance.commitRefs
        
        
        # make some blobs that we need
        bprop = RdfBlob.create('OOI:Property')
        bval = RdfBlob.create('OOI:Value')
        binstof = RdfBlob.create('OOI:InstanceOf')

        bresname = RdfBlob.create(resource_name)
        
        # start adding triples
        # Resource UUID:instanceOf:resource name
        inst.add_triple(res_instance,binstof,bresname)
        
        # Add properties to the resource instance
        for prop in property_dictionary:
            
            inst.add_triple(bresname,bprop,RdfBlob.create(prop))
            
            inst.add_triple(RdfBlob.create(prop),bval,RdfBlob.create(property_dictionary[prop]))
        
        # add associations to the resource instance
        for predicate in association_dictionary:
            inst.add_triple(res_instance,RdfBlob.create(predicate),RdfBlob.create(association_dictionary[predicate]))
        
        return inst
        
        
    def fetch_associated(self,type_key_commit):
        
        type=type_key_commit[0]
        key=type_key_commit[1][0]
        commit=type_key_commit[1][1]

        if not type == RdfBase.STATE:
            # Use the Key
            ref  = key
        else:
            # Use the commit
            ref =  commitRefs[0]
            
        return self.workspace[type][ref]
        
    def make_rdf_reference(self,head=False):
        
        if not self.key:
            raise RuntimeError('A workspace must have a key before it can be referenced')
            
        key = self.key
        
        alist=[]
        if not self.modified:
            alist = self.get_association_list()

        commit=[]        
        if not head:
            commit = self.commitRefs
    
        if not self.key:
            raise RuntimeError('A workspace must have a key before it can be referenced as an RdfState')
            
        if commit:
            ref = RdfState.load(key,set(alist),commit)
        else:
            ref = RdfEntity.load(key,set(alist))
            
        return ref
        
            
        
        
    def len_associations(self):
        return len(self.workspace[RdfBase.ASSOCIATION])

    def get_associations(self,keys=None):
        if keys:
            if not getattr(keys, '__iter__', False):
                keys = (keys,)
            ret=[]
            for key in keys:
                ret.append(self.workspace[RdfBase.ASSOCIATION].get(key))
        else:
            ret=self.workspace[RdfBase.ASSOCIATION].values()
        return ret

    def get_association_list(self):
        return self.workspace[RdfBase.ASSOCIATION].keys()

    def len_blobs(self):
        return len(self.workspace[RdfBase.BLOB])

    def get_blobs(self,keys=None):
        if keys:
            if not getattr(keys, '__iter__', False):
                keys = (keys,)
            ret=[]
            for key in keys:
                ret.append(self.workspace[RdfBase.BLOB].get(key))
        else:
            ret=self.workspace[RdfBase.BLOB].values()
        return ret

    def get_blob_list(self):
        return self.workspace[RdfBase.BLOB].keys()
        
    def len_entities(self):
        return len(self.workspace[RdfBase.ENTITY])
        
    def get_entities(self,keys=None):
        if keys:
            if not getattr(keys, '__iter__', False):
                keys = (keys,)
            ret=[]
            for key in keys:
                ret.append(self.workspace[RdfBase.ENTITY].get(key))
        else:
            ret=self.workspace[RdfBase.ENTITY].values()
        return ret
        
    def get_entity_list(self):
        return self.workspace[RdfBase.ENTITY].keys()

    def len_states(self):
        return len(self.workspace[RdfBase.STATE])

    def get_states(self,keys=None):
        if keys:
            if not getattr(keys, '__iter__', False):
                keys = (keys,)
            ret=[]
            for key in keys:
                ret.append(self.workspace[RdfBase.STATE].get(key))
        else:
            ret=self.workspace[RdfBase.STATE].values()
        return ret

    def get_state_list(self):
        return self.workspace[RdfBase.STATE].keys()

    def get_references(self):
        return self.references

    def len_refs(self,item):
        assert isinstance(item,RdfBase)
        size=0
        if item.key in self.references:
            size = len(self.references[item.key])
        return size

    def print_status(self):
        print 'WorkSpace key',self.key
        print 'WorkSpace CommitRefs',self.commitRefs
        print 'WorkSpace Modified',self.modified
        print '# of Associations in workspace', self.len_associations()
        print '# of Blobs in workspace', self.len_blobs()
        print '# of Entities in workspace', self.len_entities()
        print '# of States in workspace', self.len_states()

    def print_workspace(self):
        
        for assoc in self.get_associations():
            strng=''
            dict = assoc.get_dictionary()
            for item in dict:
                if strng:
                    strng+=':'
                    
                spo=self.fetch_associated(dict[item])
           
                if spo.type == RdfBase.BLOB:
                    strng+='Blob "'+str(spo.object) + '"'
                elif spo.type == RdfBase.ASSOCIATION:
                    strng+='Association ID "'+str(spo.key) + '"'
                elif spo.type == RdfBase.STATE:
                    strng+='State Id&CommitRefs "'+str(spo.key)+','+ str(spo.commitRefs) + '"'
                elif spo.type == RdfBase.ENTITY:
                    strng+='Entity Id "'+str(spo.key)+'"'
        
            print 'Association ID', assoc.key
            print strng

