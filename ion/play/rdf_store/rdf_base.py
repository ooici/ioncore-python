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
import hashlib

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
        if not getattr(commitRefs, '__iter__', False):
                commitRefs = [commitRefs]
        self.commitRefs=commitRefs



    def __hash__(self):
        '''
        @ Note This should not be a hashable object - it can not be used as a dict key
        http://docs.python.org/reference/datamodel.html?highlight=__cmp__#object.__hash__
        '''

        return None
    
    def rdf_id(self):
        
        if self.type == self.STATE:
            #print self
            if not len(self.commitRefs)==1:
                raise RuntimeError('A RdfState Object must have exactly one commit ref to have a unique id!')
            ref = self.commitRefs[0]
        else:
            ref = self.key
            
        return ref
        
        
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
        
        

        # Checking type may cause problems if an association is made to an entity which has been committed?
        if len(subject.commitRefs)==1 and subject.type==RdfBase.STATE:
            s=(subject.type,(subject.key, subject.commitRefs[0]))
        elif len(subject.commitRefs)==0:
            s=(subject.type,(subject.key, None))
        else:
            logging.debug('Illegal Subject:')
            logging.debug(str(subject))
            raise RuntimeError('Can not create association to a merged state')
 
        if len(object.commitRefs)==1 and object.type==RdfBase.STATE:
            o=(object.type,(object.key, object.commitRefs[0]))
        elif len(object.commitRefs)==0:
            o=(object.type,(object.key, None))
        else:
            logging.debug('Illegal Object:')
            logging.debug(str(object))
            raise RuntimeError('Can not create association to a merged state')
 
        if len(predicate.commitRefs)==1 and predicate.type==RdfBase.STATE:
            p=(predicate.type,(predicate.key, predicate.commitRefs[0]))
        elif len(predicate.commitRefs)==0:
            p=(predicate.type,(predicate.key, None))
        else:
            logging.debug('Illegal Predicate:')
            logging.debug(str(predicate))
            raise RuntimeError('Can not create association to a merged state')
        
        
        #s=(subject.type,(subject.key, subject.commitRefs.pop()))
        #p=(predicate.type,(predicate.key, predicate.commitRefs.pop()))
        #o=(object.type,(object.key, object.commitRefs.pop()))
        
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
        
    def match(self, match, position=None):
    
        assert isinstance(match, RdfBase)
        
        if not position:
            position = [RdfBase.SUBJECT,RdfBase.PREDICATE,RdfBase.OBJECT]
        
        if not getattr(position, '__iter__', False):
            position = (position,)
        
        rc=False
        for pos in position:
            type__key_commit = self.object[pos]
            type=type__key_commit[0]
            key=type__key_commit[1][0]
            commit=type__key_commit[1][1]

            if type != match.type:
                continue

            if key != match.rdf_id():
                continue
            
            if commit:
                if commit == match.commitRefs:
                    rc = True
                    break
            else:
                
                rc = True
                break
        return rc


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


class RdfDefs(object):
    
    PROPERTY = RdfBlob.create('OOI:Property')
    VALUE = RdfBlob.create('OOI:Value')
    INSTANCEOF = RdfBlob.create('OOI:InstanceOf')
    MEMBER = RdfBlob.create('OOI:Member')
    CLASS = RdfBlob.create('OOI:Class')
    TYPE = RdfBlob.create('OOI:Type')

    IDENTITY_RESOURCES = RdfBlob.create('OOI:Identity Resources')
    RESOURCES = RdfBlob.create('OOI:Resources')
    ROOT = RdfBlob.create('OOI:ROOT')
    IDENTITY = RdfBlob.create('OOI:Identity')

    
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
        
        if not ws1.get_associations().sort() == ws2.get_associations().sort():
            return False
        
        if not ws1.get_blobs().sort() == ws2.get_blobs().sort():
            return False
        
        if not ws1.get_entities().sort() == ws2.get_entities().sort():
            return False
        
        if not ws1.get_states().sort() == ws2.get_states().sort():
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


    def add_triple(self,triple):
         
        if not len(triple)==3:
            raise RuntimeError('WorkSpace:add_triple takes an iterable of length 3, object which inherit from RdfBase')
            
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

#        if not association.object[RdfBase.SUBJECT][1] == (triple[0].key,triple[0].commitRefs):
        if not association.object[RdfBase.SUBJECT][1][0] == triple[0].key:
            raise RuntimeError('Association Subject does not match triple subject identity')
#        if not association.object[RdfBase.PREDICATE][1] == (triple[1].key,triple[1].commitRefs):
        if not association.object[RdfBase.PREDICATE][1][0] == triple[1].key:
            raise RuntimeError('Association predicate does not match triple predicate identity')
#        if not association.object[RdfBase.OBJECT][1] == (triple[2].key,triple[2].commitRefs):
        if not association.object[RdfBase.OBJECT][1][0] == triple[2].key:
            raise RuntimeError('Association object does not match triple object identity')

        # Add the association to the workspace's list of associations
        self.workspace[association.type][association.key]=association
        # Make a reference counter for this association
        if not self.references.has_key(association.key):
            self.references[association.key]=set(['self'])
            
            
        for item in triple:
            assert isinstance(item,RdfBase)
            # Convienence method to get a hashable id for an RdfBase object
            ref = item.rdf_id()
            
            self.workspace[item.type][ref]=item
            
            # add a reference to the thing!
            if ref in self.references:
                self.references[ref].add(association.key)
            else:
                self.references[ref]=set([association.key])    
    
    def remove_association(self, association):
        
        assert isinstance(association, RdfAssociation)
                
        self.references[association.key].discard('self')
        # Note that the workspace is modified
        self.modified=True
        
        if len(self.references[association.key]) == 0:
            del self.workspace[RdfBase.ASSOCIATION][association.key]
        else:
            return
            
        
        for item in association.object:
            type_keycommit = association.object[item]

            type = type_keycommit[0]
            if type == RdfBase.STATE:
                # Use the Key
                ref  = type_keycommit[1][1]
                
            else:
                # Use the commit
                ref =  type_keycommit[1][0]
                # We can safely delete the state from the workspace... in the store is another matter!
            
            self.references[ref].discard(association.key)
            
            if len(self.references[ref]) == 0:
                                
                if type == RdfBase.ASSOCIATION:
                    assoc = self.get_associations(keys=ref)
                    self.remove_association(assoc[0])
                else: 
                    del self.workspace[type][ref]
                    del self.references[ref]

            
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
            inst.workspace[RdfBase.STATE][stateRef.commitRefs[0]]=stateRef
            

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
                commit = type_keycommit[1][1]

                if type == RdfBase.STATE:
                    ref = commit
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
    def resource_properties(cls, resource_name, property_dictionary, association_tuple_list, key=None, commitRefs=[]):
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
        
        
        bresname = RdfBlob.create(resource_name)
       
        # start adding triples
        # Resource UUID:instanceOf:resource name
        inst.add_triple((res_instance,RdfDefs.INSTANCEOF,bresname))
        
        
        # Add properties to the resource instance
        for prop in property_dictionary:
            
            inst.add_triple((res_instance,RdfDefs.PROPERTY,RdfBlob.create(prop)))
            inst.add_triple((RdfBlob.create(prop),RdfDefs.VALUE,RdfBlob.create(property_dictionary[prop])))
        
        # add associations to the resource instance
        for mytuple in association_tuple_list:
            
            insert = []
            for item in mytuple:
                if item == 'this':
                    item = res_instance
                elif not isinstance(item,RdfBase):
                    # make a blob out of it!
                    item = RdfBlob.create(item)
                
                insert.append(item)
            
            insert = tuple(insert)
                
            # @Todo - make this smart to put entity references/state references
            inst.add_triple(insert)
        
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
            ref =  commitRefs
            
        return self.workspace[type][ref]
        
    def make_rdf_reference(self,head=True):
        
        if not self.key:
            raise RuntimeError('A workspace must have a key before it can be referenced')
            
        key = self.key
        
        alist=[]
        if not self.modified:
            alist = self.get_association_list()

        commit=[]        
        if not head:
            commit = self.commitRefs
            if not commit:
                raise RuntimeError('A workspace must have a commitRef before it can be referenced as an RdfState')
    
        if not self.key:
            raise RuntimeError('A workspace must have a key before it can be referenced as an Rdf State or Entity')
            
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
        logging.info('WorkSpace key',self.key)
        logging.info('WorkSpace CommitRefs',self.commitRefs)
        logging.info('WorkSpace Modified',self.modified)
        logging.info('# of Associations in workspace', self.len_associations())
        logging.info('# of Blobs in workspace', self.len_blobs())
        logging.info('# of Entities in workspace', self.len_entities())
        logging.info('# of States in workspace', self.len_states())

    def print_workspace(self):
        
        for assoc in self.get_associations():
            strng=''
            dict = assoc.get_dictionary()
            # Set the order correctly!
            dkeys=(RdfBase.SUBJECT,RdfBase.PREDICATE,RdfBase.OBJECT)
            
            for item in dkeys:
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
        
            logging.info('Association ID', assoc.key)
            logging.info(strng)

