#!/usr/bin/env python


"""
@file ion/core/object/repository.py
@brief Repository for managing data structures
@author David Stuebe
@author Matt Rodriguez

TODO
Refactor Merge to use a proxy repository for the readonly objects - they must live in a seperate workspace.

"""
from ion.core.object.object_utils import ARRAY_STRUCTURE_TYPE, sha1_to_hex

import weakref
from twisted.internet import threads, reactor, defer

from ion.core.object import gpb_wrapper
from ion.core.object import object_utils

from ion.core.object import association_manager

from ion.core.exception import ApplicationError, ReceivedApplicationError, ReceivedContainerError

from ion.util import procutils as pu

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

COMMIT_TYPE = object_utils.create_type_identifier(object_id=8, version=1)
MUTABLE_TYPE = object_utils.create_type_identifier(object_id=6, version=1)
BRANCH_TYPE = object_utils.create_type_identifier(object_id=5, version=1)
LINK_TYPE = object_utils.create_type_identifier(object_id=3, version=1)

REQUEST_COMMIT_BLOBS_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=48, version=1)



from ion.core.object import object_utils

class RepositoryError(ApplicationError):
    """
    An exception class for errors in the object management repository 
    """

class IndexHash(dict):
    """
    A dictionary class to contain the objects owned by a repository. All repository objects are accessible by other
    repositories via the workbench which maintains a cache of all the local objects. Clean up is the responsibility of
    each repository.
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

        self._workbench_cache = None
        self._has_cache = False

        self._size = 0

    def _set_cache(self,cache):
        assert isinstance(cache, weakref.WeakValueDictionary), 'Invalid object passed as the cache for a repository.'
        self._workbench_cache = cache
        self.has_cache = True


    def _get_cache(self):
        return self._workbench_cache

    cache = property(_get_cache, _set_cache)

    def _set_has_cache(self, val):

        assert isinstance(val, bool), 'Invalid or non boolen value passed to set has_cache property!'
        self._has_cache = val
        # add everything currently in self to the cache!
        if val:
            self._workbench_cache.update(self)

    def _get_has_cache(self):
          return self._has_cache

    has_cache = property(_get_has_cache, _set_has_cache)


    def __sizeof__(self):
        return self._size


    def __getitem__(self, key):

        if dict.has_key(self, key):
            return dict.__getitem__(self, key)

        elif self.has_cache:
            # You get it - you own it!
            val = self.cache[key]
            # If it does not raise a KeyError - add it
            dict.__setitem__(self, key, val)
            return val
        else:
            raise KeyError('Key not found in index hash!')


    def __setitem__(self, key, val):

        if key not in self:
            self._size += val.__sizeof__()

        dict.__setitem__(self, key, val)
        if self.has_cache:
            self.cache[key]=val



    def copy(self):
        """ D.copy() -> a shallow copy of D """
        raise NotImplementedError('IndexHash does not support copy')


    @staticmethod # known case
    def fromkeys(S, v=None):
        """
        dict.fromkeys(S[,v]) -> New dict with keys from S and values equal to v.
        v defaults to None.
        """
        raise NotImplementedError('IndexHash does not support fromkeys')

    def get(self, key, d=None):
        """ Get Item from the Index Hash"""
        if dict.has_key(self, key):
            return dict.__getitem__(self, key)

        elif self.has_cache:
            # You get it - you own it!
            val = self.cache.get(key,d)

            if val != d:
                dict.__setitem__(self, key, val)

            return val
        else:
            return d


    def has_key(self, key):
        """ Check to see if the Key exists """
        if self.has_cache:
            return dict.has_key(self, key) or self.cache.has_key(key)
        else:
            return dict.has_key(self, key)

    def update(self, *args, **kwargs):
        """
        D.update(E, **F) -> None.  Update D from E and F: for k in E: D[k] = E[k]
        (if E has keys else: for (k, v) in E: D[k] = v) then: for k in F: D[k] = F[k]
        """
        dict.update(self, *args, **kwargs)
        if self.has_cache:
            self.cache.update(*args, **kwargs)

        # For now - don't bother parsing args just recount
        size = 0
        for item in self.itervalues():
            size += item.__sizeof__()
        self._size = size

    def clear(self):
        dict.clear(self)

        self._size=0

    def __delitem__(self, key):

        item = self.get(key)
        if key is not None:
            self._size -= item.__sizeof__()

        dict.__delitem__(self,key)





class ObjectContainer(object):
    """
    Base class for the repository and merge container
    """

    DefaultExcludedTypes = [ARRAY_STRUCTURE_TYPE,]

    def __init__(self):


        self._workspace = {}
        """
        A dictionary containing objects which are not yet indexed, linked by a
        counter refrence in the current workspace
        """

        self._workspace_root = None
        """
        Pointer to the current root object in the workspace
        """


        self.index_hash = IndexHash()
        """
        All content elements are stored here - from incoming messages and
        new commits - everything goes here. Now it can be decoded for a checkout
        or sent in a message.
        """

        self._commit_index = {}
        """
        Required for get_linked_object
        """

        self._process=None
        """
        Need for access to sending messages!
        """

        self.upstream=None
        """
        The upstream source of this repository.
        """

        self.excluded_types = self.DefaultExcludedTypes[:]
        """
        The list of currently excluded object types
        """


    @property
    def root_object(self):
        return self._workspace_root


    def __str__(self):
        """
        Since str is used in debugging it should never return an exception - no matter the state of the repository object
        """
        output  = '============== %s  ==============\n' % self.__class__.__name__

        output += 'Number of current workspace objects: %d \n' % len(self._workspace)
        output += 'Number of current index hash objects: %d \n' % len(self.index_hash)
        output += 'Excluded types:\n'

        try:
            for type in self.excluded_types:
                output += str(type)
        except TypeError, te:
            output += te

        output += '============== Root Object ==============\n'
        output += str(self._workspace_root) + '\n'
        output += '============ End Resource ============\n'
        return output


    def clear(self):
        """
        Clear the repository in preparation for python garbage collection
        """
         # Do some clean up!
        for item in self._workspace.itervalues():
            item.Invalidate()

        self._workspace.clear()
        self.index_hash.clear()
        self._workspace_root = None

        self._process = None
        self.upstream = None


    def _create_wrapped_object(self, rootclass, obj_id=None, addtoworkspace=True):
        """
        Factory method for making wrapped GPB objects from the repository
        """
        message = rootclass()

        obj = self._wrap_message_object(message, obj_id, addtoworkspace)

        return obj

    def _wrap_message_object(self, message, obj_id=None, addtoworkspace=True):

        if obj_id is None:
            obj_id = self.new_id()
        obj = gpb_wrapper.Wrapper(message)
        obj._repository = self
        obj._root = obj
        obj._parent_links = set()
        obj._child_links = set()
        obj._derived_wrappers={}
        obj._read_only = False
        obj._myid = obj_id
        obj._modified = True
        obj._invalid = False

        if addtoworkspace:
            self._workspace[obj_id] = obj

        return obj

    def get_linked_object(self, link):

        if link.ObjectType != LINK_TYPE:
            raise RepositoryError('Illegal argument type in get_linked_object.')


        if not link.IsFieldSet('key'):
            return None

        if self._workspace.has_key(link.key):

            obj = self._workspace.get(link.key)
            # Make sure the parent is set...
            #self.set_linked_object(link, obj)

            obj.AddParentLink(link)

            return obj

        elif self._commit_index.has_key(link.key):
            # make sure the parent is set...
            obj = self._commit_index.get(link.key)
            obj.AddParentLink(link)
            return obj

        elif self.index_hash.has_key(link.key):
            # @TODO - is this safe? Can the k/v be GC'd in the weakref dict inbetween haskey and get?
            element = self.index_hash.get(link.key)

        else:

            log.debug('Linked object not found. Need non local object: %s' % str(link))

            raise KeyError('Object not found in the local work bench.')



        if not link.type.object_id == element.type.object_id and \
                link.type.version == element.type.version:
            #@TODO Consider removing this somewhat costly check...
            raise RepositoryError('The link type does not match the element type found!')

        obj = self._load_element(element)

        # For objects loaded from the hash the parent child relationship must be set
        #self.set_linked_object(link, obj)
        obj.AddParentLink(link)

        if obj.ObjectType == COMMIT_TYPE:
            self._commit_index[obj.MyId]=obj
            obj.ReadOnly = True

        elif link.Root.ObjectType == COMMIT_TYPE:
            # if the link is a commit but the linked object is not then it is a root object
            # The default for a root object should be ReadOnly = False
            self._workspace[obj.MyId]=obj
            obj.ReadOnly = False

        else:
            # When getting an object from it's parent, use the parents readonly setting
            self._workspace[obj.MyId]=obj
            obj.ReadOnly = link.ReadOnly

        return obj

    def load_links(self, obj, excluded_types=None):
        """
        Load the child objects into the work space recursively
        """
        excluded_types = excluded_types or []
        for link in obj.ChildLinks:
            if not link.type.GPBMessage in excluded_types:
                child = self.get_linked_object(link)
                self.load_links(child, excluded_types)

    def _checkout_local_commit(self, commit, excluded_types):

        # Get the link to the root object
        link = commit.GetLink('objectroot')

        # Catch only KeyErrors!
        root_obj = self.get_linked_object(link)

        # Load the object structure
        self.load_links(root_obj, excluded_types)

        return root_obj

    @defer.inlineCallbacks
    def _checkout_remote_commit(self, commit, excluded_types):
        """
        Use the message client from the process to create a message
        Can not use the datastore client because of import problems...
        """

        proc = self._process
        if proc is None:
            raise RepositoryError('Can not make a non-local checkout without a process!')

        request = yield proc.message_client.create_instance(REQUEST_COMMIT_BLOBS_MESSAGE_TYPE)

        link = commit.GetLink('objectroot')
        request.commit_root_object = link.key
        for extype in excluded_types:
            exobj = request.excluded_types.add()
            exobj.object_id = extype.object_id
            exobj.version = extype.version

        try:
            result, headers, msg = yield proc.rpc_send(self.upstream, 'checkout', request)

        except ReceivedApplicationError, rae:
            log.error(rae)
            raise RepositoryError('Received application error during remote checkout operation!', 404)
        except ReceivedContainerError, rce:
            log.error(rce)
            raise RepositoryError('Received container error during remote checkout operation!', 404)

        for blob in result.blob_elements:
            element = gpb_wrapper.StructureElement(blob.GPBMessage)
            self.index_hash[element.key] = element
            #print element

        element = self.index_hash[link.key]
        root_obj = self._load_element(element)

        self.load_links(root_obj, excluded_types)


        defer.returnValue(root_obj)



    def checkout_commit(self, commit, excluded_types):
        """
        @brief Checkout_commit will checkout the content of a commit. It will attempt to get
        the content from the local repository. Failing that it will attempt to get it from a
        remote process.
        @param commit is the commit object to checkout
        @param excluded_types is a list of type objects to exclude while checking out the structure
        @retval the result maybe deferred as a result of the non local checkout. yielding will
        return the root_object.
        """

        # Set the excluded types used in checking out this repository
        self.excluded_types = excluded_types    # @TODO: update instead of replace?

        if not hasattr(excluded_types, '__contains__' ):
            raise RepositoryError('Invalid argument excluded_types in checkout_commit: must be a list of object types')

        link = commit.GetLink('objectroot')
        if link.type.GPBMessage in excluded_types:
            raise RepositoryError('Can not exclude the type of the root object!')

        root_obj = None

        try:
            root_obj = self._checkout_local_commit(commit, excluded_types)

        except KeyError, ke:

            log.info('Making non-local checkout')

        if root_obj is None:

            root_obj = self._checkout_remote_commit(commit, excluded_types)
            # This is a deferred!

        return root_obj


    def _load_element(self, element):

        # check that the calculated value in element.sha1 matches the stored value
        if not element.key == element.sha1:
            #@TODO Consider removing this somewhat costly check - it is already done when an object is read from a message
            raise RepositoryError('The sha1 key does not match the value. The data is corrupted! \n' +\
            'Element key %s, Calculated key %s' % (object_utils.sha1_to_hex(element.key), object_utils.sha1_to_hex(element.sha1)))

        cls = object_utils.get_gpb_class_from_type_id(element.type)

        # Do not automatically load it into a particular space...
        obj = self._create_wrapped_object(cls, obj_id=element.key, addtoworkspace=False)

        # If it is a leaf element set the bytes for the object, do not load it
        # If it is not a leaf element load it and find its child links
        if element.isleaf:

            obj._bytes = element.value

        else:
            obj.ParseFromString(element.value)
            obj.FindChildLinks()


        obj.Modified = False

        # Make a note in the element of the child links as well!
        for child in obj.ChildLinks:
            element.ChildLinks.add(child.key)

        return obj





class Repository(ObjectContainer):
    
    UPTODATE='up to date'
    MODIFIED='modified'
    NOTINITIALIZED = 'This repository is not initialized yet (No commit checked out)'
    INVALID = 'The repository or its content objects are in an invalid state!'

    MERGEREQUIRED = 'This repository is currently being merged!'


    def __init__(self, head=None, repository_key=None, persistent=False, cached=False):
        
        
        #self.status  is a property determined by the workspace root object status

        ObjectContainer.__init__(self)


        self._object_counter=1
        """
        A counter object used by this class to identify content objects untill
        they are indexed
        """

        
        self._current_branch = None
        """
        The current branch object of the mutable head
        Branch names are generallly nonsense (uuid or some such)
        """
        
        self.branchnicknames = {}
        """
        Nick names for the branches of this repository - these are purely local!
        """
        
        self._detached_head = False

        
        self.merge=None
        """
        When merging a repository state there are multiple Merge Repository objects which hold the state of the object
        which is being merged. Access objects using: repo.merge[ind].<field in the root object>
        """
        
        self._stash = {}
        """
        A place to stash the work space under a saved name.
        """


        if not isinstance(persistent, bool):
            raise RepositoryError('Invalid argument type to set the persistent property of a repository')
        self._persistent = persistent
        """
        Set the persistence of this repository. Any repository which is declared persistent will not be GC'd until
        the persistent setting is changed to false
        """

        if not isinstance(cached, bool):
            raise RepositoryError('Invalid argument type to set the cached property of a repository')
        self._cached = cached
        """
        Set the cached property of this repository. Any repository which is declared cached will not be until the
        cache memory size of the workbench has been exceeded.
        """

        self.convid_context = 'DEFAULT'
        """
        This context object is used to determine when a repository should be moved from level 1 persistent caching
        to level 2 LRU caching in the workbench.
        """



        ### Structures for managing associations to a repository:

        self.associations_as_subject = association_manager.AssociationManager()
        self.associations_as_predicate = association_manager.AssociationManager()
        self.associations_as_object = association_manager.AssociationManager()

        # New method to setup the mutable head of the repository
        mutable_cls = object_utils.get_gpb_class_from_type_id(MUTABLE_TYPE)
        self._dotgit = self._create_wrapped_object(mutable_cls, addtoworkspace = False)

        if repository_key:
            self._dotgit.repositorykey = repository_key
        else:
            self._dotgit.repositorykey = pu.create_guid()

        """
        A specially wrapped Mutable GPBObject which tracks branches and commits
        It is not 'stored' in the index - it lives in the repository
        """
    
    def __str__(self):
        """
        Since str is used in debugging it should never return an exception - no matter the state of the repository object
        """

        output  = '============== Repository (status: %s) ==============\n' % self.status

        output += 'Number of current workspace objects: %d \n' % len(self._workspace)
        output += 'Number of current index hash objects: %d \n' % len(self.index_hash)
        output += 'Current context identifier for repository: %s \n' % self.convid_context
        output += 'Cached (%s) and Persistent (%s) settings \n' % (str(self.cached), str(self.persistent))
        if self._current_branch is not None:
            output += 'Current branch name: %s \n' % self._current_branch.branchkey
        else:
            output += 'Current branch name: %s \n' % None
        output += 'Branch Nicknames: %s \n' % str(self.branchnicknames)


        if self.merge is not None:
            output += 'Repository is currently merging %d state(s)! \n' % len(self.merge)

        else:
            output += 'Not currently merging state in this repository. \n'

        output += 'Excluded types:\n'
        try:
            for type in self.excluded_types:
                output += str(type)
        except TypeError, te:
            output += te

        output += '============== .git Object ==============\n'
        output += str(self._dotgit) + '\n'
        output += '============== Root Object ==============\n'
        output += str(self._workspace_root) + '\n'

        if self.merge is not None:
            for root in self.merge:

                output += '============== Merge Root Object ==============\n'
                output += str(root) + '\n'


        output += '============ End Repository! ============\n'


        return output

    def __eq__(self, other):

        if self is other:
            return True

        if not isinstance(other, Repository):
            return False

        #print self._dotgit
        #print other._dotgit

        if self._dotgit == other._dotgit:

            return True


    @property
    def Repository(self):
        """
        Convience method to which is available for any object client or the repository of the object itself
        """
        return self


    @property
    def repository_key(self):
        return self._dotgit.repositorykey

    def _set_persistent(self, value):
        if not isinstance(value, bool):
            raise RepositoryError('Invalid argument type to set the persistent property of a repository')
        self._persistent = value

    def _get_persistent(self):
        return self._persistent

    persistent = property(_get_persistent, _set_persistent )

    def _set_cached(self, value):
        if not isinstance(value, bool):
            raise RepositoryError('Invalid argument type to set the cached property of a repository')
        self._cached = value

    def _get_cached(self):
        return self._cached

    cached = property(_get_cached, _set_cached )



    def _get_root_object(self):
        return self._workspace_root
        
    def _set_root_object(self, value):
        if not isinstance(value, gpb_wrapper.Wrapper):
            raise RepositoryError('Can not set the root object of the repository to a value which is not an instance of Wrapper')
    
        if not value.Repository is self:
            value = self.copy_object(value)
        
        if not value.MyId in self._workspace:
            self._workspace[value.MyId] = value
            
        self._workspace_root = value
        
        
    root_object = property(_get_root_object, _set_root_object)
    

    def __sizeof__(self):
        """
        Treat the index hash of serialized content as the relevant size of the repository for caching
        """

        return self.index_hash.__sizeof__()

    def noisy_clear(self):
        pass

    def clear(self):
        """
        Clear the repository in preparation for python garbage collection
        """

        self.noisy_clear()

         # Do some clean up!
        for item in self._workspace.itervalues():
            #print 'ITEM',item
            item.Invalidate()
        for item in self._commit_index.itervalues():
            #print 'ITEM',item
            item.Invalidate()

        self.purge_associations()


        self._dotgit.Invalidate()

        self._workspace.clear()
        self.index_hash.clear()
        self._commit_index.clear()
        self._current_branch = None
        self.branchnicknames.clear()
        self._stash.clear()
        self.upstream = None
        self._process = None

        if self.merge is not None:
            for mr in self.merge:
                mr.clear()



    def set_repository_reference(self, id_ref, current_state=False):
        """
        Fill in a IDREF Object using the current state of the repository
        """
        # Don't worry about type checking here.... will cause an attribute error if incorrect
        id_ref.key = self.repository_key
        id_ref.branch = self._current_branch.branchkey
        
        if current_state:
            try:
                id_ref.commit = self._current_branch.commitrefs[0].MyId
            except IndexError, ie:
                log.error(ie)
                raise RepositoryError('Can not create repository reference: no commits on the current branch!')

        return id_ref
    
    
    @property
    def branches(self):
        """
        Convience method to access the branches from the mutable head (dotgit object)
        """
        return self._dotgit.branches


    @property
    def commit_head(self):
        """
        Convenience method to access the current commit
        """
        if self._detached_head:
            log.warn('This repository is currently a detached head. The current commit is not at the head of a branch.')

        if self._current_branch is None:
            raise RepositoryError('No current branch in the repository. Must checkout first.')

        if len(self._current_branch.commitrefs) == 1:          
            return self._current_branch.commitrefs[0]
        elif len(self._current_branch.commitrefs) == 0:
            return None
        else:
            raise RepositoryError('Branch should merge on read. Invalid state with more than one commit at the head of a branch!')

    def current_heads(self):
        """
        Convenience method to get a list of the current head commits
        """
        heads = []

        for branch in self.branches:
            heads.extend(branch.commitrefs)
            # Do not error on multiple commit refs!

        return heads


    def current_branch_key(self):

        if self._detached_head:
            log.warn('This repository is currently a detached head. The current commit is not at the head of a branch.')

        if self._current_branch is None:
            raise RepositoryError('No current branch in the repository. Must checkout first.')

        return self._current_branch.branchkey


    def branch(self, nickname=None):
        """
        @brief Create a new branch from the current commit and switch the workspace to the new branch.
        """
        ## Need to check and then clear the workspace???
        #if not self.status == self.UPTODATE:
        #    raise Exception, 'Can not create new branch while the workspace is dirty'
        
        if self._current_branch != None and len(self._current_branch.commitrefs)==0:
            # Unless this is an uninitialized repository it is an error to create
            # a new branch from one which has no commits yet...
            raise RepositoryError('The current branch is empty - a new one can not be created until there is a commit!')
        
        
        brnch = self.branches.add()
        # Generate a short random string for the branch name
        brnch.branchkey = object_utils.sha1hex(pu.create_guid())[:8]
        
        if nickname:
            if nickname in self.branchnicknames.keys():
                raise RepositoryError('That branch nickname is already in use.')
            self.branchnicknames[nickname]=brnch.branchkey

        if self._current_branch:
            # Get the linked commit

            if len(self._current_branch.commitrefs)>1:
                raise RepositoryError('Branch should merge on read. Invalid state!')
            elif len(self._current_branch.commitrefs)==1:
                cref = self._current_branch.commitrefs[0]
            
                bref = brnch.commitrefs.add()

                # Set the new branch to point at the commit
                bref.SetLink(cref)
            
            
            # Making a new branch re-attaches to a head!
            if self._detached_head:
                self._workspace_root.SetStructureReadWrite()
                self._detached_head = False


        else:
            # This is a new repository with no commits yet!
            pass


        self._current_branch = brnch
        return brnch.branchkey
    
    def remove_branch(self,name):
        branchkey = self.branchnicknames.get(name,None)
        if not branchkey:
            branchkey = name
        
        for idx, item in zip(range(len(self.branches)), self.branches):
            if item.branchkey == branchkey:
                del self.branches[idx]
                break
        else:
            log.info(str(self))
            raise KeyError('Branch Key not found in repository %s: Could not delete branch name "%s"' % (self.repository_key, name))

        # Clean up the branch nickname if any...
        for k,v in self.branchnicknames.items():
            if v == name:
                del self.branchnicknames[k]

        
    def get_branch(self, name):
        
        branchkey = self.branchnicknames.get(name,None)
        if not branchkey:
            branchkey = name
        
        branch = None
        for item in self.branches:
            if item.branchkey == branchkey:
                branch = item
                break
        else:
            log.info('Branch %s not found!' % name)
            
        return branch
    
    @defer.inlineCallbacks
    def checkout(self, branchname=None, commit_id=None, older_than=None, excluded_types=None, auto_merge=True):
        """
        Check out a particular branch
        Specify a branch, a branch and commit_id or a date
        Branch can be either a local nick name or a global branch key
        """

        if excluded_types is None:
            excluded_types = self.excluded_types or self.DefaultExcludedTypes[:]
        elif not hasattr(excluded_types, '__iter__'):
            raise RepositoryError('Invalid excluded_types argument passed to checkout')

        if branchname is not None:
            branchname = str(branchname)

        if commit_id is not None:
            commit_id  = str(commit_id)

        if older_than is not None:
            older_than = float(older_than)

        log.info('checkout: branchname - "%s", commit id - "%s", older_than - "%s", excluded_types - %s' % (branchname, commit_id, older_than, excluded_types))
        if self.status == self.MODIFIED:
            raise RepositoryError('Can not checkout while the workspace is dirty')
            #What to do for uninitialized? 
        
        #Declare that it is a detached head!
        detached = False
        
        if older_than and commit_id:
            raise RepositoryError('Checkout called with both commit_id and older_than!')
        
        if not branchname:
            raise RepositoryError('Checkout must specify a branchname!')
            
            
        branch = self.get_branch(branchname)
        if not branch:
            raise RepositoryError('Branch Key: "%s" does not exist!' % branchname)
            
        if len(branch.commitrefs) is 0:
            raise RepositoryError('This branch is empty - there is nothing to checkout!')
            
            
        # Set the current branch now!
        self._current_branch = branch
        
        cref = None
        merge_refs = False
            
        if commit_id:
            
            # IF you are checking out a specific commit ID it is always a detached head!
            detached = True
            
            # Use this set to make sure we only examine each commit once!
            touched_refs = set()
                
            crefs = branch.commitrefs[:]
            
            while len(crefs) >0:
                new_set = set()
                    
                for ref in crefs:
                        
                    if ref.MyId == commit_id:
                        
                        # Empty the crefs set to exit the while loop!
                        crefs = set()
                        # Save the CRef!
                        cref = ref
                        break # Break to ref in crefs
                        
                    # For each child reference...
                    for pref in ref.parentrefs:
                        # If we have not already looked at this one...
                        p_commit = pref.commitref
                        if not p_commit in touched_refs:                            
                            new_set.add(p_commit)
                            touched_refs.add(p_commit)    
                    
                else:
                    crefs = new_set                    
            else:
                if not cref:
                    raise RepositoryError('End of Ancestors: No matching reference \
                                          found in commit history on branch name %s, \
                                          commit_id: %s' % (branchname, commit_id))

        elif older_than:
            
            # IF you are checking out a specific commit date it is always a detached head!
            detached = True
            
            # Need to make sure we get the closest commit to the older_than date!
            younger_than = -9999.99
            
            # Use this set to make sure we only examine each commit once!
            touched_refs = set()
            
            crefs = branch.commitrefs[:]
            
            while len(crefs) >0:
                
                new_set = set()
                
                for ref in crefs:
                        
                    if ref.date <= older_than & ref.date > younger_than:
                        cref = ref
                        younger_than = ref.date
                        
                    # Only keep looking at parent references if they are to young
                    elif ref.date > older_than:
                        # For each child reference...
                        for pref in ref.parentrefs:
                            # If we have not already looked at this one...
                            if not pref in touched_refs:                            
                                new_set.add(pref)
                                touched_refs.add(pref)    
                       
                crefs = new_set
                       
            else:
                if not cref:
                    raise RepositoryError('End of Ancestors: No matching commit \
                                          found in commit history on branch name %s, \
                                          older_than: %s' % (branch_name, older_than))
                
        # Just checking out the current head - need to make sure it has not diverged! 
        else:
            
            if len(branch.commitrefs) ==1:
                cref = branch.commitrefs[0]

            elif len(branch.commitrefs) == 0:
                raise RepositoryError('This branch has not commits - cant check it out!')
            else:

                if auto_merge:
                    cref = self.merge_by_date(branch)

                else:
                    merge_refs = True

                    cref = self.get_common_ancestor(branch.commitrefs)


        # Do some clean up!
        for item in self._workspace.itervalues():
            item.Invalidate()
        self._workspace = {}
        self._workspace_root = None
            
        # Automatically fetch the object from the hashed dictionary
        rootobj = yield defer.maybeDeferred(self.checkout_commit, cref, excluded_types)

        self._workspace_root = rootobj

        self._detached_head = detached
        
        if detached:
            branch_cls = object_utils.get_gpb_class_from_type_id(BRANCH_TYPE)
            self._current_branch = self._create_wrapped_object(branch_cls, addtoworkspace=False)
            bref = self._current_branch.commitrefs.add()
            bref.SetLink(cref)
            self._current_branch.branchkey = 'detached head'
            
            rootobj.SetStructureReadOnly()

        if merge_refs:
            yield self.merge_with(branchname=branchname)


        log.debug('Checkout Complete!')
        defer.returnValue(rootobj)

        
        
    def merge_by_date(self, branch):
        
        crefs=branch.commitrefs[:]

        keys_are_the_same = True
        last_key = None
        newest = -999.99
        for cref in crefs:
            if cref.date > newest:
                head_cref = cref
                newest = cref.date


            key = cref.GetLink('objectroot').key

            if last_key is None:
                last_key = key

            if key != last_key:
                keys_are_the_same = False


        if keys_are_the_same:
            log.warn('BRANCH STATE HAS DIVERGED BUT CONTENT IS THE SAME - MERGING WITH NO INFORMATION LOST, REPO KEY %s' % self.repository_key)
        else:
            log.warn('BRANCH STATE HAS DIVERGED - MERGING BY DATE WITH INFORMATION LOST, REPO KEY %s' % self.repository_key)


        # Deal with the newest ref seperately
        crefs.remove(head_cref)
            
        # make a new commit ref
        commit_cls = object_utils.get_gpb_class_from_type_id(COMMIT_TYPE)
        cref = self._create_wrapped_object(commit_cls, addtoworkspace=False)
                    
        cref.date = pu.currenttime()

        pref = cref.parentrefs.add()
        pref.SetLinkByName('commitref',head_cref)
        pref.relationship = pref.Relationship.PARENT

        new_link = cref.GetLink('objectroot')
        old_link = head_cref.GetLink('objectroot')
        new_link.CopyLink(old_link)

        cref.comment = 'Merged divergent branch by date keeping the newest value'

        for ref in crefs:
            pref = cref.parentrefs.add()
            pref.SetLinkByName('commitref',ref)
            pref.relationship = pref.Relationship.MERGEDFROM
        
        structure={}                            
        # Add the CRef to the hashed elements
        cref.RecurseCommit(structure)
        
        # set the cref to be readonly
        cref.ReadOnly = True
        
        # Add the cref to the active commit objects - for convienance
        self._commit_index[cref.MyId] = cref

        # update the hashed elements
        self.index_hash.update(structure)
        
        del branch.commitrefs[:]
        bref = branch.commitrefs.add()
        bref.SetLink(cref)
        
        return cref


    def get_common_ancestor(self,crefs):

        ancestor = crefs[0]
        found_common = False
        tested = set()

        # Now look for the common ancestor
        while not found_common:

            tested.add(ancestor.MyId)

            for cref in crefs:

                if not ancestor.InParents(cref):
                    break
            else:
                found_common = True
                break

            try:
                ancestor = ancestor.parentrefs[0].commitref
            except IndexError, ex:
                log.exception('No common ancestor found in Repository!\n%s' % str(self))
                raise RepositoryError('No common ancestor found for commit ref.')

        return ancestor

    def reset(self):
        
        if self.status != self.MODIFIED:
            # What about not initialized
            return
        
        if len(self._current_branch.commitrefs)==0:
            raise RepositoryError('This current branch is empty - there is nothing to reset too!')
        
        cref = self._current_branch.commitrefs[0]

        # Do some clean up!
        self.purge_workspace()
            
            
        # Automatically fetch the object from the hashed dictionary or fetch if needed!
        rootobj = cref.objectroot
        self._workspace_root = rootobj
        
        self.load_links(rootobj, self.excluded_types)
                
        return rootobj


    def purge_workspace(self):

        if self.status == self.MODIFIED:

            #@TODO consider changing this to a warning rather than an exception
            log.warn('Called purge repository in a modified state! The current workspace is being lost!')

        # Do some clean up!
        for item in self._workspace.itervalues():
            item.Invalidate()

        self._workspace.clear()
        self._workspace_root = None


    def purge_associations(self):

        self.associations_as_object.predicate_sorted_associations.clear()
        self.associations_as_predicate.predicate_sorted_associations.clear()
        self.associations_as_subject.predicate_sorted_associations.clear()

        
    def commit(self, comment=''):
        """
        Commit the current workspace structure
        """
        
        # If the repo is in a valid state - make the commit even if it is up to date
        if self.status == self.MODIFIED or self.status == self.UPTODATE:
            structure={}

            # Reset the commit counter - used for debuging only
            gpb_wrapper.WrapperType.recurse_counter.count=0

            self._workspace_root.RecurseCommit(structure)

            cref = self._create_commit_ref(comment=comment)


            # Add the CRef to the hashed elements
            cref.RecurseCommit(structure)


            # set the cref to be readonly
            cref.ReadOnly = True
            
            # Add the cref to the active commit objects - for convenience
            self._commit_index[cref.MyId] = cref

            # update the hashed elements
            self.index_hash.update(structure)

            log.debug('Commited repository - Comment: "%s"' % cref.comment)
                            
        else:
            raise RepositoryError('Repository in invalid state to commit')
        
        # Like git, return the commit id
        branch = self._current_branch
        return branch.commitrefs.GetLink(0).key
            
            
    def _create_commit_ref(self, comment='', date=None):
        """
        @brief internal method to create commit references
        @param comment a string that describes this commit
        @param date the date to associate with this commit. If not given then 
        the current time is used.
        @retval a string which is the commit reference
        """
        # Now add a Commit Ref
        # make a new commit ref

        commit_cls = object_utils.get_gpb_class_from_type_id(COMMIT_TYPE)
        cref = self._create_wrapped_object(commit_cls, addtoworkspace=False)
        
        if not date:
            date = pu.currenttime()
            
        cref.date = date
            
        branch = self._current_branch
            
        # If this is the first commit to a new repository the current branch is a dummy
        # If it is initialized it is real and we need to link to it!
        if len(branch.commitrefs)==1:
            
            # This branch is real - add it to our ancestors
            pref = cref.parentrefs.add()
            parent = branch.commitrefs[0] # get the parent commit ref
            pref.SetLinkByName('commitref',parent)
            pref.relationship = pref.Relationship.PARENT
        elif len(branch.commitrefs)>1:
            raise RepositoryError('The Branch is in an invalid state and should have been merged on read!')
        else:
            # This is a new repository and we must add a place for the commit ref!
            branch.commitrefs.add()
            # Since the commit has no parents, set the root_seed
            cref.root_seed = self.repository_key
        
        
        # For each branch that we merged from - add a  reference
        if self.merge is not None:
            for mr in self.merge.merge_repos:
                mrgd = mr.commit
                pref = cref.parentrefs.add()
                pref.SetLinkByName('commitref',mrgd)
                pref.relationship = pref.Relationship.MERGEDFROM
            
        cref.comment = comment
        cref.SetLinkByName('objectroot', self._workspace_root)            
        
        # Clear the merge root and merged from
        self.merge = None
        # Update the cref in the branch
        branch.commitrefs.SetLink(0,cref)
        
        return cref
            

    @defer.inlineCallbacks
    def merge_with(self, branchname=None, commit_id = None):
        """
        merge the named branch in to the current branch
        
        This method does not 'do' the merger of state.
        It simply adds the parent ref to the repositories merged from list!
        
        """
        
        if self.status == self.MODIFIED:
            raise RepositoryError('Merging while the workspace is dirty better to make a new commit first!')
            #What to do for uninitialized?
            
        if self.status == self.NOTINITIALIZED:
            raise RepositoryError('Can not merge in a repository which is not initialized (Checkout something first!)')
        
        crefs=[]
                
        if commit_id:
            if commit_id == self._current_branch.commitrefs[0].MyId:
                raise RepositoryError('Can not merge into self!')
            try:
                crefs.append(self._commit_index[commit_id])
            except KeyError, ex:
                raise RepositoryError('Can not merge from unknown commit_id %s' % commit_id)
        
        elif branchname:
            
            branch = self.get_branch(branchname)
            if not branch:
                raise RepositoryError('Branch Key: "%s" does not exist!' % branchname)
            
            if branch.branchkey == self._current_branch.branchkey:
                if len(branch.commitrefs)<2:
                    raise RepositoryError('Can not merge with current branch head (self into self)')
                
                # Merge the divergent states of this branch!
                crefs = branch.commitrefs[:]
                if self.root_object.MyId == self._current_branch.commitrefs[0].GetLink('objectroot').key:
                    crefs.remove(self._current_branch.commitrefs[0])

            else:
                # Assume we merge any and all states of this branch?
                crefs.extend( branch.commitrefs)
        
        else:
            log.debug('''Arguments to Repository.merge - branchname: %s; commit_id: %s''' \
                      % (branchname, commit_id))
            raise RepositoryError('merge takes either a branchname argument or a commit_id argument!')
        
        assert len(crefs) > 0, 'Illegal state reached in repository Merge With function!'

        for cref in crefs:
            # Create a merge container to hold the merge object state for access

            mr = MergeRepository(cref, self.index_hash.cache, process=self._process, upstream=self.upstream)

            yield mr.load_root(excluded_types = self.excluded_types)

            if self.merge is None:
                self.merge = MergeContainer()

            self.merge.append(mr)

        
    @property
    def status(self):
        """
        Check the status of the current workspace - return a status

        # Be very careful with this method - it must not raise exceptions!
        """
        if self._workspace_root is not None:

            try:
                modified = self._workspace_root._source._modified
            except AttributeError, ae:
                log.error(ae)
                return self.INVALID


            if modified:
                retval = self.MODIFIED
            else:
                retval = self.UPTODATE
        else:
            retval = self.NOTINITIALIZED

        return retval
        
    def log_commits(self,branchname=None):
        
        if branchname is None:
            branchname = self._current_branch.branchkey
        
        branch = self.get_branch(branchname)
        log.info('$$ Logging commits on Branch %s $$' % branchname)
        cntr = 0
        for cref in branch.commitrefs:
            cntr+=1
            log.info('$$ Branch Head Commit # %s $$' % cntr)
            
            log.info('Commit: \n' + str(cref))
        
            while len(cref.parentrefs) >0:
                for pref in cref.parentrefs:
                    if pref.relationship == pref.Relationship.PARENT:
                            cref = pref.commitref
                            log.info('Commit: \n' + str(cref))
                            break # There should be only one parent ancestor from a branch

    def list_parent_commits(self,branchname=None):

        if branchname is None:
            branchname = self._current_branch.branchkey

        branch = self.get_branch(branchname)

        if len(branch.commitrefs) is not 1:
            return None

        cref = branch.commitrefs[0]
        keys=[]

        while cref:

            keys.append(sha1_to_hex(cref.MyId))

            if cref.parentrefs:
                cref = cref.parentrefs[0].commitref
            else:
                cref = False
        return keys

    def stash(self, name):
        """
        Stash the current workspace for later reference
        """
        raise Exception('Not implemented yet')
        
    def create_object(self, type_id):
        """
        @brief CreateObject is used to make new locally create objects which can
        be added to the resource's data structure.
        @param type_id is the type_id of the object to be created
        @retval the new object which can now be attached to the resource
        """
        
        cls = object_utils.get_gpb_class_from_type_id(type_id)
        obj = self._wrap_message_object(cls())
        return obj
        
    def new_id(self):
        """
        This id is a purely local concern - not used outside the local scope.
        """
        self._object_counter += 1
        return str(self._object_counter)


    @defer.inlineCallbacks
    def fetch_links(self, links):

        if hasattr(self._process, 'fetch_links'):
            # Get the method from the process if it overrides workbench
            fetch_links = self._process.fetch_links
        elif hasattr(self._process, 'workbench'):
            fetch_links = self._process.workbench.fetch_links

        else:
            raise RepositoryError('The repository object has no process to send a message with. It can not get the linked objects!')

        #@TODO provide catch mechanism to use the service name instead of the process name if the process does not respond...
        elements = yield fetch_links(self.upstream, links)

        self.index_hash.update(elements)

        # Load the content by the link!
        for link in links:
            self.get_linked_object(link)



    '''
    @defer.inlineCallbacks
    def get_remote_linked_object(self, link):
            
        try:
            obj = self.get_linked_object(link)
        except KeyError, ex:
            log.info('"get_remote_linked_object": Caught object not found:'+str(ex))
            res = yield self._fetch_remote_objects([link,])
            # Object is now in the hashed objects dictionary
            obj = self.get_linked_object(link)
        
        defer.returnValue(obj)
        return 
        
         
    @defer.inlineCallbacks
    def _fetch_remote_objects(self, links):

        if not self._process:
            raise RepositoryError('Linked Object not found and repository has no process to get it with!')
            
        if hasattr(self._process, 'fetch_links'):
            # Get the method from the process if it overrides workbench
            fetch_links = self._process.fetch_links
        else:
            fetch_links = self._process.workbench.fetch_links

        #@TODO provide catch mechanism to use the service name instead of the process name if the process does not respond...
        elements = yield fetch_links(self.upstream, links)

        self.index_hash.update(elements)

    @defer.inlineCallbacks
    def load_remote_links(self, items):
        """
        Load links which may require remote (deferred) access
        """
        if not hasattr(items, '__iter__'):
            items = [items,]

        remote_objects = []
        local_objects = []
        for item in items:
            for link in item.ChildLinks:
                try:
                    child = self.get_linked_object(link)
                    local_objects.append(child)
                except KeyError, ex:
                    log.info('"load_remote_links": Caught object not found:'+str(ex))
                    remote_objects.append(link)
            
        if remote_objects:
            res = yield self._fetch_remote_objects(remote_objects)
            local_objects.append(link.Root)
            # Rerun Load_remote_links after getting the child objects

        if len(local_objects) >0:
            res = yield self.load_remote_links(local_objects)
        else:
            res = True

        defer.returnValue(res)
    '''
    
    def copy_object(self, value, deep_copy=True, ignore_copy_errors=False):
        """
        Copy an object. This method will serialize the current state of the value.
        Then read it back in as new objects in the repository. The copies will all be
        created in a modified state. Copy can move from one repository to another.
        The deep_copy parameter determines whether all child objects are also copied.
        """

        log.debug('Copy Object:')
        if not isinstance(value, gpb_wrapper.Wrapper):
            raise RepositoryError('Can not copy an object which is not an instance of Wrapper')    
        
        if not value.IsRoot:
            # @TODO provide for transfer by serialization and re instantiation
            raise RepositoryError('You can not copy only part of a gpb composite, only the root!')
            

        if value.Modified:
            structure={}
            value_repo = value.Repository
            value.RecurseCommit(structure)
            # Deal with the case where this serialization causes a hash conflict...
            value_repo.index_hash.update(structure)





        element = self.index_hash.get(value.MyId)

        if element is None:
            raise RepositoryError('Could not get element from the index hash during copy.')
        new_obj = self._load_element(element)

        new_obj._set_parents_modified()

        if deep_copy:
            # Deep copy from the original!
            for link in new_obj.ChildLinks:
                # Use the copies link to get the child - possibly from a different repo!

                try:
                    child = self.get_linked_object(link)
                    new_child = self.copy_object(child, True)
                    link.SetLink(new_child)
                except KeyError, ke:
                    if ignore_copy_errors:
                        log.debug("Copy Object: ignored unfound child link %s" % link)
                    else:
                        # reraise
                        raise ke

        log.debug('Copy Object: Complete')

        return new_obj
    
    
        
    def set_linked_object(self,link, value, ignore_copy_errors=False):
        # If it is a link - set a link to the value in the wrapper
        if link.ObjectType != LINK_TYPE:
            # Should never happen - checked in the caller...
            raise RepositoryError('Can not set a composite field unless it is of type Link')

        if not isinstance(value, gpb_wrapper.Wrapper):
            log.debug('Error Setting Link in Object - Root Object Containing the Link: \n %s' % link.Root.Debug())
            log.error('Error Setting Link in Object - Attempting to set the link equal to a non GPBWrapper Value: "%s"' % str(value))

            raise RepositoryError('You can not assign an object link equal to a none GPB Wrapper value. Value type "%s", see log errors and log debug for more details' % type(value))

        if not value.IsRoot == True:
            # @TODO provide for transfer by serialization and re instantiation
            raise RepositoryError('You can not set a link equal to part of a gpb composite, only the root!')
            
        
        # if this value is from another repository... you need to load it from the hashed objects into this repository
        if not value.Repository is self:
            value = self.copy_object(value, ignore_copy_errors=ignore_copy_errors)
        
        
        if link.key == value.MyId:
                # Add the new link to the list of parents for the object
                value.AddParentLink(link) 
                # Setting it again is a pass...
                return
        
        if link.InParents(value):
            raise RepositoryError('You can not create a recursive structure - this value is also a parent of the link you are setting.')

        # Add the new link to the list of parents for the object
        value.AddParentLink(link) 
        
        # If the link is currently set
        if link.key:
                            
            old_obj = self._workspace.get(link.key,None)
            if old_obj:
                plinks = old_obj.ParentLinks
                plinks.remove(link)
                
                
                # Don't do this - read below!
                # If there are no parents left for the object delete it
                #if len(plinks)==0:
                    
                    # This could lead to an invalid state
                    #del self._workspace[link.key]
                    
                    # to do this correctly make a weak reference
                    # Remove it from the work space
                    # del the old_obj
                    # if it is still there in the weak ref put it back
                    # because something is still referenceing it
                    
                    # But really who cares - just leaving it hanging in the
                    # workbench, it will be garbage collected later.
                
        else:
            
            #Make sure the link is in the objects set of child links
            link.ChildLinks.add(link) # Adds to the links root wrapper!
            
        
            
        # Set the id of the linked wrapper
        link.key = value.MyId
        
        # Set the type
        tp = link.type
        object_utils.set_type_from_obj(value, tp)
        #link.type = object_utils.get_type_from_obj(value)

    def resolve_treeish(self, treeish, branch=None):
        """
        Resolves a git-style treeish from the specified branch.

        Treeishes have the ability to go back in commit history either linearly (parent) or along merges (2nd parent).
        You can use a treeish to see the state of an object previously. For example:

            ^^^^^      // 5 parents above the branch head
            ~3^~2      // 3 parents above, then one parent above, then another 2 parents above
            ^2~1       // 2nd parent of the branch head (of a merge commit), then 1 parent above that

        Current support is for carrot-parent and tilde spec.  See this document for more details: http://book.git-scm.com/4_git_treeishes.html

        @param  treeish     A string of the treeish characters to follow. O rder is important.  Point of reference is the tip of the
                            branch's head.
        @param  branch      A branch name to use as a reference point.  If not specified, master is used.
        @returns    A commit that the treeish resolves to.  If not found, a RepositoryError is raised.
                    It is your responsibility to do something with the commit returned - typical procedure
                    would be to call checkout and specify the commit_id parameter (as the returned object's .MyId
                    attribute).
        """
        origtreeish = treeish
        log.debug("resolve_treeish: %s" % origtreeish)

        branch = branch or "master"

        curbranch = self.get_branch(branch)
        curcommit = curbranch.commitrefs[0]

        while treeish:
            op = treeish[0]
            num = 1

            treeish = treeish[1:]

            # figure out additional numbers
            numbuf = ""
            while len(treeish) > 0 and treeish[0].isdigit():
                numbuf += treeish[0]
                treeish = treeish[1:]

            if numbuf.isdigit():
                num = int(numbuf)

            log.debug("treeish chunk: op %s, num %d, curcommit %s" % (op, num, sha1_to_hex(curcommit.MyId)))

            if op == "~":
                # Tilde Spec:
                # The tilde spec will give you the Nth grandparent of a commit object.
                for i in xrange(num):
                    if len(curcommit.parentrefs) == 0:
                        raise RepositoryError("Could not resolve treeish (%s): no parent of commit (%s), still %d parents left" % (origtreeish, sha1_to_hex(curcommit.MyId), num-i))

                    curcommit = curcommit.parentrefs[0].commitref
            elif op == "^":
                # Carrot Parent:
                # This will give you the Nth parent of a particular commit. This format is only useful on merge
                # commits - commit objects that have more than one direct parent.
                curparents = curcommit.parentrefs
                if num > len(curparents):
                    raise RepositoryError("Could not resolve treeish (%s): parent %d of commit (%s) requested, only %d present" % (origtreeish, num, sha1_to_hex(curcommit.MyId), len(curparents)))

                curcommit = curparents[num-1].commitref
            else:
                raise RepositoryError("Unknown treeish char: %s (treeish: %s)" % (op, origtreeish))

        log.debug("Treeish (%s) resolved to commit %s" % (origtreeish, sha1_to_hex(curcommit.MyId)))
        return curcommit


class MergeRepository(ObjectContainer):


    def __init__(self, commit, cache, process=None, upstream=None):

        ObjectContainer.__init__(self)

        self.index_hash.cache = cache

        self._process = process

        self.upstream = upstream

        # The commit does not belong to the Merge container - it is an object from the repository
        self.commit = commit


    @defer.inlineCallbacks
    def load_root(self, excluded_types):

        # may take a non-deferred path here
        self._workspace_root = yield defer.maybeDeferred(self.checkout_commit, self.commit, excluded_types)

        self._workspace_root.SetStructureReadOnly()

class MergeContainer(object):

    def __init__(self):

        self.parent = None

        self._merge_commits = {}
        self.merge_repos = []


    def append(self,item):

        if not isinstance(item, MergeRepository):
            raise RepositoryError('Can not add item to MergeContainer - it is not a MergeRepository')

        if item.commit in self._merge_commits:
            raise RepositoryError('Can not add item to MergeContainer - this commit is already merged')

        # Add it to the list - for primary access, also add it to the dict!
        self.merge_repos.append(item)

        self._merge_commits[item.commit] = item



    def _root_objects(self):

        root_list=[]
        for repo in self.merge_repos:

            root_list.append(repo.root_object)

        return root_list


    def __iter__(self):

        return self._root_objects().__iter__()

    def __len__(self):
        return len(self.merge_repos)


    def __getitem__(self, index):

        return self.merge_repos[index].root_object
