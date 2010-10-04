


class StoreFactory(object):
    """
    make instances of a specific implementation of store
    like a client creator

    has and holds onto a configuration for a Store backend
    
    XXX NOTE for Core Refactoring!!:
    The reactor gets control of this. 
    The base process class provides access to the reactor.
    The base process class is where the container api is exposed, as it is
    a common denominator api for all things running in the container. 
    Any reactor driven thing must be done through this api.
    """

class KeyValueStore(object):
    """
    an implementation of store

    the store factory instantiates this class and gives it a working
    backend object.
    The invariant things between any two implementations are the IStore
    methods (name, arguments, and return type) and the 'backend' object
    used to implement the methods.

    Store could be the central registry of encoding types.

    implements(IStore)
    """


class DataObjectNode(object):
    """
    provides IDataObject 

    has actual dataobject (and dataobject description)
    has a storage place for the encoding result
    has a dataobjectnode encoding

    integrates DataObject with DataObject encoding and DataStore
    (DataObjectNodeStore and ContentNodeStoer)

    ---
    data_object_chassis = datastore.clone(id)
    data_object_chassis.object_node # python object representation 
                                         # of Data Object Node
    data_object_chassis.content_node # Python obj of Commit


    """


    

class DataObjectNodeStore(object):
    """Adapter
    Adapts bare key value store to Data Object Node Store.

    Main methods deal in Data Object Node objects

    Has way to use different encoders based on local registry
    """

    def __init__(self, store):
        """store or backend?
        """
        self.store = store
        self._keyspace = "objectnode"

    def _full_key(self, id):
        return "%s.%s" % (self._keyspace, id,)

    def encode(self, o):
        """
        make sure o is a DataObjectNode instance

        use an encoder directly
        """

    def get(self, id):
        """
        get data object node
        @retval object that implements IDataObjectNode
        """
        data = self.store.get(self._full_key(id))
        o = self.decode(data) # Throw DatastoreException when decode fails
        return o


    def put(self, id, node, oldnode=None):
        """
        The node contains a flag that specifies how put can go if the id
        already exists. Flag options:
            * oldnode must match the current node (test and set)
            * always put (no test and set)
        @note This needs to be atomic. When using the test and set option,
        the read and write must occur in sequence, or the underlying store
        must provide a test and set version of put
        """
        self.store.put(self._full_key(id), self.encode(node))

    def create(self, id, flag=None):
        """
        returns new node
        optional arg for data object here?
        """

    def delete(self, id):
        """
        """

class DataStore(object):
    """
    implements(IDataStore)

    is this essentially the process / service?
    """

    def start(self):
        """
        create backend 
        instantiate Store
        """










