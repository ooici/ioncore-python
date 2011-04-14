"""
Created on Apr 12, 2011

@author: Matt Rodriguez
"""
from twisted.internet import defer

from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG
from ion.test.iontest import IonTestCase
from ion.core.process.process import Process

from ion.services.coi.datastore_bootstrap import dataset_bootstrap
from ion.services.coi.datastore_bootstrap.ion_preload_config import ID_CFG, TYPE_CFG
from ion.services.coi.datastore_bootstrap.ion_preload_config import NAME_CFG,DESCRIPTION_CFG 
from ion.services.coi.datastore_bootstrap.ion_preload_config import CONTENT_CFG, CONTENT_ARGS_CFG

from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_DATASETS
from ion.services.dm.inventory.association_service import AssociationServiceClient
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
CONF = ioninit.config(__name__)

import cProfile
from ion.core.object import object_utils

class AssociationServiceTest(IonTestCase):
    """
    Testing association service.
    """
    services = [
            {'name':'ds1',
             'module':'ion.services.coi.datastore',
             'class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True}}
            },
            {'name':'workbench_test1',
             'module':'ion.core.object.test.test_workbench',
             'class':'WorkBenchProcess',
             'spawnargs':{'proc-name':'wb1'}
            },

            {'name':'association_service',
             'module':'ion.services.dm.inventory.association_service',
             'class':'AssociationService'
              }
        ]



    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        TESTING_SIGNIFIER = '3319A67F'
        DATASET_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)
        station_dataset_name = 'sample_station_dataset'
        stn_dataset_loc = CONF.getValue(station_dataset_name, None)
        station_dataset_template =  {ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F4',
                                     TYPE_CFG:DATASET_TYPE,
                                     NAME_CFG:station_dataset_name,
                                     DESCRIPTION_CFG:'An example of a station dataset',
                                     CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                                     CONTENT_ARGS_CFG:{'filename':stn_dataset_loc},
                                     }
        log.info("stn_dataset_loc: %s " % (stn_dataset_loc))
        def make_datasets(num):
            ds = station_dataset_template
            id_cfg = ds[ID_CFG]
            for i in range(num):
                ds[ID_CFG] = "-".join((id_cfg,str(i)))
                ds[NAME_CFG] = "".join((station_dataset_name, str(i)))
                yield ds[NAME_CFG],ds
                
        datasets = dict([ds for ds in make_datasets(500)])
        ION_DATASETS.update(datasets)
        
        import cProfile
        #self.sup = yield self._spawn_processes(self.services)
        #cProfile.run("wrap_spawn_process(self, self.services)", "prof_datastore")
        cProfile.run("make_datasets(3)")
        self.proc = Process()
        self.proc.op_fetch_blobs = self.proc.workbench.op_fetch_blobs
        yield self.proc.spawn()
        # run the tests in a completely separate process.
        self.asc = AssociationServiceClient(proc=self.proc)

    
    @defer.inlineCallbacks
    def tearDown(self):
       log.info('Tearing Down Test Container')

       yield self._shutdown_processes()
       yield self._stop_container()

    def test_instantiate(self):
        pass

def boo():
    i = 2 + 2
    
@defer.inlineCallbacks
def wrap_spawn_process(cls_instance, services):
    sup = yield cls_instance._spawn_processes(services)        
    defer.returnValue(sup)    
        
        
        