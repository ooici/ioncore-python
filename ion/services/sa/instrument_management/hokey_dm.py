#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/hokey_dm.py
@author
@brief very simplistic DB
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


class HokeyDM(object):
    
    def __init__(self, tables = []):
        self.db =       {} # db[tablename][integer_key] = {some struct}
        self.next_id =  {} # for a table, the next ID to assign
        for t in tables:   # create any default tables
            self.init_table(t)


    def init_table(self, tablename):
        #make it if it wasn't there already
        if not tablename in self.db:
            log.debug("table '" + tablename + "' didn't exist, so adding")
            self.db[tablename]      = {}
            self.next_id[tablename] = 1

    def insert(self, table, some_dict):
        log.debug("inserting into " + table)
        log.debug("" + str(some_dict))
        self.init_table(table)

        ret = table + "_" + str(self.next_id[table])
        self.db[table][ret] = some_dict
        self.next_id[table] = self.next_id[table] + 1
        return ret

    #don't replace the whole record, just update the individual fields
    def update(self, table, id, some_dict):
        log.info("update table=" + table)
        log.debug("with some_dict=" + str(some_dict))
        self.init_table(table)
        if not id in self.db[table]: return False
        for k, v in some_dict.iteritems():
            self._update_field(table, id, k, v)
        return True

    def update_field(self, table, id, field, some_val):
        self.init_table(table)
        if not id in self.db[table]: return False
        self._update_field(self, table, id, field, some_val)
        return True

    #NO ERROR CHECKING, this is a lower0level function
    def _update_field(self, table, id, field, some_val):
        log.info("update id=" + id + ", field=" + field)
        self.db[table][id][field] = some_val

    def delete(self, table, id):
        self.db[table][id] = None

    #return records for values that match test_fn criteria
    def select(self, table, test_fn):
        self.init_table(table)
        output = {}
        for k, v in self.db[table].iteritems():
            if test_fn(k, v):
                output[k] = v

        return output

    # get single record
    def get_one(self, table, id):
        self.init_table(table)
        if id in self.db[table]:
            return self.db[table][id]
        else:
            return None

    #return how many records match test_fn criteria
    def count(self, table, test_fn):
        self.init_table(table)
        output = 0
        log.debug("count: looking through table=" + table)
        for k, v in self.db[table].iteritems():
            log.debug("k=" + str(k) + " v=" + str(v))
            if test_fn(k, v):
                output = output + 1

        return output

    def get_id(self, table, id):
        self.init_table(table)
        return self.db[table][id]

    def key_exists(self, table, id):
        self.init_table(table)
        return id in self.db[table]
