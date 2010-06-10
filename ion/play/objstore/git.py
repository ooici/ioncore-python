"""
@file git.py
@author Dorian Raymer
@brief Example application showing the CAStore implementation power.
Read git objects using ion.data.objstore CAStore and Storable objects.
@todo To really show off, need to be able to read git packfiles (otherwise,
most repositories history's will be unreadable...only their last commit will
[probably] be readable)
"""
import os
import zlib
import time

from ion.data.datastore import cas

class GitObjectStore(object):

    def __init__(self, root):
        self.prefix = os.path.join(root, '.git', 'objects')

    def _full_path(self, obj_id):
        sub_dir, file_path = obj_id[:2], obj_id[2:]
        return os.path.join(self.prefix, sub_dir, file_path)

    def get(self, obj_id):
        objz = open(self._full_path(obj_id)).read()
        obj = zlib.decompress(objz)
        return obj
        
    def put(self, file_path, file):
        return

class GitRefStore(object):

    def __init__(self, root):
        self.prefix = os.path.join(root, '.git', 'refs')

    def _full_path(self, ref, ref_type='heads'):
        return os.path.join(self.prefix, ref_type, ref)

    def get(self, ref, ref_type='heads'):
        return open(self._full_path(ref, ref_type)).read().strip()

    def list(self, ref_type='heads'):
        return os.listdir(self._full_path(''))


class GitDB(cas.CAStore):
    """
    Git is the information manager from hell.

    Implements store.IStore interface
    """
    
    def __init__(self, project_root):
        """
        @param project_root full path to root of git repository (the
        directory containing the .git directory
        """
        assert os.path.isdir(project_root)
        self.root = project_root
        self.objstore = GitObjectStore(project_root)
        self.refstore = GitRefStore(project_root)

class Git(GitDB):
    """
    Some of git's porcelain commands
    """

    def get_heads(self):
        return self.refstore.list()

    def get_head(self, head='master'):
        """
        @retval commit object id
        """
        return self.refstore.get(head)

    @property
    def head(self):
        return self.get_head()

    def get_commit(self, id):
        commit = self.objstore.get(id)
        commit_obj = self.decode(commit)
        return commit_obj

    def get_tree(self, id):
        o = self.objstore.get(id)
        return self.decode(o)

    def get_commit_history(self):
        commits = []
        id = self.get_head()
        if not id:
            return 
        while True:
            c = self.get_commit(id)
            commits.append(c)
            if c.parents:
                # ignore multiples...
                id = c.parents[0]
                continue
            break
        return commits
        
    def log(self):
        commits = self.get_commit_history()
        for commit in commits:
            print 'commit', objstore.sha1hex(commit.value)
            print 'tree', commit.tree
            for k,v in commit.other.items():
                if k in ('author', 'committer'):
                    vs = v.split(' ')
                    vs[-2] = time.ctime(float(vs[-2]))
                    v = ' '.join(vs)
                print k, v
            print commit.log

    def get_obj_info(self, id):
        o = self.objstore.get(id)
        obj = self.decode(o)
        return obj.type

    def list_files(self):
        c = self.get_commit(self.head)
        ls = {}
        def _walk_tree(t):
            """
            """
        t = self.get_tree(c.tree)
        for child in t.children:
            name = child[0]
            type = self.get_obj_info(objstore.sha1_to_hex(child[1]))
            ls[name] = type
        return ls


        

