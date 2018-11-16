import os
import re
from pathlib import Path
import plyvel
from threading import Lock, Condition, Thread
from queue import Queue
import shutil
import pickle
from datetime import datetime, timedelta
from time import sleep
import traceback

def lock_deco(func):
    def wrapper(self, *args, **kwargs):
        _from_outside = kwargs.get('_from_outside', True)
        if _from_outside:
            self.lock.acquire()
        ret = func(self, *args, **kwargs)
        if _from_outside:
            self.lock.release()
        return ret
    return wrapper

def load_leveldb_deco(release_interval=None, timeout=None):
    def _load_leveldb_deco(func):
        def wrapper(self, *args, **kwargs):
            if not self.acquire(release_interval=release_interval, timeout=timeout):
                raise LevelDBAcquisitionFailure('Failed to acquire db "{}"'.format(self.path))
            return func(self, *args, **kwargs)
        return wrapper
    return _load_leveldb_deco

INF_RELEASE_TIME = -1

class WriteBatch(object):
    def __init__(self, udb, transaction=False, sync=False):
        self.udb = udb
        self.wb = udb.db.write_batch(transaction=transaction, sync=sync)
        #self._restart_releaser = False
        
    def write(self):
        self.wb.write()
        #if self._restart_releaser:
        self.udb.release()
    
    def __enter__(self):
        #if not self.udb.releaser_is_suspended():
        self.udb.suspend_releaser()
        #self._restart_releaser = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.wb.transaction and exc_type is not None:
            # Exception occurred in transaction; do not write the batch
            self.wb.clear()
            return
        #if self._restart_releaser:
        self.write()
        self.wb.clear()
        
    def put(self, *args, **kwargs):
        return self.wb.put(*args, **kwargs)
    
class Iterator(object):
    def __init__(self, udb, *args, **kwargs):
        self.udb = udb
        self.args = args
        self.kwargs = kwargs
        self.iterator = None
        
    def __iter__(self):
        #if not self.udb.releaser_is_suspended():
        try:
            if self.udb.is_acquired():
                self.udb.suspend_releaser()
            else:
                while not self.udb.acquire(release_interval=INF_RELEASE_TIME):
                    pass
            self.iterator = self.udb.db.iterator(*self.args, **self.kwargs)
            yield from self.iterator
        finally:
            self.close()
    
    def close(self):
        self.udb.release()
        del self.iterator
        self.iterator = None

    def __del__(self):
        if self.iterator:
            self.close()
        
class DBUnit(object):
    __slots__ = [
        "default_timeout",
        'is_closed',
        "lock",
        "path",
        "_auto_release_interval",
        "_auto_releaser",
        "_db",
        "_release_time",
        "_retry_interval"
    ]
    
    def __init__(self, path, auto_release_interval=5, retry_interval=0.1, default_timeout=5):
        self.path = Path(path)
        self._auto_release_interval = auto_release_interval
        self._release_time = INF_RELEASE_TIME
        self._db = None
        self._retry_interval = retry_interval
        self.lock = Condition()
        self.default_timeout = default_timeout
        self.is_closed = False
        self._auto_releaser = Thread(target=self._auto_release)
        self._auto_releaser.start()     
        
    def __getstate__(self):
        return {
            "default_timeout": self.default_timeout,
            "path": self.path,
            "_auto_release_interval": self._auto_release_interval,
            "_retry_interval": self._retry_interval 
        }
    
    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)
        self._release_time = INF_RELEASE_TIME
        self._db = None
        self.lock = Condition()
        self.is_closed = False
        self._auto_releaser = Thread(target=self._auto_release)
        self._auto_releaser.start()
    
    @lock_deco
    def close(self, _from_outside=True):
        self.is_closed = True
        if self._auto_releaser:
            self.release()
            self.lock.release()
            self._auto_releaser.join()
            self.lock.acquire()

    def releaser_is_suspended(self):
        return self._release_time == INF_RELEASE_TIME
            
    @lock_deco
    def suspend_releaser(self, acquire=True, timeout=None, _from_outside=True):
        if self.is_acquired(_from_outside=False):
            self._set_release_time(INF_RELEASE_TIME, _from_outside=False)
        elif acquire:
            self.acquire(release_interval=INF_RELEASE_TIME, timeout=timeout, _from_outside=False)

    @lock_deco
    def restart_releaser(self, release_interval=None, acquire=True, timeout=None, _from_outside=True):
        if self.is_acquired(_from_outside=False):
            self._set_release_time(release_time=release_interval or self._auto_release_interval, _from_outside=False)
        elif acquire:
            self.acquire(release_interval=release_interval, timeout=timeout, _from_outside=False)
            
    @lock_deco
    def release(self, _from_outside=True):
        if self.is_acquired(_from_outside=False):
            self._set_release_time(datetime.now(), _from_outside=False)

    @lock_deco
    def _set_release_time(self, release_time, _from_outside=True):
        if release_time == INF_RELEASE_TIME:
            #print('released reservation: Unlimited')
            self._release_time = INF_RELEASE_TIME
            return
        if isinstance(release_time, (int, float)):
            release_time = max(timedelta(seconds=0), timedelta(seconds=release_time)) + datetime.now()
        if isinstance(release_time, timedelta):
            release_time = release_time + datetime.now()
        elif self._release_time == INF_RELEASE_TIME \
            or (release_time > self._release_time and (release_time - self._release_time).seconds > 0.1)\
            or (release_time < self._release_time and (self._release_time - release_time).seconds > 0.1):
            #print('released reservation:  ', release_time)
            self._release_time = release_time
            self.lock.notify()
        else:
            pass
            #print('No reservation')

    def _wait_predicate(self):
        if self._db is None:
            return True
        elif self._release_time is INF_RELEASE_TIME:
            return True
        elif self._release_time > datetime.now():
            return True
        return False
            
    def _auto_release(self):
        #print('auto releaser launched')
        self.lock.acquire()
        while not self.is_closed:
            rt = self._release_time
            #rt != INF_RELEASE_TIME and print('next release_time:', rt)
            while self._wait_predicate():
                if self._release_time != rt:
                    #print('changed from', rt, 'to', self._release_time)
                    rt = self._release_time
                self.lock.wait(timeout=0.1)
            self._db.close()
            del self._db
            self._db = None
            self._release_time = INF_RELEASE_TIME
            #print('*released')
        self.lock.release()
        #print('auto releaser finished')

    @property
    def db(self):
        return self._db or {}
    
    def _plyvel_db_factory(self):
        return plyvel.DB(str(self.path), create_if_missing=True)
    
    @lock_deco
    def acquire(self, release_interval=None, timeout=None, _from_outside=True):
        timeout = max(timeout or self.default_timeout, 0)
        time_limit = datetime.now() + timedelta(seconds=timeout)
        release_interval = release_interval or self._auto_release_interval
        #print('release interval:', release_interval)
        first_flag=True
        while first_flag or time_limit > datetime.now():
            first_flag=False
            # exm = None
            try:
                if not self.is_acquired(_from_outside=False):
                    #print('try to open database', self.path)
                    self._db = self._plyvel_db_factory()
                    #print('successfully opened db', self.path)
                self._set_release_time(release_interval, _from_outside=False)
                return True
            except plyvel.Error as e:
                # exm = traceback.format_exc()
                sleep(self._retry_interval)
                # timeout >= 0 and print('retry')
                continue
        return False

    @lock_deco
    def is_acquired(self, _from_outside=True):
        return self._db is not None

    @load_leveldb_deco()
    def get(self, key, default=None):
        return self.db.get(key, default)

    @load_leveldb_deco()
    def put(self, key, value):
        return self.db.put(key, value)

    @load_leveldb_deco()
    def delete(self, key):
        return self.db.delete(key)

    @load_leveldb_deco(release_interval=INF_RELEASE_TIME)
    def iterator(self, include_key=True, include_value=True):
        return Iterator(self, include_key=include_key, include_value=include_value)

    @load_leveldb_deco(release_interval=INF_RELEASE_TIME)
    def write_batch(self, *args, **kwargs):
        return WriteBatch(self, *args, **kwargs)

    @load_leveldb_deco(release_interval=INF_RELEASE_TIME)
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            raise
        self.release()


class LevelDBAcquisitionFailure(Exception):
    pass

