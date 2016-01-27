#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.01

A file system that interacts with an xmlrpc HT.
"""

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from collections import OrderedDict
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
from socket import *
import threading

class Queue:  
    def __init__(self,size = 16):  
        self.queue = OrderedDict([])
        self.size = size  
        self.front = 0  
        self.rear = 0  

    def isEmpty(self):  
        return (self.rear-self.front) == 0  

    def isFull(self):  
        if (self.rear - self.front) == self.size:  
            return True  
        else:  
            return False  

    def get(self,key):  
        if key in self.queue:
            return self.queue[key]
        else:  
            return False

    def add(self,key,value):  
        if self.isFull():  
            #raise Exception("QueueOverFlow") 
            self.queue.popitem(last=False)
            self.queue[key] = value 
            print self.queue.keys()
        else:  
            self.queue[key] = value  
            #print self.queue
            self.rear += 1  

    def delete(self,key):  
        if key in self.queue:  
           #return 

           a = self.queue.pop(key)
           #print self.queue.keys()
           #return a
           self.rear -= 1 
           return a 
        else:  
           return False

    def delete_all(self):
       self.queue.clear()

    def show(self):  
         pass
       # print self.queue.keys()

cache = Queue()
#cache.delete(path)
class invalidate_server(threading.Thread) :

  def __init__(self) :
    threading.Thread.__init__(self)
    self.ADDR = ('' ,51235)
    self.sock = socket(AF_INET ,SOCK_DGRAM)
    self.sock.setsockopt(SOL_SOCKET,SO_REUSEADDR,1) 
    self.sock.bind(self.ADDR)
    self.thStop = False

  def __del__(self) :
    self.sock.close()

  def transMsg(self): 
    (data , curAddr) = self.sock.recvfrom(1024)
    print 'client receives invalidation signal:' , data
    if cache.get(data):
      print 'client invalidates file:' , data
      cache.delete(data)
    #print '< '+data

  def run(self) :
    while not self.thStop :
   #   print "invalidate_server_run"
      self.transMsg()


  def stop(self) :
    thStop = True


class HtProxy:
  """ Wrapper functions so the FS doesn't need to worry about HT primitives."""
  # A hashtable supporting atomic operations, i.e., retrieval and setting
  # must be done in different operations
  def __init__(self, url):

    self.rpc = xmlrpclib.Server(url)

  # Retrieves a value from the SimpleHT, returns KeyError, like dictionary, if
  # there is no entry in the SimpleHT
  def __getitem__(self, key):
    rv = self.get(key)
    if rv == None:
      raise KeyError()
    return pickle.loads(rv)
    
  # Stores a value in the SimpleHT
  def __setitem__(self, key, value):
    self.put(key, pickle.dumps(value))

  # Sets the TTL for a key in the SimpleHT to 0, effectively deleting it
  def __delitem__(self, key):
    self.put(key, "", 0)
      
  # Retrieves a value from the DHT, if the results is non-null return true,
  # otherwise false
  def __contains__(self, key):
    return self.get(key) != None

  def get(self, key):
    res = self.rpc.get(Binary(key))
    if "value" in res:
      return res["value"].data
    else:
      return None

  def put(self, key, val, ttl=10000):
    return self.rpc.put(Binary(key), Binary(val), ttl)

  def close_file(self, filename,fh):
    return self.rpc.close_file(Binary(filename),Binary(fh))

  def open_file(self, filename,flags):
    return self.rpc.open_file(Binary(filename),Binary(pickle.dumps(flags)))

class Memory(LoggingMixIn, Operations):
  """Example memory filesystem. Supports only one level of files."""
  def __init__(self, ht):
    self.files = ht
    self.f = {}
    #print type(self.files),type(ht)
    self.fd = 0
    now = time()
    if '/' not in self.files:
      a = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
        st_mtime=now, st_atime=now, st_nlink=2, contents=['/'])
      #print a.keys()
      #cache.delete('/')
      cache.delete_all()
      self.files['/'] = a

      #cache.add('/',a)

  def chmod(self, path, mode):
    print 'chmod' 
    #cache.show()
   # print type(ht)
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path]
    ht = self.files[path]
      #cache.add(path,ht)
    ht['st_mode'] &= 077000
    ht['st_mode'] |= mode
    self.files[path] = ht
    #cache.delete(path)
    #cache.add(path,ht)
   #cache.show()
    return 0

  def chown(self, path, uid, gid):
    print 'chown'
    # cache.show()
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path]
      #cache.add(path,ht)
    ht = self.files[path]
    if uid != -1:
      ht['st_uid'] = uid
    if gid != -1:
      ht['st_gid'] = gid
    self.files[path] = ht
    cache.delete(path)
   # cache.add(path,ht)
    #cache.show()

  def create(self, path, mode):
    print 'create'
   # cache.show()
    a = dict(st_mode=(S_IFREG | mode), st_nlink=1, st_size=0,
        st_ctime=time(), st_mtime=time(), st_atime=time(), contents='')   
    self.files[path] = a
    cache.delete(path)
    
    # if cache.get('/'):
    #   ht = cache.get('/')
    # else:
    #   ht = self.files['/']   
    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht
    cache.delete('/')

    self.fd += 1
    self.f[path] = self.files.open_file(path,32768) 
   #cache.add('/',ht)
    #cache.add(path,a)
   # cache.show()
    return self.fd
      
  def getattr(self, path, fh=None):
    print 'getattr'
    if path not in self.files['/']['contents']:
      raise FuseOSError(ENOENT)
    return self.files[path]

   # cache.show()
      #cache.add(path,ht)
    # if cache.get('/'):
    #   a = cache.get('/')
    # else:
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path]
    #   cache.add(path,ht)
    # a = self.files['/']
    # #   cache.add(path,ht)   
    # # if path not in self.files['/']['contents']:
    # #  raise FuseOSError(ENOENT)
    # if path not in a['contents']:
    #   raise FuseOSError(ENOENT)
    # # if cache.get(path):
    # #   ht = cache.get(path)
    # # else:
    # #   ht = self.files[path]
    # #cache.add(path,ht)
    # #cache.show()   
    # return ht
  
  def getxattr(self, path, name, position=0):
    print 'getxattr'
    #cache.show()
    if cache.get(path):
      ht = cache.get(path)
      attrs = ht.get('attrs', {})
    else:    
      #attrs = 
      a = self.files[path].get('attrs', {})
      attrs = a.get('attrs', {})
      cache.add(path,a)
    #cache.show()
    try:
      return attrs[name]
    except KeyError:
      return ''    # Should return ENOATTR
  
  def listxattr(self, path):
    print 'listxattr'
    cache.show()
    if cache.get(path):
      ht = cache.get(path)
      attrs = ht.get('attrs', {})
    else:    
      #attrs = 
      a = self.files[path]
      attrs = a.get('attrs', {})
      cache.add(path,a)
  #  cache.show()
   # return self.files[path].get('attrs', {}).keys()
    return attrs.keys()

  def mkdir(self, path, mode):
    print 'mkdir'
    #cache.show()
    #print type(ht)
  
    a =dict(st_mode=(S_IFDIR | mode), st_nlink=2, st_size=0, st_ctime=time(),
    st_mtime=time(), st_atime=time(), contents=[])
    self.files[path] = a
    cache.delete(path)
    # if cache.get('/'):   
    #   ht = cache.get('/') 
    # else:  
    #   ht = self.files['/']

    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht
    cache.delete('/')
    #cache.add('/',ht)
    #cache.add(path,a)
   # cache.show()
  # def opendir(self, path):

  #   'Returns a numerical file handle.'
  #   print 'opendir'
  #   return 0

  def open(self, path, flags):
    print 'open'
    print str(threading.currentThread())
 #   def open_file(self, filename,flags):
 #   return self.rpc.open_file(Binary(filename,flags))
   # print self.f
    self.f[path] = self.files.open_file(path,flags) 
  #  print self.f
   #cache.show()
    self.fd += 1
    #cache.show()
    return self.fd

  def release(self, path, fh):
      print 'release'
 #       def close_file(self, filename,fh):
  #  return self.rpc.close_file(Binary(filename),Binary(fh))
      #del self.f[path]
      self.files.close_file(path,self.f[path])
      #print self.f
      del self.f[path]
     # print self.f
      self.fd +=1
      #print self.fd
      return self.fd

  def read(self, path, size, offset, fh):
    print 'read'
  #  cache.show()

    if cache.get(path):
      ht = cache.get(path)
    else:
      ht = self.files[path]   
     #cache.add()  
    #ht = self.files[path]
    if 'contents' in ht:
      cache.add(path,ht)
      return ht['contents'][offset:offset+size]
    #cache.show()
    return None
  
  def readdir(self, path, fh):
    print 'readdir'
   # cache.show()
    if cache.get('/'):
      ht = cache.get('/')
    else:
      ht = self.files['/']   
      cache.add('/',ht)
    #cache.show()
    return ['.', '..'] + [x[1:] for x in ht['contents'] if x != '/']
    #return ['.', '..'] + [x[1:] for x in self.files['/']['contents'] if x != '/']
  
  def readlink(self, path):
    print 'readlink'
    #ache.show()
    if cache.get(path):
      ht = cache.get(path)
    else:
      ht = self.files[path]     
    #return self.files[path]['contents']
      cache.add(path,ht)
    #cache.show()
    return ht['contents']

  def removexattr(self, path, name):
    print 'removexattr'
    #cache.show()
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path]  

    ht = self.files[path]
    attrs = ht.get('attrs', {})
    if name in attrs:
      del attrs[name]
      ht['attrs'] = attrs
      self.files[path] = ht
      cache.delete(path)
      #cache.add(path,ht)
    else:
      pass    # Should return ENOATTR
    #cache.add(path,ht)
   # cache.show()

  def rename(self, old, new):
    print 'rename'
    # cache.show()
    # if cache.get(old):
    #   f = cache.get(old)
    # else:
    #   f = self.files[old]  


    f = self.files[old]
    self.files[new] = f
    cache.delete(new)
    del self.files[old]
    # if cache.get('/'):
    #   ht = cache.get('/')
    # else:
    #   ht = self.files['/'] 

    ht = self.files['/']

    ht['contents'].append(new)
    ht['contents'].remove(old)
    self.files['/'] = ht
    cache.delete('/')
    #cache.add('/',ht)
   # cache.show()
  
  def rmdir(self, path):
    print 'rmdir'
   # cache.show()
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path] 
    del self.files[path]
    # if cache.get('/'):
    #   ht = cache.get('/')
    # else:
    #   ht = self.files['/'] 
    ht = self.files['/']
    ht['st_nlink'] -= 1
    ht['contents'].remove(path)
    self.files['/'] = ht
    cache.delete('/')
    #cache.add('/',ht)
   #cache.show()
  
  def setxattr(self, path, name, value, options, position=0):
    print 'setxattr'
    #cache.show()

    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path]    
    # Ignore options
    ht = self.files[path]
    attrs = ht.get('attrs', {})
    attrs[name] = value
    ht['attrs'] = attrs
    self.files[path] = ht
    cache.delete(path)
    #cache.add(path,ht)
  #  cache.show()
  
  def statfs(self, path):
    print 'statfs'
    #cache.show()
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
  
  def symlink(self, target, source):
    print 'symlink'
    #cache.show()

    # if cache.get(target):
    #   ht = cache.get(target)
    # else:
    #   ht = self.files[target] 
   
    self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
      st_size=len(source), contents=source)
    cache.delete(target)
    # if cache.get('/'):
    #   ht = cache.get('/')
    # else:
    #   ht = self.files['/'] 
    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(target)
    self.files['/'] = ht
    cache.delete('/')
    #cache.add('/',ht)
    #cache.show()
  
  def truncate(self, path, length, fh=None):
    print 'truncate'
    # cache.show()
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path] 
    ht = self.files[path]
    if 'contents' in ht:
      ht['contents'] = ht['contents'][:length]
    ht['st_size'] = length
    self.files[path] = ht
    cache.delete(path)
   #cache.add(path,ht)
    #cache.show()
  
  def unlink(self, path):
    print 'unlink'
    # cache.show()
    # if cache.get('/'):
    #   ht = cache.get('/')
    # else:
    #   ht = self.files['/'] 
    ht = self.files['/']
    ht['contents'].remove(path)
    self.files['/'] = ht
    cache.delete('/')
    del self.files[path]
  #  cache.show()
  
  def utimens(self, path, times=None):
    print 'utimens'
#    cache.show()
    now = time()
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path] 
    ht = self.files[path]
    atime, mtime = times if times else (now, now)
    ht['st_atime'] = atime
    ht['st_mtime'] = mtime
    self.files[path] = ht
    cache.delete(path)
    #cache.add(path,ht)
   # cache.show()
  
  def write(self, path, data, offset, fh):
    print 'write'
 #   cache.show()
    # Get file data
    # if cache.get(path):
    #   ht = cache.get(path)
    # else:
    #   ht = self.files[path] 
    ht = self.files[path]
    tmp_data = ht['contents']
    toffset = len(data) + offset
    if len(tmp_data) > toffset:
      # If this is an overwrite in the middle, handle correctly
      ht['contents'] = tmp_data[:offset] + data + tmp_data[toffset:]
    else:
      # This is just an append
      ht['contents'] = tmp_data[:offset] + data
    ht['st_size'] = len(ht['contents'])
    self.files[path] = ht
    cache.delete(path)
    #cache.add(path,ht)
    cache.show()
    return len(data)

if __name__ == "__main__":
  if len(argv) != 3:
    print 'usage: %s <mountpoint> <remote hashtable>' % argv[0]
    exit(1)
  url = argv[2]
  invalidate = invalidate_server()
  invalidate.start()

  # Create a new HtProxy object using the URL specified at the command-line
  fuse = FUSE(Memory(HtProxy(url)), argv[1], foreground=True)
