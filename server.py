#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.02

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value and ttl associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "evalue"
  put(base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"), 1000)
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import SocketServer
from socket import *
import fcntl
import os
import stat

# class RWLock(object):  
    # def __init__(self):  
       # self.rlock = threading.Lock()   
       # self.wlock = threading.Lock()   
       # self.reader = 0
# 
# Global_keylock = {}
# 
# def write_acquire(self):  
    # self.wlock.acquire()    
# def write_release(self):  
    # self.wlock.release()    
# 
# def read_acquire(self):  
    # self.rlock.acquire()  
    # self.reader += 1  
    # if self.reader == 1:  
        # self.wlock.acquire()  
    # self.rlock.release()   
# def read_release(self):  
    # self.rlock.acquire()  
    # self.reader -= 1  
    # if self.reader == 0:  
        # self.wlock.release()  
    # self.rlock.release()    

# Presents a HT interface

Global_dict = {}
Global_int  = 0

class SimpleHT:
  def __init__(self):
    self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)

  def count(self):
    # Remove expired entries
    self.next_check = datetime.now() - timedelta(minutes = 5)
    self.check()
    return len(self.data)

  # Retrieve something from the HT
  def get(self, key):     
    #print 'get' , key.data
   # Remove expired entries
    self.check()
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results
    key = key.data

    # if key not in Global_keylock:
       # Global_keylock[key] = RWLock()
    # Lock = Global_keylock[key]
    # read_acquire(Lock)
    # print key, "get rlock"
     
    #print ent[0][0]

    if key in self.data:
      ent = self.data[key]
      now = datetime.now()
      if ent[1] > now:
        ttl = (ent[1] - now).seconds
        rv = {"value": Binary(ent[0]), "ttl": ttl}
       #  print ent[0][0]
        # a = ent[0]
        # a=pickle.loads(a)
        # print a['contents']
      else:
        del self.data[key]
    # read_release(Lock)
    # print key, "release rlock"
    return rv

  # Insert something into the HT
  def put(self, key, value, ttl):
    print 'put' , key.data
    # Remove expired entries
    self.check()
    key = key.data
    
    # if key not in Global_keylock:
    #    Global_keylock[key] = RWLock()
    # Lock = Global_keylock[key]
    # write_acquire(Lock)
    # print key, "get wlock"    
    
    invalidate_client1 = invalidate_Client('',51235,key)
    invalidate_client2 = invalidate_Client('',51236,key)
    invalidate_client1.start()
    invalidate_client2.start()
 
    end = datetime.now() + timedelta(seconds = ttl)
    self.data[key] = (value.data, end)
    # write_release(Lock)
    # print key, "release wlock"   
    #a=pickle.loads(value.data)
   # print a['contents']
    return True
    
  # Load contents from a file
  def open_file(self, filename,flags):
   # print 'read_file'l
    a = pickle.loads(flags.data)
    f = open(filename.data, "w")
    l = filename.data
    while True:
     if a == 32768:
       fcntl.flock(f,fcntl.LOCK_SH)
       d = 'get shared lock'
     else:
       fcntl.flock(f, fcntl.LOCK_EX)
       d = 'get exclusive lock'
     #fcntl.flock(f, fcntl.LOCK_EX)
     fnew = open(filename.data,"w")
     if os.path.sameopenfile(f.fileno(), fnew.fileno()):
         fnew.close()
         os.chmod(filename.data,stat.S_IWUSR) 
         break
     else:
         f.close()
         f = fnew

#os.chmod(path,mode) 

  # f = open(filename.data, "w")
   # a = pickle.loads(flags.data)
  # b = filename
  #  d = []
    #d.append(f)

    # if a == 32768:
    #   fcntl.flock(f,fcntl.LOCK_SH)
    # #  d = 'get shared lock'
    # else:
    #   fcntl.flock(f, fcntl.LOCK_EX)
      #d = 'get exclusive lock'
 
    print str(threading.currentThread()) , d , "on" , l
    # print "thread list" 
    # print threading.enumerate()
    # print 
    #threading.currentThread()
#threading.enumerate()
    #self.data = pickle.load(f)
    #f.close()
    global Global_int
    global Global_dict
 #   Global_int +=1
    Global_int  +=1
    gstr = str(Global_int)
    Global_dict[gstr] = f
    print gstr
    print type(gstr)
    return gstr

# fcntl.flock(f, fcntl.LOCK_EX)
# fcntl.flock(f,fcntl.LOCK_UN)
# fcntl.flock(f,fcntl.LOCK_SH)

  # Write contents to a file
  def close_file(self, filename,g):
   #print'write_file'
#    f = open(filename.data, "wb")
    #pickle.dump(self.data, f)
    global Global_dict
    # print g
    # print type(g)
    f = Global_dict[g.data] 
    b = filename.data
    d = 'release lock'

    fcntl.flock(f,fcntl.LOCK_UN)
    print str(threading.currentThread()), d , 'on' , b
    print "thread list" 
    print threading.enumerate()
    print
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

  # Remove expired entries
  def check(self):
    now = datetime.now()
    if self.next_check > now:
      return
    self.next_check = datetime.now() + timedelta(minutes = 5)
    to_remove = []
    for key, value in self.data.items():
      if value[1] < now:
        to_remove.append(key)
    for key in to_remove:
      del self.data[key]
       
def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = 51233
  if "--port" in ol:
    port = int(ol["--port"])  
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  #serve(port)
  #invalidate = invalidate_Client('','youdi')
  #invalidate.start()
  file_server = RPCThreading(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.open_file)
  file_server.register_function(sht.close_file)
  file_server.serve_forever()
  # rpc1 = rpcserver(port)
  # rpc1.start()
 # invalidate = invalidate_Client('',youdi)


class RPCThreading(SocketServer.ThreadingMixIn, SimpleXMLRPCServer.SimpleXMLRPCServer):
    pass


# Start the xmlrpc server
# class rpcserver(threading.Thread):
#       def __init__(self,port) :
#         threading.Thread.__init__(self)
#         self.port = port
      
#       # def sendMsg(self,msg) :
#       #    self.sock.sendto(self.uname+': '+msg,self.ADDR)      

#       def run(self) :
#          # while not self.thStop :
#          #   msg = raw_input()
#         #   if not msg.strip() :
#          #     print u'< !'
#          #     continue
#          #   print '> '+self.uname+':'+msg
#          #   self.sendMsg(msg)     
        # file_server = RPCThreading(('', self.port))
        # file_server.register_introspection_functions()
        # sht = SimpleHT()
        # file_server.register_function(sht.get)
        # file_server.register_function(sht.put)
        # file_server.register_function(sht.print_content)
        # file_server.register_function(sht.open_file)
        # file_server.register_function(sht.close_file)
        # file_server.serve_forever()
# file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))

class invalidate_Client(threading.Thread) :
  
  def __init__(self ,ip ,port, name) :
    threading.Thread.__init__(self)
    self.ADDR = (ip,port)
    self.sock = socket(AF_INET ,SOCK_DGRAM)
    #self.sock.setsockopt(SOL_SOCKET,SO_REUSEADDR,1)
    self.uname = name
    #self.thStop = False
    
  def __del__(self) :
    self.sock.close()

  def sendMsg(self) :
    self.sock.sendto(self.uname,self.ADDR)

  def run(self) :
   # while not self.thStop :
      print 'server sends invalidation signal '+self.uname
      self.sendMsg()

 # def stop(self) :
  #  thStop = True














# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51233, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51233"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()

