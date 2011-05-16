"""
Tests for the low level socket functions.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong.
irmen@razorvine.net - http://www.razorvine.net/python/Pyro
"""

import unittest
import socket, os
import Pyro4.socketutil as SU
from Pyro4 import threadutil
from Pyro4.socketserver.selectserver import SocketServer_Select
from Pyro4.socketserver.threadpoolserver import SocketServer_Threadpool
import Pyro4
from testsupport import *


class TestSocketutil(unittest.TestCase):
    def setUp(self):
        Pyro4.config.POLLTIMEOUT=0.1
        
    def testGetIP(self):
        localip=SU.getIpAddress()
        localhost=socket.getfqdn(localip)
        self.assertEqual(localip,SU.getIpAddress(localhost))
        myip=SU.getMyIpAddress()
        self.assertTrue(len(myip)>4)
        myip=SU.getMyIpAddress(workaround127=True)
        self.assertTrue(len(myip)>4)
        self.assertFalse(myip.startswith("127."))
        self.assertEqual("127.0.0.1", SU.getMyIpAddress("127.0.0.1",workaround127=False))
        self.assertNotEqual("127.0.0.1", SU.getMyIpAddress("127.0.0.1",workaround127=True))
        
    def testUnusedPort(self):
        port1=SU.findUnusedPort()
        port2=SU.findUnusedPort()
        self.assertTrue(port1>0)
        self.assertNotEqual(port1,port2)
        port1=SU.findUnusedPort(socktype=socket.SOCK_DGRAM)
        port2=SU.findUnusedPort(socktype=socket.SOCK_DGRAM)
        self.assertTrue(port1>0)
        self.assertNotEqual(port1,port2)
    def testBindUnusedPort(self):
        sock1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port1=SU.bindOnUnusedPort(sock1)
        port2=SU.bindOnUnusedPort(sock2)
        self.assertTrue(port1>0)
        self.assertNotEqual(port1,port2)
        sockname=sock1.getsockname()
        self.assertEquals(("127.0.0.1",port1), sockname)
        sock1.close()
        sock2.close()
    def testCreateUnboundSockets(self):
        s=SU.createSocket()
        bs=SU.createBroadcastSocket()
        try:
            host,port=s.getsockname()
            self.assertEqual(0, port)
        except socket.error:
            pass
        try:
            if os.name!="java":
                host,port=bs.getsockname()
                self.assertEqual(0, port)
        except socket.error:
            pass
        s.close()
        bs.close()
    def testCreateBoundSockets(self):
        s=SU.createSocket(bind=('localhost',0))
        bs=SU.createBroadcastSocket(bind=('localhost',0))
        self.assertEqual('127.0.0.1',s.getsockname()[0])
        self.assertEqual('127.0.0.1',bs.getsockname()[0])
        s.close()
        bs.close()
        self.assertRaises(ValueError, SU.createSocket, bind=('localhost',12345), connect=('localhost',1234))
            
    def testSend(self):
        ss=SU.createSocket(bind=("localhost",0))
        port=ss.getsockname()[1]
        cs=SU.createSocket(connect=("localhost",port))
        SU.sendData(cs,tobytes("foobar!"*10))
        cs.shutdown(socket.SHUT_WR)
        a=ss.accept()
        data=SU.receiveData(a[0], 5)
        self.assertEqual(tobytes("fooba"),data)
        data=SU.receiveData(a[0], 5)
        self.assertEqual(tobytes("r!foo"),data)
        a[0].close()
        ss.close()
        cs.close()
    def testBroadcast(self):
        ss=SU.createBroadcastSocket((None, 0))
        port=ss.getsockname()[1]
        cs=SU.createBroadcastSocket()
        cs.sendto(tobytes("monkey"),0,('<broadcast>',port))
        data,_=ss.recvfrom(500)
        self.assertEqual(tobytes("monkey"),data)
        cs.close()
        ss.close()
        
    def testMsgWaitallProblems(self):
        ss=SU.createSocket(bind=("localhost",0), timeout=0.5)
        port=ss.getsockname()[1]
        cs=SU.createSocket(connect=("localhost",port), timeout=0.5)
        a=ss.accept()
        # test some sizes that might be problematic with MSG_WAITALL
        for size in [1000,10000,32000,32768,32780,41950,41952,42000,65000,65535,65600,80000]:
            SU.sendData(cs,tobytes("x")*size)
            data=SU.receiveData(a[0],size)
            SU.sendData(a[0], data)
            data=SU.receiveData(cs,size)
            self.assertEqual(size, len(data))
        a[0].close()
        ss.close()
        cs.close()
        
    def testMsgWaitallProblems2(self):
        class ReceiveThread(threadutil.Thread):
            def __init__(self, sock, sizes):
                super(ReceiveThread,self).__init__()
                self.sock=sock
                self.sizes=sizes
            def run(self):
                cs,_ = self.sock.accept()
                for size in self.sizes:
                    data=SU.receiveData(cs,size)
                    SU.sendData(cs, data)
                cs.close()
        ss=SU.createSocket(bind=("localhost",0))
        SIZES=[1000,10000,32000,32768,32780,41950,41952,42000,65000,65535,65600,80000,999999]
        serverthread=ReceiveThread(ss, SIZES)
        serverthread.setDaemon(True)
        serverthread.start()
        port=ss.getsockname()[1]
        cs=SU.createSocket(connect=("localhost",port), timeout=0.5)
        # test some sizes that might be problematic with MSG_WAITALL
        for size in SIZES:
            SU.sendData(cs,tobytes("x")*size)
            data=SU.receiveData(cs,size)
            self.assertEqual(size, len(data))
        serverthread.join()
        ss.close()
        cs.close()

class ServerCallback(object):
    def handshake(self, connection):
        if not isinstance(connection, SU.SocketConnection):
            raise TypeError("handshake expected SocketConnection parameter")
    def handleRequest(self, connection):
        if not isinstance(connection, SU.SocketConnection):
            raise TypeError("handleRequest expected SocketConnection parameter")

class TestSocketServer(unittest.TestCase):
    def testServer_thread(self):
        callback=ServerCallback()
        port=SU.findUnusedPort()
        serv=SocketServer_Threadpool(callback,"localhost",port)
        self.assertEqual("localhost:"+str(port), serv.locationStr)
        self.assertTrue(serv.sock is not None)
        self.assertTrue(serv.fileno() > 0)
        conn=SU.SocketConnection(serv.sock, "ID12345")
        self.assertEqual("ID12345",conn.objectId)
        self.assertTrue(conn.sock is not None)
        conn.close()
        conn.close()
        self.assertFalse(conn.sock is None, "connections keep their socket object even if it's closed")
        serv.close()
        serv.close()
        self.assertTrue(serv.sock is None)
    def testServer_select(self):
        callback=ServerCallback()
        port=SU.findUnusedPort()
        if os.name=="java":
            # select-based server is not available in Jython
            self.assertRaises(NotImplementedError, SocketServer_Select, callback, "localhost", port)
            return
        serv=SocketServer_Select(callback,"localhost",port)
        self.assertEqual("localhost:"+str(port), serv.locationStr)
        self.assertTrue(serv.sock is not None)
        self.assertTrue(serv.fileno() > 0)
        conn=SU.SocketConnection(serv.sock, "ID12345")
        self.assertEqual("ID12345",conn.objectId)
        self.assertTrue(conn.sock is not None)
        conn.close()
        conn.close()
        self.assertFalse(conn.sock is None, "connections keep their socket object even if it's closed")
        serv.close()
        serv.close()
        self.assertTrue(serv.sock is None)
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
