"""
Core logic (uri, daemon, proxy stuff).

Pyro - Python Remote Objects.  Copyright by Irmen de Jong (irmen@razorvine.net).
"""

from __future__ import with_statement
from Pyro4 import constants, threadutil, util, socketutil, errors
from Pyro4.socketserver.multiplexserver import SocketServer_Select, SocketServer_Poll
from Pyro4.socketserver.threadpoolserver import SocketServer_Threadpool
import concurrent.futures as cfutures
import Pyro4
import hashlib
import hmac
import inspect
import logging
import os
import re
import struct
import sys
import time
import uuid
try:
    import copyreg
except ImportError:
    import copy_reg as copyreg
from Pyro4 import futures

__all__=["URI", "Proxy", "Daemon", "callback", "batch"]

if sys.version_info>=(3,0):
    basestring=str

log=logging.getLogger("Pyro4.core")


class URI(object):
    """
    Pyro object URI (universal resource identifier).
    The uri format is like this: ``PYRO:objectid@location`` where location is one of:

    - ``hostname:port`` (tcp/ip socket on given port)
    - ``./u:sockname`` (Unix domain socket on localhost)

    There is also a 'Magic format' for simple name resolution using Name server:
      ``PYRONAME:objectname[@location]``  (optional name server location, can also omit location port)
    """
    uriRegEx=re.compile(r"(?P<protocol>PYRO[A-Z]*):(?P<object>\S+?)(@(?P<location>\S+))?$")
    __slots__=("protocol", "object", "sockname", "host", "port", "object")

    def __init__(self, uri):
        if isinstance(uri, URI):
            state=uri.__getstate__()
            self.__setstate__(state)
            return
        if not isinstance(uri, basestring):
            raise TypeError("uri parameter object is of wrong type")
        self.sockname=self.host=self.port=None
        match=self.uriRegEx.match(uri)
        if not match:
            raise errors.PyroError("invalid uri")
        self.protocol=match.group("protocol")
        self.object=match.group("object")
        location=match.group("location")
        if self.protocol=="PYRONAME":
            self._parseLocation(location, Pyro4.config.NS_PORT)
            return
        if self.protocol=="PYRO":
            if not location:
                raise errors.PyroError("invalid uri")
            self._parseLocation(location, None)
        else:
            raise errors.PyroError("invalid uri (protocol)")

    def _parseLocation(self, location, defaultPort):
        if not location:
            return
        if location.startswith("./u:"):
            self.sockname=location[4:]
            if (not self.sockname) or ':' in self.sockname:
                raise errors.PyroError("invalid uri (location)")
        else:
            if location.startswith("["):  # ipv6
                if location.startswith("[["):  # possible mistake: double-bracketing
                    raise errors.PyroError("invalid ipv6 address: enclosed in too many brackets")
                self.host, _, self.port = re.match(r"\[([0-9a-fA-F:%]+)](:(\d+))?", location).groups()
            else:
                self.host, _, self.port = location.partition(":")
            if not self.port:
                self.port=defaultPort
            try:
                self.port=int(self.port)
            except (ValueError, TypeError):
                raise errors.PyroError("invalid port in uri, port="+str(self.port))

    @staticmethod
    def isUnixsockLocation(location):
        """determine if a location string is for a Unix domain socket"""
        return location.startswith("./u:")

    @property
    def location(self):
        """property containing the location string, for instance ``"servername.you.com:5555"``"""
        if self.host:
            if ":" in self.host:    # ipv6
                return "[%s]:%d" % (self.host, self.port)
            else:
                return "%s:%d" % (self.host, self.port)
        elif self.sockname:
            return "./u:"+self.sockname
        else:
            return None

    def asString(self):
        """the string representation of this object"""
        result=self.protocol+":"+self.object
        location=self.location
        if location:
            result+="@"+location
        return result

    def __str__(self):
        string=self.asString()
        if sys.version_info<(3,0) and type(string) is unicode:
            return string.encode("ascii", "replace")
        return string

    def __unicode__(self):
        return self.asString()

    def __repr__(self):
        return "<%s.%s at 0x%x, %s>" % (self.__class__.__module__, self.__class__.__name__, id(self), str(self))

    def __eq__(self, other):
        if not isinstance(other, URI):
            return False
        return (self.protocol, self.object, self.sockname, self.host, self.port) \
                == (other.protocol, other.object, other.sockname, other.host, other.port)
    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.protocol, self.object, self.sockname, self.host, self.port))

    # note: getstate/setstate are not needed if we use pickle protocol 2,
    # but this way it helps pickle to make the representation smaller by omitting all attribute names.

    def __getstate__(self):
        return self.protocol, self.object, self.sockname, self.host, self.port

    def __setstate__(self, state):
        self.protocol, self.object, self.sockname, self.host, self.port = state


class _RemoteMethod(object):
    """method call abstraction"""
    def __init__(self, send, name):
        self.__send = send
        self.__name = name

    def __getattr__(self, name):
        return _RemoteMethod(self.__send, "%s.%s" % (self.__name, name))

    def __call__(self, *args, **kwargs):
        return self.__send(self.__name, args, kwargs)


def _check_hmac():
    if Pyro4.config.HMAC_KEY is None or len(Pyro4.config.HMAC_KEY)==0:
        import warnings
#        warnings.warn("HMAC_KEY not set, protocol data may not be secure")
    elif sys.version_info>=(3,0) and type(Pyro4.config.HMAC_KEY) is not bytes:
        raise errors.PyroError("HMAC_KEY must be bytes type")


class Proxy(object):
    """
    Pyro proxy for a remote object. Intercepts method calls and dispatches them to the remote object.

    .. automethod:: _pyroBind
    .. automethod:: _pyroRelease
    .. automethod:: _pyroReconnect
    .. automethod:: _pyroBatch
    """
    _pyroSerializer=util.Serializer()
    __pyroAttributes=frozenset(["__getnewargs__", "__getinitargs__", "_pyroConnection", "_pyroFutureDaemon", "_pyroUri", "_pyroOneway", "_pyroAsyncs", "_pyroTimeout", "_pyroSeq"])

    def __init__(self, uri):
        """
        .. autoattribute:: _pyroOneway
        .. autoattribute:: _pyroAsyncs
        .. autoattribute:: _pyroTimeout
        """
        _check_hmac()  # check if hmac secret key is set
        if isinstance(uri, basestring):
            uri=URI(uri)
        elif not isinstance(uri, URI):
            raise TypeError("expected Pyro URI")
        self._pyroUri=uri
        self._pyroConnection=None
        self._pyroFutureDaemon=None
        self._pyroOneway=set()
        self._pyroAsyncs=set()
        self._pyroSeq=0    # message sequence number
        self.__pyroTimeout=Pyro4.config.COMMTIMEOUT
        self.__pyroLock=threadutil.Lock()
        self.__pyroConnLock=threadutil.Lock()

    def __del__(self):
        if hasattr(self, "_pyroConnection"):
            self._pyroRelease()
        if hasattr(self, "_pyroFutureDaemon") and self._pyroFutureDaemon:
            self._pyroFutureDaemon.shutdown()

    def __getattr__(self, name):
        if name in Proxy.__pyroAttributes:
            # allows it to be safely pickled
            raise AttributeError(name)
        return _RemoteMethod(self._pyroInvoke, name)

    def __repr__(self):
        connected="connected" if self._pyroConnection else "not connected"
        return "<%s.%s at 0x%x, %s, for %s>" % (self.__class__.__module__, self.__class__.__name__,
            id(self), connected, self._pyroUri)

    def __unicode__(self):
        return str(self)

    def __getstate__(self):
        return self._pyroUri, self._pyroOneway, self._pyroAsyncs, self._pyroSerializer, self.__pyroTimeout    # skip the connection

    def __setstate__(self, state):
        self._pyroUri, self._pyroOneway, self._pyroAsyncs, self._pyroSerializer, self.__pyroTimeout = state
        self._pyroConnection=None 
        self._pyroFutureDaemon=None
        self._pyroSeq=0
        self.__pyroLock=threadutil.Lock()
        self.__pyroConnLock=threadutil.Lock()

    def __copy__(self):
        uriCopy=URI(self._pyroUri)
        return Proxy(uriCopy) # TODO need to duplicate oneways and async as well

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._pyroRelease()

    def __eq__(self, other):
        if other is self:
            return True
        return isinstance(other, Proxy) and other._pyroUri == self._pyroUri and other._pyroOneway == self._pyroOneway

    def __ne__(self, other):
        if other and isinstance(other, Proxy):
            return other._pyroUri != self._pyroUri or other._pyroOneway != self._pyroOneway
        return True

    def __hash__(self):
        try:
            return hash(self._pyroUri) ^ hash(frozenset(self._pyroOneway))
        except AttributeError:
            # In case of cyclic-dependency, the unpickler can call __hash__ on an
            # uninitialised object. If so, do something bad: return another hash.
            # This is described in Python issue 1761028 (status=wontfix) 
            return object.__hash__(self)

    def _pyroRelease(self):
        """release the connection to the pyro daemon"""
        with self.__pyroConnLock:
            if self._pyroConnection is not None:
                self._pyroConnection.close()
                self._pyroConnection=None
                log.debug("connection released")

    def _pyroBind(self):
        """
        Bind this proxy to the exact object from the uri. That means that the proxy's uri
        will be updated with a direct PYRO uri, if it isn't one yet.
        If the proxy is already bound, it will not bind again.
        """
        return self.__pyroCreateConnection(True)

    def __pyroGetTimeout(self):
        return self.__pyroTimeout

    def __pyroSetTimeout(self, timeout):
        self.__pyroTimeout=timeout
        if self._pyroConnection is not None:
            self._pyroConnection.timeout=timeout
    _pyroTimeout=property(__pyroGetTimeout, __pyroSetTimeout)

    def _pyroInvoke(self, methodname, vargs, kwargs, flags=0):
        """perform the remote method call communication"""
        if self._pyroConnection is None:
            # rebind here, don't do it from inside the invoke because deadlock will occur
            self.__pyroCreateConnection()
        if methodname in self._pyroAsyncs:
            flags |= MessageFactory.FLAGS_ASYNC
            future = futures.ClientFuture(self)
            # daemon needed to register the special asynchronous calls
            if not self._pyroFutureDaemon:
                self.__pyroCreateFutureDaemon()
            self._pyroFutureDaemon.register(future)
            logging.debug("Going to send client future %s", self._pyroFutureDaemon.uriFor(future))
            vargs = (future,) + vargs # special way to send the future
        data, compressed=self._pyroSerializer.serialize(
            (self._pyroConnection.objectId, methodname, vargs, kwargs),
            compress=Pyro4.config.COMPRESSION)
        if compressed:
            flags |= MessageFactory.FLAGS_COMPRESSED
        if methodname in self._pyroOneway:
            flags |= MessageFactory.FLAGS_ONEWAY
        with self.__pyroLock:
            self._pyroSeq=(self._pyroSeq+1)&0xffff
            data=MessageFactory.createMessage(MessageFactory.MSG_INVOKE, data, flags, self._pyroSeq)
            try:
                self._pyroConnection.send(data)
                del data  # invite GC to collect the object, don't wait for out-of-scope
                if flags & MessageFactory.FLAGS_ONEWAY:
                    return None    # oneway call, no response data
                # TODO methods marked both oneway and isasync should returning an ImmediateFuture with None as result
                elif flags & MessageFactory.FLAGS_ASYNC:
                    return future
                else:
                    msgType, flags, seq, data = MessageFactory.getMessage(self._pyroConnection, MessageFactory.MSG_RESULT)
                    self.__pyroCheckSequence(seq)
                    data=self._pyroSerializer.deserialize(data, compressed=flags & MessageFactory.FLAGS_COMPRESSED)
                    if flags & MessageFactory.FLAGS_EXCEPTION:
                        if sys.platform=="cli":
                            util.fixIronPythonExceptionForPickle(data, False)
                        raise data
                    else:
                        return data
            except (errors.CommunicationError, KeyboardInterrupt):
                # Communication error during read. To avoid corrupt transfers, we close the connection.
                # Otherwise we might receive the previous reply as a result of a new methodcall!
                # Special case for keyboardinterrupt: people pressing ^C to abort the client
                # may be catching the keyboardinterrupt in their code. We should probably be on the
                # safe side and release the proxy connection in this case too, because they might
                # be reusing the proxy object after catching the exception...
                self._pyroRelease()
                raise

    def _pyroCancelFuture(self, client_future_uri):
        """
        Ask the server to cancel the future
        """
        # tricky way to cancel the future: a method without name with first arg the future uri and a special flag
        # Cannot use just the future because it might already be unregistered
        return self._pyroInvoke("", (client_future_uri, ), None, MessageFactory.FLAGS_ASYNC_CANCEL)

    def __pyroCreateFutureDaemon(self):
        """
        Find or create a pyro4 daemon
        """
        # TODO try to reuse a daemon if there is already one running in the process
        # looking for _pyroDaemon is useless, because a proxy is never registered
        self._pyroFutureDaemon = Daemon()
        thread_daemon = threadutil.Thread(name="Pyro4 daemon for async calls",
                                          target=self._pyroFutureDaemon.requestLoop)
        thread_daemon.daemon = True
        thread_daemon.start()

    def __pyroCheckSequence(self, seq):
        if seq!=self._pyroSeq:
            err="invoke: reply sequence out of sync, got %d expected %d" % (seq, self._pyroSeq)
            log.error(err)
            raise errors.ProtocolError(err)

    def __pyroCreateConnection(self, replaceUri=False):
        """
        Connects this proxy to the remote Pyro daemon. Does connection handshake.
        Returns true if a new connection was made, false if an existing one was already present.
        """
        with self.__pyroConnLock:
            if self._pyroConnection is not None:
                return False     # already connected
            from Pyro4.naming import resolve  # don't import this globally because of cyclic dependancy
            uri=resolve(self._pyroUri)
            # socket connection (normal or Unix domain socket)
            conn=None
            log.debug("connecting to %s", uri)
            connect_location=uri.sockname if uri.sockname else (uri.host, uri.port)
            with self.__pyroLock:
                try:
                    if self._pyroConnection is not None:
                        return False    # already connected
                    sock=socketutil.createSocket(connect=connect_location, reuseaddr=Pyro4.config.SOCK_REUSE, timeout=self.__pyroTimeout)
                    conn=socketutil.SocketConnection(sock, uri.object)
                    # Do handshake. For now, no need to send anything.
                    msgType, flags, seq, data = MessageFactory.getMessage(conn, None)
                    # any trailing data (dataLen>0) is an error message, if any
                except Exception:
                    x=sys.exc_info()[1]
                    if conn:
                        conn.close()
                    err="cannot connect: %s" % x
                    log.error(err)
                    if isinstance(x, errors.CommunicationError):
                        raise
                    else:
                        raise errors.CommunicationError(err)
                else:
                    if msgType==MessageFactory.MSG_CONNECTFAIL:
                        error="connection rejected"
                        if data:
                            if sys.version_info>=(3,0):
                                data=str(data,"utf-8")
                            error+=", reason: "+data
                        conn.close()
                        log.error(error)
                        raise errors.CommunicationError(error)
                    elif msgType==MessageFactory.MSG_CONNECTOK:
                        self._pyroConnection=conn
                        if replaceUri:
                            log.debug("replacing uri with bound one")
                            self._pyroUri=uri
                        log.debug("connected to %s", self._pyroUri)
                        return True
                    else:
                        conn.close()
                        err="connect: invalid msg type %d received" % msgType
                        log.error(err)
                        raise errors.ProtocolError(err)

    def _pyroReconnect(self, tries=100000000):
        """(re)connect the proxy to the daemon containing the pyro object which the proxy is for"""
        self._pyroRelease()
        while tries:
            try:
                self.__pyroCreateConnection()
                return
            except errors.CommunicationError:
                tries-=1
                if tries:
                    time.sleep(2)
        msg="failed to reconnect"
        log.error(msg)
        raise errors.ConnectionClosedError(msg)

    def _pyroBatch(self):
        """returns a helper class that lets you create batched method calls on the proxy"""
        return _BatchProxyAdapter(self)

    def _pyroInvokeBatch(self, calls, oneway=False):
        flags=MessageFactory.FLAGS_BATCH
        if oneway:
            flags|=MessageFactory.FLAGS_ONEWAY
        return self._pyroInvoke("<batch>", calls, None, flags)


class _BatchedRemoteMethod(object):
    """method call abstraction that is used with batched calls"""
    def __init__(self, calls, name):
        self.__calls = calls
        self.__name = name

    def __getattr__(self, name):
        return _BatchedRemoteMethod(self.__calls, "%s.%s" % (self.__name, name))

    def __call__(self, *args, **kwargs):
        self.__calls.append((self.__name, args, kwargs))


class _BatchProxyAdapter(object):
    """Helper class that lets you batch multiple method calls into one.
    It is constructed with a reference to the normal proxy that will
    carry out the batched calls. Call methods on this object thatyou want to batch,
    and finally call the batch proxy itself. That call will return a generator
    for the results of every method call in the batch (in sequence)."""
    def __init__(self, proxy):
        self.__proxy=proxy
        self.__calls=[]

    def __getattr__(self, name):
        return _BatchedRemoteMethod(self.__calls, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __copy__(self):
        return self

    def __resultsgenerator(self, results):
        for result in results:
            if isinstance(result, futures._ExceptionWrapper):
                result.raiseIt()   # re-raise the remote exception locally.
            else:
                yield result   # it is a regular result object, yield that and continue.

    def __call__(self, oneway=False):
        results=self.__proxy._pyroInvokeBatch(self.__calls, oneway)
        self.__calls=[]   # clear for re-use
        if not oneway:
            return self.__resultsgenerator(results)

    def _pyroInvoke(self,name,args,kwargs):
        # ignore all parameters, we just need to execute the batch
        results=self.__proxy._pyroInvokeBatch(self.__calls)
        self.__calls=[]   # clear for re-use
        return self.__resultsgenerator(results)



def batch(proxy):
    """convenience method to get a batch proxy adapter"""
    return proxy._pyroBatch()


class MessageFactory(object):
    """internal helper class to construct Pyro protocol messages"""
    headerFmt = '!4sHHHHiH20s'    # header (id, version, msgtype, flags, sequencenumber, dataLen, checksum, hmac)
    # note: the sequencenumber is used to check if response messages correspond to the
    # actual request message. This prevents the situation where Pyro would perhaps return
    # the response data from another remote call (which would not result in an error otherwise!)
    # This could happen for instance if the socket data stream gets out of sync, perhaps due To
    # some form of signal that interrupts I/O.
    # The header checksum is a simple sum of the header fields to make reasonably sure
    # that we are dealing with an actual correct PYRO protocol header and not some random
    # data that happens to start with the 'PYRO' protocol identifier.
    HEADERSIZE=struct.calcsize(headerFmt)
    MSG_CONNECT = 1
    MSG_CONNECTOK = 2
    MSG_CONNECTFAIL = 3
    MSG_INVOKE = 4
    MSG_RESULT = 5
    FLAGS_EXCEPTION = 1<<0
    FLAGS_COMPRESSED = 1<<1
    FLAGS_ONEWAY = 1<<2
    FLAGS_HMAC = 1<<3
    FLAGS_BATCH = 1<<4
    FLAGS_ASYNC = 1<<5
    FLAGS_ASYNC_CANCEL = 1<<6 
    MAGIC = 0x34E9
    if sys.version_info>=(3,0):
        empty_bytes = bytes([])
        pyro_tag = bytes("PYRO", "ASCII")
        empty_hmac = bytes(hashlib.sha1().digest_size)
    else:
        empty_bytes = ""
        pyro_tag = "PYRO"
        empty_hmac = "\0"*hashlib.sha1().digest_size

    @classmethod
    def createMessage(cls, msgType, databytes, flags, seq):
        """creates a message containing a header followed by the given databytes"""
        databytes=databytes or cls.empty_bytes
        if 0 < Pyro4.config.MAX_MESSAGE_SIZE < len(databytes):
            raise errors.ProtocolError("max message size exceeded (%d where max=%d)" % (len(databytes), Pyro4.config.MAX_MESSAGE_SIZE))
        if Pyro4.config.HMAC_KEY:
            flags|=MessageFactory.FLAGS_HMAC
            bodyhmac=hmac.new(Pyro4.config.HMAC_KEY, databytes, digestmod=hashlib.sha1).digest()
        else:
            bodyhmac=MessageFactory.empty_hmac
        headerchecksum=(msgType+constants.PROTOCOL_VERSION+len(databytes)+flags+seq+MessageFactory.MAGIC)&0xffff
        msg=struct.pack(cls.headerFmt, cls.pyro_tag, constants.PROTOCOL_VERSION, msgType, flags, seq, len(databytes), headerchecksum, bodyhmac)
        return msg+databytes

    @classmethod
    def parseMessageHeader(cls, headerData):
        """Parses a message header. Returns a tuple of messagetype, messageflags, sequencenumber, datalength, datahmac."""
        if not headerData or len(headerData)!=cls.HEADERSIZE:
            raise errors.ProtocolError("header data size mismatch")
        tag, ver, msgType, flags, seq, dataLen, headerchecksum, datahmac = struct.unpack(cls.headerFmt, headerData)
        if tag!=cls.pyro_tag or ver!=constants.PROTOCOL_VERSION:
            raise errors.ProtocolError("invalid data or unsupported protocol version")
        if headerchecksum!=(msgType+ver+dataLen+flags+seq+MessageFactory.MAGIC)&0xffff:
            raise errors.ProtocolError("header checksum mismatch")
        return msgType, flags, seq, dataLen, datahmac

    @classmethod
    def getMessage(cls, connection, requiredMsgType):
        headerdata = connection.recv(cls.HEADERSIZE)
        msgType, flags, seq, datalen, datahmac = cls.parseMessageHeader(headerdata)
        if 0 < Pyro4.config.MAX_MESSAGE_SIZE < datalen:
            errorMsg = "max message size exceeded (%d where max=%d)" % (datalen, Pyro4.config.MAX_MESSAGE_SIZE)
            log.error("connection "+str(connection)+": "+errorMsg)
            connection.close()   # close the socket because at this point we can't return the correct sequence number for returning an error message
            raise errors.ProtocolError(errorMsg)
        if requiredMsgType is not None and msgType != requiredMsgType:
            err="invalid msg type %d received" % msgType
            log.error(err)
            raise errors.ProtocolError(err)
        databytes=connection.recv(datalen)
        local_hmac_set=Pyro4.config.HMAC_KEY is not None and len(Pyro4.config.HMAC_KEY) > 0
        if flags&MessageFactory.FLAGS_HMAC and local_hmac_set:
            if datahmac != hmac.new(Pyro4.config.HMAC_KEY, databytes, digestmod=hashlib.sha1).digest():
                raise errors.SecurityError("message hmac mismatch")
        elif flags&MessageFactory.FLAGS_HMAC != local_hmac_set:
            # Message contains hmac and local HMAC_KEY not set, or vice versa. This is not allowed.
            err="hmac key config not symmetric"
            log.warn(err)
            raise errors.SecurityError(err)
        return msgType, flags, seq, databytes

def get_oneways(self):
    """
    list the names of all the methods declared oneway in an object
    self: the object (instance of a class)
    return (list of strings)
    """
    oneways = []
    for name, method in inspect.getmembers(self, inspect.ismethod):
        if getattr(method, "_pyroIsOneway", False):
            oneways.append(name)
    return set(oneways)

def get_asyncs(self):
    """
    list the names of all the methods declared async in an object
    self: the object (instance of a class)
    return (list of strings)
    """
    asyncs = []
    for name, method in inspect.getmembers(self, inspect.ismethod):
        if getattr(method, "_pyroIsAsync", False):
            asyncs.append(name)
    return set(asyncs)

def pyroObjectSerializer(self):
    """reduce function that automatically replaces Pyro objects by a Proxy"""
    daemon=getattr(self,"_pyroDaemon",None)
    if daemon:
        # only return a proxy if the object is a registered pyro object
        return (Pyro4.core.Proxy, (daemon.uriFor(self),), 
                (daemon.uriFor(self), get_oneways(self), get_asyncs(self),
                 util.Serializer(), Pyro4.config.COMMTIMEOUT))
    else:
        return self.__reduce__()


def defaultObjectSerializer(self):
    """reduce function that uses the default implementation"""
    return self.__reduce__()


class DaemonObject(object):
    """The part of the daemon that is exposed as a Pyro object."""
    def __init__(self, daemon):
        self.daemon=daemon

    def registered(self):
        """returns a list of all object names registered in this daemon"""
        return list(self.daemon.objectsById.keys())

    def ping(self):
        """a simple do-nothing method for testing purposes"""
        pass
    
    def getObject(self, objectId):
        """
        return a registered object from its object id.
        This is mostly useful to get a proxy with all the information about the
        object automatically.
        It will raise an exception if the object is not registered.
        """
        assert isinstance(objectId, basestring)
        return self.daemon.objectsById[objectId]
    
class Daemon(object):
    """
    Pyro daemon. Contains server side logic and dispatches incoming remote method calls
    to the appropriate objects.
    """
    serializers=dict() # dict of type -> serializer
    
    def __init__(self, host=None, port=0, unixsocket=None, nathost=None, natport=None, interface=DaemonObject):
        _check_hmac()  # check if hmac secret key is set
        if host is None:
            host=Pyro4.config.HOST
        if nathost is None:
            nathost=Pyro4.config.NATHOST
        if natport is None:
            natport=Pyro4.config.NATPORT or None
        if nathost and unixsocket:
            raise ValueError("cannot use nathost together with unixsocket")
        if (nathost is None) ^ (natport is None):
            raise ValueError("must provide natport with nathost")
        if Pyro4.config.SERVERTYPE=="thread":
            self.transportServer=SocketServer_Threadpool()
        elif Pyro4.config.SERVERTYPE=="multiplex":
            # choose the 'best' multiplexing implementation
            if os.name=="java":
                raise NotImplementedError("select or poll-based server is not supported for jython, use thread server instead")
            import select
            if hasattr(select,"poll"):
                self.transportServer=SocketServer_Poll()
            else:
                self.transportServer=SocketServer_Select()
        else:
            raise errors.PyroError("invalid server type '%s'" % Pyro4.config.SERVERTYPE)
        self.transportServer.init(self, host, port, unixsocket)
        #: The location (str of the form ``host:portnumber``) on which the Daemon is listening
        self.locationStr=self.transportServer.locationStr
        log.debug("created daemon on %s", self.locationStr)
        natport_for_loc = natport
        if natport==0:
            # expose internal port number as NAT port as well. (don't use port because it could be 0 and will be chosen by the OS)
            natport_for_loc = int(self.locationStr.split(":")[1])
        #: The NAT-location (str of the form ``nathost:natportnumber``) on which the Daemon is exposed for use with NAT-routing
        self.natLocationStr = "%s:%d" % (nathost, natport_for_loc) if nathost else None
        if self.natLocationStr:
            log.debug("NAT address is %s", self.natLocationStr)
        self.serializer=util.Serializer()
        pyroObject=interface(self)
        pyroObject._pyroId=constants.DAEMON_NAME
        #: Dictionary from Pyro object id to the actual Pyro object registered by this id
        self.objectsById={pyroObject._pyroId: pyroObject}
        self.__mustshutdown=threadutil.Event()
        self.__loopstopped=threadutil.Event()
        self.__loopstopped.set()
        self._uriToFuture = {}

    @property
    def sock(self):
        return self.transportServer.sock

    @property
    def sockets(self):
        return self.transportServer.sockets

    @staticmethod
    def serveSimple(objects, host=None, port=0, daemon=None, ns=True, verbose=True):
        """
        Very basic method to fire up a daemon (or supply one yourself).
        objects is a dict containing objects to register as keys, and
        their names (or None) as values. If ns is true they will be registered
        in the naming server as well, otherwise they just stay local.
        """
        if not daemon:
            daemon=Daemon(host, port)
        with daemon:
            if ns:
                ns=Pyro4.naming.locateNS()
            for obj, name in objects.items():
                if ns:
                    localname=None   # name is used for the name server
                else:
                    localname=name   # no name server, use name in daemon
                uri=daemon.register(obj, localname)
                if verbose:
                    print("Object {0}:\n    uri = {1}".format(repr(obj), uri))
                if name and ns:
                    ns.register(name, uri)
                    if verbose:
                        print("    name = {0}".format(name))
            if verbose:
                print("Pyro daemon running.")
            daemon.requestLoop()

    def requestLoop(self, loopCondition=lambda: True):
        """
        Goes in a loop to service incoming requests, until someone breaks this
        or calls shutdown from another thread.
        """
        self.__mustshutdown.clear()
        log.info("daemon %s entering requestloop", self.locationStr)
        try:
            self.__loopstopped.clear()
            condition=lambda: not self.__mustshutdown.is_set() and loopCondition()
            self.transportServer.loop(loopCondition=condition)
        finally:
            self.__loopstopped.set()
        log.debug("daemon exits requestloop")

    def events(self, eventsockets):
        """for use in an external event loop: handle any requests that are pending for this daemon"""
        return self.transportServer.events(eventsockets)

    def shutdown(self):
        """Cleanly terminate a daemon that is running in the requestloop. It must be running
        in a different thread, or this method will deadlock."""
        log.debug("daemon shutting down")
        self.__mustshutdown.set()
        self.transportServer.wakeup()
        time.sleep(0.05)
        self.close()
        self.__loopstopped.wait()
        log.info("daemon %s shut down", self.locationStr)

    def _handshake(self, conn):
        """Perform connection handshake with new clients"""
        # For now, client is not sending anything. Just respond with a CONNECT_OK.
        # We need a minimal amount of data or the socket will remain blocked
        # on some systems... (messages smaller than 40 bytes)
        # Return True for successful handshake, False if something was wrong.
        data="ok"
        if sys.version_info>=(3,0):
            data=bytes(data,"utf-8")
        msg=MessageFactory.createMessage(MessageFactory.MSG_CONNECTOK, data, 0, 1)
        conn.send(msg)
        return True

    def handleRequest(self, conn):
        """
        Handle incoming Pyro request. Catches any exception that may occur and
        wraps it in a reply to the calling side, as to not make this server side loop
        terminate due to exceptions caused by remote invocations.
        """
        flags=0
        seq=0
        wasBatched=False
        isCallback=False
        client_future = None
        try:
            msgType, flags, seq, data = MessageFactory.getMessage(conn, MessageFactory.MSG_INVOKE)
            objId, method, vargs, kwargs=self.serializer.deserialize(
                                           data, compressed=flags & MessageFactory.FLAGS_COMPRESSED)
            del data  # invite GC to collect the object, don't wait for out-of-scope
            obj=self.objectsById.get(objId)
            
            if flags & MessageFactory.FLAGS_ASYNC:
                client_future = vargs[0]
                client_future._pyroOneway.update(["set_cancelled", "set_result", "set_exception", "set_progress"])
                vargs = vargs[1:]
            elif flags & MessageFactory.FLAGS_ASYNC_CANCEL:
                client_future_uri = vargs[0]
            
            if obj is not None:
                if kwargs and sys.version_info<(2, 6, 5) and os.name!="java":
                    # Python before 2.6.5 doesn't accept unicode keyword arguments
                    kwargs = dict((str(k), kwargs[k]) for k in kwargs)
                if flags & MessageFactory.FLAGS_BATCH:
                    # batched method calls, loop over them all and collect all results
                    data=[]
                    for method,vargs,kwargs in vargs:
                        method=util.resolveDottedAttribute(obj, method, Pyro4.config.DOTTEDNAMES)
                        try:
                            result=method(*vargs, **kwargs)   # this is the actual method call to the Pyro object
                        except Exception:
                            xt,xv=sys.exc_info()[0:2]
                            log.debug("Exception occurred while handling batched request: %s", xv)
                            xv._pyroTraceback=util.formatTraceback(detailed=Pyro4.config.DETAILED_TRACEBACK)
                            if sys.platform=="cli":
                                util.fixIronPythonExceptionForPickle(xv, True)  # piggyback attributes
                            data.append(futures._ExceptionWrapper(xv))
                            break   # stop processing the rest of the batch
                        else:
                            data.append(result)
                    wasBatched=True
                elif flags & MessageFactory.FLAGS_ASYNC_CANCEL:
                    data=self._cancelFuture(client_future_uri)
                else:
                    # normal single method call
                    method=util.resolveDottedAttribute(obj, method, Pyro4.config.DOTTEDNAMES)
                    if flags & MessageFactory.FLAGS_ONEWAY and Pyro4.config.ONEWAY_THREADED:
                        # oneway call to be run inside its own thread
                        thread=threadutil.Thread(target=method, args=vargs, kwargs=kwargs)
                        thread.daemon = True
                        thread.start()
                    elif flags & MessageFactory.FLAGS_ASYNC:
                        future=method(*vargs, **kwargs)
                        self._followFuture(future, client_future)
                    else:
                        isCallback=getattr(method, "_pyroCallback", False)
                        data=method(*vargs, **kwargs)   # this is the actual method call to the Pyro object
            else:
                log.debug("unknown object requested: %s", objId)
                raise errors.DaemonError("unknown object")
            if flags & MessageFactory.FLAGS_ONEWAY:
                return   # oneway call, don't send a response
            elif flags & MessageFactory.FLAGS_ASYNC:
                return  # async call, don't send a response yet
            else:
                data, compressed=self.serializer.serialize(data, compress=Pyro4.config.COMPRESSION)
                flags=0
                if compressed:
                    flags |= MessageFactory.FLAGS_COMPRESSED
                if wasBatched:
                    flags |= MessageFactory.FLAGS_BATCH
                msg=MessageFactory.createMessage(MessageFactory.MSG_RESULT, data, flags, seq)
                del data
                conn.send(msg)
        except Exception as ex:
            xt,xv=sys.exc_info()[0:2]
            if xt is not errors.ConnectionClosedError:
                log.debug("Exception occurred while handling request: %r", xv)
                if client_future is not None:
                    # send exception to the client future
                    client_future.set_exception(ex)
                elif not flags & MessageFactory.FLAGS_ONEWAY:
                    # only return the error to the client if it wasn't a oneway call
                    tblines=util.formatTraceback(detailed=Pyro4.config.DETAILED_TRACEBACK)
                    self._sendExceptionResponse(conn, seq, xv, tblines)
            if isCallback or isinstance(xv, (errors.CommunicationError, errors.SecurityError)):
                raise       # re-raise if flagged as callback, communication or security error.

    def _sendExceptionResponse(self, connection, seq, exc_value, tbinfo):
        """send an exception back including the local traceback info"""
        exc_value._pyroTraceback=tbinfo
        if sys.platform=="cli":
            util.fixIronPythonExceptionForPickle(exc_value, True)  # piggyback attributes
        try:
            data, _=self.serializer.serialize(exc_value)
        except:
            # the exception object couldn't be serialized, use a generic PyroError instead
            xt, xv, tb = sys.exc_info()
            msg = "Error serializing exception: %s. Original exception: %s: %s" % (str(xv), type(exc_value), str(exc_value))
            exc_value = errors.PyroError(msg)
            exc_value._pyroTraceback=tbinfo
            if sys.platform=="cli":
                util.fixIronPythonExceptionForPickle(exc_value, True)  # piggyback attributes
            data, _=self.serializer.serialize(exc_value)
        msg=MessageFactory.createMessage(MessageFactory.MSG_RESULT, data, MessageFactory.FLAGS_EXCEPTION, seq)
        del data
        connection.send(msg)

    def _followFuture(self, future, client_future):
        uri = client_future._pyroUri.asString()
        self._uriToFuture[uri] = future
        def on_future_completion(f):
            try:
                client_future.set_result(f.result())
            except cfutures.CancelledError:
                client_future.set_cancelled()
            except Exception as ex:
                try:
                    client_future.set_exception(ex)
                except: # exception cannot be sent => simplify
                    logging.info("Failed to send full exception, will send summary")
                    msg = "Exception %s %s (Error serializing exception)" % (type(ex), str(ex))
                    exc_value = errors.PyroError(msg)
                    client_future.set_exception(exc_value)
            finally:
                del self._uriToFuture[uri] # that should be the only ref, so kill connection
        future.add_done_callback(on_future_completion)

        if hasattr(future, "add_update_callback"):
            def on_future_progess(f, s, e):
                client_future.set_progress(s, e)
            # called at least once immediately, and once just before completion callback
            future.add_update_callback(on_future_progess)

    def _cancelFuture(self, client_future_uri):
        if client_future_uri in self._uriToFuture:
            future = self._uriToFuture[client_future_uri]
            return future.cancel()
        else:
            log.debug("Couldn't find future %s in %s", client_future_uri, str(self._uriToFuture))
            return False

    def register(self, obj, objectId=None):
        """
        Register a Pyro object under the given id. Note that this object is now only
        known inside this daemon, it is not automatically available in a name server.
        This method returns a URI for the registered object.
        """
        if objectId:
            if not isinstance(objectId, basestring):
                raise TypeError("objectId must be a string or None")
        else:
            objectId="obj_"+uuid.uuid4().hex   # generate a new objectId
        if hasattr(obj, "_pyroId") and obj._pyroId != "":     # check for empty string is needed for Cython
            raise errors.DaemonError("object (%s) already has a Pyro id (%s)" % (obj, obj._pyroId))
        if objectId in self.objectsById:
            raise errors.DaemonError("object already registered with that id")
        # set some pyro attributes
        obj._pyroId=objectId
        obj._pyroDaemon=self
        if Pyro4.config.AUTOPROXY:
            # register a custom serializer for the type to automatically return proxies
            try:
                if isinstance(obj, tuple(self.serializers)):
                    # Find the most fitting serializer by picking the highest in the mro
                    for t in type(obj).__mro__:
                        if t in self.serializers:
                            copyreg.pickle(type(obj), self.serializers[t])
                            break
                else:
                    copyreg.pickle(type(obj),pyroObjectSerializer)
            except TypeError:
                pass
        # register the object in the mapping
        self.objectsById[obj._pyroId]=obj
        return self.uriFor(objectId)

    def unregister(self, objectOrId):
        """
        Remove an object from the known objects inside this daemon.
        You can unregister an object directly or with its id.
        """
        if objectOrId is None:
            raise ValueError("object or objectid argument expected")
        if not isinstance(objectOrId, basestring):
            objectId=getattr(objectOrId, "_pyroId", None)
            if objectId is None:
                raise errors.DaemonError("object isn't registered")
        else:
            objectId=objectOrId
            objectOrId=None
        if objectId==constants.DAEMON_NAME:
            return
        if objectId in self.objectsById:
            del self.objectsById[objectId]
            if objectOrId is not None:
                del objectOrId._pyroId
                del objectOrId._pyroDaemon
                # Don't remove the custom type serializer (copyreg.pickle) because there
                # may be other registered objects of the same type still depending on it.
                # Also, it would require an inefficient linear search through the registered
                # objects map to scan for types. Finally, the copyreg module doesn't seem
                # to be designed with cleanup in mind (it has no explicit unregister function)

    def uriFor(self, objectOrId=None, nat=True):
        """
        Get a URI for the given object (or object id) from this daemon.
        Only a daemon can hand out proper uris because the access location is
        contained in them.
        Note that unregistered objects cannot be given an uri, but unregistered
        object names can (it's just a string we're creating in that case).
        If nat is set to False, the configured NAT address (if any) is ignored and it will
        return an URI for the internal address.
        """
        if not isinstance(objectOrId, basestring):
            objectOrId=getattr(objectOrId, "_pyroId", None)
            if objectOrId is None:
                raise errors.DaemonError("object isn't registered")
        if nat:
            loc=self.natLocationStr or self.locationStr
        else:
            loc=self.locationStr
        return URI("PYRO:%s@%s" % (objectOrId, loc))
        
    def close(self):
        """Close down the server and release resources"""
        log.debug("daemon closing")
        if self.transportServer:
            self.transportServer.close()
            self.transportServer=None

    def __repr__(self):
        return "<%s.%s at 0x%x, %s, %d objects>" % (self.__class__.__module__, self.__class__.__name__,
            id(self), self.locationStr, len(self.objectsById))

    def __enter__(self):
        if not self.transportServer:
            raise errors.PyroError("cannot reuse this object")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getstate__(self):
        return {}   # a little hack to make it possible to serialize Pyro objects.


# decorators

def callback(object):
    """
    decorator to mark a method to be a 'callback'. This will make Pyro
    raise any errors also on the callback side, and not only on the side
    that does the callback call.
    """
    object._pyroCallback=True
    return object

def oneway(func):
    """
    Decorator to mark a function "one way": the caller don't need to wait for 
    the result. The caller will always receive None from the call to such function.
    Note that it has to be a method (not a function) as we share always a whole 
    object remotely.
    """
    func._pyroIsOneway = True
    return func

# would be better called async, but there is already a function named like this
def isasync(func):
    """
    Decorator to mark a function "asynchronous": the caller receives a Future to
    get the result later on. The given callable _must return a Future_. If the 
    object is remote, Pyro will take care of converting the Future to something
    remote. Note that for efficiency, if the object actually returns immediately
    a Future with the result done, it is better to _not_ declare the the method
    asynchronous. Futures are as defined by PEP 3148.
    """
    func._pyroIsAsync = True
    return func
