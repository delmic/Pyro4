"""
Support for Futures (asynchronously executed callables).
If you're using Python 3.2 or newer, also see
http://docs.python.org/3/library/concurrent.futures.html#future-objects

Pyro - Python Remote Objects.  Copyright by Irmen de Jong (irmen@razorvine.net).
"""

from __future__ import with_statement

from Pyro4 import threadutil, util
import functools
import logging
import sys
import time

import concurrent.futures as cfutures


__all__=["Future", "FutureResult", "_ExceptionWrapper"]

log=logging.getLogger("Pyro4.futures")


class Future(object):
    """
    Holds a callable that will be executed asynchronously and provide its
    result value some time in the future.
    This is a more general implementation than the AsyncRemoteMethod, which
    only works with Pyro proxies (and provides a bit different syntax).
    """
    def __init__(self, callable):
        self.callable = callable
        self.chain = []

    def __call__(self, *args, **kwargs):
        """
        Start the future call with the provided arguments.
        Control flow returns immediately, with a FutureResult object.
        """
        chain = self.chain
        del self.chain  # make it impossible to add new calls to the chain once we started executing it
        result=FutureResult()  # notice that the call chain doesn't sit on the result object
        thread=threadutil.Thread(target=self.__asynccall, args=(result, chain, args, kwargs))
        thread.setDaemon(True)
        thread.start()
        return result

    def __asynccall(self, asyncresult, chain, args, kwargs):
        try:
            value = self.callable(*args, **kwargs)
            # now walk the callchain, passing on the previous value as first argument
            for call, args, kwargs in chain:
                call = functools.partial(call, value)
                value = call(*args, **kwargs)
            asyncresult.value = value
        except Exception:
            # ignore any exceptions here, return them as part of the async result instead
            asyncresult.value=_ExceptionWrapper(sys.exc_info()[1])

    def then(self, call, *args, **kwargs):
        """
        Add a callable to the call chain, to be invoked when the results become available.
        The result of the current call will be used as the first argument for the next call.
        Optional extra arguments can be provided in args and kwargs.
        """
        self.chain.append((call, args, kwargs))


class FutureResult(object):
    """
    The result object for asynchronous calls.
    """
    def __init__(self):
        self.__ready=threadutil.Event()
        self.callchain=[]
        self.valueLock=threadutil.Lock()

    def wait(self, timeout=None):
        """
        Wait for the result to become available, with optional timeout (in seconds).
        Returns True if the result is ready, or False if it still isn't ready.
        """
        result=self.__ready.wait(timeout)
        if result is None:
            # older pythons return None from wait()
            return self.__ready.isSet()
        return result

    @property
    def ready(self):
        """Boolean that contains the readiness of the async result"""
        return self.__ready.isSet()

    def get_value(self):
        self.__ready.wait()
        if isinstance(self.__value, _ExceptionWrapper):
            self.__value.raiseIt()
        else:
            return self.__value

    def set_value(self, value):
        with self.valueLock:
            self.__value=value
            # walk the call chain but only as long as the result is not an exception
            if not isinstance(value, _ExceptionWrapper):
                for call, args, kwargs in self.callchain:
                    call = functools.partial(call, self.__value)
                    self.__value = call(*args, **kwargs)
                    if isinstance(self.__value, _ExceptionWrapper):
                        break
            self.callchain=[]
            self.__ready.set()

    value=property(get_value, set_value, None, "The result value of the call. Reading it will block if not available yet.")

    def then(self, call, *args, **kwargs):
        """
        Add a callable to the call chain, to be invoked when the results become available.
        The result of the current call will be used as the first argument for the next call.
        Optional extra arguments can be provided in args and kwargs.
        """
        if self.__ready.isSet():
            # value is already known, we need to process it immediately (can't use the callchain anymore)
            call = functools.partial(call, self.__value)
            self.__value = call(*args, **kwargs)
        else:
            # add the call to the callchain, it will be processed later when the result arrives
            with self.valueLock:
                self.callchain.append((call, args, kwargs))
        return self


class _ExceptionWrapper(object):
    """Class that wraps a remote exception. If this is returned, Pyro will
    re-throw the exception on the receiving side. Usually this is taken care of
    by a special response message flag, but in the case of batched calls this
    flag is useless and another mechanism was needed."""
    def __init__(self, exception):
        self.exception=exception

    def raiseIt(self):
        if sys.platform=="cli":
            util.fixIronPythonExceptionForPickle(self.exception, False)
        raise self.exception

# This is support for functions returning a Future
# Such function must be decorated by @isasync (or be part of proxy._pyroAsyncs)
# When such a function is called (on the proxy), a new ClientFuture is first
# created, registered to a special "FutureDaemon" (a standard Pyro server, but
# running on the client side), and then passed to the remote call. Note that
# as it is registered to the FutureDaemon, the server receives a proxy to this
# ClientFuture. Note that the ClientFuture is returned as soon as the message
# is sent to the server. So this is fast, but if the method ends up with an
# exception, it will not behave as it would locally (ie, the exception is
# hidden).
# On the server side, the method is called and its return value, a future,
# is then connected to the (proxy of the) ClientFuture using _followFuture().
#
# When/if the future is cancelled (which can only happen at the client side),
# the ClientFuture sends a special ASYNC_CANCEL message (via the proxy connection)
# and the server calls cancel() on the real Future and return the result.
# Note: Currently, in such a case set_cancel() on the client future is called
# from the server, but that's probably not needed. (it's needed only
# on the case that the future is cancelled on the server side, which can happen
# but is pretty corner case).
#
# When the real future is completed, either set_result() or set_exception() is
# called (oneway) on the ClientFuture, which will then afterwards unregister, as no
# information from the real future will ever come again (the future is static).
# Note that when the ClientFuture is not used by the client (a very common case),
# it is still kept (by the futureDaemon), until the real future sends one of
# outcomes.

# TODO: reorganise so that:
# * The remote call only returns when the call is done. => slower but we get the
#   exceptions.
# * Future is created when receiving the result from the remote call.
#   Could use "ProxyFuture" (= register the Future to the server daemon)
#   => this allows to create the right type of Future
# * If the future is already finished, don't register it (and so it's a
#   static object) (Neat optimisation, but probably 99.9% of the time not
#   useful).
# * Proxy on client side only connects back if a method is called and
#   requires info from the original future. => if the future is immediately
#   discarded by the user (very common pattern), nothing more will happen.
# * Share the FutureDaemon on the module level.
# * When to drop the reference on the server side? When it's finished? (then
#   save result/exception?

# from the futures implementation
# Possible future states (for internal use by the futures package).
PENDING = 'PENDING'
RUNNING = 'RUNNING'
# The future was cancelled by the user...
CANCELLED = 'CANCELLED'
FINISHED = 'FINISHED'

class ClientFuture(object):
    """
    A future object which represents the future from a remote asynchronous call
    """
    def __init__(self, proxy):
        """
        proxy: Proxy of the component that contains the call to the method
          returning the Future
        """
        self._condition = threadutil.Condition() # to be thread-safe
        self._state = PENDING
        self._result = None
        self._exception = None
        self._waiters = []
        self._done_callbacks = []
        self._proxy = proxy

        # For ProgressiveFuture
        self._upd_callbacks = []
        self._start_time = time.time() + 0.1
        self._end_time = self._start_time

        logging.debug("Created future %r", self)

    # copy-paste
    def _invoke_callbacks(self):
        for callback in self._done_callbacks:
            try:
                callback(self)
            except Exception:
                log.exception('exception calling callback for %r', self)

    def __repr__(self):
        return '<ClientFuture at %s for %s>' % (hex(id(self)), self._proxy)

    def cancel(self):
        with self._condition:
            # already done? => no need to even ask the real Future
            if self._state == CANCELLED:
                return True
            elif self._state == FINISHED:
                return False
            # get the uri before we release the lock
            # in case the future gets unregistered just after
            uri = self._pyroDaemon.uriFor(self).asString()

        # need to cancel the real future
        # One problem: we cannot take the lock when calling remote
        # (because it might call set_cancelled which also needs the lock)
        # TODO => don't call set_cancelled when cancelled from remote
        result = self._proxy._pyroCancelFuture(uri)
        with self._condition:
            if result:
                self._state = CANCELLED
            elif self._state == PENDING:
                # cannot cancel and not finished => it's running
                self._state = RUNNING
            return result

    def cancelled(self):
        """Return True if the future has cancelled."""
        with self._condition:
            return self._state == CANCELLED

    def running(self):
        # FIXME: if still in PENDING state => request state of the real future
        # almost always False because we don't get informed normally
        # about the RUNNING state over the network
        with self._condition:
            return self._state == RUNNING

    def done(self):
        """Return True of the future was cancelled or finished executing."""
        with self._condition:
            return self._state in (CANCELLED, FINISHED)

    def add_done_callback(self, fn):
        with self._condition:
            if self._state not in (CANCELLED, FINISHED):
                self._done_callbacks.append(fn)
                return
        fn(self)

    def __get_result(self):
        if self._exception:
            raise self._exception
        else:
            return self._result

    def result(self, timeout=None):
        with self._condition:
            if self._state == CANCELLED:
                raise cfutures.CancelledError()
            elif self._state == FINISHED:
                return self.__get_result()

            self._condition.wait(timeout)

            if self._state == CANCELLED:
                raise cfutures.CancelledError()
            elif self._state == FINISHED:
                return self.__get_result()
            else:
                raise cfutures.TimeoutError()

    def exception(self, timeout=None):
        with self._condition:
            if self._state == CANCELLED:
                raise cfutures.CancelledError()
            elif self._state == FINISHED:
                return self._exception

            self._condition.wait(timeout)

            if self._state == CANCELLED:
                raise cfutures.CancelledError()
            elif self._state == FINISHED:
                return self._exception
            else:
                raise cfutures.TimeoutError()

    # These three methods are to send the result of the asynchronous call
    # after such a call, the future should be frozen.
    def set_cancelled(self):
        """Sets the state of the future to cancel.

        Should only be used by the server daemon
        """
        with self._condition:
            self._state = CANCELLED
            self._condition.notify_all()
            self._unregister()
        self._invoke_callbacks()

    def set_result(self, result):
        """Sets the return value of work associated with the future.

        Should only be used by the server daemon
        """
        with self._condition:
            self._result = result
            self._state = FINISHED
            self._condition.notify_all()
            self._unregister()
        self._invoke_callbacks()

    def set_exception(self, exception):
        """Sets the result of the future as being the given exception.

        Should only be used by the server daemon
        """
        logging.debug("ClientFuture received notification of exception")
        with self._condition:
            self._exception = exception
            self._state = FINISHED
            self._condition.notify_all()
            self._unregister()
        self._invoke_callbacks()

    def _unregister(self):
        logging.debug("Unregistered future %r", self)
        # needed to be sure to have all references removed once it's not used
        if hasattr(self, "_pyroDaemon"):
            self._pyroDaemon.unregister(self)

    # For ProgressiveFuture
    # TODO: find a way to put get_progress() and add_update_callback() iff
    # the real future is a ProgressiveFuture.
    def get_progress(self):
        """
        Return the current known start and end time
        return (float, float): start and end time (in s from epoch)
        """
        # Return what is known locally
        with self._condition:
            start, end = self._start_time, self._end_time
            # FIXME: cannot do this for now because we don't get info on the
            # change of state PENDING -> RUNNING (ie, it's never RUNNING)
#             if self._state == PENDING:
#                 # ensure we say the start time is not (too much) in the past
#                 now = time.time()
#                 if start < now:
#                     dur = end - start
#                     start = now
#                     end = now + dur
            # TODO: Cannot do this either because we don't get info that it's
            # done until _after_ the last progress update
#             if self._state in (PENDING, RUNNING):
#                 # ensure we say the end time is not (too much) in the past
#                 end = max(end, time.time())

        return start, end

    def add_update_callback(self, fn):
        with self._condition:
            if self._state not in (CANCELLED, FINISHED):
                self._upd_callbacks.append(fn)

        # Immediately report the current known information (even if finished)
        self._report_update(fn)

    def set_progress(self, start=None, end=None):
        """
        Should only be used by the server daemon
        """
        with self._condition:
            if start is not None:
                self._start_time = start
            if end is not None:
                self._end_time = end

        self._invoke_upd_callbacks()

    def _report_update(self, fn):
        start, end = self.get_progress()
        fn(self, start, end)

    def _invoke_upd_callbacks(self):
        for callback in self._upd_callbacks:
            try:
                self._report_update(callback)
            except Exception:
                logging.exception('exception calling callback for %r', self)
