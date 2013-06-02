# -*- coding: utf-8 -*-
# -*- test-case-name: vumi.tests.test_reconnecting_client -*-

"""A service to provide the functionality of ReconnectingClientFactory
   when using Twisted's endpoints.

   Melded together from code and ideas from:

   * Twisted's existing ReconnectingClientFactory code.
   * https://github.com/keturn/twisted/blob/persistent-client-service-4735/
     twisted/application/internet.py
   """

import random

from twisted.application.service import Service
from twisted.python import log


class _RestartableProtocolProxy(object):
    """A proxy for a Protocol to provide connectionLost notification."""

    def __init__(self, protocol, clientService):
        self.__protocol = protocol
        self.__clientService = clientService

    def connectionLost(self, reason):
        result = self.__protocol.connectionLost(reason)
        self.__clientService.clientConnectionLost(reason)
        return result

    def __getattr__(self, item):
        return getattr(self.__protocol, item)

    def __repr__(self):
        return '<%s.%s wraps %r>' % (__name__, self.__class__.__name__,
            self.__protocol)



class _RestartableProtocolFactoryProxy(object):
    """A wrapper for a ProtocolFactory to facilitate restarting Protocols."""

    _protocolProxyFactory = _RestartableProtocolProxy

    def __init__(self, protocolFactory, clientService):
        self.protocolFactory = protocolFactory
        self.clientService = clientService


    def buildProtocol(self, addr):
        protocol = self.protocolFactory.buildProtocol(addr)
        wrappedProtocol = self._protocolProxyFactory(
            protocol, self.clientService)
        return wrappedProtocol


    def __getattr__(self, item):
        # maybe components.proxyForInterface is the thing to do here, but that
        # gave me a metaclass conflict.
        return getattr(self.protocolFactory, item)


    def __repr__(self):
        return '<%s.%s wraps %r>' % ( __name__, self.__class__.__name__,
            self.protocolFactory)



class ReconnectingClientService(Service):
    """
    Service which auto-reconnects clients with an exponential back-off.

    Note that clients should call my resetDelay method after they have
    connected successfully.

    @ivar factory: A L{protocol.Factory} which will be used to create clients
        for the endpoint.
    @ivar endpoint: An L{IStreamClientEndpoint
        <twisted.internet.interfaces.IStreamClientEndpoint>} provider
        which will be used to connect when the service starts.

    @ivar maxDelay: Maximum number of seconds between connection attempts.
    @ivar initialDelay: Delay for the first reconnection attempt.
    @ivar factor: A multiplicitive factor by which the delay grows
    @ivar jitter: Percentage of randomness to introduce into the delay length
        to prevent stampeding.
    @ivar clock: The clock used to schedule reconnection. It's mainly useful to
        be parametrized in tests. If the factory is serialized, this attribute
        will not be serialized, and the default value (the reactor) will be
        restored when deserialized.
    @type clock: L{IReactorTime}
    @ivar maxRetries: Maximum number of consecutive unsuccessful connection
        attempts, after which no further connection attempts will be made. If
        this is not explicitly set, no maximum is applied.
    """
    maxDelay = 3600
    initialDelay = 1.0
    # Note: These highly sensitive factors have been precisely measured by
    # the National Institute of Science and Technology.  Take extreme care
    # in altering them, or you may damage your Internet!
    # (Seriously: <http://physics.nist.gov/cuu/Constants/index.html>)
    factor = 2.7182818284590451 # (math.e)
    # Phi = 1.6180339887498948 # (Phi is acceptable for use as a
    # factor if e is too large for your application.)
    jitter = 0.11962656472 # molar Planck constant times c, joule meter/mole

    delay = initialDelay
    retries = 0
    maxRetries = None
    clock = None
    noisy = False

    continueTrying = 1

    _delayedRetryDeferred = None
    _connectingDeferred = None


    def __init__(self, endpoint, factory):
        self.endpoint = endpoint
        self.factory = factory

        if self.clock is None:
            from twisted.internet import reactor
            self.clock = reactor


    def startService(self):
        self.continueTrying = 1
        self.retry()


    def stopService(self):
        """
        Stop attempting to reconnect and close any existing connections.
        """
        # XXX: Return a deferred that fires once connecting
        #      attempts have stopped.
        self.continueTrying = 0

        if self._delayedRetryDeferred is not None:
            self._delayedRetryDeferred.cancel()
            self._delayedRetryDeferred = None

        if self._connectingDeferred is not None:
            self._connectingDeferred.cancel()
            self._connectingDeferred = None


    def clientConnected(self, protocol):
        self.resetDelay()


    def clientConnectionFailed(self, unused_reason):
        # TODO: log the reason?
        self.retry()


    def clientConnectionLost(self, unused_reason):
        # TODO: log the reason?
        self.retry()


    def retry(self):
        """
        Have this connector connect again, after a suitable delay.
        """
        if not self.continueTrying:
            if self.noisy:
                log.msg("Abandoning %s on explicit request" % (self.endpoint,))
            return

        self.retries += 1
        if self.maxRetries is not None and (self.retries > self.maxRetries):
            if self.noisy:
                log.msg("Abandoning %s after %d retries." %
                        (self.endpoint, self.retries))
            return

        self.delay = min(self.delay * self.factor, self.maxDelay)
        if self.jitter:
            self.delay = random.normalvariate(self.delay,
                                              self.delay * self.jitter)

        if self.noisy:
            log.msg("%s will retry in %d seconds"
                    % (self.endpoint, self.delay))

        def reconnector():
            proxied_factory = _RestartableProtocolFactoryProxy(
                self.factory, self)
            self._connectingDeferred = self.endpoint.connect(proxied_factory)
            self._connectingDeferred.addCallback(self.clientConnected)
            self._connectingDeferred.errBack(self.clientConnectionFailed)

        self._delayedRetryDeferred = self.clock.callLater(
            self.delay, reconnector)


    def resetDelay(self):
        """
        Call this method after a successful connection: it resets the delay and
        the retry counter.
        """
        self.delay = self.initialDelay
        self.retries = 0
