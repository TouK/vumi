from collections import namedtuple

from twisted.python import failure

from vumi.transports.parlayx_ussd.client import format_address
from vumi.transports.parlayx.soaputil import soap_envelope
from vumi.transports.parlayx_ussd.server import NOTIFICATION_NS, normalize_address
from vumi.transports.parlayx.xmlutil import LocalNamespace as L, tostring


# XXX: This can be deleted as soon as we're using Twisted > 13.0.0.
class _FailureResultOfMixin(object):
    """
    Mixin providing the more recent and useful version of
    `TestCase.failureResultOf`.
    """
    def failureResultOf(self, deferred, *expectedExceptionTypes):
        """
        Return the current failure result of C{deferred} or raise
        C{self.failException}.

        @param deferred: A L{Deferred<twisted.internet.defer.Deferred>} which
            has a failure result.  This means
            L{Deferred.callback<twisted.internet.defer.Deferred.callback>} or
            L{Deferred.errback<twisted.internet.defer.Deferred.errback>} has
            been called on it and it has reached the end of its callback chain
            and the last callback or errback raised an exception or returned a
            L{failure.Failure}.
        @type deferred: L{Deferred<twisted.internet.defer.Deferred>}

        @param expectedExceptionTypes: Exception types to expect - if
            provided, and the the exception wrapped by the failure result is
            not one of the types provided, then this test will fail.

        @raise SynchronousTestCase.failureException: If the
            L{Deferred<twisted.internet.defer.Deferred>} has no result, has a
            success result, or has an unexpected failure result.

        @return: The failure result of C{deferred}.
        @rtype: L{failure.Failure}
        """
        result = []
        deferred.addBoth(result.append)
        if not result:
            self.fail(
                "Failure result expected on %r, found no result instead" % (
                    deferred,))
        elif not isinstance(result[0], failure.Failure):
            self.fail(
                "Failure result expected on %r, "
                "found success result (%r) instead" % (deferred, result[0]))
        elif (expectedExceptionTypes and
              not result[0].check(*expectedExceptionTypes)):
            expectedString = " or ".join([
                '.'.join((t.__module__, t.__name__)) for t in
                expectedExceptionTypes])

            self.fail(
                "Failure of type (%s) expected on %r, "
                "found type %r instead: %s" % (
                    expectedString, deferred, result[0].type,
                    result[0].getTraceback()))
        else:
            return result[0]


class MockResponse(namedtuple('MockResponse', ['code', 'delivered_body'])):
    """
    Mock response from ``http_request_full``.
    """
    @classmethod
    def build(cls, code, body, header=None):
        """
        Build a `MockResponse` containing a SOAP envelope.
        """
        return cls(
            code=code,
            delivered_body=tostring(soap_envelope(body, header)))


def create_ussd_reception_element(msgType, senderCB, ussdOpType, ussdString, sender_address,
                                  serviceCode):
    """
    Helper for creating an ``notifyUssdReception`` element.
    """
    return NOTIFICATION_NS.notifyUssdReception(
        NOTIFICATION_NS.msgType(msgType),
        NOTIFICATION_NS.senderCB(senderCB),
        NOTIFICATION_NS.receiveCB(senderCB),
        NOTIFICATION_NS.ussdOpType(ussdOpType),
        NOTIFICATION_NS.msIsdn(sender_address),
        NOTIFICATION_NS.serviceCode(serviceCode),
        NOTIFICATION_NS.ussdString(ussdString))



__all__ = [
    'MockResponse', 'create_ussd_reception_element']
