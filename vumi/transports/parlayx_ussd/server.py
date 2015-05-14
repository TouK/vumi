# -*- test-case-name: vumi.transports.parlayx_ussd.tests.test_server -*-

from collections import namedtuple

from twisted.internet.defer import maybeDeferred, fail
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.parlayx.client import PARLAYX_COMMON_NS
from vumi.transports.parlayx.soaputil import (
    soap_envelope, unwrap_soap_envelope, soap_fault, SoapFault)
from vumi.transports.parlayx.server import  ( normalize_address )
from vumi.transports.parlayx.xmlutil import (
    Namespace, gettext, split_qualified, parse_document, tostring)



NOTIFICATION_NS = Namespace(
    'http://www.csapi.org/schema/parlayx/ussd/notification/v1_0/local', 'loc')



class USSDMessage(namedtuple('USSDMessage',
                            ['msgType', 'senderCB','receiveCB','ussdOpType', 'msisdn', 'serviceCode',
                             'codeScheme', 'ussdString'])):
    """
    ParlayX `USSDMessage` complex type.
    """
    @classmethod
    def from_element(cls, root):
        """
        Create an `USSDMessage` instance from an ElementTree element.
        """
        return cls(
            msgType=gettext(root, './/' + str(NOTIFICATION_NS.msgType)),
            senderCB=gettext(root, './/' + str(NOTIFICATION_NS.senderCB)),
            receiveCB=gettext(root, './/' + str(NOTIFICATION_NS.receiveCB)),
            ussdOpType=gettext(root, './/' + str(NOTIFICATION_NS.ussdOpType)),
            serviceCode=gettext(root, './/' + str(NOTIFICATION_NS.serviceCode)),
            codeScheme=gettext(root, './/' + str(NOTIFICATION_NS.codeScheme)),
            ussdString=gettext(root, './/' + str(NOTIFICATION_NS.ussdString)),
            msisdn=gettext(
                root, './/' + str(NOTIFICATION_NS.msIsdn), parse=normalize_address))


class USSDNotificationService(Resource):
    """
    Web resource to handle SOAP requests for ParlayX USSD deliveries and
    delivery receipts.
    """
    isLeaf = True

    def __init__(self, callback_message_received, callback_message_delivered):
        self.callback_message_received = callback_message_received
        self.callback_message_delivered = callback_message_delivered
        Resource.__init__(self)

    def render_POST(self, request):
        """
        Process a SOAP request and convert any exceptions into SOAP faults.
        """
        def _writeResponse(response):
            request.setHeader('Content-Type', 'text/xml; charset="utf-8"')
            request.write(tostring(soap_envelope(response)))
            request.finish()

        def _handleSuccess(result):
            request.setResponseCode(http.OK)
            return result

        def _handleError(f):
            # XXX: Perhaps report this back to the transport somehow???
            log.err(f, 'Failure processing SOAP request')
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            faultcode = u'soapenv:Server'
            if f.check(SoapFault):
                return f.value.to_element()
            return soap_fault(faultcode, f.getErrorMessage())

        try:
            tree = parse_document(request.content)
            body, header = unwrap_soap_envelope(tree)
        except:
            d = fail()
        else:
            d = maybeDeferred(self.process, request, body, header)
            d.addCallback(_handleSuccess)

        d.addErrback(_handleError)
        d.addCallback(_writeResponse)
        return NOT_DONE_YET

    def process(self, request, body, header=None):
        """
        Process a SOAP request.
        """
        for child in body.getchildren():
            # Since there is no SOAPAction header, and these requests are not
            # made to different endpoints, the only way to handle these is to
            # switch on the root element's name. Yuck.
            localname = split_qualified(child.tag)[1]
            meth = getattr(self, 'process_' + localname, self.process_unknown)
            return meth(child, header, localname)

        raise SoapFault(u'soapenv:Client', u'No actionable items')

    def process_unknown(self, root, header, name):
        """
        Process unknown notification deliverables.
        """
        raise SoapFault(u'soapenv:Server', u'No handler for %s' % (name,))

    def process_notifyUssdReception(self, root, header, name):
        """
        Process a received text message.
        """
        linkid = None
        if header is not None:
            linkid = gettext(header, './/' + str(PARLAYX_COMMON_NS.linkid))

        # correlator = gettext(root, NOTIFICATION_NS.correlator)
        message = USSDMessage.from_element(root)
        log.err("received ussd message " + str(self.callback_message_received))
        d = maybeDeferred(self.callback_message_received, message.senderCB, linkid, message)
        # self.callback_message_received( message.senderCB, linkid, message)
        d.addCallback(
            lambda ignored: NOTIFICATION_NS.notifyUssdReceptionResponse(NOTIFICATION_NS.result('0')))
        return d


# XXX: Only used for debugging with SoapUI:
# twistd web --class=vumi.transports.parlayx_ussd.server.Root --port=9080
class Root(Resource):
    def getChild(self, path, request):
        from twisted.internet.defer import succeed
        noop = lambda *a, **kw: succeed(None)
        if request.postpath == ['services', 'USSDNotification']:
            return USSDNotificationService(noop, noop)
        return None


__all__ = [
    'USSDMessage',
    'USSDNotificationService']
