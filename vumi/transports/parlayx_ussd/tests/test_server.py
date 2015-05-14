
from StringIO import StringIO

from twisted.trial.unittest import TestCase
from twisted.web import http
from twisted.web.test.requesthelper import DummyRequest

from vumi.transports.parlayx.server import ( PARLAYX_COMMON_NS )

from vumi.transports.parlayx_ussd.server import ( USSDNotificationService)
from vumi.transports.parlayx.soaputil import SoapFault, SOAP_ENV, soap_envelope
from vumi.transports.parlayx.xmlutil import (
    ParseError, LocalNamespace as L, tostring, fromstring, element_to_dict)
from vumi.transports.parlayx_ussd.tests.utils import (
    create_ussd_reception_element)




# class USSDMessageTests(TestCase):
#     """
#     Tests for `vumi.transports.parlayx_ussd.server.USSDMessage`.
#     """
#     def test_from_element(self):
#         """
#         `USSDMessage.from_element` parses a ParlayX ``USSDMessage`` complex type,
#         with an ISO8601 timestamp, into an `USSDMessage` instance.
#         """
#         timestamp = datetime(
#             2013, 6, 12, 13, 15, 0, tzinfo=iso8601.iso8601.Utc())
#         msg = USSDMessage.from_element(
#             NOTIFICATION_NS.message(
#                 L.message('message'),
#                 L.senderAddress('tel:27117654321'),
#                 L.ussdServiceActivationNumber('54321'),
#                 L.dateTime('2013-06-12T13:15:00')))
#         self.assertEqual(
#             ('message', '+27117654321', '54321', timestamp),
#             (msg.message, msg.sender_address, msg.service_activation_number,
#              msg.timestamp))
#
#     def test_from_element_missing_timestamp(self):
#         """
#         `USSDMessage.from_element` parses a ParlayX ``USSDMessage`` complex type,
#         without a timestamp, into an `USSDMessage` instance.
#         """
#         msg = USSDMessage.from_element(
#             NOTIFICATION_NS.message(
#                 L.message('message'),
#                 L.senderAddress('tel:27117654321'),
#                 L.ussdServiceActivationNumber('54321')))
#         self.assertEqual(
#             ('message', '+27117654321', '54321', None),
#             (msg.message, msg.sender_address, msg.service_activation_number,
#              msg.timestamp))


class USSDNotificationServiceTests(TestCase):
    """
    Tests for `vumi.transports.parlayx_ussd.server.USSDNotificationService`.
    """
    def test_process_empty(self):
        """
        `USSDNotificationService.process` raises `SoapFault` if there are no
        actionable child elements in the request body.
        """
        service = USSDNotificationService(None, None)
        exc = self.assertRaises(SoapFault,
            service.process, None, L.root())
        self.assertEqual(
            ('soapenv:Client', 'No actionable items'),
            (exc.code, str(exc)))

    def test_process_unknown(self):
        """
        `USSDNotificationService.process` invokes
        `USSDNotificationService.process_unknown`, for handling otherwise
        unknown requests, which raises `SoapFault`.
        """
        service = USSDNotificationService(None, None)
        exc = self.assertRaises(SoapFault,
            service.process, None, L.root(L.WhatIsThis))
        self.assertEqual(
            ('soapenv:Server', 'No handler for WhatIsThis'),
            (exc.code, str(exc)))

    def test_process_notifyUssdReception(self):
        """
        `USSDNotificationService.process_notifyUssdReception` invokes the
        message delivery callback with the correlator (message identifier) and
        a `USSDMessage` instance containing the details of the delivered
        message.
        """
        def callback(*a):
            self.callbacks.append(a)
        self.callbacks = []
        service = USSDNotificationService(callback, None)
        body = SOAP_ENV.Body(
                        create_ussd_reception_element(
                            '0', '123456', '1', '*909*100#', '27117654321', '909'))
        self.successResultOf(service.process(None,
            body,
            SOAP_ENV.Header(
                PARLAYX_COMMON_NS.NotifySOAPHeader(
                    PARLAYX_COMMON_NS.linkid('linkid')))))

        self.assertEqual(1, len(self.callbacks))
        correlator, linkid, msg = self.callbacks[0]
        self.assertEqual(
            ('123456', 'linkid', '*909*100#', '+27117654321', '909'),
            (correlator, linkid, msg.ussdString, msg.msisdn, msg.serviceCode))


    def test_render(self):
        """
        `USSDNotificationService.render_POST` parses a SOAP request and
        dispatches it to `USSDNotificationService.process` for processing.
        """
        service = USSDNotificationService(None, None)
        service.process = lambda *a, **kw: L.done()
        request = DummyRequest([])
        request.content = StringIO(tostring(soap_envelope('hello')))
        d = request.notifyFinish()
        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.OK, request.responseCode)
        self.assertEqual(
            {str(SOAP_ENV.Envelope): {
                str(SOAP_ENV.Body): {
                    'done': None}}},
            element_to_dict(fromstring(''.join(request.written))))

    def test_render_soap_fault(self):
        """
        `USSDNotificationService.render_POST` logs any exceptions that occur
        during processing and writes a SOAP fault back to the request. If the
        logged exception is a `SoapFault` its ``to_element`` method is invoked
        to serialize the fault.
        """
        service = USSDNotificationService(None, None)
        service.process = lambda *a, **kw: L.done()
        request = DummyRequest([])
        request.content = StringIO(tostring(L.hello()))
        d = request.notifyFinish()

        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.INTERNAL_SERVER_ERROR, request.responseCode)
        failures = self.flushLoggedErrors(SoapFault)
        self.assertEqual(1, len(failures))
        self.assertEqual(
            {str(SOAP_ENV.Envelope): {
                str(SOAP_ENV.Body): {
                    str(SOAP_ENV.Fault): {
                        'faultcode': 'soapenv:Client',
                        'faultstring': 'Malformed SOAP request'}}}},
            element_to_dict(fromstring(''.join(request.written))))

    def test_render_exceptions(self):
        """
        `USSDNotificationService.render_POST` logs any exceptions that occur
        during processing and writes a SOAP fault back to the request.
        """
        def process(*a, **kw):
            raise ValueError('What is this')
        service = USSDNotificationService(None, None)
        service.process = process
        request = DummyRequest([])
        request.content = StringIO(tostring(soap_envelope('hello')))
        d = request.notifyFinish()

        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.INTERNAL_SERVER_ERROR, request.responseCode)
        failures = self.flushLoggedErrors(ValueError)
        self.assertEqual(1, len(failures))
        self.assertEqual(
            {str(SOAP_ENV.Envelope): {
                str(SOAP_ENV.Body): {
                    str(SOAP_ENV.Fault): {
                        'faultcode': 'soapenv:Server',
                        'faultstring': 'What is this'}}}},
            element_to_dict(fromstring(''.join(request.written))))

    def test_render_invalid_xml(self):
        """
        `USSDNotificationService.render_POST` does not accept invalid XML body
        content.
        """
        service = USSDNotificationService(None, None)
        request = DummyRequest([])
        request.content = StringIO('sup')
        d = request.notifyFinish()

        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.INTERNAL_SERVER_ERROR, request.responseCode)
        failures = self.flushLoggedErrors(ParseError)
        self.assertEqual(1, len(failures))
