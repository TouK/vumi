from datetime import datetime
from functools import partial

from twisted.internet.defer import succeed
from twisted.trial.unittest import TestCase
from twisted.web import http

from vumi.transports.parlayx.client import (
    PARLAYX_COMMON_NS, PARLAYX_HEAD_NS )

from vumi.transports.parlayx_ussd.client import (
    NOTIFICATION_MANAGER_NS, SEND_NS,
    ServiceException,
    PolicyException, ParlayXUSSDClient )
from vumi.transports.parlayx.soaputil import (
    perform_soap_request, unwrap_soap_envelope, soap_fault)
from vumi.transports.parlayx.xmlutil import (
    LocalNamespace as L, elemfind, fromstring, element_to_dict)
from vumi.transports.parlayx_ussd.tests.utils import (
    MockResponse, _FailureResultOfMixin)


class ParlayXClientTests(_FailureResultOfMixin, TestCase):
    """
    Tests for `vumi.transports.parlayx_ussd.client.ParlayXClient`.
    """
    def setUp(self):
        self.requests = []

    def _http_request_full(self, response, uri, body, headers):
        """
        A mock for `vumi.utils.http_request_full`.

        Store an HTTP request's information and return a canned response.
        """
        self.requests.append((uri, body, headers))
        return succeed(response)

    def _perform_soap_request(self, response, *a, **kw):
        """
        Perform a SOAP request with a canned response.
        """
        return perform_soap_request(
            http_request_full=partial(
                self._http_request_full, response), *a, **kw)

    def _make_client(self, response=''):
        """
        Create a `ParlayXClient` instance that uses a stubbed
        `perform_soap_request` function.
        """
        return ParlayXUSSDClient(
            'service_id', 'user', 'password', 'short', 'endpoint', 'send',
            'notification',
            perform_soap_request=partial(self._perform_soap_request, response))

    def test_start_ussd_notification(self):
        """
        `ParlayXClient.start_ussd_notification` performs a SOAP request to the
        remote ParlayX notification endpoint indicating where delivery and
        receipt notifications for a particular service activation number can be
        delivered.
        """
        client = self._make_client(
            MockResponse.build(
                http.OK, NOTIFICATION_MANAGER_NS.startUSSDNotificationResponse))
        client._now = partial(datetime, 2013, 6, 18, 10, 59, 33)
        self.successResultOf(client.start_ussd_notification())
        self.assertEqual(1, len(self.requests))
        self.assertEqual('notification', self.requests[0][0])
        body, header = unwrap_soap_envelope(fromstring(self.requests[0][1]))
        self.assertEqual(
            {str(NOTIFICATION_MANAGER_NS.startUSSDNotification): {
                str(NOTIFICATION_MANAGER_NS.reference): {
                    'correlator': client._service_correlator,
                    'endpoint': 'endpoint',
                    'interfaceName': 'notifyUssdReception'},
                str(NOTIFICATION_MANAGER_NS.ussdServiceActivationNumber):
                    'short'}},
            element_to_dict(
                elemfind(body, NOTIFICATION_MANAGER_NS.startUSSDNotification)))
        self.assertEqual(
            {str(PARLAYX_HEAD_NS.RequestSOAPHeader): {
                str(PARLAYX_HEAD_NS.serviceId): 'service_id',
                str(PARLAYX_HEAD_NS.spId): 'user',
                str(PARLAYX_HEAD_NS.spPassword):
                    '1f2e67e642b16f6623459fa76dc3894f',
                str(PARLAYX_HEAD_NS.timeStamp): '20130618105933'}},
            element_to_dict(
                elemfind(header, PARLAYX_HEAD_NS.RequestSOAPHeader)))

    def test_start_ussd_notification_service_fault(self):
        """
        `ParlayXClient.start_ussd_notification` expects `ServiceExceptionDetail`
        fault details in SOAP requests that fail for remote service-related
        reasons.
        """
        detail = PARLAYX_COMMON_NS.ServiceExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.start_ussd_notification(), ServiceException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))

    def test_stop_ussd_notification(self):
        """
        `ParlayXClient.stop_ussd_notification` performs a SOAP request to the
        remote ParlayX notification endpoint indicating that delivery and
        receipt notifications for a particular service activation number can be
        deactivated.
        """
        client = self._make_client(
            MockResponse.build(
                http.OK, NOTIFICATION_MANAGER_NS.stopUSSDNotificationResponse))
        client._now = partial(datetime, 2013, 6, 18, 10, 59, 33)
        self.successResultOf(client.stop_ussd_notification())
        self.assertEqual(1, len(self.requests))
        self.assertEqual('notification', self.requests[0][0])
        body, header = unwrap_soap_envelope(fromstring(self.requests[0][1]))
        self.assertEqual(
            {str(NOTIFICATION_MANAGER_NS.stopUSSDNotification): {
                'correlator': client._service_correlator}},
            element_to_dict(
                elemfind(body, NOTIFICATION_MANAGER_NS.stopUSSDNotification)))
        self.assertEqual(
            {str(PARLAYX_HEAD_NS.RequestSOAPHeader): {
                str(PARLAYX_HEAD_NS.serviceId): 'service_id',
                str(PARLAYX_HEAD_NS.spId): 'user',
                str(PARLAYX_HEAD_NS.spPassword):
                    '1f2e67e642b16f6623459fa76dc3894f',
                str(PARLAYX_HEAD_NS.timeStamp): '20130618105933'}},
            element_to_dict(
                elemfind(header, PARLAYX_HEAD_NS.RequestSOAPHeader)))

    def test_stop_ussd_notification_service_fault(self):
        """
        `ParlayXClient.stop_ussd_notification` expects `ServiceExceptionDetail`
        fault details in SOAP requests that fail for remote service-related
        reasons.
        """
        detail = PARLAYX_COMMON_NS.ServiceExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.stop_ussd_notification(), ServiceException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))

    def test_send_ussd(self):
        """
        `ParlayXClient.send_ussd` performs a SOAP request to the
        remote ParlayX send endpoint to deliver a message via USSD.
        """
        client = self._make_client(
            MockResponse.build(
                http.OK, SEND_NS.sendUSSDResponse(SEND_NS.result('reference'))))
        client._now = partial(datetime, 2013, 6, 18, 10, 59, 33)
        response = self.successResultOf(
            client.send_ussd('+27117654321', 'content', 'senderCB', 'msgType', 'ussdOpType', 'serviceCode', 'codeScheme'))
        self.assertEqual('reference', response)
        self.assertEqual(1, len(self.requests))
        self.assertEqual('send', self.requests[0][0])

        body, header = unwrap_soap_envelope(fromstring(self.requests[0][1]))
        self.assertEqual(
            {str(SEND_NS.sendUssd): {
                str(SEND_NS.msIsdn): 'tel:27117654321',
                str(SEND_NS.ussdString): 'content',
                str(SEND_NS.msgType): 'msgType',
                str(SEND_NS.senderCB): 'senderCB',
                str(SEND_NS.receiveCB): 'senderCB',
                str(SEND_NS.ussdOpType): 'ussdOpType',
                str(SEND_NS.serviceCode): 'serviceCode',
                str(SEND_NS.codeScheme): 'codeScheme'}},
            element_to_dict(elemfind(body, SEND_NS.sendUssd)))
        self.assertEqual(
            {str(PARLAYX_HEAD_NS.RequestSOAPHeader): {
                str(PARLAYX_HEAD_NS.serviceId): 'service_id',
                str(PARLAYX_HEAD_NS.spId): 'user',
                str(PARLAYX_HEAD_NS.spPassword):
                    '1f2e67e642b16f6623459fa76dc3894f',
                str(PARLAYX_HEAD_NS.timeStamp): '20130618105933',
                str(PARLAYX_HEAD_NS.OA): '+27117654321'}},
            element_to_dict(
                elemfind(header, PARLAYX_HEAD_NS.RequestSOAPHeader)))

    def test_send_ussd_service_fault(self):
        """
        `ParlayXClient.send_ussd` expects `ServiceExceptionDetail` fault details
        in SOAP requests that fail for remote service-related reasons.
        """
        detail = PARLAYX_COMMON_NS.ServiceExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.send_ussd('+27117654321', 'content', 'senderCB', 'msgType', 'ussdOpType', 'serviceCode', 'codeScheme')            ,
            ServiceException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))

    def test_send_ussd_policy_fault(self):
        """
        `ParlayXClient.send_ussd` expects `PolicyExceptionDetail` fault details
        in SOAP requests that fail for remote policy-related reasons.
        """
        detail = PARLAYX_COMMON_NS.PolicyExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.send_ussd('+27117654321', 'content', 'senderCB', 'msgType', 'ussdOpType', 'serviceCode', 'codeScheme'),
            PolicyException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))
