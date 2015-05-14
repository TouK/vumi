from functools import partial

from twisted.internet.defer import inlineCallbacks, succeed, fail

from vumi.tests.helpers import VumiTestCase
from vumi.transports.failures import PermanentFailure
from vumi.transports.parlayx_ussd import ParlayXUSSDTransport

from vumi.transports.parlayx_ussd.client import PolicyException, ServiceException
from vumi.transports.parlayx.soaputil import perform_soap_request
from vumi.transports.parlayx_ussd.tests.utils import (
    create_ussd_reception_element)
from vumi.transports.tests.helpers import TransportHelper


class MockParlayXClient(object):
    """
    A mock ``ParlayXClient`` that doesn't involve real HTTP requests but
    instead uses canned responses.
    """
    def __init__(self, start_ussd_notification=None, stop_ussd_notification=None,
                 send_ussd=None):
        if start_ussd_notification is None:
            start_ussd_notification = partial(succeed, None)
        if stop_ussd_notification is None:
            stop_ussd_notification = partial(succeed, None)
        if send_ussd is None:
            send_ussd = partial(succeed, 'request_message_id')

        self.responses = {
            'start_ussd_notification': start_ussd_notification,
            'stop_ussd_notification': stop_ussd_notification,
            'send_ussd': send_ussd}
        self.calls = []

    def _invoke_response(self, name, args):
        """
        Invoke the canned response for the method name ``name`` and log the
        invocation.
        """
        self.calls.append((name, args))
        return self.responses[name]()

    def start_ussd_notification(self):
        return self._invoke_response('start_ussd_notification', [])

    def stop_ussd_notification(self):
        return self._invoke_response('stop_ussd_notification', [])

    def send_ussd(self, to_addr, content, linkid, message_id):
        return self._invoke_response(
            'send_ussd', [to_addr, content, linkid, message_id])


class TestParlayXUSSDTransport(VumiTestCase):
    """
    Tests for `vumi.transports.parlayx_ussd.ParlayXUSSDTransport`.
    """

    @inlineCallbacks
    def setUp(self):
        # TODO: Get rid of this hardcoded port number.
        self.port = 19999
        config = {
            'web_notification_path': '/hello',
            'web_notification_port': self.port,
            'notification_endpoint_uri': 'endpoint_uri',
            'short_code': '54321',
            'remote_send_uri': 'send_uri',
            'remote_notification_uri': 'notification_uri',
        }
        self.tx_helper = self.add_helper(TransportHelper(ParlayXUSSDTransport))
        self.uri = 'http://127.0.0.1:%s%s' % (
            self.port, config['web_notification_path'])

        def _create_client(transport, config):
            return MockParlayXClient()
        self.patch(
            self.tx_helper.transport_class, '_create_client',
            _create_client)
        self.transport = yield self.tx_helper.get_transport(
            config, start=False)

    @inlineCallbacks
    def test_ack(self):
        """
        Basic message delivery.
        """
        yield self.transport.startWorker()
        msg = yield self.tx_helper.make_dispatch_outbound("hi")
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

        client = self.transport._parlayx_client
        self.assertEqual(1, len(client.calls))
        linkid = client.calls[0][1][3]
        self.assertIdentical(None, linkid)

    @inlineCallbacks
    def test_ack_linkid(self):
        """
        Basic message delivery uses stored ``linkid`` from transport metadata
        if available.
        """
        yield self.transport.startWorker()
        msg = yield self.tx_helper.make_dispatch_outbound(
            "hi", transport_metadata={'linkid': 'linkid'})
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

        client = self.transport._parlayx_client
        self.assertEqual(1, len(client.calls))
        linkid = client.calls[0][1][3]
        self.assertEqual('linkid', linkid)

    @inlineCallbacks
    def test_nack(self):
        """
        Exceptions raised in an outbound message handler result in the message
        delivery failing, and a failure event being logged.
        """
        def _create_client(transport, config):
            return MockParlayXClient(
                send_ussd=partial(fail, ValueError('failed')))
        self.patch(
            self.tx_helper.transport_class, '_create_client',
            _create_client)

        yield self.transport.startWorker()
        msg = yield self.tx_helper.make_dispatch_outbound("hi")
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], 'failed')

        failures = self.flushLoggedErrors(ValueError)
        # Logged once by the transport and once by Twisted for being unhandled.
        self.assertEqual(2, len(failures))

    @inlineCallbacks
    def _test_nack_permanent(self, expected_exception):
        """
        The expected exception, when raised in an outbound message handler,
        results in a `PermanentFailure` and is logged along with the original
        exception.
        """
        def _create_client(transport, config):
            return MockParlayXClient(
                send_ussd=partial(
                    fail, expected_exception('soapenv:Client', 'failed')))
        self.patch(
            self.tx_helper.transport_class, '_create_client',
            _create_client)

        yield self.transport.startWorker()
        msg = yield self.tx_helper.make_dispatch_outbound("hi")
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], 'failed')

        failures = self.flushLoggedErrors(expected_exception, PermanentFailure)
        self.assertEqual(2, len(failures))

    def test_nack_service_exception(self):
        """
        When `ServiceException` is raised in an outbound message handler, it
        results in a `PermanentFailure` exception.
        """
        return self._test_nack_permanent(ServiceException)

    def test_nack_policy_exception(self):
        """
        When `PolicyException` is raised in an outbound message handler, it
        results in a `PermanentFailure` exception.
        """
        return self._test_nack_permanent(PolicyException)

    @inlineCallbacks
    def test_receive_ussd(self):
        """
        When a text message is submitted to the Vumi ParlayX
        ``notifyUssdReception`` SOAP endpoint, a message is
        published containing the message identifier, message content, from
        address and to address that accurately match what was submitted.
        """
        yield self.transport.startWorker()
        body = create_ussd_reception_element(
            '0', '123456', '1', '*909*100#', '27117654321', '909')
        yield perform_soap_request(self.uri, '', body)
        [msg] = self.tx_helper.get_dispatched_inbound()
        # log.debug("received inbound ussd message %")
        self.assertEqual(
            ( 'message', '27117654321', '54321'),
            (msg['content'], msg['from_addr'],
             msg['to_addr']))

