# -*- test-case-name: vumi.transports.parlayx_ussd.tests.test_parlayx -*-
import uuid

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi import log
from vumi.config import ConfigText, ConfigInt, ConfigBool, ConfigDict
from vumi.transports.base import Transport
from vumi.transports.failures import TemporaryFailure, PermanentFailure
from vumi.transports.parlayx_ussd.client import (
    ParlayXUSSDClient, ServiceException, PolicyException)
from vumi.transports.parlayx_ussd.server import USSDNotificationService
from vumi.transports.parlayx.soaputil import SoapFault
from vumi.components.session import SessionManager
from vumi.message import TransportUserMessage

class ParlayXUSSDTransportConfig(Transport.CONFIG_CLASS):
    web_notification_path = ConfigText(
        'Path to listen for delivery and receipt notifications on',
        static=True)
    web_notification_port = ConfigInt(
        'Port to listen for delivery and receipt notifications on',
        default=0, static=True)
    notification_endpoint_uri = ConfigText(
        'URI of the ParlayX USSDNotificationService in Vumi', static=True)
    short_code = ConfigText(
        'Service activation number or short code to receive deliveries for',
        static=True)
    remote_send_uri = ConfigText(
        'URI of the remote ParlayX SendUSSDService', static=True)
    remote_notification_uri = ConfigText(
        'URI of the remote ParlayX USSDNotificationService', static=True)
    start_notifications = ConfigBool(
        'Start (and stop) the ParlayX notification service?', static=True)
    service_provider_service_id = ConfigText(
        'Provisioned service provider service identifier', static=True)
    service_provider_id = ConfigText(
        'Provisioned service provider identifier/username', static=True)
    service_provider_password = ConfigText(
        'Provisioned service provider password', static=True)
    timeout_period = ConfigInt(
        "How long (in seconds) after sending an enquire link request the "
        "client should wait for a response before timing out. NOTE: The "
        "timeout period should not be longer than the enquire link interval",
        default=30, static=True)
    user_termination_response = ConfigText(
        "Response given back to the user if the user terminated the session.",
        default='Session Ended', static=True)
    redis_manager = ConfigDict(
        "Parameters to connect to Redis with",
        default={}, static=True)
    session_timeout_period = ConfigInt(
        "Max length (in seconds) of a USSD session",
        default=600, static=True)


class ParlayXUSSDTransport(Transport):
    """ParlayX USSD transport.

    ParlayX is a defunkt standard web service API for telephone networks.
    See http://en.wikipedia.org/wiki/Parlay_X for an overview.

    .. warning::

       This transport has not been tested against another ParlayX
       implementation. If you use it, please provide feedback to the
       Vumi development team on your experiences.
    """

    CONFIG_CLASS = ParlayXUSSDTransportConfig
    transport_type = 'ussd'

    # The encoding we use internally
    ENCODING = 'UTF-8'

    REQUIRED_METADATA_FIELDS = set(['session_id', 'clientId'])


    def _create_client(self, config):
        """
        Create a `ParlayXClient` instance.
        """
        return ParlayXUSSDClient(
            service_provider_service_id=config.service_provider_service_id,
            service_provider_id=config.service_provider_id,
            service_provider_password=config.service_provider_password,
            short_code=config.short_code,
            endpoint=config.notification_endpoint_uri,
            send_uri=config.remote_send_uri,
            notification_uri=config.remote_notification_uri)

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        log.info('Starting ParlayX USSD transport: %s' % (self.transport_name,))
        self.user_termination_response = config.user_termination_response

        r_prefix = "vumi.transports.parlayx_ussd:%s" % self.transport_name
        self.session_manager = yield SessionManager.from_redis_config(
           config.redis_manager, r_prefix,
           config.session_timeout_period)

        self.web_resource = yield self.start_web_resources(
            [(USSDNotificationService(self.handle_raw_inbound_message,
                                     self.publish_delivery_report),
              config.web_notification_path)],
            config.web_notification_port)
        self._parlayx_client = self._create_client(config)
        if config.start_notifications:
            yield self._parlayx_client.start_ussd_notification()

    @inlineCallbacks
    def teardown_transport(self):
        config = self.get_static_config()
        log.info('Stopping ParlayX USSD transport: %s' % (self.transport_name,))
        yield self.web_resource.loseConnection()
        if config.start_notifications:
            yield self._parlayx_client.stop_ussd_notification()

    @staticmethod
    def determine_session_event(msg_type):
        if msg_type == '0':
            return TransportUserMessage.SESSION_NEW
        if msg_type == '1':
            return TransportUserMessage.SESSION_RESUME
        return TransportUserMessage.SESSION_CLOSE

    def handle_outbound_message(self, message):
        """
        Send a text message via the ParlayX USSD client.
        """
        log.info('Sending USSD via ParlayX: %r' % (message.to_json(),))
        session_event = message['session_event']
        parlayx_ussd = message.get('transport_metadata', {}).get('parlayx_ussd', {})
        senderCB = parlayx_ussd[1]
        # msgType = parlayx_ussd[0]
        # ussdOpType = parlayx_ussd[3]
        serviceCode = parlayx_ussd[5]
        codeScheme = parlayx_ussd[6]
        if session_event == 'close':
            msgType = '2'
            ussdOpType = '3'
        else:
            msgType = '1'
            ussdOpType = '1'

        d = self._parlayx_client.send_ussd(
            message['to_addr'],
            message['content'],
            senderCB, msgType, ussdOpType, serviceCode, codeScheme)
        d.addErrback(self.handle_outbound_message_failure, message)
        d.addCallback(
            lambda requestIdentifier: self.publish_ack(
                message['message_id'], requestIdentifier))
        return d

    @inlineCallbacks
    def handle_outbound_message_failure(self, f, message):
        """
        Handle outbound message failures.

        `ServiceException`, `PolicyException` and client-class SOAP faults
        result in `PermanentFailure` being raised; server-class SOAP faults
        instances result in `TemporaryFailure` being raised; and other failures
        are passed through.
        """
        log.error(f, 'Sending USSD failure on ParlayX: %r' % (
            self.transport_name,))

        if not f.check(ServiceException, PolicyException):
            if f.check(SoapFault):
                # We'll give server-class unknown SOAP faults the benefit of
                # the doubt as far as temporary failures go.
                if f.value.code.endswith('Server'):
                    raise TemporaryFailure(f)

        yield self.publish_nack(message['message_id'], f.getErrorMessage())
        if f.check(SoapFault):
            # We've ruled out unknown SOAP faults, so this must be a permanent
            # failure.
            raise PermanentFailure(f)
        returnValue(f)

    @inlineCallbacks
    def handle_raw_inbound_message(self, session_id, linkid, inbound_message):
        """
        Handle incoming text messages from `USSDNotificationService` callbacks.
        """
        log.info('Receiving USSD via ParlayX: %r: %r' % (
            session_id, inbound_message,))

        session_event = self.determine_session_event(inbound_message.msgType)

        # send close message
        #
        # if session_event == TransportUserMessage.SESSION_CLOSE:
        #     self.factory.client.send_data_response(
        #         session_id=inbound_message.senderCB,
        #         request_id=linkid,
        #         star_code=inbound_message.ussdString,
        #         client_id=params['clientId'],
        #         msisdn=from_addr,
        #         user_data=self.user_termination_response,
        #         end_session=True)

  # For the first message of a session, the `user_data` field is the ussd
        # code. For subsequent messages, 'user_data' is the user's content.  We
        # need to keep track of the ussd code we get in in the first session
        # message so we can link the correct `to_addr` to subsequent messages
        if session_event == TransportUserMessage.SESSION_NEW:
            # Set the content to none if this the start of the session.
            # Prevents this inbound message being mistaken as a user message.
            content = None

            to_addr = inbound_message.serviceCode
            session = yield self.session_manager.create_session(
                session_id, ussd_code=to_addr)
        else:
            session = yield self.session_manager.load_session(session_id)
            to_addr = session['ussd_code']
            content = inbound_message.ussdString

    
        #  change to json fields
        #    transport_metadata={'parlayx_ussd': inbound_message})

        yield self.publish_message(
            content=content,
            to_addr=to_addr,
            from_addr=inbound_message.msisdn,
            provider='parlayx_ussd',
            session_event=session_event,
            transport_type=self.transport_type,
            transport_metadata={'parlayx_ussd': inbound_message})
