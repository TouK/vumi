# -*- test-case-name: vumi.transports.parlayx_ussd.tests.test_client -*-
import uuid
from datetime import datetime

from vumi.transports.parlayx.soaputil import perform_soap_request
from vumi.transports.parlayx.xmlutil import (
    gettext, Namespace, LocalNamespace as L)

from vumi.transports.parlayx.client import ( format_address, format_timestamp, make_password,
                                             ServiceException,
                                            PolicyException, PARLAYX_HEAD_NS)

SEND_NS = Namespace(
    'http://www.csapi.org/schema/parlayx/ussd/send/v1_0/local', 's')
NOTIFICATION_MANAGER_NS = Namespace(
    'http://www.csapi.org/schema/osg/ussd/notification_manager/v1_0/local',
    'nm')

class ParlayXUSSDClient(object):
    """
    ParlayX SOAP client.

    :ivar _service_correlator:
        A unique identifier for this service, used when registering and
        deregistering for USSD notifications.
    """
    def __init__(self, service_provider_service_id, service_provider_id,
                 service_provider_password, short_code, endpoint, send_uri,
                 notification_uri, perform_soap_request=perform_soap_request):
        """
        :param service_provider_service_id:
            Provisioned service provider service identifier.
        :param service_provider_id:
            Provisioned service provider identifier/username.
        :param service_provider_password:
            Provisioned service provider password.
        :param short_code:
            USSD shortcode or service activation number.
        :param endpoint:
            URI to which the remote ParlayX service will deliver notification
            messages.
        :param send_uri:
            URI for the ParlayX ``SendUSSDService`` SOAP endpoint.
        :param notification_uri:
            URI for the ParlayX ``USSDNotificationService`` SOAP endpoint.
        """
        self.service_provider_service_id = service_provider_service_id
        self.service_provider_id = service_provider_id
        self.service_provider_password = service_provider_password
        self.short_code = short_code
        self.endpoint = endpoint
        self.send_uri = send_uri
        self.notification_uri = notification_uri
        self.perform_soap_request = perform_soap_request
        self._service_correlator = uuid.uuid4().hex

    def _now(self):
        """
        The current date and time.
        """
        return datetime.now()

    def _make_header(self, service_subscription_address=None, linkid=None):
        """
        Create a ``RequestSOAPHeader`` element.

        :param service_subscription_address:
            Service subscription address for the ``OA`` header field, this
            field is omitted if its value is ``None``.
        """
        NS = PARLAYX_HEAD_NS
        other = []
        timestamp = format_timestamp(self._now())
        if service_subscription_address is not None:
            other.append(NS.OA(service_subscription_address))
        if linkid is not None:
            other.append(NS.linkid(linkid))
        return NS.RequestSOAPHeader(
            NS.spId(self.service_provider_id),
            NS.spPassword(
                make_password(
                    self.service_provider_id,
                    self.service_provider_password,
                    timestamp)),
            NS.serviceId(self.service_provider_service_id),
            NS.timeStamp(timestamp),
            *other)

    def start_ussd_notification(self):
        """
        Register a notification delivery endpoint with the remote ParlayX
        service.
        """
        body = NOTIFICATION_MANAGER_NS.startUSSDNotification(
            NOTIFICATION_MANAGER_NS.reference(
                L.endpoint(self.endpoint),
                L.interfaceName('notifyUssdReception'),
                L.correlator(self._service_correlator)),
            NOTIFICATION_MANAGER_NS.ussdServiceActivationNumber(
                self.short_code))
        header = self._make_header()
        return self.perform_soap_request(
            uri=self.notification_uri,
            action='',
            body=body,
            header=header,
            expected_faults=[ServiceException])

    def stop_ussd_notification(self):
        """
        Deregister notification delivery with the remote ParlayX service.
        """
        body = NOTIFICATION_MANAGER_NS.stopUSSDNotification(
            L.correlator(self._service_correlator))
        header = self._make_header()
        return self.perform_soap_request(
            uri=self.notification_uri,
            action='',
            body=body,
            header=header,
            expected_faults=[ServiceException])

    def send_ussd(self, to_addr, content, senderCB, msgType, ussdOpType, serviceCode,
                  codeScheme, linkid=None):
        """
        Send an USSD.
        """
        def _extractRequestIdentifier((body, header)):
            return gettext(body, './/' + str(SEND_NS.result), default='')

        body = SEND_NS.sendUssd(
            SEND_NS.msgType(msgType),
            SEND_NS.senderCB(senderCB),
            SEND_NS.receiveCB(senderCB),
            SEND_NS.ussdOpType(ussdOpType),
            SEND_NS.msIsdn(format_address(to_addr)),
            SEND_NS.serviceCode(serviceCode),
            SEND_NS.codeScheme(codeScheme),
            SEND_NS.ussdString(content))
        header = self._make_header(
            service_subscription_address=to_addr,
            linkid=linkid)
        d = self.perform_soap_request(
            uri=self.send_uri,
            action='',
            body=body,
            header=header,
            expected_faults=[PolicyException, ServiceException])
        d.addCallback(_extractRequestIdentifier)
        return d

    def send_ussd_abort(self, to_addr, content, message_id, linkid=None):
        """
        Send an USSD.
        """
        def _extractRequestIdentifier((body, header)):
            return gettext(body, './/' + str(SEND_NS.result), default='')

        body = SEND_NS.sendUssdAbort(
            SEND_NS.addresses(format_address(to_addr)),
            SEND_NS.message(content),
            SEND_NS.receiptRequest(
                L.endpoint(self.endpoint),
                L.interfaceName(u'USSDNotification'),
                L.correlator(message_id)))
        header = self._make_header(
            service_subscription_address=to_addr,
            linkid=linkid)
        d = self.perform_soap_request(
            uri=self.send_uri,
            action='',
            body=body,
            header=header,
            expected_faults=[PolicyException, ServiceException])
        d.addCallback(_extractRequestIdentifier)
        return d

__all__ = ['ParlayXUSSDClient']
