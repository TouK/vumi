# -*- test-case-name: vumi.transports.mtech_ussd.tests.test_mtech_ussd -*-

from xml.etree import ElementTree as ET

import redis
from vumi import log

from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport
from vumi.application.session import SessionManager


class MtechUssdTransport(HttpRpcTransport):
    """MTECH USSD transport.

    Configuration parameters:

    :param str transport_name:
        The name this transport instance will use to create its queues
    :param int ussd_session_timeout:
        Number of seconds before USSD session information stored in
        Redis expires. Default is 600s.
    :param str web_path:
        The HTTP path to listen on.
    :param int web_port:
        The HTTP port to listen on.

    NOTE: We currently only support free-text USSD, not menus.
          At the time of writing, vumi has no suitable message format for
          specifying USSD menus. This may change in the future.
    """

    def setup_transport(self):
        super(MtechUssdTransport, self).setup_transport()
        self.redis_config = self.config.get('redis', {})
        self.r_prefix = "mtech_ussd:%s" % self.transport_name
        session_timeout = int(self.config.get("ussd_session_timeout", 600))
        self.r_server = self.connect_to_redis()
        self.session_manager = SessionManager(
            self.r_server, self.r_prefix, max_session_length=session_timeout)

    def teardown_transport(self):
        self.session_manager.stop()
        super(MtechUssdTransport, self).teardown_transport()

    def connect_to_redis(self):
        return redis.Redis(**self.redis_config)

    def save_session(self, session_id, from_addr, to_addr):
        return self.session_manager.create_session(
            session_id, from_addr=from_addr, to_addr=to_addr)

    def handle_status_message(self, msgid, session_id):
        mur = MtechUssdResponse(session_id)
        response_body = unicode(mur).encode('utf-8')
        log.msg("Outbound message: %r" % (response_body,))
        return self.finish_request(msgid, response_body)

    def handle_raw_inbound_message(self, msgid, request):
        request_body = request.content.read()
        log.msg("Inbound message: %r" % (request_body,))
        try:
            body = ET.fromstring(request_body)
        except:
            log.warning("Error parsing request XML: %s" % (request_body,))
            return self.finish_request(msgid, "", code=400)

        # We always get this.
        session_id = body.find('session_id').text

        status_elem = body.find('status')
        if status_elem is not None:
            # We have a status message. These are all variations on "cancel".
            return self.handle_status_message(msgid, session_id)

        page_id = body.find('page_id').text

        # They sometimes send us page_id=0 in the middle of a session.
        if page_id == '0' and body.find('mobile_number') is not None:
            # This is a new session.
            session = self.save_session(
                session_id,
                from_addr=body.find('mobile_number').text,
                to_addr=body.find('gate').text)  # ???
            session_event = TransportUserMessage.SESSION_NEW
        else:
            # This is an existing session.
            session = self.session_manager.load_session(session_id)
            if 'from_addr' not in session:
                # We have a missing or broken session.
                return self.finish_request(msgid, "", code=400)
            session_event = TransportUserMessage.SESSION_RESUME

        content = body.find('data').text

        transport_metadata = {'session_id': session_id}
        self.publish_message(
                message_id=msgid,
                content=content,
                to_addr=session['to_addr'],
                from_addr=session['from_addr'],
                session_event=session_event,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata=transport_metadata,
                )

    def handle_outbound_message(self, message):
        mur = MtechUssdResponse(message['transport_metadata']['session_id'])
        mur.add_text(message['content'])
        if message['session_event'] != TransportUserMessage.SESSION_CLOSE:
            mur.add_freetext_option()
        response_body = unicode(mur).encode('utf-8')
        log.msg("Outbound message: %r" % (response_body,))
        self.finish_request(message['in_reply_to'], response_body)


class MtechUssdResponse(object):
    def __init__(self, session_id):
        self.session_id = session_id
        self.title = None
        self.text = []
        self.nav = []

    def add_title(self, title):
        self.title = title

    def add_text(self, text):
        self.text.append(text)

    def add_menu_item(self, text, option):
        self.nav.append({
                'text': text,
                'pageId': 'index%s' % (option,),
                'accesskey': option,
                })

    def add_freetext_option(self):
        self.nav.append({'text': None, 'pageId': 'indexX', 'accesskey': '*'})

    def to_xml(self):
        page = ET.fromstring('<page version="2.0" />')
        ET.SubElement(page, "session_id").text = self.session_id

        if self.title is not None:
            ET.SubElement(page, "title").text = self.title

        for text in self.text:
            lines = text.split('\n')
            div = ET.SubElement(page, "div")
            div.text = lines.pop(0)
            for line in lines:
                ET.SubElement(div, "br").tail = line

        if self.nav:
            nav = ET.SubElement(page, "navigation")
            for link in self.nav:
                ET.SubElement(
                    nav, "link", pageId=link['pageId'],
                    accesskey=link['accesskey']).text = link['text']

        # We can't have "\n" in the output at all, it seems.
        return ET.tostring(page, encoding="UTF-8").replace("\n", "")

    def __str__(self):
        return self.to_xml()