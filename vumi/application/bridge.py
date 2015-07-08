# -*- test-case-name: vumi.application.tests.test_bridge -*-

from base64 import b64encode

from twisted.python import log
from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi.application.base import ApplicationWorker
from vumi.utils import http_request_full
from vumi.errors import VumiError
from vumi.config import ConfigText, ConfigUrl

from datetime import datetime


class BridgeError(VumiError):
    pass


class BridgeConfig(ApplicationWorker.CONFIG_CLASS):

    # TODO: Make these less static?
    url = ConfigUrl(
        "URL to submit incoming message to.", required=True, static=True)
    http_method = ConfigText(
        "HTTP method for submitting messages.", default='POST', static=True)
    content_type = ConfigText(
        "HTTP Content-Type.", default='application/json', static=True)

    auth_method = ConfigText(
        "HTTP authentication method.", default='basic', static=True)

    username = ConfigText("Username for HTTP authentication.", default='')
    password = ConfigText("Password for HTTP authentication.", default='')


class BridgeApplication(ApplicationWorker):
    CONFIG_CLASS = BridgeConfig

    reply_header = 'X-Vumi-HTTPRelay-Reply'
    reply_header_good_value = 'true'
    end_of_session_header = 'end-of-session'

    def validate_config(self):
        self.supported_auth_methods = {
            'basic': self.generate_basic_auth_headers,
        }
        config = self.get_static_config()
        if config.auth_method not in self.supported_auth_methods:
            raise BridgeError(
                'HTTP Authentication method %s not supported' %
                (repr(config.auth_method,)))

    def generate_basic_auth_headers(self, username, password):
        credentials = ':'.join([username, password])
        auth_string = b64encode(credentials.encode('utf-8'))
        return {
            'Authorization': ['Basic %s' % (auth_string,)]
        }

    def get_auth_headers(self, config):
        if config.username:
            handler = self.supported_auth_methods.get(config.auth_method)
            return handler(config.username, config.password)
        return {}

    def get_content_type_headers(self, config):
        config_content_type = config.content_type
        if config_content_type:
            return {'Content-Type': config_content_type}
        return {}

    def _handle_bad_response(self, failure, message):
        now = datetime.now()
        seconds = now.second + 60*now.minute + 360*now.hour
        log.msg("Darkness [%s] is comming: %s" %
                (seconds, failure.getErrorMessage()))
        self.reply_to(message, "Error [%s]" % seconds, continue_session=False)

    def _handle_good_response(self, response, message, config):
        headers = response.headers
        if response.code == http.OK:
            if headers.hasHeader(self.reply_header):
                raw_reply_headers = headers.getRawHeaders(self.reply_header)
                content = response.delivered_body.strip()
                if (raw_reply_headers[0].lower() ==
                   self.reply_header_good_value) and content:
                    continue_session = True
                    if headers.hasHeader(self.end_of_session_header):
                        raw_end_of_session_headers = \
                            headers.getRawHeaders(self.end_of_session_header)
                        if raw_end_of_session_headers[0].lower() == 'true':
                            continue_session = False
                    self.reply_to(message, content,
                                  continue_session=continue_session)
        else:
            log.err('%s responded with %s' % (config.url.geturl(),
                                              response.code))

    @inlineCallbacks
    def consume_user_message(self, message):
        config = yield self.get_config(message)
        headers = self.get_auth_headers(config)
        headers.update(self.get_content_type_headers(config))

        response = http_request_full(config.url.geturl(), message.to_json(),
                                     headers, config.http_method)

        response.addCallback(lambda response:
                             self._handle_good_response(
                                 response, message, config))
        response.addErrback(lambda fail:
                            self._handle_bad_response(fail, message))

        yield response
