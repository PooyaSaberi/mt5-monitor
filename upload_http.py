"""Bounded HTTP transport, verified HTTPS, explicit literal-loopback HTTP only."""

import http.client
import json
import socket
import ssl
import threading

from upload_config import endpoint_parts

MAX_RESPONSE = 64 * 1024


class DeliveryFailure(Exception):
    def __init__(self, code, retryable=False, retry_after=0.):
        super().__init__(code)
        self.code, self.retryable, self.retry_after = code, retryable, retry_after


def strict_json(payload):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate key')
            result[key] = value
        return result
    def nonfinite(_):
        raise ValueError('non-finite JSON')
    return json.loads(payload, object_pairs_hook=unique, parse_constant=nonfinite)


def post(config, payload):
    if len(payload) > 512 * 1024:
        raise DeliveryFailure('request_too_large')
    parts = endpoint_parts(config.endpoint, config.local_test)
    cls = http.client.HTTPSConnection if parts.scheme == 'https' else http.client.HTTPConnection
    kwargs = {'timeout': config.timeout}
    if parts.scheme == 'https':
        kwargs['context'] = ssl.create_default_context()
    connection = cls(parts.hostname, parts.port, **kwargs)
    sockets = []
    expired = threading.Event()
    def abort():
        expired.set()
        for sock in sockets:
            try: sock.shutdown(socket.SHUT_RDWR)
            except OSError: pass
    timer = threading.Timer(config.attempt_timeout, abort)
    timer.daemon = True
    timer.start()
    try:
        connection.connect()
        sockets.append(connection.sock)
        if expired.is_set():
            raise DeliveryFailure('transport_timeout', True)
        connection.request('POST', parts.path or '/', body=payload, headers={
            'Authorization': 'Bearer '+config.token, 'Content-Type': 'application/json',
            'Connection': 'close'})
        response = connection.getresponse()
        status = response.status
        if status != 200:
            retry = status == 429 or status in (500, 502, 503, 504)
            delay = response.getheader('Retry-After', '')
            delay = min(float(delay), 300.) if delay.isdigit() and len(delay) < 8 else 0.
            raise DeliveryFailure('redirect_rejected' if 300 <= status < 400 else f'http_{status}', retry, delay)
        length = response.getheader('Content-Length')
        if length and (not length.isdigit() or len(length) > 8 or int(length) > MAX_RESPONSE):
            raise DeliveryFailure('response_too_large')
        body = response.read(MAX_RESPONSE+1)
        if len(body) > MAX_RESPONSE:
            raise DeliveryFailure('response_too_large')
        if expired.is_set():
            raise DeliveryFailure('transport_timeout', True)
        try:
            return strict_json(body)
        except (ValueError, UnicodeError, RecursionError):
            raise DeliveryFailure('invalid_acknowledgement') from None
    except DeliveryFailure:
        raise
    except ssl.SSLError:
        raise DeliveryFailure('tls_verification_failed') from None
    except (TimeoutError, socket.timeout):
        raise DeliveryFailure('transport_timeout', True) from None
    except (OSError, http.client.HTTPException):
        raise DeliveryFailure('network_error', True) from None
    finally:
        timer.cancel()
        connection.close()
