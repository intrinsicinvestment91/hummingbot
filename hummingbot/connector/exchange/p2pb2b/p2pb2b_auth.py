import hashlib
import hmac
import json
import logging
from base64 import b64encode
from collections import OrderedDict
from typing import Any, Dict, Optional

import hummingbot.connector.exchange.p2pb2b.p2pb2b_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class P2pb2bAuth(AuthBase):
    _logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        # No need to authenticate for GET requests
        if request.method == RESTMethod.POST:
            request.data = json.loads(request.data) if request.data is not None else {}
            request.data = self.add_params_to_body(request=request)
            headers = {}
            if request.headers is not None:
                headers.update(request.headers)
            headers.update(self.header_for_authentication(body=request.data or {}))
            request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. P2pb2b does not need to authenticate
        for websocket requests since only public endpoints are available.
        """
        return request  # pass-through

    def add_params_to_body(self, request: RESTRequest):
        timestamp = int(self.time_provider.time() * 1e3)
        endpoint = "/api" + request.url.split(CONSTANTS.REST_URLS.get("p2pb2b_main"))[1]

        request_params = OrderedDict(request.data if isinstance(request.data, dict) else {})
        request_params["nonce"] = timestamp
        request_params["request"] = endpoint

        return json.dumps(request_params, separators=(',', ':'))

    def header_for_authentication(self, body: Dict[str, Any]) -> Dict[str, str]:

        payload = b64encode(body.encode())
        signature = self._generate_signature(payload=payload)

        return {
            "Content-Type": "application/json",
            "X-TXC-APIKEY": self.api_key,
            "X-TXC-PAYLOAD": payload.decode(),
            "X-TXC-SIGNATURE": signature
        }

    def _generate_signature(self, payload: bytes) -> str:
        self.logger().info(f"P2PB2B Auth - Signing: {payload}")
        self.logger().info(f"Secret key: {self.secret_key}")
        digest = hmac.new(self.secret_key.encode("utf8"), payload, hashlib.sha512).hexdigest()
        return digest
