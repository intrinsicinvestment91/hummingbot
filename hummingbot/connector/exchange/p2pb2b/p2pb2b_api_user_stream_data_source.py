from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.p2pb2b import p2pb2b_constants as CONSTANTS
from hummingbot.connector.exchange.p2pb2b.p2pb2b_auth import P2pb2bAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.p2pb2b.p2pb2b_exchange import P2pb2bExchange


class P2pb2bAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: P2pb2bAuth,
                 trading_pairs: List[str],
                 connector: 'P2pb2bExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: P2pb2bAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Creates a new WSAssistant instance.
        """
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange.
        """
        # Get a websocket assistant and connect it
        ws = await self._get_ws_assistant()
        url = CONSTANTS.WSS_URL

        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        P2pb2b does not require any channel subscription.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        pass
