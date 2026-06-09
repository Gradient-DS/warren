import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RMQConnectionConfig,
)


class RMQConnectionManager(Base):
    def __init__(
        self,
        config: RMQConnectionConfig,
        *,
        name: str | None = None,
    ) -> None:
        classname = type(self).__name__
        logger_name = f"[{classname}] {name}" if name else None
        super().__init__(pybase_logger_name=logger_name)

        self._config = config
        self._connection: AbstractRobustConnection | None = None

    async def setup(self) -> None:
        self._connection = await aio_pika.connect_robust(
            host=self._config.host,
            port=self._config.port,
            login=self._config.login,
            password=self._config.password.get_secret_value(),
            virtualhost=self._config.virtualhost,
            ssl=self._config.ssl,
            ssl_options=self._config.ssl_options,
            ssl_context=self._config.ssl_context,
            timeout=self._config.timeout,
            client_properties=self._config.client_properties,
        )

    async def teardown(self) -> None:
        if self._connection and not self._connection.is_closed:
            try:
                await self._connection.close()
            except Exception as e:
                self._log.warning(
                    f"Error closing connection: {summarize_exception_chain(e)}"
                )

    async def create_channel(self) -> AbstractChannel:
        if not self._connection:
            raise RuntimeError("Must call setup() first.")
        return await self._connection.channel()
