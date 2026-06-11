"""
Kafka connection management.

``KafkaConnectionManager`` is the shared infrastructure object of the
``aiokafka`` implementation — the counterpart of ``RMQConnectionManager``
for RabbitMQ. It owns the cluster-level settings (bootstrap servers,
security protocol, SSL context built from the configured cert paths) and
hands out clients bound to those settings:

- the started ``AIOKafkaAdminClient`` (via the ``admin`` property, used
  by the topology helpers),
- unstarted ``AIOKafkaConsumer`` / ``AIOKafkaProducer`` instances via the
  ``create_consumer()`` / ``create_producer()`` factory helpers.
"""

from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.helpers import create_ssl_context
from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from warren.pubsub.kafka.config import KafkaConnectionConfig


if TYPE_CHECKING:
    import ssl


class KafkaConnectionManager(Base):
    def __init__(
        self,
        config: KafkaConnectionConfig,
        *,
        name: str | None = None,
    ) -> None:
        classname = type(self).__name__
        logger_name = f"[{classname}] {name}" if name else None
        super().__init__(pybase_logger_name=logger_name)

        self._config = config
        self._ssl_context: ssl.SSLContext | None = None
        self._admin: AIOKafkaAdminClient | None = None

    async def setup(self) -> None:
        """Start the admin client and verify the cluster is reachable.

        Builds the ``ssl.SSLContext`` from the configured cert paths when
        ``security_protocol == "SSL"``. Fails loud when the cluster
        cannot be reached — the boot-time behavior of
        ``aio_pika.connect_robust`` for the RabbitMQ backend.
        """
        if self._config.security_protocol == "SSL":
            self._ssl_context = create_ssl_context(
                cafile=self._config.ssl_cafile,
                certfile=self._config.ssl_certfile,
                keyfile=self._config.ssl_keyfile,
            )

        admin = AIOKafkaAdminClient(**self._client_kwargs())
        # Published onto the instance before the reachability check so a
        # partial setup leaves truthful state and teardown() can close it.
        self._admin = admin
        await admin.start()

        # Fetch cluster metadata — fail loud at boot.
        await admin.describe_cluster()

    async def teardown(self) -> None:
        if self._admin is not None:
            try:
                await self._admin.close()
            except Exception as e:
                self._log.warning(
                    f"Error closing admin client: {summarize_exception_chain(e)}"
                )
            self._admin = None

    @property
    def admin(self) -> AIOKafkaAdminClient:
        """The started admin client, for the topology helpers."""
        if self._admin is None:
            msg = "Must call setup() first."
            raise RuntimeError(msg)
        return self._admin

    def create_consumer(self, *topics: str, **kwargs: Any) -> AIOKafkaConsumer:
        """Create an (unstarted) consumer bound to the configured cluster.

        :param topics: Topics to subscribe to.
        :param kwargs: Passed through to ``AIOKafkaConsumer`` (group id,
            offset management, etc. — the consumer manager's concerns).
        """
        return AIOKafkaConsumer(*topics, **self._client_kwargs(), **kwargs)

    def create_producer(self, **kwargs: Any) -> AIOKafkaProducer:
        """Create an (unstarted) producer bound to the configured cluster.

        :param kwargs: Passed through to ``AIOKafkaProducer``
            (acknowledgment and idempotence settings — the publisher's
            concerns).
        """
        return AIOKafkaProducer(**self._client_kwargs(), **kwargs)

    def _client_kwargs(self) -> dict[str, Any]:
        """Cluster-level kwargs shared by every client this manager creates."""
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self._config.bootstrap_servers,
            "security_protocol": self._config.security_protocol,
            "ssl_context": self._ssl_context,
        }
        if self._config.client_id is not None:
            kwargs["client_id"] = self._config.client_id
        return kwargs
