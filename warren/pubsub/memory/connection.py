"""Connection manager for the in-process backend: it owns the broker."""

from basics.base import Base

from warren.pubsub.memory.broker import MemoryBroker


class MemoryConnectionManager(Base):
    """Holds the one ``MemoryBroker`` every publisher and consumer shares.

    Exists so the memory backend has the same ``setup()`` / ``teardown()``
    shape as the RabbitMQ and Kafka connection managers and can sit in
    ``RuntimeInfra.pubsub_connection_manager``. Sharing a broker means
    sharing this object: build one ``RuntimeInfra`` per process and inject
    it into every runner.
    """

    def __init__(self, *, name: str | None = None) -> None:
        super().__init__(pybase_logger_name=name)
        self._broker: MemoryBroker | None = None

    async def setup(self) -> None:
        if self._broker is None:
            self._broker = MemoryBroker()

    async def teardown(self) -> None:
        self._broker = None

    @property
    def broker(self) -> MemoryBroker:
        if self._broker is None:
            msg = "Must call setup() before using the memory broker."
            raise RuntimeError(msg)
        return self._broker
