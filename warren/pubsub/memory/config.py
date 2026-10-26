from pydantic import BaseModel, ConfigDict, Field


class MemoryConsumerConfig(BaseModel):
    """Handler limits for an in-process consumer manager.

    :param concurrency: Maximum number of handlers that run at the same
        time. None means 1.
    :param handler_timeout_seconds: Time limit for one handler call. A
        handler that times out is a soft failure. ``/live`` fails when a
        handler runs for twice this value. None means no limit.
    :param on_shutdown_timeout: Seconds to wait for in-flight work on stop.
    """

    model_config = ConfigDict(extra="forbid")

    concurrency: int | None = Field(default=None, ge=1)
    handler_timeout_seconds: float | None = Field(
        default=None, gt=0, allow_inf_nan=False
    )
    on_shutdown_timeout: float = 30.0
