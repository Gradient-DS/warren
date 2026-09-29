from pydantic import BaseModel, ConfigDict, Field


class MemoryConsumerConfig(BaseModel):
    """Handler limits for an in-process consumer manager."""

    model_config = ConfigDict(extra="forbid")

    concurrency: int | None = Field(default=None, ge=1)
    handler_timeout_seconds: float | None = Field(
        default=None, gt=0, allow_inf_nan=False
    )
    on_shutdown_timeout: float = 30.0
