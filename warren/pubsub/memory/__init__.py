"""In-process pubsub backend: one broker, one event loop, no infrastructure.

Selected with ``backend: memory`` in ``RuntimeConfig``. Single-process and
non-durable by construction. See ``warren/docs/memory.md``.
"""
