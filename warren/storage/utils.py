from datetime import UTC, datetime


def current_time_str() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(UTC).isoformat()
