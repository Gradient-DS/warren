"""HTTP(S) document resolver.

Fetches document bytes from an HTTP(S) URL — e.g. a short-lived presigned
GET URL. Follows the same contract as resolve_s3 / resolve_gcs: a standalone
async function, bound to an httpx.AsyncClient via functools.partial at wiring
time.

httpx is an optional dependency (the ``http`` extra). This module is only
imported once httpx is confirmed importable (see runner._create_resolvers),
so httpx is imported at module top level here — mirroring resolve_gcs.

This is the "a file happens to live at a URL" resolver (PDF/XML/office/…),
distinct from WebFetchWorker's browser-crawl path.

``build_client`` owns the client's construction so the redirect and timeout
policy lives beside the resolver that depends on it rather than inline at
the wiring site. Redirects are FOLLOWED BY DEFAULT: httpx defaults
``follow_redirects`` to False, which turns an ordinary 307 into
``raise_for_status`` and then a resolution failure, and document URLs
redirect constantly — repository permalinks to a CDN, DOIs to a publisher,
a landing path to the file itself. Measured on a real corpus, 91 of 96
download failures in one 500-document run were 307s and 303s whose
redirect target was the PDF being asked for.

Environment overrides, read once at wiring time:

===========================  ========  =======================================
``HTTP_FOLLOW_REDIRECTS``    ``true``  set false to restore httpx's own default
``HTTP_TIMEOUT_S``           ``60``    total request timeout, seconds
``HTTP_MAX_REDIRECTS``       ``20``    httpx's own default; guards redirect loops
===========================  ========  =======================================
"""

from typing import cast

import os

import httpx

from warren.storage.documents.interface import DocumentNotFoundError
from warren.storage.documents.location import (
    DocumentLocation,
    DocumentURLLocation,
)


# Follow by default. See the module docstring: not following is the
# behaviour that silently loses documents whose URL is a permalink.
DEFAULT_FOLLOW_REDIRECTS: bool = True
DEFAULT_TIMEOUT_S: float = 60.0
DEFAULT_MAX_REDIRECTS: int = 20

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


def _env_flag(name: str, *, default: bool) -> bool:
    """Read a boolean environment variable, tolerating the usual spellings.

    An unset or empty variable takes ``default``. An unrecognised value is
    a configuration mistake worth failing on rather than silently reading
    as False — ``HTTP_FOLLOW_REDIRECTS=flase`` should not quietly turn
    redirect following off.

    :param name: Environment variable name.
    :param default: Value to use when unset or empty.

    :return: The parsed flag.

    :raises ValueError: If set to something that is not a known spelling.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    msg = f"{name} must be one of {sorted(_TRUE | _FALSE)}; got {raw!r}"
    raise ValueError(msg)


def build_client(
    *,
    follow_redirects: bool | None = None,
    timeout_s: float | None = None,
    max_redirects: int | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Construct the AsyncClient the URL resolver is bound to.

    Explicit arguments win; anything left as ``None`` falls back to the
    environment, then to the module defaults.

    :param follow_redirects: Override for ``HTTP_FOLLOW_REDIRECTS``.
    :param timeout_s: Override for ``HTTP_TIMEOUT_S``.
    :param max_redirects: Override for ``HTTP_MAX_REDIRECTS``.
    :param transport: Substitute transport. Exists so the redirect policy
        can be exercised against ``httpx.MockTransport`` without a network
        or a reach into httpx's private attributes.

    :return: A configured httpx.AsyncClient.

    :raises ValueError: If an environment override cannot be parsed.
    """
    if follow_redirects is None:
        follow_redirects = _env_flag(
            "HTTP_FOLLOW_REDIRECTS", default=DEFAULT_FOLLOW_REDIRECTS
        )
    if timeout_s is None:
        timeout_s = float(os.environ.get("HTTP_TIMEOUT_S") or DEFAULT_TIMEOUT_S)
    if max_redirects is None:
        max_redirects = int(
            os.environ.get("HTTP_MAX_REDIRECTS") or DEFAULT_MAX_REDIRECTS
        )
    return httpx.AsyncClient(
        timeout=timeout_s,
        follow_redirects=follow_redirects,
        max_redirects=max_redirects,
        transport=transport,
    )


async def resolve_http(
    location: DocumentLocation,
    *,
    client: httpx.AsyncClient,
) -> bytes:
    """Read document bytes from an HTTP(S) URL.

    :param location: Must be a DocumentURLLocation.
    :param client: httpx.AsyncClient instance, bound via functools.partial
        at wiring time.

    :return: Raw document bytes.

    :raises DocumentNotFoundError: If the server responds 404 (hard failure).
    """
    url_location = cast("DocumentURLLocation", location)

    response = await client.get(url_location.url)
    if response.status_code == httpx.codes.NOT_FOUND:
        msg = f"Document not found at URL: {url_location.url}"
        raise DocumentNotFoundError(msg)
    response.raise_for_status()
    return response.content
