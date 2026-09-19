"""Sample PDF sources shared by the RAG example's publish scripts.

Kept free of transport imports so the no-infrastructure example
(``run_local.py``) can use it without the ``rmq`` extra installed.
"""

# Two arXiv papers, so the example runs out of the box.
DEFAULT_URLS: list[str] = [
    "https://arxiv.org/pdf/1706.03762",  # Attention Is All You Need
    "https://arxiv.org/pdf/2103.15348",  # LayoutParser
]


def doc_id_for(url: str) -> str:
    """Derive a readable doc_id from a URL (its last path segment)."""
    return url.rstrip("/").rsplit("/", 1)[-1] or url
