"""
Generate the tiny sample PDFs shipped with the ``examples/rag`` pipeline.

Pure standard-library — no reportlab/pypdf needed to *write* these. Each
PDF is a single page of Helvetica text laid out one line per source line,
which ``pypdf`` reads back cleanly. Run once to (re)generate the files in
``examples/rag/documents/``; they are committed so the example runs
out-of-the-box.

Usage:
    python -m examples.rag.make_sample_pdfs
"""

from pathlib import Path


# doc_id (filename stem) -> the lines of text on its single page.
SAMPLE_DOCS: dict[str, list[str]] = {
    "warren-overview": [
        "Warren: a message-driven document processing framework.",
        "",
        "Workers consume messages from a RabbitMQ exchange and self-select",
        "which ones to process. Each worker writes its results to a cached",
        "storage layer (MongoDB and Redis), then publishes a new message",
        "describing where those results live.",
        "",
        "Downstream workers pick that up, fetch what they need, and publish",
        "their own result locations. Adding a worker type is purely additive:",
        "no routing configuration changes, no upstream modifications.",
        "",
        "You scale a pipeline by running more replicas of any worker type.",
    ],
    "exchanges": [
        "Choosing a RabbitMQ exchange type in Warren.",
        "",
        "A fanout exchange broadcasts every message to every worker, which",
        "self-selects via should_process. Use it for linear pipelines where",
        "adding a worker should not require touching any routing.",
        "",
        "A topic exchange routes by a key. Use it when inputs are mixed and",
        "each kind needs a different worker: PDFs to a PDF parser, HTML to",
        "an OCR worker, and so on. The broker filters; workers do not.",
        "",
        "A direct exchange with a per-job routing plan lets different jobs",
        "take different paths through the same deployed workers.",
    ],
    "retrieval": [
        "Retrieval-augmented generation in three stages.",
        "",
        "First, parse each document into text. Second, split the text into",
        "chunks small enough to embed and retrieve precisely. Third, embed",
        "each chunk into a vector and store it for similarity search.",
        "",
        "At query time you embed the question, find the nearest chunks, and",
        "pass them to a language model as grounding context.",
        "",
        "Warren models the first three stages as a pipeline of workers, one",
        "per stage, each scalable on its own.",
    ],
}


def _escape(text: str) -> str:
    """Escape the characters that are special inside a PDF text string."""
    return text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _content_stream(lines: list[str]) -> bytes:
    """Build a page content stream placing each line top-to-bottom."""
    parts = ["BT", "/F1 12 Tf", "14 TL", "72 720 Td"]
    for line in lines:
        # Empty line -> just advance one line (T*); otherwise show text.
        parts.append(f"({_escape(line)}) Tj" if line else "()" + " Tj")
        parts.append("T*")
    parts.append("ET")
    return "\n".join(parts).encode("latin-1")


def build_pdf(lines: list[str]) -> bytes:
    """Assemble a minimal single-page PDF with a correct xref table."""
    content = _content_stream(lines)
    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n" % len(content) + content + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % i + body + b"\nendobj\n"

    xref_pos = len(out)
    n = len(objects) + 1
    out += b"xref\n0 %d\n" % n
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += b"%010d 00000 n \n" % off
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\n" % n
    out += b"startxref\n%d\n%%%%EOF\n" % xref_pos
    return bytes(out)


def main() -> None:
    out_dir = Path(__file__).parent / "documents"
    out_dir.mkdir(exist_ok=True)
    for stem, lines in SAMPLE_DOCS.items():
        path = out_dir / f"{stem}.pdf"
        path.write_bytes(build_pdf(lines))
        print(f"wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
