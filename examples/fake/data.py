"""
Fake document content for the E2E test.

Each document maps to a fake markdown string that would be the output
of a real document parser. Paragraphs are separated by double newlines
so the text chunker can split on them.

Expected chunk counts: doc-001=4, doc-002=6, doc-003=3, doc-004=5. Total=18.
"""

from typing import Dict


FAKE_DOCUMENTS: Dict[str, str] = {
    "doc-001": (
        "# Annual Report 2024\n\n"
        "This is the executive summary of the annual report for fiscal year 2024. "
        "Key highlights include revenue growth and market expansion.\n\n"
        "Revenue increased by 15% year over year. Operating costs remained stable "
        "due to efficiency improvements across all departments.\n\n"
        "We expect continued growth in the coming fiscal year driven by new product "
        "launches and international expansion."
    ),
    "doc-002": (
        "# Technical Specification v3.2\n\n"
        "This document describes the technical requirements for the distributed "
        "processing system.\n\n"
        "The system must handle at least 10,000 messages per second with a "
        "99.9% uptime guarantee.\n\n"
        "All workers must be stateless and horizontally scalable. State is "
        "maintained in external storage.\n\n"
        "Message ordering is guaranteed within a single document but not across "
        "documents in the same job.\n\n"
        "Failure handling uses a two-tier approach: soft failures trigger retries "
        "with exponential backoff, hard failures route to dead-letter queues."
    ),
    "doc-003": (
        "# Meeting Notes - Sprint Review\n\n"
        "Discussed progress on the distributed pipeline. Parser and chunker "
        "workers are complete and tested.\n\n"
        "Next sprint will focus on embedding generation with cross-document "
        "batching for GPU efficiency."
    ),
    "doc-004": (
        "# Security Audit Report\n\n"
        "All RabbitMQ connections use TLS in production. Development environments "
        "use plaintext for simplicity.\n\n"
        "MongoDB credentials are stored in environment variables, never in config "
        "files committed to version control.\n\n"
        "Redis is configured with authentication enabled and network access "
        "restricted to the application subnet.\n\n"
        "Recommendation: implement message-level encryption for sensitive "
        "document content in transit."
    ),
}
