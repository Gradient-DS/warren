"""
Framework-level constants for the worker system.
"""

PUBLISHER_ORIGIN_TYPE: str = "publisher"
"""Origin type convention for publishers.

Publishers must set ``origin.type`` to this value on all messages
they publish to the exchange. This lets the JobStatusWorker distinguish
"a document was published into the pipeline" from "a document was
processed by a worker." See the job status management design doc,
Section 4.
"""
